//! Realm: the event/call fabric. Phase 1 — virtual-actor registry, runtime
//! loop driving mailboxes, idle-TTL eviction (scale-to-zero: on_sleep →
//! drop, on_wake on reactivation). Event namespace and emit/on arrive in
//! Phase 3; the unified CallSlot model in Phase 3.5.

pub mod event;

use aura_actor::call::{CallId, CallSlot, CallSpec, PendingEntry, Tier, Waited};
use aura_actor::{ActorType, Instance, InstanceId, Job, SharedStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Shared realm handle: the dispatcher closes over this.
pub type SharedRealm = Arc<tokio::sync::Mutex<Realm>>;

pub struct Realm {
    /// Registered actor types by name.
    types: HashMap<String, ActorType>,
    /// Live instances by (type, key).
    instances: HashMap<(String, String), Instance>,
    /// Mailbox capacity per instance.
    mailbox_capacity: usize,
    /// Instance state store (in-memory now; Fjall in Phase 4).
    pub store: SharedStore,
    /// Idle TTL: an instance with no job for this long is evicted
    /// (scale-to-zero). State survives via the store; hooks run around it.
    pub idle_ttl: Duration,
    /// Event namespace: routing table + emits whitelist (Phase 3).
    pub router: event::EventRouter,
    /// Static call declarations per actor type (Phase 3.5). Defaults to
    /// hot + 30s when a type registers without a spec.
    call_specs: HashMap<String, CallSpec>,
    /// Registered calls awaiting results (Phase 3.5). Hot in-flight calls
    /// carry a deadline (expiry → failure value); cold calls carry their
    /// session for re-entry routing.
    pending_calls: HashMap<CallId, PendingEntry>,
    call_seq: u64,
    /// Unmatched events (bounded ring, diagnostic output).
    pub dead_events: event::DeadEvents,
}

impl Realm {
    pub fn new(store: SharedStore) -> Self {
        Self {
            types: HashMap::new(),
            instances: HashMap::new(),
            mailbox_capacity: 64,
            store,
            idle_ttl: Duration::from_secs(30),
            router: event::EventRouter::default(),
            call_specs: HashMap::new(),
            pending_calls: HashMap::new(),
            call_seq: 0,
            dead_events: event::DeadEvents::default(),
        }
    }

    pub fn register_type(&mut self, actor: ActorType) {
        self.call_specs
            .entry(actor.name.clone())
            .or_insert_with(|| CallSpec::hot(Duration::from_secs(30)));
        self.types.insert(actor.name.clone(), actor);
    }

    /// Static call declaration for an actor type (Phase 3.5): tier +
    /// timeout. Split point is the entry, decided here at registration.
    pub fn declare_call(&mut self, actor_type: &str, spec: CallSpec) {
        self.call_specs.insert(actor_type.into(), spec);
    }

    pub fn actor_type(&self, name: &str) -> Option<&ActorType> {
        self.types.get(name)
    }

    /// Build a ctx for an instance: state backed by the shared store,
    /// invoke routed through the realm's dispatch.
    fn ctx_for(self_arc: SharedRealm, store: aura_actor::SharedStore, id: &InstanceId) -> aura_actor::Ctx {
        // The store is already a shared Arc: handlers get a direct handle.
        // (Phase 1 single-node: the store is lock-free per operation. The
        // realm lock only guards registry/instances, never state.)
        let dispatch_realm = self_arc.clone();
        aura_actor::Ctx::new(
            id.clone(),
            store,
            Arc::new(move |target, args| {
                let realm = dispatch_realm.clone();
                Box::pin(async move { dispatch_call(realm, target, args).await })
            }),
        )
    }

    /// Resolve (type, key) to a live instance, activating on first touch
    /// (virtual actor). Reactivation after eviction runs on_wake.
    async fn instance(&mut self, self_arc: SharedRealm, id: &InstanceId) -> anyhow::Result<&mut Instance> {
        let key = (id.actor_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let inst = Instance::new(id.clone(), self.mailbox_capacity);
            // on_wake: fresh residency. Runs on first activation too —
            // symmetric with on_sleep; a first-time wake is still a wake.
            if let Some(actor) = self.types.get(&id.actor_type) {
                if let Some(on_wake) = actor.on_wake.clone() {
                    let ctx = Self::ctx_for(self_arc.clone(), self.store.clone(), id);
                    on_wake(ctx, serde_json::Value::Null).await?;
                }
            }
            self.instances.insert(key.clone(), inst);
        } else if let Some(inst) = self.instances.get_mut(&key) {
            inst.last_activity = Instant::now();
        }
        Ok(self.instances.get_mut(&key).expect("just inserted"))
    }

    /// Drive one job through its instance's handler. Called by the runtime
    /// loop; serial per instance (single consumer per mailbox).
    async fn run_job(self_arc: SharedRealm, id: &InstanceId, job: Job) {
        let mut realm = self_arc.lock().await;
        realm.instances.get_mut(&(id.actor_type.clone(), id.key.clone()))
            .map(|i| i.last_activity = Instant::now());
        let Some(actor) = realm.types.get(&id.actor_type) else {
            let _ = job
                .reply
                .send(Err(anyhow::anyhow!("unknown actor type: {}", id.actor_type)));
            return;
        };
        let body = actor.body.clone();
        let ctx = Self::ctx_for(self_arc.clone(), realm.store.clone(), id);
        drop(realm);
        let result = match body {
            aura_actor::Body::Rust(handler) => handler(ctx, job.args).await,
            aura_actor::Body::Script { language, source, entry } => {
                // Script actors are pure functions in this phase; the ctx
                // bridge (state/invoke from scripts) is the remaining
                // Phase 2 work. The ctx is still constructed so hooks and
                // future bridge wiring see a uniform shape.
                let _ = ctx;
                // Carriers are blocking (in-process VMs, nu subprocess) —
                // keep them off the async workers.
                tokio::task::spawn_blocking(move || {
                    probe_runtime::carrier::execute(
                        &language,
                        probe_runtime::carrier::ExecRequest {
                            source: &source,
                            entry: entry.as_deref(),
                            args: &job.args,
                        },
                    )
                })
                .await
                .unwrap_or_else(|e| Err(anyhow::anyhow!("script task join: {e}")))
            }
        };
        let _ = job.reply.send(result);
    }

    /// The unified call (Phase 3.5): same path for realm Actor / remote
    /// Probe / future HTTP targets. Tier split happens HERE at entry —
    /// hot queues + returns the parking slot; cold registers in
    /// pending_calls and returns a Pending slot (the caller's task ends).
    pub async fn call(
        self_arc: &SharedRealm,
        caller: Option<&str>,
        target: InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<CallSlot> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let call_id = {
            let mut realm = self_arc.lock().await;
            if !realm.types.contains_key(&target.actor_type) {
                anyhow::bail!("unknown actor type: {}", target.actor_type);
            }
            let spec = realm
                .call_specs
                .get(&target.actor_type)
                .cloned()
                .unwrap_or_else(|| CallSpec::hot(Duration::from_secs(30)));
            match spec.tier {
                Tier::Hot => {
                    let deadline = spec
                        .timeout
                        .map(|t| (tokio::time::Instant::now() + t, t));
                    let inst = realm.instance(self_arc.clone(), &target).await?;
                    inst.mailbox
                        .tx
                        .try_send(Job { args: args.clone(), reply: reply_tx })
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "mailbox full: {}/{}",
                                target.actor_type,
                                target.key
                            )
                        })?;
                    drop(realm);
                    // Spawn the consumer that drains this job (submit's
                    // per-job consumer shape).
                    let spawn_realm = self_arc.clone();
                    tokio::spawn(async move {
                        let job = {
                            let mut r = spawn_realm.lock().await;
                            r.instances
                                .get_mut(&(target.actor_type.clone(), target.key.clone()))
                                .and_then(|i| {
                                    // Take only OUR job: recv from this instance's rx.
                                    i.mailbox.rx.try_recv().ok()
                                })
                        };
                        if let Some(job) = job {
                            Self::run_job(spawn_realm, &target, job).await;
                        }
                    });
                    return Ok(CallSlot::Hot { rx: reply_rx, deadline });
                }
                Tier::Cold => {
                    realm.call_seq += 1;
                    let call_id = CallId(format!("call-{}", realm.call_seq));
                    realm.pending_calls.insert(
                        call_id.clone(),
                        PendingEntry {
                            registered_at: Instant::now(),
                            deadline: None,
                            reply: Some(reply_tx),
                            session: caller.map(|s| s.to_string()),
                        },
                    );
                    call_id
                }
            }
        };
        // Cold path: the job still reaches the target's mailbox (the
        // target executes without a parked caller); the result is
        // resolved back through resolve_call when it completes.
        let resolve_id = call_id.clone();
        {
            let realm = self_arc.clone();
            tokio::spawn(async move {
                let rx = Self::submit(&realm, target, args).await;
                if let Ok(rx) = rx {
                    if let Ok(result) = rx.await {
                        Self::resolve_call(&realm, &resolve_id, result).await;
                    }
                }
            });
        }
        Ok(CallSlot::Cold { call_id })
    }

    /// Resolve a pending call: deliver the value through the registered
    /// reply channel (cold re-entry routing reads `session`). Unknown
    /// call_id = completed calls never replay (idempotent resolve).
    pub async fn resolve_call(
        self_arc: &SharedRealm,
        call_id: &CallId,
        result: anyhow::Result<serde_json::Value>,
    ) -> bool {
        let Some(mut entry) = self_arc.lock().await.pending_calls.remove(call_id) else {
            return false;
        };
        if let Some(reply) = entry.reply.take() {
            let _ = reply.send(result);
        }
        true
    }

    /// Registered (pending) call count — observation helper.
    pub fn pending_calls_len(&self) -> usize {
        self.pending_calls.len()
    }

    /// Deadline scan: expired hot in-flight calls become failure values
    /// delivered through their oneshots (timeout = failure value, never a
    /// hang). Cold calls have no deadline. Runs on the evictor tick.
    pub async fn sweep_deadlines(&mut self) {
        let now = Instant::now();
        let expired: Vec<CallId> = self
            .pending_calls
            .iter()
            .filter(|(_, e)| matches!(e.deadline, Some(d) if d <= now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(mut entry) = self.pending_calls.remove(&id) {
                if let Some(reply) = entry.reply.take() {
                    let _ = reply.send(Err(anyhow::anyhow!("call timed out: {}", id.0)));
                }
            }
        }
    }

    /// Submit a job to an instance: activation + mailbox send. The sender
    /// awaits the reply oneshot (internal plumbing; the actor-facing call
    /// is `Realm::call`).
    pub async fn submit(
        self_arc: &SharedRealm,
        target: InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<serde_json::Value>>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        {
            let mut realm = self_arc.lock().await;
            if !realm.types.contains_key(&target.actor_type) {
                anyhow::bail!("unknown actor type: {}", target.actor_type);
            }
            let inst = realm.instance(self_arc.clone(), &target).await?;
            inst.mailbox
                .tx
                .try_send(Job { args, reply: reply_tx })
                .map_err(|_| anyhow::anyhow!("mailbox full: {}/{}", target.actor_type, target.key))?;
        }
        // Runtime loop drains the mailbox; spawn a consumer for this job
        // (per-job spawn is Phase 1's simple shape; the persistent loop
        // task arrives with the scheduler work).
        {
            let realm = self_arc.clone();
            let target2 = target.clone();
            tokio::spawn(async move {
                let job = {
                    let mut r = realm.lock().await;
                    let inst = r
                        .instances
                        .get_mut(&(target2.actor_type.clone(), target2.key.clone()));
                    match inst {
                        Some(i) => i.mailbox.rx.recv().await,
                        None => None,
                    }
                };
                if let Some(job) = job {
                    Self::run_job(realm, &target2, job).await;
                }
            });
        }
        Ok(reply_rx)
    }

    /// Evict instances idle longer than `idle_ttl`: run on_sleep, drop the
    /// instance. State survives in the store — scale-to-zero drops the
    /// resident, not the data.
    pub async fn evict_idle(&mut self, self_arc: SharedRealm) -> Vec<InstanceId> {
        let ttl = self.idle_ttl;
        let mut evicted = Vec::new();
        let keys: Vec<(String, String)> = self
            .instances
            .iter()
            .filter(|(_, inst)| inst.last_activity.elapsed() > ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            let Some(inst) = self.instances.remove(&key) else { continue };
            if let Some(actor) = self.types.get(&key.0) {
                if let Some(on_sleep) = actor.on_sleep.clone() {
                    let ctx = Self::ctx_for(self_arc.clone(), self.store.clone(), &inst.id);
                    if let Err(e) = on_sleep(ctx).await {
                        // Eviction proceeds regardless: the hook is
                        // advisory; state is already in the store.
                        eprintln!("on_sleep failed for {}/{}: {e}", key.0, key.1);
                    }
                }
            }
            evicted.push(inst.id);
        }
        evicted
    }

    /// Emit an event into the realm (Phase 3): whitelist check → route
    /// match → per-route delivery. Fire-and-forget: returns Ok(()) once
    /// every matched route's job is queued; handler results are discarded
    /// (an actor that must return values is invoked, not emitted to).
    ///
    /// - emitter = None: system/external emission (bypasses whitelist —
    ///   the whitelist constrains actors, not the host surface).
    /// - Whitelist violation = error value (audit point, wiki §5.4).
    /// - No matching route = dead event (stored in the ring, not an error:
    ///   emitting ahead of a subscriber coming up is legitimate).
    pub async fn emit(
        self_arc: &SharedRealm,
        emitter: Option<&str>,
        event: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let routes = {
            let mut realm = self_arc.lock().await;
            if let Some(emitter_type) = emitter {
                if !realm.router.may_emit(emitter_type, event) {
                    anyhow::bail!(
                        "emit rejected: actor type `{}` has not declared event `{}` in emits",
                        emitter_type,
                        event
                    );
                }
            }
            let matched = realm.router.matches(event);
            if matched.is_empty() {
                realm.dead_events.push(event, data);
                return Ok(());
            }
            matched
        };
        for route in routes {
            // Partition key from event data (wiki §5.4: the key comes from
            // the event, not the emitter). Empty field = singleton instance.
            let key = if route.partition_key_field.is_empty() {
                "__singleton__".to_string()
            } else {
                data.get(&route.partition_key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("__default__")
                    .to_string()
            };
            let target = InstanceId { actor_type: route.actor_type.clone(), key };
            // Fire-and-forget: spawn, drop the reply receiver.
            let _ = Realm::submit(self_arc, target, data.clone()).await?;
        }
        Ok(())
    }

    /// Periodic eviction tick, spawned once per engine. Holds a Weak
    /// handle: the evictor never keeps the realm (and its storage engine)
    /// alive — engine shutdown drops the realm even with the task running.
    pub fn spawn_evictor(realm: &SharedRealm) {
        let realm = Arc::downgrade(realm);
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let Some(realm) = realm.upgrade() else { break };
                let mut locked = realm.lock().await;
                locked.sweep_deadlines().await;
                locked.evict_idle(realm.clone()).await;
            }
        });
    }
}

async fn dispatch_call(
    realm: SharedRealm,
    target: InstanceId,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    match Realm::call(&realm, None, target, args).await?.wait().await? {
        Waited::Done(result) => result,
        Waited::Pending(id) => Err(anyhow::anyhow!(
            "cold target cannot return a value to a parked caller (call {}); emit instead",
            id.0
        )),
    }
}

impl Default for Realm {
    fn default() -> Self {
        Self::new(Arc::new(aura_storage::InMemoryStore::default()))
    }
}
