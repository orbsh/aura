//! Realm: the event/call fabric. Phase 1 — virtual-actor registry, runtime
//! loop driving queuees, idle-TTL eviction (scale-to-zero: on_sleep →
//! drop, on_wake on reactivation). Event namespace and emit/on arrive in
//! Phase 3; the unified CallSlot model in Phase 3.5.

pub mod event;
pub mod mq;
pub mod meta;
pub mod value;
pub mod state;
pub mod namespace;

use aura_actor::call::{CallId, CallSlot, CallSpec, PendingEntry, Tier, Waited};
use aura_actor::{ActorType, Instance, InstanceId, Job, SharedStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Extract a field name from a host-bridge JSON argument: a bare string
/// (`"count"`) or an object (`{"field": "count"}`) — both documented forms.
fn json_str_field(arg: &serde_json::Value) -> anyhow::Result<String> {
    match arg {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Object(o) => o
            .get("field")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| anyhow::anyhow!("expected a string field name")),
        _ => anyhow::bail!("expected a string field name"),
    }
}

/// Extract (field, value) from `{ "field": ..., "value": ... }`.
fn json_field_value(arg: &serde_json::Value) -> anyhow::Result<(String, serde_json::Value)> {
    let obj = arg
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected an object with `field` and `value`"))?;
    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing `field`"))?;
    let value = obj
        .get("value")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing `value`"))?;
    Ok((field.to_string(), value))
}

/// Shared realm handle: the dispatcher closes over this.
pub type SharedRealm = Arc<tokio::sync::Mutex<Realm>>;

pub struct Realm {
    /// Registered actor types by name.
    types: HashMap<String, ActorType>,
    /// Live instances by (type, key).
    instances: HashMap<(String, String), Instance>,
    /// Queue capacity per instance.
    queue_capacity: usize,
    /// Instance state store (in-memory now; Fjall in Phase 4).
    pub store: SharedStore,
    /// The mq byte engine (ADR-0018 step 1): the event-queue tables bind
    /// to a byte store — the okm `FjallStore` keyspace on fjall, the
    /// in-memory byte stand-in otherwise. NEVER the JSON state store.
    pub mq: mq::MqStore,
    /// Idle TTL: an instance with no job for this long is evicted
    /// (scale-to-zero). State survives via the store; hooks run around it.
    pub idle_ttl: Duration,
    /// Event namespace: routing table + emits whitelist (Phase 3).
    pub router: event::EventRouter,
    /// Resident script sessions (Phase 2.6): per-instance VM/PTY, owned by
    /// the realm — sessions die with the realm (test isolation) and hot
    /// type replacement can evict selectively.
    pub sessions: probe_runtime::carrier::session::Sessions,
    /// Live probe outbound connections by node alias (Phase 3). Each value
    /// is the writer half of the probe's WS connection; the reader task
    /// (serve_probes) correlates Result frames back through pending_remote.
    pub probes: HashMap<String, tokio::sync::mpsc::UnboundedSender<probe_protocol::Frame>>,
    /// In-flight remote calls awaiting the probe's Result frame. The
    /// instance id scopes the ctx-bridge host calls the probe makes while
    /// executing this call (state fields are the instance's own).
    pub pending_remote:
        HashMap<String, RemotePending>,
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
    /// Back-compat constructor during the ADR-0018 migration: the
    /// passed-in JSON store is no longer the state engine (state rides
    /// the document model over the mq engine); callers should move to
    /// `with_mq`.
    pub fn new(store: SharedStore) -> Self {
        let _ = store;
        Self::with_mq(mq::MqStore::mem())
    }

    /// Full constructor (ADR-0018 step 2): actor state IS a document
    /// store over the same okm engine the mq tables ride — one engine,
    /// two tables namespaces (mq ns 30-33, state ns 34), JSON only at
    /// the ctx seam. The engine's meta plane keeps its own SharedStore
    /// (PersistedActor records — JSON there is a API-currency record,
    /// not a storage value; its migration is a separate concern).
    pub fn with_mq(mq_store: mq::MqStore) -> Self {
        Self {
            types: HashMap::new(),
            instances: HashMap::new(),
            queue_capacity: 64,
            mq: mq_store.clone(),
            store: std::sync::Arc::new(state::StateDocumentStore::new(mq_store)),
            idle_ttl: Duration::from_secs(30),
            router: event::EventRouter::default(),
            sessions: probe_runtime::carrier::session::Sessions::new(),
            probes: HashMap::new(),
            pending_remote: HashMap::new(),
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
        // One declaration surface per type: routes assemble from the
        // type's own `receives` as a side effect of registration.
        for decl in &actor.receives {
            if decl.wildcard {
                self.router.on_wildcard(&decl.event, &actor.name);
            } else {
                self.router.on(decl.event.clone(), &actor.name, &decl.key_field);
            }
        }
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
            Arc::new(move |target, handler: &str, args| {
                let realm = dispatch_realm.clone();
                let handler = handler.to_string();
                Box::pin(async move { dispatch_call(realm, target, &handler, args).await })
            }),
        )
    }

    /// Host functions exposed to script actors (Phase 2.5 ctx bridge).
    /// Contract: one JSON-string argument in, one JSON value out — the
    /// carrier marshals; the host owns semantics. `ctx_state_get` /
    /// `ctx_state_set` / `ctx_state_delete` hit the instance's own state
    /// (the store scopes reads/writes to self_id — no cross-instance
    /// reach); `ctx_invoke` blocks on the unified call model.
    fn host_bridge_for(
        ctx: &aura_actor::Ctx,
    ) -> std::collections::BTreeMap<String, probe_runtime::carrier::HostFn> {
        use probe_runtime::carrier::HostFn;

        let self_id = ctx.self_id.clone();
        let store = ctx.state_store();
        let get_id = self_id.clone();
        let get_store = store.clone();
        let set_id = self_id.clone();
        let set_store = store.clone();
        let del_id = self_id.clone();
        let del_store = store;
        let dispatch = ctx.invoke_handle();
        let handle = tokio::runtime::Handle::current();

        let mut fns: std::collections::BTreeMap<String, HostFn> = Default::default();
        fns.insert(
            "ctx_state_get".into(),
            Arc::new(move |arg: serde_json::Value| {
                let field = json_str_field(&arg)?;
                match get_store.get(&get_id, &field)? {
                    Some(v) => Ok(serde_json::json!({ "present": true, "value": v })),
                    None => Ok(serde_json::json!({ "present": false })),
                }
            }) as HostFn,
        );
        fns.insert(
            "ctx_state_set".into(),
            Arc::new(move |arg: serde_json::Value| {
                let (field, value) = json_field_value(&arg)?;
                set_store.set(&set_id, &field, value)?;
                Ok(serde_json::json!({ "ok": true }))
            }) as HostFn,
        );
        fns.insert(
            "ctx_state_delete".into(),
            Arc::new(move |arg: serde_json::Value| {
                let field = json_str_field(&arg)?;
                del_store.delete(&del_id, &field)?;
                Ok(serde_json::json!({ "ok": true }))
            }) as HostFn,
        );
        fns.insert(
            "ctx_invoke".into(),
            Arc::new(move |arg: serde_json::Value| {
                // arg: { "type": ..., "key": ..., "args": ... }. Blocks the
                // script thread on the unified call model (Phase 3.5) — the
                // script itself runs in spawn_blocking, so this is bounded
                // by the call's own tier/timeout semantics.
                let obj = arg.as_object().ok_or_else(|| anyhow::anyhow!("ctx_invoke expects an object"))?;
                let ty = obj.get("type").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx_invoke: missing `type`"))?;
                let key = obj.get("key").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx_invoke: missing `key`"))?;
                let handler = obj.get("handler").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx_invoke: missing `handler` (the function to call)"))?;
                let args = obj.get("args").cloned().unwrap_or(serde_json::Value::Null);
                let target = InstanceId { actor_type: ty.to_string(), key: key.to_string() };
                handle.block_on(dispatch(target, handler, args))
            }) as HostFn,
        );
        fns
    }

    /// Run one queued event job against an instance: same execution path
    /// as run_job, but the result is discarded (event delivery is
    /// fire-and-forget; an actor that must return values is invoked).
    async fn run_job_queued(self_arc: SharedRealm, id: &InstanceId, job: aura_actor::QueuedJob) {
        let reply_tx = tokio::sync::oneshot::channel();
        let job = Job { handler: job.handler, args: job.args, reply: reply_tx.0 };
        // Drop the receiver: no one observes the reply.
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm.instances.get_mut(&(id.actor_type.clone(), id.key.clone())) {
            i.last_activity = std::time::Instant::now();
        }
        drop(realm);
        Self::run_job(self_arc, id, job).await;
    }

    /// Resolve (type, key) to a live instance, activating on first touch
    /// (virtual actor). Reactivation after eviction runs on_wake.
    async fn instance(&mut self, self_arc: SharedRealm, id: &InstanceId) -> anyhow::Result<&mut Instance> {
        let key = (id.actor_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let mut inst = Instance::new(id.clone(), self.queue_capacity);
            // on_wake: fresh residency. Runs on first activation too —
            // symmetric with on_sleep; a first-time wake is still a wake.
            if let Some(actor) = self.types.get(&id.actor_type) {
                if let Some(on_wake) = actor.on_wake.clone() {
                    let ctx = Self::ctx_for(self_arc.clone(), self.store.clone(), id);
                    on_wake(ctx, serde_json::Value::Null).await?;
                }
            }
            // Subscribe to the event queues this type's @on declarations
            // bind (Phase 4.5c step 2b): persistent partitions over the
            // store, one per (event, partition); the subscriber holds a
            // named cursor. Key-less routes bind the singleton partition.
            let mut subs: Vec<(String, String)> = Vec::new();
            if let Some(actor) = self.types.get(&id.actor_type) {
                for route in self.router.routes_of(&id.actor_type) {
                    let partition = if route.partition_key_field.is_empty() {
                        "__singleton__".to_string()
                    } else {
                        id.key.clone()
                    };
                    // NOTE: for keyed routes the partition value equals the
                    // instance key only when the route derives the key from
                    // the same field emit used — which it does by
                    // construction (emit set key = data[field]).
                    subs.push((route.event.clone(), partition));
                }
            }
            self.instances.insert(key.clone(), inst);
            // Spawn the instance's subscription consumer: drains every
            // bound partition serially (backlog scan → run → advance
            // cursor → repeat; idle = short park). The serial-per-instance
            // guarantee lives in this loop; re-activation replays the
            // unconsumed backlog (scale-to-zero keeps triggers alive).
            let consumer_realm = self_arc.clone();
            let consumer_id = id.clone();
            let actor_key = id.key.clone();
            tokio::spawn(async move {
                for (event, part) in subs {
                    // Cursor name = the actor type + instance key: two
                    // types on one event hold independent cursors.
                    let actor = format!("{}/{}", consumer_id.actor_type, actor_key);
                    loop {
                        // Fetch the backlog under a short lock; run jobs
                        // OUTSIDE the realm lock.
                        let batch = {
                            let realm = consumer_realm.lock().await;
                            let mut vs = realm.mq.clone();
                            let after = mq::cursor(&mut vs, &event, &part, &actor)
                                .unwrap_or(0);
                            mq::backlog(&mut vs, &event, &part, after).unwrap_or_default()
                        };
                        if batch.is_empty() {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        for (seq, payload) in batch {
                            let job = aura_actor::QueuedJob {
                                handler: event.clone(),
                                args: payload,
                            };
                            Self::run_job_queued(consumer_realm.clone(), &consumer_id, job).await;
                            let realm = consumer_realm.lock().await;
                            let mut vs = realm.mq.clone();
                            let _ = mq::advance(&mut vs, &event, &part, &actor, seq);
                        }
                    }
                }
            });
        } else if let Some(inst) = self.instances.get_mut(&key) {
            inst.last_activity = Instant::now();
        }
        Ok(self.instances.get_mut(&key).expect("just inserted"))
    }

    /// Drive one job through its instance's handler. Called by the runtime
    /// loop; serial per instance (single consumer per queue).
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
        let sessions = realm.sessions.clone();
        let probes = realm.probes.clone();
        drop(realm);
        let result = match body {
            aura_actor::Body::RemoteProbe { node_alias, language, source } => {
                // Remote probe execution (Phase 3): find the probe's live
                // outbound connection, send Frame::Call (inline payload),
                // await the correlated reply. The probe's resident
                // sessions own the VM; no ctx bridge crosses the wire yet
                // (host functions over WS arrive with the frame path).
                let Some(conn) = probes.get(&node_alias) else {
                    let _ = job
                        .reply
                        .send(Err(anyhow::anyhow!("probe '{node_alias}' not connected")));
                    return;
                };
                let conn = conn.clone();
                let call_id = format!("rp-{}", next_seq(&self_arc).await);
                let (tx, rx) = tokio::sync::oneshot::channel();
                self_arc.lock().await.pending_remote.insert(
                    call_id.clone(),
                    RemotePending { reply: tx, instance: id.clone() },
                );
                // Residency identity = this actor INSTANCE (type/key), not the handler:
                // two instances of one remote type must never share the probe's
                // resident runtime, and every handler of one instance must.
                // `entry` is the handler the call addresses in the delivered code.
                let session = format!("{}/{}", id.actor_type, id.key);
                let call = probe_protocol::ToolCall {
                    call_id: call_id.clone(),
                    session,
                    entry: job.handler.clone(),
                    language,
                    args: job.args,
                    code: probe_protocol::CodePayload::Inline { bytes: source.into_bytes() },
                };
                let result = match conn.send(probe_protocol::Frame::Call(call)) {
                    Ok(()) => match rx.await {
                        Ok(Ok(v)) => Ok(v),
                        Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
                        Err(_) => Err(anyhow::anyhow!("probe '{node_alias}' dropped the call")),
                    },
                    Err(_) => Err(anyhow::anyhow!("probe '{node_alias}' connection closed")),
                };
                result
            }
            aura_actor::Body::Rust(handler) => handler(ctx, job.args).await,
            aura_actor::Body::Script { language, source, entry: _ } => {
                // Resident sessions (Phase 2.6): one VM/PTY per actor
                // instance, loaded once, called per event. Cross-call
                // state lives in the session (module globals / $env);
                // eviction drops it. spawn_blocking so host fns may block
                // on the async ctx. Nushell (PTY REPL) cannot call back —
                // no bridge there.
                let pure_nushell = language == "nushell";
                let host = if pure_nushell {
                    None
                } else {
                    Some(probe_runtime::carrier::HostBridge {
                        functions: Self::host_bridge_for(&ctx),
                    })
                };
                let instance_key = format!("{}/{}", id.actor_type, id.key);
                tokio::task::spawn_blocking(move || {
                    sessions.with_session(
                        &instance_key,
                        &language,
                        &source,
                        host.as_ref(),
                        &probe_runtime::sandbox::SandboxPolicy::None,
                        |s| s.call(&job.handler, &job.args),
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
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<CallSlot> {
        let handler = handler.to_string();
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
                    inst.queue
                        .tx
                        .try_send(Job { handler: handler.to_string(), args: args.clone(), reply: reply_tx })
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "queue full: {}/{}",
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
                                    i.queue.rx.try_recv().ok()
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
        // Cold path: the job still reaches the target's queue (the
        // target executes without a parked caller); the result is
        // resolved back through resolve_call when it completes.
        let resolve_id = call_id.clone();
        {
            let realm = self_arc.clone();
            tokio::spawn(async move {
                let rx = Self::submit(&realm, target, &handler, args).await;
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

    /// Submit a job to an instance: activation + queue send. The sender
    /// awaits the reply oneshot (internal plumbing; the actor-facing call
    /// is `Realm::call`).
    pub async fn submit(
        self_arc: &SharedRealm,
        target: InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<serde_json::Value>>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        {
            let mut realm = self_arc.lock().await;
            if !realm.types.contains_key(&target.actor_type) {
                anyhow::bail!("unknown actor type: {}", target.actor_type);
            }
            let inst = realm.instance(self_arc.clone(), &target).await?;
            inst.queue
                .tx
                .try_send(Job { handler: handler.to_string(), args, reply: reply_tx })
                .map_err(|_| anyhow::anyhow!("queue full: {}/{}", target.actor_type, target.key))?;
        }
        // Runtime loop drains the queue; spawn a consumer for this job
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
                        Some(i) => i.queue.rx.recv().await,
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
        let default_ttl = self.idle_ttl;
        // Per-type residency policy: the type's own TTL wins; `None` falls
        // back to the realm-wide default. Residency value differs by role —
        // a turn-executor dwells through its retention window while an
        // entity actor can be reclaimed quickly (Phase 6.5).
        let ttl_of = |type_name: &str| -> Duration {
            self.types
                .get(type_name)
                .and_then(|a| a.idle_ttl)
                .unwrap_or(default_ttl)
        };
        let mut evicted = Vec::new();
        let keys: Vec<(String, String)> = self
            .instances
            .iter()
            .filter(|(k, inst)| {
                inst.last_activity.elapsed() > ttl_of(&k.0)
            })
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
            // The resident session dies WITH the instance: the VM/PTY
            // holds no durable truth (ctx_state_* wrote through to the
            // store), so eviction is a plain drop. The next activation
            // cold-starts a fresh session and reloads the source.
            self.sessions.evict(&format!("{}/{}", key.0, key.1));
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
            // ADR-0012: no emits whitelist — the receiver set is a runtime
            // fact; an emit with no subscribers lands in the dead ring.
            // `emitter` stays in the signature for audit/recording.
            let matched = realm.router.matches(event);
            if matched.is_empty() {
                realm.dead_events.push(event, data);
                return Ok(());
            }
            matched
        };
        // Dedupe by queue id: several routes may bind the same queue
        // (two subscriber types on one event) — the queue fans out to all
        // of them; a second send would double-deliver. Activation of every
        // matched route's target happens in the SAME pass, before any
        // send, so every subscriber binds its Receiver before the message
        // lands.
        let mut queued: std::collections::HashSet<(String, String)> = Default::default();
        let mut targets: Vec<(event::Route, String)> = Vec::new();
        for route in routes {
            // Queue identity: @on-declared key → per-(event, partition);
            // no key → per-event singleton queue. The key comes from the
            // event data (wiki §5.4), not the emitter.
            let partition = if route.partition_key_field.is_empty() {
                "__singleton__".to_string()
            } else {
                data.get(&route.partition_key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("__default__")
                    .to_string()
            };
            // Virtual-actor activation: emitting to an instance that has
            // never run activates it first, so its @on subscriptions bind
            // before the event lands in the queue.
            let target = InstanceId {
                actor_type: route.actor_type.clone(),
                key: partition.clone(),
            };
            {
                let mut realm = self_arc.lock().await;
                if !realm.instances.contains_key(&(route.actor_type.clone(), partition.clone())) {
                    drop(realm);
                    let mut r = self_arc.lock().await;
                    r.instance(self_arc.clone(), &target).await?;
                }
            }
            if queued.insert((route.event.clone(), partition.clone())) {
                targets.push((route, partition));
            }
        }
        for (route, partition) in targets {
            let mut realm = self_arc.lock().await;
            // Persistent queues (step 2b): events are passively persisted
            // on emit — an evicted/not-yet-active subscriber's backlog is
            // delivered on re-activation. The dead ring only sees events
            // with NO matching route (checked above): a matched route with
            // no live instance is a backlog write, not a loss.
            let event_name = route.event.clone();
            let mut store = realm.mq.clone();
            if let Err(e) = mq::append(&mut store, &event_name, &partition, &data) {
                eprintln!("mq append failed for {event_name}/{partition}: {e}");
                realm.dead_events.push(&event, data.clone());
                continue;
            }
            // Retention (step 2b follow-up): min-watermark over REGISTERED
            // subscribers — the route registry is the denominator (evicted
            // instances still count: their backlog replays; a type whose
            // @on for this event is gone does not). Cursor rows whose actor
            // name has no matching registered (type, key) instance fall
            // out; compaction deletes mq-data below the watermark. Runs on
            // the emit path (write-path compaction per the ruling); the
            // scan cost is bounded by the subscriber count.
            if let Err(e) = Self::compact_queue_locked(&mut realm, &event_name, &partition, &mut store).await {
                eprintln!("mq compaction failed for {event_name}/{partition}: {e}");
            }
        }
        Ok(())
    }


    /// Min-watermark compaction for one queue partition (Phase 4.5c step 2b
    /// follow-up). Watermark = min cursor over subscribers REGISTERED for
    /// this event: for each route (actor type), every instance key that the
    /// cursor rows mention AND whose type still holds this route counts.
    /// Cursor rows for actors with no matching route are skipped (and are
    /// the reason the denominator never comes from raw cursor keys).
    /// No cursor rows at all = nobody ever consumed = no compaction (the
    /// backlog must survive for the first activation).
    async fn compact_queue_locked(
        realm: &mut Realm,
        event: &str,
        partition: &str,
        store: &mut mq::MqStore,
    ) -> anyhow::Result<()> {
        let event_id = match mq::event_id_of(store, event)? {
            Some(id) => id,
            None => return Ok(()),
        };
        let part_id = mq::part_hash_of(partition);
        let rows = mq::cursor_rows(store, event_id, part_id)?;
        if rows.is_empty() {
            return Ok(());
        }
        // Registered subscriber check: the cursor name ("type/key") must
        // belong to a type whose routes include this event.
        let mut min_seq: Option<u64> = None;
        for (actor_id, cursor) in &rows {
            let Some(name) = mq::actor_name_of(store, *actor_id)? else {
                continue;
            };
            let Some((type_name, _key)) = name.split_once('/') else {
                continue;
            };
            let registered = realm
                .router
                .routes_of(type_name)
                .iter()
                .any(|r| r.event == event);
            if registered {
                min_seq = Some(match min_seq {
                    Some(m) => m.min(*cursor),
                    None => *cursor,
                });
            }
        }
        if let Some(min_seq) = min_seq {
            if min_seq > 0 {
                let _ = mq::delete_before(store, event_id, part_id, min_seq)?;
            }
        }
        Ok(())
    }

    /// Test/ops wrapper: run min-watermark compaction for one partition
    /// (locks the realm internally).
    pub async fn compact_queue_for_test(
        self_arc: &SharedRealm,
        event: &str,
        partition: &str,
    ) -> anyhow::Result<()> {
        let mut realm = self_arc.lock().await;
        let mut store = realm.mq.clone();
        Self::compact_queue_locked(&mut realm, event, partition, &mut store).await
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
    handler: &str,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    match Realm::call(&realm, None, target, handler, args).await?.wait().await? {
        Waited::Done(result) => result,
        Waited::Pending(id) => Err(anyhow::anyhow!(
            "cold target cannot return a value to a parked caller (call {}); emit instead",
            id.0
        )),
    }
}

impl Default for Realm {
    fn default() -> Self {
        // State + mq both ride the okm TestStore through the document
        // model (ADR-0018): there is no JSON state store left to default
        // to — and none is needed.
        Self::with_mq(crate::mq::MqStore::mem())
    }
}

/// Introspect a script type's `interface_schema()` (no ctx — a pure
/// function the host calls at registration; direction host ← script, the
/// script never touches the engine) and extract `lifecycle.idle_ttl` if
/// declared. Accepts a number (seconds) or a string with a mandatory
/// unit suffix ("300s" / "5m" / "2h"). Introspection failure or missing
/// declaration = `None`, never a registration error — declaration is
/// optional metadata.
pub async fn introspect_schema(actor: &aura_actor::ActorType) -> Option<serde_json::Value> {
    let aura_actor::Body::Script { language, source, entry: _ } = &actor.body else {
        return None; // Rust types declare TTL via the builder
    };
    let raw = tokio::task::spawn_blocking({
        let language = language.clone();
        let source = source.clone();
        move || {
            // Uniform contract: carrier::introspect dispatches per
            // language; every carrier assembles/merges `interface_schema`
            // behind this one call. No language branch in the host.
            probe_runtime::carrier::introspect(&language, &source)
        }
    })
    .await;
    raw.ok()?.ok()
}

/// Extract `lifecycle.idle_ttl` from a script type's introspected schema.
pub async fn introspect_idle_ttl(actor: &aura_actor::ActorType) -> Option<Duration> {
    let result = introspect_schema(actor).await?;
    let ttl = result.get("lifecycle")?.get("idle_ttl")?;
    match ttl {
        serde_json::Value::Number(n) => n.as_u64().map(Duration::from_secs),
        serde_json::Value::String(s) => parse_duration_suffix(s),
        _ => None,
    }
}

/// Parse a human duration suffix: "300s" / "5m" / "2h". Bare digits are
/// rejected — units are mandatory so declarations are unambiguous.
fn parse_duration_suffix(s: &str) -> Option<Duration> {
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num.parse().ok()?;
    match unit {
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n * 60)),
        "h" => Some(Duration::from_secs(n * 3600)),
        _ => None,
    }
}



/// One in-flight remote call: the reply path plus the instance whose ctx
/// the probe's host calls resolve against.
pub struct RemotePending {
    pub reply: tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
    pub instance: InstanceId,
}

/// Next remote-call correlation id (realm-owned counter, taken under lock).
async fn next_seq(self_arc: &SharedRealm) -> u64 {
    let mut realm = self_arc.lock().await;
    realm.call_seq += 1;
    realm.call_seq
}

