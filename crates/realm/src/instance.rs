//! Instance lifecycle: construction, activation, job execution,
//! eviction, timer glue, the evictor driver. Split out of lib.rs per
//! ADR-0029.

use super::{next_seq, Realm, SharedRealm, RemotePending};
use crate::{event, mq, timer};
use aura_actor::{Instance, InstanceId, Job};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

impl Realm {
    pub async fn shared_async(self) -> SharedRealm {
        let arc = Arc::new(tokio::sync::Mutex::new(self));
        let handle = timer::TimerDriver::spawn(arc.clone());
        arc.lock().await.timers = handle;
        arc
    }

    pub fn with_mq(mq_store: mq::MqStore) -> Self {
        Self {
            types: HashMap::new(),
            instances: HashMap::new(),
            queue_capacity: 64,
            mq: mq_store.clone(),
            idle_ttl: Duration::from_secs(30),
            router: event::EventRouter::default(),
            sessions: probe_runtime::carrier::session::Sessions::new(),
            probes: HashMap::new(),
            code_base_url: None,
            pending_remote: HashMap::new(),
            call_specs: HashMap::new(),
            pending_calls: HashMap::new(),
            call_seq: 0,
            store_plans: HashMap::new(),
            persisted_schemas: HashMap::new(),
            dead_events: event::DeadEvents::default(),
            // Placeholder; the real handle lands right after the realm
            // is wrapped in its Arc (the driver needs the SharedRealm).
            timers: timer::TimerHandle::detached(),
        }
    }

    pub(crate) async fn instance(&mut self, self_arc: SharedRealm, id: &InstanceId) -> anyhow::Result<&mut Instance> {
        let key = (id.actor_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let inst = Instance::new(id.clone(), self.queue_capacity);
            // on_wake: fresh residency. Runs on first activation too —
            // symmetric with on_sleep; a first-time wake is still a wake.
            if let Some(actor) = self.types.get(&id.actor_type) {
                if let Some(on_wake) = actor.on_wake.clone() {
                    let ctx = Self::ctx_for(
                        self_arc.clone(),
                        self.mq.clone(),
                        self.plan_of(&id.actor_type),
                        self.schema_of(&id.actor_type).cloned().flatten(),
                        id,
                    );
                    on_wake(ctx, serde_json::Value::Null).await?;
                }
            }
            // Subscribe to the event queues this type's @on declarations
            // bind (Phase 4.5c step 2b): persistent partitions over the
            // store, one per (event, partition); the subscriber holds a
            // named cursor. Key-less routes bind the singleton partition.
            let mut subs: Vec<(String, String, bool)> = Vec::new();
            if let Some(_actor) = self.types.get(&id.actor_type) {
                for route in self.router.routes_of(&id.actor_type) {
                    let partition = if route.instance_key_field.is_empty() {
                        mq::SINGLETON.to_string()
                    } else {
                        id.key.clone()
                    };
                    // NOTE: for keyed routes the partition value equals the
                    // instance key only when the route derives the key from
                    // the same field emit used — which it does by
                    // construction (emit set key = data[field]). The third
                    // element marks a wildcard subscription: route.event is
                    // a PATTERN, expanded to concrete names at consume time
                    // (queues are keyed by concrete names — emit writes
                    // there).
                    subs.push((route.event.clone(), partition, route.event.ends_with('*')));
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
                for (event, part, is_wildcard) in subs {
                    // Cursor name = the actor type + instance key: two
                    // types on one event hold independent cursors.
                    let actor = format!("{}/{}", consumer_id.actor_type, actor_key);
                    // Concrete queue names: an exact subscription is one
                    // name; a wildcard subscription expands to every
                    // registered event matching its prefix (re-expanded
                    // each pass — new concrete names join automatically).
                    let mut names: Vec<String> = Vec::new();
                    loop {
                        if is_wildcard {
                            let prefix = event.trim_end_matches('*').to_string();
                            let found = {
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                mq::events_matching(&vs, &prefix).unwrap_or_default()
                            };
                            if found != names {
                                names = found;
                            }
                        } else if names.is_empty() {
                            names = vec![event.clone()];
                        }
                        let mut progressed = false;
                        for concrete in &names {
                            // Fetch the backlog under a short lock; run jobs
                            // OUTSIDE the realm lock.
                            let batch = {
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                let after = mq::cursor(&vs, concrete, &part, &actor)
                                    .unwrap_or(0);
                                mq::backlog(&vs, concrete, &part, after).unwrap_or_default()
                            };
                            for (seq, payload) in batch {
                                let job = aura_actor::QueuedJob {
                                    handler: concrete.clone(),
                                    args: payload,
                                };
                                Self::run_job_queued(consumer_realm.clone(), &consumer_id, job).await;
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                let _ = mq::advance(&vs, concrete, &part, &actor, seq);
                                progressed = true;
                            }
                        }
                        if !progressed {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            });
        } else if let Some(inst) = self.instances.get_mut(&key) {
            inst.last_activity = Instant::now();
        }
        Ok(self.instances.get_mut(&key).expect("just inserted"))
    }

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

    pub(crate) async fn run_job(self_arc: SharedRealm, id: &InstanceId, job: Job) {
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm.instances.get_mut(&(id.actor_type.clone(), id.key.clone())) {
            i.last_activity = Instant::now();
        }
        // ADR-0016 revised: the instance's pending idle-reclaim entry is
        // void the moment work arrives (work CANCELLED it — idempotent
        // cancel covers a timer that fired between tick and execution).
        // The watchdog (max_exec budget) arms for the job's duration; the
        // max_exec is per-type, falling back to no watchdog when unset.
        let watchdog_ttl = realm.types.get(&id.actor_type).and_then(|a| a.max_exec);
        realm.timers.cancel_target(id);
        if let Some(budget) = watchdog_ttl {
            realm.timers.register_reclaim(id.clone(), timer::ReclaimKind::Watchdog, budget);
        }
        let Some(actor) = realm.types.get(&id.actor_type) else {
            let _ = job
                .reply
                .send(Err(anyhow::anyhow!("unknown actor type: {}", id.actor_type)));
            return;
        };
        let body = actor.body.clone();
        let realm_plan = realm.plan_of(&id.actor_type).cloned();
        let realm_mq = realm.mq.clone();
        let ctx = Self::ctx_for(
            self_arc.clone(),
            realm.mq.clone(),
            realm_plan.as_ref(),
            realm.schema_of(&id.actor_type).cloned().flatten(),
            id,
        );
        let sessions = realm.sessions.clone();
        let probes = realm.probes.clone();
        let probes_base_url = realm.code_base_url.clone();
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
                // Code travels by reference (ADR-0027): the bytes were
                // stored under their hash at registration; the frame
                // carries the address the probe fetches and verifies.
                let sha = crate::meta::code_hash(&source);
                let code = match probes_base_url {
                    Some(base) => probe_protocol::CodeRef {
                        url: format!("{}/{}", base.trim_end_matches('/'), crate::meta::code_hex(&sha)),
                        sha256: crate::meta::code_hex(&sha),
                    },
                    None => {
                        let _ = job.reply.send(Err(anyhow::anyhow!(
                            "remote actor '{node_alias}': no code_base_url configured \
                             (ADR-0027 — code is content-addressed; set node {{ code_base_url }})"
                        )));
                        return;
                    }
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
                    code,
                };
                let result = match conn.sender.send(probe_protocol::Frame::Call(call)) {
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
            aura_actor::Body::Script { language, source } => {
                // Resident sessions (Phase 2.6): one VM/PTY per actor
                // instance, loaded once, called per event. Cross-call
                // state lives in the session (module globals / $env);
                // eviction drops it. spawn_blocking so host fns may block
                // on the async ctx. Nushell carries the bridge too now —
                // host fns ride the session dir's request/response files.
                let host = {
                    // Wasm full-power raw surface: the type's resolved
                    // plan carries its ns; the raw engine handle rides
                    // the mq clone the ctx already holds.
                    let wasm_raw = realm_plan
                        .as_ref()
                        .map(|p| (p.ns, realm_mq.clone()));
                    let fns = Self::host_bridge_for(&ctx, wasm_raw);
                    Some(probe_runtime::carrier::HostBridge { functions: fns })
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
        // ADR-0016 revised §3: idle_ttl is measured from job COMPLETION.
        // Cancel the watchdog (budget consumed by a finished job is not a
        // violation) and re-arm idle from now. last_activity keeps its
        // meaning as the observation surface (last job arrival).
        {
            let mut realm = self_arc.lock().await;
            realm.timers.cancel_target(id);
            let idle = realm
                .types
                .get(&id.actor_type)
                .and_then(|a| a.idle_ttl)
                .unwrap_or(realm.idle_ttl);
            realm.timers.register_reclaim(id.clone(), timer::ReclaimKind::Idle, idle);
            if let Some(i) = realm.instances.get_mut(&(id.actor_type.clone(), id.key.clone())) {
                i.last_activity = Instant::now();
            }
        }
        let _ = job.reply.send(result);
    }

    pub async fn evict_instance(realm: SharedRealm, id: &InstanceId) {
        let mut locked = realm.lock().await;
        let key = (id.actor_type.clone(), id.key.clone());
        let Some(_inst) = locked.instances.remove(&key) else { return };
        locked.timers.cancel_target(id);
        if let Some(actor) = locked.types.get(&id.actor_type) {
            if let Some(on_sleep) = actor.on_sleep.clone() {
                let ctx = Self::ctx_for(
                    realm.clone(),
                    locked.mq.clone(),
                    locked.plan_of(&id.actor_type),
                    locked.schema_of(&id.actor_type).cloned().flatten(),
                    id,
                );
                if let Err(e) = on_sleep(ctx).await {
                    eprintln!("on_sleep failed for {}/{}: {e}", key.0, key.1);
                }
            }
        }
        locked.sessions.evict(&format!("{}/{}", key.0, key.1));
    }

    pub async fn watchdog_expiry(realm: SharedRealm, id: &InstanceId) {
        Self::evict_instance(realm, id).await;
    }

    pub async fn deliver_timer(realm: SharedRealm, target: InstanceId, tag: String) {
        let job = aura_actor::QueuedJob {
            handler: "__on_timer".into(),
            args: serde_json::json!({ "tag": tag }),
        };
        Self::run_job_queued(realm, &target, job).await;
    }

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
                    let ctx = Self::ctx_for(
                        self_arc.clone(),
                        self.mq.clone(),
                        self.plan_of(&key.0),
                        self.schema_of(&key.0).cloned().flatten(),
                        &inst.id,
                    );
                    if let Err(e) = on_sleep(ctx).await {
                        // Eviction proceeds regardless: the hook is
                        // advisory; state is already in the store.
                        eprintln!("on_sleep failed for {}/{}: {e}", key.0, key.1);
                    }
                }
            }
            // The resident session dies WITH the instance: the VM/PTY
            // holds no durable truth (durable state lives in the type's
            // declared collections, ADR-0026 §3), so eviction is a plain
            // drop. The next activation cold-starts a fresh session and
            // reloads the source.
            self.sessions.evict(&format!("{}/{}", key.0, key.1));
            evicted.push(inst.id);
        }
        evicted
    }

    pub fn spawn_evictor(realm: &SharedRealm) {
        // Evictor task (post-ADR-0016-revision): only the pending-call
        // deadline sweep remains. Idle eviction is fully timer-driven —
        // the wheel's reclaim entries fire `evict_instance` on expiry
        // (idle measured from job completion), so the O(instances) linear
        // scan per tick is gone.
        let realm = Arc::downgrade(realm);
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let Some(realm) = realm.upgrade() else { break };
                let mut locked = realm.lock().await;
                locked.sweep_deadlines().await;
            }
        });
    }

    pub fn is_resident(&self, id: &InstanceId) -> bool {
        self.instances.contains_key(&(id.actor_type.clone(), id.key.clone()))
    }
}
