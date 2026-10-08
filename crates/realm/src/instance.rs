//! Instance lifecycle: construction, activation, job execution,
//! eviction, timer glue, the evictor driver. Split out of lib.rs per
//! ADR-0029.

use super::{next_seq, Realm, SharedRealm, RemotePending};
use crate::{event, mq, timer};
use crate::mq::InstanceKey;
use crate::DEFAULT_CURSOR_TTL;
use aura_booth::{Instance, InstanceId, Job};
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
            cursor_ttl: DEFAULT_CURSOR_TTL,
            router: event::EventRouter::default(),
            sessions: probe_runtime::carrier::session::Sessions::new(),
            probes: HashMap::new(),
            code_base_url: None,
            pending_remote: HashMap::new(),
            streams: HashMap::new(),
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
        let key = (id.booth_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let inst = Instance::new(id.clone(), self.queue_capacity);
            // on_wake: fresh residency. Runs on first activation too —
            // symmetric with on_sleep; a first-time wake is still a wake.
            if let Some(booth) = self.types.get(&id.booth_type) {
                if let Some(on_wake) = booth.on_wake.clone() {
                    let ctx = Self::ctx_for(
                        self_arc.clone(),
                        self.mq.clone(),
                        self.plan_of(&id.booth_type),
                        self.schema_of(&id.booth_type).cloned().flatten(),
                        id,
                    );
                    on_wake(ctx, serde_json::Value::Null).await?;
                }
            }
            // Subscribe to the event queues this type's @on declarations
            // bind (Phase 4.5c step 2b): persistent partitions over the
            // store, one per (event, partition); the subscriber holds a
            // cursor. ADR-0038 §1: a KEY-LESS route delivers to the type's
            // singleton instance, so only that instance binds the singleton
            // queue — the old broadcast (every instance holding its own
            // participant cursor on one shared queue behind one shared
            // cursor row) made the retention denominator an open set.
            let mut subs: Vec<(String, InstanceKey, bool)> = Vec::new();
            if let Some(_booth) = self.types.get(&id.booth_type) {
                for route in self.router.routes_of(&id.booth_type) {
                    let partition = if route.instance_key_field.is_empty() {
                        if id.key != mq::SINGLETON {
                            continue;
                        }
                        InstanceKey::Singleton
                    } else {
                        // For keyed routes the partition value equals the
                        // instance key only when the route derives the key
                        // from the same field emit used — which it does by
                        // construction (emit set key = data[field]).
                        InstanceKey::Named(id.key.clone())
                    };
                    // The third element marks a wildcard subscription:
                    // route.event is a PATTERN, expanded to concrete names
                    // at consume time (queues are keyed by concrete names —
                    // emit writes there).
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
            tokio::spawn(async move {
                // One pass over ALL bound queues per cycle. (The earlier
                // shape — a `for` over subs wrapping an infinite `loop`
                // per subscription — starved every queue but the first
                // for a multi-@on type, and tripped clippy::never_loop.)
                // The cursor's subject is the booth TYPE (ADR-0038 §2): two
                // types on one event hold independent cursors, and one type
                // no longer needs a per-participant id.
                let booth_type = consumer_id.booth_type.clone();
                // Concrete queue names per subscription: an exact
                // subscription is one name; a wildcard expands to every
                // registered event matching its prefix (re-expanded each
                // pass — new concrete names join automatically).
                let mut queues: Vec<(String, InstanceKey, bool, Vec<String>)> = subs
                    .into_iter()
                    .map(|(event, part, is_wildcard)| {
                        (event, part, is_wildcard, Vec::new())
                    })
                    .collect();
                // An instance with no bound queue has nothing to drain —
                // exit (releasing the realm Arc) instead of spinning the
                // poll loop forever holding the store open.
                if queues.is_empty() {
                    return;
                }
                loop {
                    let mut progressed = false;
                    for (event, part, is_wildcard, names) in &mut queues {
                        if *is_wildcard {
                            let prefix = event.trim_end_matches('*').to_string();
                            let found = {
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                mq::events_matching(&vs, &prefix).unwrap_or_default()
                            };
                            if found != *names {
                                *names = found;
                            }
                        } else if names.is_empty() {
                            names.push(event.clone());
                        }
                        for concrete in &*names {
                            // Fetch the backlog under a short lock; run jobs
                            // OUTSIDE the realm lock.
                            let batch = {
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                let after = mq::cursor(&vs, concrete, part, &booth_type)
                                    .unwrap_or(0);
                                mq::backlog(&vs, concrete, part, after).unwrap_or_default()
                            };
                            for (seq, payload) in batch {
                                let job = aura_booth::QueuedJob {
                                    handler: concrete.clone(),
                                    args: payload,
                                };
                                Self::run_job_queued(consumer_realm.clone(), &consumer_id, job).await;
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                let _ = mq::advance(&vs, concrete, part, &booth_type, seq);
                                progressed = true;
                            }
                        }
                    }
                    if !progressed {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            });
        } else if let Some(inst) = self.instances.get_mut(&key) {
            inst.last_activity = Instant::now();
        }
        Ok(self.instances.get_mut(&key).expect("just inserted"))
    }

    async fn run_job_queued(self_arc: SharedRealm, id: &InstanceId, job: aura_booth::QueuedJob) {
        let reply_tx = tokio::sync::oneshot::channel();
        // Unified seam (ADR-0036): an event delivery is a Start whose
        // reply nobody parks on — the envelope is produced and
        // discarded by the drop (§4). The stream id still rides the job
        // (one code path: registration + reply, never a special case).
        let sid = {
            let mut realm = self_arc.lock().await;
            realm.call_seq += 1;
            format!("stream-{}", realm.call_seq)
        };
        let job = Job {
            kind: aura_booth::JobKind::Start,
            stream: sid,
            handler: job.handler,
            args: job.args,
            reply: reply_tx.0,
        };
        // Drop the receiver: no one observes the reply.
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm.instances.get_mut(&(id.booth_type.clone(), id.key.clone())) {
            i.last_activity = std::time::Instant::now();
        }
        drop(realm);
        Self::run_job(self_arc, id, job).await;
    }

    pub(crate) async fn run_job(self_arc: SharedRealm, id: &InstanceId, job: Job) {
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm.instances.get_mut(&(id.booth_type.clone(), id.key.clone())) {
            i.last_activity = Instant::now();
        }
        // ADR-0016 revised: the instance's pending idle-reclaim entry is
        // void the moment work arrives (work CANCELLED it — idempotent
        // cancel covers a timer that fired between tick and execution).
        // The watchdog (max_exec budget) arms for the job's duration; the
        // max_exec is per-type, falling back to no watchdog when unset.
        let watchdog_ttl = realm.types.get(&id.booth_type).and_then(|a| a.max_exec);
        realm.timers.cancel_target(id);
        if let Some(budget) = watchdog_ttl {
            realm.timers.register_reclaim(id.clone(), timer::ReclaimKind::Watchdog, budget);
        }
        let Some(booth) = realm.types.get(&id.booth_type) else {
            let _ = job
                .reply
                .send(Err(anyhow::anyhow!("unknown booth type: {}", id.booth_type)));
            return;
        };
        let body = booth.body.clone();
        let realm_plan = realm.plan_of(&id.booth_type).cloned();
        let realm_mq = realm.mq.clone();
        let ctx = Self::ctx_for(
            self_arc.clone(),
            realm.mq.clone(),
            realm_plan.as_ref(),
            realm.schema_of(&id.booth_type).cloned().flatten(),
            id,
        );
        let sessions = realm.sessions.clone();
        let probes = realm.probes.clone();
        let probes_base_url = realm.code_base_url.clone();
        drop(realm);
        let mut result = match body {
            aura_booth::Body::RemoteProbe { node_alias, language, source, encoding } => {
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
                            "remote probe '{node_alias}': no code_base_url configured \
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
                // Residency identity = this booth INSTANCE (type/key), not the handler:
                // two instances of one remote type must never share the probe's
                // resident runtime, and every handler of one instance must.
                // `entry` is the handler the call addresses in the delivered code.
                let session = format!("{}/{}", id.booth_type, id.key);
                // ADR-0036: the frame kind vocabulary is the stream
                // vocabulary — every dispatch job crosses the wire as a
                // stream op (invoke is a Start whose reply is terminal).
                let kind = match job.kind {
                    aura_booth::JobKind::Start => probe_protocol::CallKind::IterateStart,
                    aura_booth::JobKind::Next => probe_protocol::CallKind::IterateNext,
                    aura_booth::JobKind::Dispose => probe_protocol::CallKind::IterateDispose,
                };
                let call = probe_protocol::ToolCall {
                    call_id: call_id.clone(),
                    kind,
                    stream: Some(job.stream.clone()),
                    session,
                    entry: job.handler.clone(),
                    language,
                    // ADR-0037 §2: the declared codec crosses with the
                    // call — the node's process carriers speak it.
                    encoding: map_encoding(encoding),
                    args: job.args.clone(),
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
            aura_booth::Body::Rust(handler) => {
                // A Rust closure body has no resident session to hold a
                // stream (ADR-0034: the producer shape lives in the
                // session — a recorded residual for Rust bodies). A
                // Start is legal (the handler runs once — the invoke
                // shape, wrapped to a terminal envelope per ADR-0036
                // §2); the stream verbs after it are an error value,
                // never a silent single-shot fallback.
                match job.kind {
                    aura_booth::JobKind::Start => {
                        handler(ctx, job.args.clone()).await.map(|v| {
                            aura_booth::Envelope::terminal(Some(v))
                        })
                    }
                    aura_booth::JobKind::Next | aura_booth::JobKind::Dispose => {
                        Err(anyhow::anyhow!(
                            "iterate: Rust booth bodies carry no stream state (ADR-0034) — use a script booth"
                        ))
                    }
                }
            }
            aura_booth::Body::Script { language, source, encoding } => {
                // Resident sessions (Phase 2.6): one VM/child per booth
                // instance, loaded once, called per event. Cross-call
                // state lives in the session (module globals / the child's
                // own memory); eviction drops it. spawn_blocking so host
                // fns may block on the async ctx. The out-of-process
                // shapes carry the bridge over their channels (bgi frames,
                // the retired PTY's file round trips are gone).
                let host = {
                    // Wasm full-power raw surface: the type's resolved
                    // plan carries its ns; the raw engine handle rides
                    // the mq clone the ctx already holds.
                    let wasm_raw = realm_plan
                        .as_ref()
                        .map(|p| (p.ns, realm_mq.clone()));
                    let fns = Self::host_bridge_for(&ctx, wasm_raw, &realm_mq);
                    // Python injection slot (ADR-0037 4.16a): the byte
                    // face rides the SAME realm-mq handle the ctx
                    // executor uses — `ns_raw` is the wasm plane
                    // (guest-side Collection binds no ns); here the
                    // injected Collection binds ns itself, so a raw
                    // handle keeps the two paths byte-identical
                    // (realm-prefixed [realm][ns][slot]...).
                    let storage = realm_plan.as_ref().map(|plan| {
                        use okm_core::storage::VirtualStorage;
                        use probe_runtime::carrier::{StorageCollection, StorageEngineFns, StorageSlot};
                        let store = realm_mq.clone();
                        let engine = std::sync::Arc::new(StorageEngineFns {
                            put: std::sync::Arc::new(move |k, v| store.put(k, v)),
                            get: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |k| store.get(k)
                            }),
                            del: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |k| store.del(k)
                            }),
                            scan_range: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |b, e| store.scan_range(b, e)
                            }),
                        });
                        let collections = plan
                            .entries
                            .iter()
                            .map(|(name, entry)| StorageCollection {
                                name: name.clone(),
                                entry: entry.clone(),
                            })
                            .collect();
                        std::sync::Arc::new(StorageSlot {
                            ns: plan.ns,
                            collections,
                            engine,
                        })
                    });
                    Some(probe_runtime::carrier::HostBridge {
                        functions: fns,
                        storage,
                    })
                };
                let instance_key = format!("{}/{}", id.booth_type, id.key);
                // Unified seam (ADR-0036): every job drives the
                // session's iterate surface — the job's verb IS the
                // stream op (generator mode parks the native generator
                // in the session; envelope mode re-invokes the handler
                // with the injected `{stream_id, op}`; a plain handler's
                // bare Start reply wraps to `{done:true,value}` AT THE
                // CARRIER — `s.call` is no longer a dispatch arm).
                let sjob = stream_op(&job);
                let encoding = map_encoding(encoding);
                tokio::task::spawn_blocking(move || {
                    sessions.with_session_encoded(
                        &instance_key,
                        &language,
                        &source,
                        host.as_ref(),
                        &probe_runtime::sandbox::SandboxPolicy::None,
                        // ADR-0037 §2: the declared codec drives the
                        // process carriers' channel; embedded carriers
                        // ignore it (no channel).
                        encoding,
                        move |s| s.iterate(sjob),
                    )
                })
                .await
                .unwrap_or_else(|e| Err(anyhow::anyhow!("script task join: {e}")))
            }
        };
        // Stream bookkeeping on the reply path — carrier-independent
        // (ADR-0034 §2: a frame identity, and the identity travels the
        // same way for in-process and remote producers), association
        // derived from `done` (ADR-0036 §4):
        // - Start: a NON-terminal first reply gets the realm-minted
        //   stream id merged in (the consumer reads it back; no carrier
        //   — session or remote probe — invents identities). A terminal
        //   first reply is the invoke shape: no id merged (no stream
        //   opened), and the registration unwinds immediately — mint
        //   and discard costs nothing measurable and keeps ONE code
        //   path ("invoke jobs skip registration" is the special case
        //   §4 removes).
        // - Next: exhaustion is terminal — the routing entry goes when
        //   the producer says done (the producer-side entry already
        //   dropped itself). A stream never disposed dies with
        //   eviction, per §3.
        if job.kind == aura_booth::JobKind::Start {
            let terminal_first = match &result {
                Ok(v) => v.get("done").and_then(|d| d.as_bool()) == Some(true),
                Err(_) => true,
            };
            if !terminal_first {
                // The carrier guarantees an object reply here; merge the
                // realm-minted id in (association rides `done`, §4 — a
                // non-object first reply simply never opens a stream).
                if let Ok(serde_json::Value::Object(map)) = &mut result {
                    map.insert(
                        "stream_id".into(),
                        serde_json::Value::String(job.stream.clone()),
                    );
                }
            }
            // Terminal first reply OR a failed Start: undo the
            // registration (the same rollback discipline as a
            // queue-full send: no orphaned routing entries).
            if terminal_first {
                self_arc.lock().await.streams.remove(&job.stream);
            }
        }
        if job.kind == aura_booth::JobKind::Next {
            let ended = match &result {
                Ok(v) => v.get("done").and_then(|d| d.as_bool()) != Some(false),
                // failed pull: the producer side is dead — drop the
                // routing entry too.
                Err(_) => true,
            };
            if ended {
                self_arc.lock().await.streams.remove(&job.stream);
            }
        }
        // ADR-0016 revised §3: idle_ttl is measured from job COMPLETION.
        // Cancel the watchdog (budget consumed by a finished job is not a
        // violation) and re-arm idle from now. last_activity keeps its
        // meaning as the observation surface (last job arrival).
        {
            let mut realm = self_arc.lock().await;
            realm.timers.cancel_target(id);
            let idle = realm
                .types
                .get(&id.booth_type)
                .and_then(|a| a.idle_ttl)
                .unwrap_or(realm.idle_ttl);
            realm.timers.register_reclaim(id.clone(), timer::ReclaimKind::Idle, idle);
            if let Some(i) = realm.instances.get_mut(&(id.booth_type.clone(), id.key.clone())) {
                i.last_activity = Instant::now();
            }
        }
        let _ = job.reply.send(result);
    }

    pub async fn evict_instance(realm: SharedRealm, id: &InstanceId) {
        let mut locked = realm.lock().await;
        let key = (id.booth_type.clone(), id.key.clone());
        let Some(_inst) = locked.instances.remove(&key) else { return };
        locked.timers.cancel_target(id);
        if let Some(booth) = locked.types.get(&id.booth_type) {
            if let Some(on_sleep) = booth.on_sleep.clone() {
                let ctx = Self::ctx_for(
                    realm.clone(),
                    locked.mq.clone(),
                    locked.plan_of(&id.booth_type),
                    locked.schema_of(&id.booth_type).cloned().flatten(),
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
        let job = aura_booth::QueuedJob {
            handler: "__on_timer".into(),
            args: serde_json::json!({ "tag": tag }),
        };
        Self::run_job_queued(realm, &target, job).await;
    }

    /// The cold-tier submission arm (ADR-0036): like every dispatch job,
    /// a Start with a realm-minted stream id — the caller is not parked
    /// (the result resolves through `resolve_call`); a non-terminal
    /// first reply registers the stream for whoever holds the id (a
    /// cold-tier streaming producer has no known consumer — recorded
    /// residual, same stance as ADR-0034 §5 for iterate).
    pub async fn submit(
        self_arc: &SharedRealm,
        target: InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<serde_json::Value>>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        {
            let mut realm = self_arc.lock().await;
            if !realm.types.contains_key(&target.booth_type) {
                anyhow::bail!("unknown booth type: {}", target.booth_type);
            }
            realm.call_seq += 1;
            let sid = format!("stream-{}", realm.call_seq);
            realm.streams.insert(
                sid.clone(),
                crate::StreamEntry {
                    target: target.clone(),
                    handler: handler.to_string(),
                    args: args.clone(),
                },
            );
            let inst = realm.instance(self_arc.clone(), &target).await?;
            let send = inst.queue.tx.try_send(Job {
                kind: aura_booth::JobKind::Start,
                stream: sid.clone(),
                handler: handler.to_string(),
                args,
                reply: reply_tx,
            });
            if send.is_err() {
                realm.streams.remove(&sid);
            }
            send.map_err(|_| anyhow::anyhow!("queue full: {}/{}", target.booth_type, target.key))?;
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
                        .get_mut(&(target2.booth_type.clone(), target2.key.clone()));
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
        // entity booth can be reclaimed quickly (Phase 6.5).
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
            if let Some(booth) = self.types.get(&key.0) {
                if let Some(on_sleep) = booth.on_sleep.clone() {
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
        // Evictor task (post-ADR-0016-revision): idle eviction is fully
        // timer-driven — the wheel's reclaim entries fire `evict_instance`
        // on expiry (idle measured from job completion), so the O(instances)
        // linear scan per tick is gone. Two sweeps remain on the 5s tick:
        // pending-call deadlines, and the exec registry's dead children
        // (ADR-0035: a crashed child must not hold its instance resident
        // until the next call fails — the slot is a cache entry whose
        // process died; sweep reaps it and the next call cold-starts).
        let realm = Arc::downgrade(realm);
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let Some(realm) = realm.upgrade() else { break };
                let sessions;
                {
                    let mut locked = realm.lock().await;
                    locked.sweep_deadlines().await;
                    sessions = locked.sessions.clone();
                }
                // The exec sweep takes only the registry lock briefly per
                // slot — never held across anything blocking.
                let _dead = sessions.sweep_dead();
            }
        });
    }

    pub fn is_resident(&self, id: &InstanceId) -> bool {
        self.instances.contains_key(&(id.booth_type.clone(), id.key.clone()))
    }
}

/// Map the declared booth codec (aura-booth's probe-free enum) onto the
/// runtime's form (ADR-0037 §2 — the two crates keep their own types,
/// the values are the same wire pair "json"/"cbor").
pub(crate) fn map_encoding(
    e: aura_booth::ChannelEncoding,
) -> probe_protocol::ChannelEncoding {
    match e {
        aura_booth::ChannelEncoding::Json => probe_protocol::ChannelEncoding::Json,
        aura_booth::ChannelEncoding::Cbor => probe_protocol::ChannelEncoding::Cbor,
    }
}

/// Build the session-facing stream op from a job (ADR-0034, unified
/// seam ADR-0036 — every verb is a stream op now; a plain handler's bare
/// Start reply wraps to `{done:true,value}` at the carrier). Every verb
/// carries handler + args: envelope-mode sessions re-invoke the handler
/// each turn (the guard state is theirs, the args are the stream's
/// start args), generator-mode sessions use only the stream id.
pub(crate) fn stream_op(job: &Job) -> probe_runtime::carrier::session::StreamOp {
    use probe_runtime::carrier::session::StreamOp;
    let stream_id = job.stream.clone();
    match job.kind {
        aura_booth::JobKind::Start => StreamOp::Start {
            stream_id,
            handler: job.handler.clone(),
            args: job.args.clone(),
        },
        aura_booth::JobKind::Next => {
            StreamOp::Next { stream_id, handler: job.handler.clone(), args: job.args.clone() }
        }
        aura_booth::JobKind::Dispose => {
            StreamOp::Dispose { stream_id, handler: job.handler.clone(), args: job.args.clone() }
        }
    }
}
