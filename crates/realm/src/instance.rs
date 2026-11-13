//! Instance lifecycle: construction, activation, job execution,
//! eviction, timer glue, the evictor driver. Split out of lib.rs per
//! ADR-0029.

use super::{next_seq, Realm, RemotePending, SharedRealm};
use crate::mq::InstanceKey;
use crate::DEFAULT_CURSOR_TTL;
use crate::{event, mq, timer};
use aura_booth::{Instance, InstanceId, Job};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

impl Realm {
    /// THE STOP DOOR (ADR-0047 §3): ask an activation to stop at its next boundary.
    ///
    /// A framework call, not an event: a delivery addressed to a busy instance
    /// queues behind the work it means to stop (measured on gravity's turn
    /// booth). Stage two — abandoning the operation in flight — is the INTERRUPT and is
    /// not here yet; it needs the execution layer's one primitive.
    pub fn stop_set(&self, instance: &aura_booth::InstanceId) {
        self.stops.set(instance);
    }

    /// The activation answered the request (or none is running): forget it. A
    /// bit that outlives its activation would stop the next one.
    pub fn stop_clear(&self, instance: &aura_booth::InstanceId) {
        self.stops.clear(instance);
    }

    /// THE INTERRUPT DOOR (ADR-0047 §4, stage two): stop waiting for the call that is
    /// running on this instance, and let the caller take the terminal value. `false`
    /// means nothing was running — a page's cancel arriving after its turn ended is
    /// exactly that, and it is not an error.
    pub fn stop_interrupt(&self, instance: &aura_booth::InstanceId) -> bool {
        self.stops.interrupt(instance)
    }

    pub async fn shared_async(self) -> SharedRealm {
        let store = self.mq.clone();
        let arc = Arc::new(tokio::sync::Mutex::new(self));
        let handle = timer::TimerDriver::spawn(arc.clone(), store);
        arc.lock().await.timers = handle;
        arc
    }

    pub fn with_mq(mq_store: mq::MqStore) -> Self {
        Self {
            types: HashMap::new(),
            instances: HashMap::new(),
            queue_capacity: 64,
            mq: mq_store.clone(),
            stops: crate::stop::Stops::default(),
            idle_ttl: Duration::from_secs(30),
            cursor_ttl: DEFAULT_CURSOR_TTL,
            router: event::EventRouter::default(),
            executors: effector_runtime::carrier::executor::Executors::new(),
            effectors: HashMap::new(),
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

    pub(crate) async fn instance(
        &mut self,
        self_arc: SharedRealm,
        id: &InstanceId,
    ) -> anyhow::Result<&mut Instance> {
        let key = (id.booth_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let inst = Instance::new(id.clone(), self.queue_capacity);
            // __wake: fresh residency. Runs on first activation too —
            // symmetric with __sleep; a first-time wake is still a wake.
            // ADR-0043 §3: the hook lives in the type's hook table —
            // Compiled runs directly, Declared rides the executor
            // (the handler VM executed ctx.on at load).
            if let Some(booth) = self.types.get(&id.booth_type) {
                if let Some(hook) = booth.hooks.get("__wake").cloned() {
                    let body = booth.body.clone();
                    let executors = self.executors.clone();
                    let ctx = Self::ctx_for(
                        self_arc.clone(),
                        self.mq.clone(),
                        self.plan_of(&id.booth_type),
                        self.schema_of(&id.booth_type).cloned().flatten(),
                        id,
                    );
                    let host = self.delivery_bridge_for(&ctx, id);
                    Self::run_hook(
                        &hook,
                        Some(body),
                        &executors,
                        host,
                        id,
                        &ctx,
                        "__wake",
                        booth.concurrency,
                    )
                    .await?;
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
            // (event, partition, wildcard, declared entry — `None` = the event name).
            let mut subs: Vec<(String, InstanceKey, bool, Option<String>)> = Vec::new();
            if let Some(_booth) = self.types.get(&id.booth_type) {
                for route in self.router.routes_of(&id.booth_type) {
                    let partition = match route.resolution {
                        // ADR-0038 §1: a key-less route's queue is consumed
                        // by the type's singleton instance only — variant
                        // matching (ADR-0042), never a string compare.
                        aura_booth::RouteResolution::Singleton => {
                            if !matches!(id.key, aura_booth::InstanceKey::Singleton) {
                                continue;
                            }
                            InstanceKey::Singleton
                        }
                        // For keyed/scan routes the partition value equals
                        // the instance key by construction (emit set key =
                        // the resolved variant; a scan route's hit-row key
                        // is the instance key it delivered to).
                        aura_booth::RouteResolution::Field(_)
                        | aura_booth::RouteResolution::Scan { .. } => {
                            InstanceKey::Named(id.key.render().to_string())
                        }
                    };
                    // The third element marks a wildcard subscription:
                    // route.event is a PATTERN, expanded to concrete names
                    // at consume time (queues are keyed by concrete names —
                    // emit writes there).
                    subs.push((
                        route.event.clone(),
                        partition,
                        route.event.ends_with('*'),
                        route.entry.clone(),
                    ));
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
                let mut queues: Vec<(String, InstanceKey, bool, Vec<String>, Option<String>)> =
                    subs.into_iter()
                        .map(|(event, part, is_wildcard, handler)| {
                            (event, part, is_wildcard, Vec::new(), handler)
                        })
                        .collect();
                // An instance with no bound queue has nothing to drain —
                // exit (releasing the realm Arc) instead of spinning the
                // poll loop forever holding the store open.
                if queues.is_empty() {
                    return;
                }
                // A plane that cannot answer is NOT an idle plane: every
                // read below either yields its rows or says why not. There is
                // no caller to return the failure to (this is a spawned task),
                // so it goes to the log and the pass is dropped -- the loop
                // backs off instead of mistaking a broken store for "no
                // events".
                'poll: loop {
                    let mut progressed = false;
                    for (event, part, is_wildcard, names, declared) in &mut queues {
                        if *is_wildcard {
                            let prefix = event.trim_end_matches('*').to_string();
                            let found = {
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                match mq::events_matching(&vs, &prefix) {
                                    Ok(found) => found,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e, prefix = %prefix,
                                            "consumer: the plane cannot list its queues; pass skipped"
                                        );
                                        continue 'poll;
                                    }
                                }
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
                                let after = match mq::cursor(&vs, concrete, part, &booth_type) {
                                    Ok(after) => after,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e, queue = %concrete,
                                            "consumer: no cursor for this queue; pass skipped"
                                        );
                                        continue 'poll;
                                    }
                                };
                                match mq::backlog(&vs, concrete, part, after) {
                                    Ok(batch) => batch,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e, queue = %concrete,
                                            "consumer: the backlog cannot be read; pass skipped"
                                        );
                                        continue 'poll;
                                    }
                                }
                            };
                            for (seq, payload) in batch {
                                // The body is called at the declared entry when the declaration
                                // bound one; otherwise the event name IS the entry.
                                let job = aura_booth::QueuedJob {
                                    handler: declared.clone().unwrap_or_else(|| concrete.clone()),
                                    args: payload,
                                };
                                Self::run_job_queued(
                                    consumer_realm.clone(),
                                    &consumer_id,
                                    job,
                                    concrete,
                                )
                                .await;
                                let realm = consumer_realm.lock().await;
                                let vs = realm.mq.clone();
                                // A cursor that will not advance is not
                                // progress: claiming it here would re-run this
                                // same entry on the next pass with no backoff.
                                match mq::advance(&vs, concrete, part, &booth_type, seq) {
                                    Ok(()) => progressed = true,
                                    Err(e) => tracing::warn!(
                                        error = %e, queue = %concrete, seq = seq,
                                        "consumer: the cursor would not advance; entry stays queued"
                                    ),
                                }
                            }
                        }
                    }
                    if !progressed {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            });
        } else if let Some(inst) = self.instances.get_mut(&key) {
            // Resident already: refresh the stamp only. The borrow ends at
            // this block so the returned borrow below is the sole one.
            inst.last_activity = Instant::now();
        }
        // The insert path above guarantees the entry exists; a missing key
        // here would be a state inconsistency, so it is an error value
        // rather than a panic the caller has to trust.
        self.instances.get_mut(&key).ok_or_else(|| {
            anyhow::anyhow!("instance {} missing right after creation", id.key.render())
        })
    }

    async fn run_job_queued(
        self_arc: SharedRealm,
        id: &InstanceId,
        job: aura_booth::QueuedJob,
        event: &str,
    ) {
        let entry = job.handler.clone();
        let args = job.args.clone();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
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
            reply: reply_tx,
        };
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm
            .instances
            .get_mut(&(id.booth_type.clone(), id.key.clone()))
        {
            i.last_activity = std::time::Instant::now();
        }
        drop(realm);
        Self::run_job(self_arc.clone(), id, job).await;
        // NOBODY PARKS ON A DELIVERY'S REPLY (§4) — except here: a body that could not be CALLED
        // (an entry the code does not define, a carrier fault) is a fact the operator must be
        // able to see. It is recorded in the dead ring with BOTH names, and said in the log,
        // because the alternative is a subscription that silently never runs (ADR-0045).
        match reply_rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!(
                    "aura: delivery of `{event}` failed at entry `{entry}` on {}: {e}",
                    id.booth_type
                );
                let mut realm = self_arc.lock().await;
                realm.dead_events.push(
                    event,
                    serde_json::json!({
                        "entry": entry,
                        "booth_type": id.booth_type,
                        "error": e.to_string(),
                        "payload": args,
                    }),
                    crate::event::DeadReason::DeliveryFailed,
                );
            }
            // The reply sender was dropped without an answer: run_job returned early. Not a
            // delivery failure to report (nothing was attempted) — and not our business here.
            Err(_) => {}
        }
    }

    pub(crate) async fn run_job(self_arc: SharedRealm, id: &InstanceId, job: Job) {
        let mut realm = self_arc.lock().await;
        if let Some(i) = realm
            .instances
            .get_mut(&(id.booth_type.clone(), id.key.clone()))
        {
            i.last_activity = Instant::now();
        }
        // ADR-0016 revised: the instance's pending idle-reclaim entry is
        // void the moment work arrives (work CANCELLED it — idempotent
        // cancel covers a timer that fired between tick and execution).
        // RECLAIM timers only: the instance's ctx.timer deliveries are
        // its own scheduled wakes and stay armed across a job. The
        // watchdog (max_exec budget) arms for the job's duration; the
        // max_exec is per-type, falling back to no watchdog when unset.
        let watchdog_ttl = realm.types.get(&id.booth_type).and_then(|a| a.max_exec);
        realm.timers.cancel_reclaim_target(id);
        if let Some(budget) = watchdog_ttl {
            realm
                .timers
                .register_reclaim(id.clone(), timer::ReclaimKind::Watchdog, budget);
        }
        let Some(booth) = realm.types.get(&id.booth_type) else {
            let _ = job.reply.send(Err(anyhow::anyhow!(
                "unknown booth type: {}",
                id.booth_type
            )));
            return;
        };
        let body = booth.body.clone();
        // ADR-0046: how many calls this instance may run at once — the type's
        // declaration, read here while the type is borrowed.
        let concurrency = booth.concurrency;
        let realm_plan = realm.plan_of(&id.booth_type).cloned();
        let realm_mq = realm.mq.clone();
        let ctx = Self::ctx_for(
            self_arc.clone(),
            realm.mq.clone(),
            realm_plan.as_ref(),
            realm.schema_of(&id.booth_type).cloned().flatten(),
            id,
        );
        let executors = realm.executors.clone();
        let effectors = realm.effectors.clone();
        let effectors_base_url = realm.code_base_url.clone();
        let realm_timers = realm.timers.clone();
        let realm_stops = realm.stops.clone();
        drop(realm);
        // ADR-0047 stage two: every dispatch arms an interrupt slot, so the door has
        // something to notify. It is removed when this call ends — the guard drops
        // with the future, whichever way the call finished.
        let interrupt = realm_stops.begin_interrupt(id);
        // The same call's handle as effector sees it. It rides the CALL (the carrier
        // signature takes it), because an unbounded booth serves many calls at once.
        let control = interrupt.control();
        let mut result = match body {
            aura_booth::Body::Remote {
                node_alias,
                language,
                source,
                encoding,
            } => {
                // Remote effector execution (Phase 3): find the effector's live
                // outbound connection, send Frame::Call (inline payload),
                // await the correlated reply. The effector's resident
                // executors own the VM; no ctx bridge crosses the wire yet
                // (host functions over WS arrive with the frame path).
                let Some(conn) = effectors.get(&node_alias) else {
                    let _ = job.reply.send(Err(anyhow::anyhow!(
                        "effector '{node_alias}' not connected"
                    )));
                    return;
                };
                // Code travels by reference (ADR-0027): the bytes were
                // stored under their hash at registration; the frame
                // carries the address the effector fetches and verifies.
                let sha = crate::meta::code_hash(&source);
                let code = match effectors_base_url {
                    Some(base) => effector_protocol::CodeRef {
                        url: format!(
                            "{}/{}",
                            base.trim_end_matches('/'),
                            crate::meta::code_hex(&sha)
                        ),
                        sha256: crate::meta::code_hex(&sha),
                    },
                    None => {
                        let _ = job.reply.send(Err(anyhow::anyhow!(
                            "remote effector '{node_alias}': no code_base_url configured \
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
                    RemotePending {
                        reply: tx,
                        instance: id.clone(),
                    },
                );
                // Residency identity = this booth INSTANCE (type/key), not the handler:
                // two instances of one remote type must never share the effector's
                // resident runtime, and every handler of one instance must.
                // `entry` is the handler the call addresses in the delivered code.
                // Executor key format (ADR-0042): `{type}/{key}` with the
                // singleton's key segment EMPTY (it has no name) — InstanceKey's
                // Display renders Named(k) as k and Singleton as "".
                let executor = format!("{}/{}", id.booth_type, id.key);
                // ADR-0036: the frame kind vocabulary is the stream
                // vocabulary — every dispatch job crosses the wire as a
                // stream op (invoke is a Start whose reply is terminal).
                let kind = match job.kind {
                    aura_booth::JobKind::Start => effector_protocol::CallKind::IterateStart,
                    aura_booth::JobKind::Next => effector_protocol::CallKind::IterateNext,
                    aura_booth::JobKind::Dispose => effector_protocol::CallKind::IterateDispose,
                };
                let call = effector_protocol::ToolCall {
                    call_id: call_id.clone(),
                    kind,
                    stream: Some(job.stream.clone()),
                    executor,
                    entry: job.handler.clone(),
                    language,
                    // ADR-0037 §2: the declared codec crosses with the
                    // call — the node's process carriers speak it.
                    encoding,
                    args: job.args.clone(),
                    code,
                };
                let result = match conn.sender.send(effector_protocol::Frame::Call(call)) {
                    Ok(()) => {
                        // The node's answer and the interrupt, whichever comes first. On
                        // one the node is TOLD (`Frame::Interrupt`) — it is the only
                        // party that can reach the work — but the caller does not wait
                        // for its confirmation: the terminal value is already decided,
                        // and `Interrupted` says whether a carrier could act, which
                        // changes the node's cost, not this call's outcome.
                        tokio::select! {
                            reply = rx => match reply {
                                Ok(Ok(v)) => Ok(v),
                                Ok(Err(e)) => Err(anyhow::anyhow!("{e}")),
                                Err(_) => Err(anyhow::anyhow!(
                                    "effector '{node_alias}' dropped the call"
                                )),
                            },
                            () = interrupt.notified() => {
                                let _ = conn.sender.send(effector_protocol::Frame::Interrupt {
                                    call_id: call_id.clone(),
                                });
                                // The pending entry belongs to a reply that is no
                                // longer wanted; leaving it would be a leak with a
                                // timeout attached.
                                self_arc.lock().await.pending_remote.remove(&call_id);
                                Err(anyhow::Error::new(crate::stop::Interrupted))
                            }
                        }
                    }
                    Err(_) => Err(anyhow::anyhow!("effector '{node_alias}' connection closed")),
                };
                result
            }
            aura_booth::Body::Rust(handler) => {
                // A Rust closure body has no executor to hold a
                // stream (ADR-0034: the producer shape lives in the
                // executor — a recorded residual for Rust bodies). A
                // Start is legal (the handler runs once — the invoke
                // shape, wrapped to a terminal envelope per ADR-0036
                // §2); the stream verbs after it are an error value,
                // never a silent single-shot fallback.
                match job.kind {
                    aura_booth::JobKind::Start => {
                        let args = job.args.clone();
                        let call = handler(&ctx, args);
                        tokio::select! {
                            r = call => r.map(|v| {
                                aura_booth::Envelope::terminal(Some(v))
                            }),
                            () = interrupt.notified() => Err(anyhow::Error::new(
                                crate::stop::Interrupted
                            )),
                        }
                    }
                    aura_booth::JobKind::Next | aura_booth::JobKind::Dispose => {
                        Err(anyhow::anyhow!(
                            "iterate: Rust booth bodies carry no stream state (ADR-0034) — use a script booth"
                        ))
                    }
                }
            }
            aura_booth::Body::Script {
                language,
                source,
                encoding,
            } => {
                // Executors (Phase 2.6): one VM/child per booth
                // instance, loaded once, called per event. Cross-call
                // state lives in the executor (module globals / the child's
                // own memory); eviction drops it. spawn_blocking so host
                // fns may block on the async ctx. The out-of-process
                // shapes carry the bridge over their channels (induction frames,
                // the retired PTY's file round trips are gone).
                // The injected byte engine answers `Result<_, String>`
                // (ADR-0038/0039): the store's own error crosses the seam
                // as its text, so a failed read surfaces as the guest
                // call's error — never as an absent row.
                let host = {
                    // Wasm full-power raw surface: the type's resolved
                    // plan carries its ns; the raw engine handle rides
                    // the mq clone the ctx already holds.
                    let wasm_raw = realm_plan.as_ref().map(|p| (p.ns, realm_mq.clone()));
                    let fns = Self::host_bridge_for(&ctx, wasm_raw, &realm_mq, &realm_timers, &realm_stops);
                    // Python injection slot (ADR-0037 4.16a): the byte
                    // face rides the SAME realm-mq handle the ctx
                    // executor uses — `ns_raw` is the wasm plane
                    // (guest-side Collection binds no ns); here the
                    // injected Collection binds ns itself, so a raw
                    // handle keeps the two paths byte-identical
                    // (realm-prefixed [realm][ns][slot]...).
                    let storage = realm_plan.as_ref().map(|plan| {
                        use effector_runtime::carrier::{
                            StorageCollection, StorageEngineFns, StorageSlot,
                        };
                        use okm_core::storage::VirtualStorage;
                        let store = realm_mq.clone();
                        let engine = std::sync::Arc::new(StorageEngineFns {
                            put: std::sync::Arc::new(move |k, v| {
                                store.put(k, v).map_err(|e| e.to_string())
                            }),
                            get: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |k| store.get(k).map_err(|e| e.to_string())
                            }),
                            del: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |k| store.del(k).map_err(|e| e.to_string())
                            }),
                            scan_range: std::sync::Arc::new({
                                let store = realm_mq.clone();
                                move |b, e| {
                                    store
                                        .scan_range(b, e)
                                        .map(|item| item.map(|(k, _)| k))
                                        .collect::<okm_core::Result<Vec<Vec<u8>>>>()
                                        .map_err(|e| e.to_string())
                                }
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
                    Some(effector_runtime::carrier::HostBridge {
                        functions: fns,
                        storage,
                    })
                };
                let instance_key = format!("{}/{}", id.booth_type, id.key);
                // Unified seam (ADR-0036): every job drives the
                // executor's iterate surface — the job's verb IS the
                // stream op (generator mode parks the native generator
                // in the executor; envelope mode re-invokes the handler
                // with the injected `{stream_id, op}`; a plain handler's
                // bare Start reply wraps to `{done:true,value}` AT THE
                // CARRIER — `s.call` is no longer a dispatch arm).
                let sjob = stream_op(&job);
                // The instance's declared concurrency decides the residency
                // (ADR-0046): a queue (the default) holds one executor and orders
                // calls; a pool holds up to N and lets the extra call wait;
                // `Unbounded` holds ONE and never waits, because the carrier
                // pairs each reply with the request that asked for it.
                match concurrency {
                    aura_booth::Concurrency::Unbounded => {
                        let call_host = host.clone();
                        let control = control.clone();
                        tokio::task::spawn_blocking(move || {
                            executors.with_shared(
                                &instance_key,
                                &language,
                                &source,
                                &effector_runtime::sandbox::SandboxPolicy::None,
                                encoding,
                                host.as_ref(),
                                move |s| {
                                    s.iterate(sjob, call_host.as_ref(), Some(&control))
                                },
                            )
                        })
                    }
                    concurrency => {
                        let control = control.clone();
                        tokio::task::spawn_blocking(move || {
                        executors.with_slot_encoded(
                            &instance_key,
                            &language,
                            &source,
                            host.as_ref(),
                            &effector_runtime::sandbox::SandboxPolicy::None,
                            // ADR-0037 §2: the declared codec drives the
                            // process carriers' channel; embedded carriers
                            // ignore it (no channel).
                            encoding,
                            concurrency.slots(),
                            move |s| {
                                // The serial seam arms the handle, then runs: the slot is
                                // this call's alone until it returns.
                                s.arm_interrupt(&control);
                                s.iterate(sjob)
                            },
                        )
                    })
                    }
                }
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
        //   — executor or remote effector — invents identities). A terminal
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
            realm.timers.cancel_reclaim_target(id);
            let idle = realm
                .types
                .get(&id.booth_type)
                .and_then(|a| a.idle_ttl)
                .unwrap_or(realm.idle_ttl);
            realm
                .timers
                .register_reclaim(id.clone(), timer::ReclaimKind::Idle, idle);
            if let Some(i) = realm
                .instances
                .get_mut(&(id.booth_type.clone(), id.key.clone()))
            {
                i.last_activity = Instant::now();
            }
        }
        let _ = job.reply.send(result);
    }

    pub async fn evict_instance(realm: SharedRealm, id: &InstanceId) {
        let mut locked = realm.lock().await;
        let key = (id.booth_type.clone(), id.key.clone());
        let Some(_inst) = locked.instances.remove(&key) else {
            return;
        };
        locked.timers.cancel_target(id);
        // A dead activation has nothing left to ask (ADR-0047): the bit goes with it,
        // so the next activation of this instance starts unanswered.
        locked.stops.clear(id);
        if let Some(booth) = locked.types.get(&id.booth_type) {
            if let Some(hook) = booth.hooks.get("__sleep").cloned() {
                let body = booth.body.clone();
                let executors = locked.executors.clone();
                let ctx = Self::ctx_for(
                    realm.clone(),
                    locked.mq.clone(),
                    locked.plan_of(&id.booth_type),
                    locked.schema_of(&id.booth_type).cloned().flatten(),
                    id,
                );
                let host = locked.delivery_bridge_for(&ctx, id);
                if let Err(e) =
                    Self::run_hook(
                        &hook,
                        Some(body),
                        &executors,
                        host,
                        id,
                        &ctx,
                        "__sleep",
                        booth.concurrency,
                    )
                    .await
                {
                    eprintln!("__sleep failed for {}/{}: {e}", key.0, key.1);
                }
            }
        }
        locked.executors.evict(&format!("{}/{}", key.0, key.1));
    }

    pub async fn watchdog_expiry(realm: SharedRealm, id: &InstanceId) {
        Self::evict_instance(realm, id).await;
    }

    /// The delivery-arm host bridge, assembled for a hook dispatch (the
    /// verbatim run_job Script-arm assembly, extracted): a hook-spawned
    /// executor must carry the SAME ctx face later deliveries
    /// reuse — the slot is shared.
    fn delivery_bridge_for(
        &self,
        ctx: &aura_booth::Ctx,
        id: &InstanceId,
    ) -> Option<effector_runtime::carrier::HostBridge> {
        let realm_plan = self.plan_of(&id.booth_type).cloned();
        let wasm_raw = realm_plan.as_ref().map(|p| (p.ns, self.mq.clone()));
        let fns = Self::host_bridge_for(ctx, wasm_raw, &self.mq, &self.timers, &self.stops);
        // Same seam, same rule as the per-job arm: the store's error crosses
        // the byte face as its text, so the hook call itself carries it.
        let storage = realm_plan.as_ref().map(|plan| {
            use effector_runtime::carrier::{StorageCollection, StorageEngineFns, StorageSlot};
            use okm_core::storage::VirtualStorage;
            let store = self.mq.clone();
            let engine = std::sync::Arc::new(StorageEngineFns {
                put: std::sync::Arc::new(move |k, v| store.put(k, v).map_err(|e| e.to_string())),
                get: std::sync::Arc::new({
                    let store = self.mq.clone();
                    move |k| store.get(k).map_err(|e| e.to_string())
                }),
                del: std::sync::Arc::new({
                    let store = self.mq.clone();
                    move |k| store.del(k).map_err(|e| e.to_string())
                }),
                scan_range: std::sync::Arc::new({
                    let store = self.mq.clone();
                    move |b, e| {
                        store
                            .scan_range(b, e)
                            .map(|item| item.map(|(k, _)| k))
                            .collect::<okm_core::Result<Vec<Vec<u8>>>>()
                            .map_err(|e| e.to_string())
                    }
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
        Some(effector_runtime::carrier::HostBridge {
            functions: fns,
            storage,
        })
    }

    /// One lifecycle hook dispatch (ADR-0043 §3): a Compiled hook runs the
    /// Rust handler directly; a Declared hook rides the instance's resident
    /// executor — the handler the script bound at load under the reserved
    /// name, called with the payload the runtime chooses (Null; the hook
    /// reads its situation from ctx, not from a per-delivery payload).
    /// Script executors wrap a plain return to the terminal envelope
    /// (ADR-0036 §2) — the value is discarded, failures surface as errors
    /// (eviction proceeds regardless: the hook is advisory, state lives in
    /// the type's collections).
    #[allow(clippy::too_many_arguments)]
    async fn run_hook(
        hook: &aura_booth::HookBody,
        body: Option<aura_booth::Body>,
        executors: &effector_runtime::carrier::executor::Executors,
        host: Option<effector_runtime::carrier::HostBridge>,
        id: &InstanceId,
        ctx: &aura_booth::Ctx,
        name: &str,
        concurrency: aura_booth::Concurrency,
    ) -> anyhow::Result<()> {
        // NO realm lock here: every call site holds the realm mutex (the
        // activation path runs inside `instance()`, awaited under the
        // caller's guard) — re-locking inline deadlocks. The caller
        // captures body + executors BEFORE awaiting; the blocking executor
        // work runs in spawn_blocking against the (lock-free per-op)
        // executors registry.
        match hook {
            aura_booth::HookBody::Compiled(h) => h(ctx, serde_json::Value::Null).await.map(|_| ()),
            aura_booth::HookBody::Declared => match body {
                Some(aura_booth::Body::Script {
                    language,
                    source,
                    encoding,
                }) => {
                    let instance_key = format!("{}/{}", id.booth_type, id.key);
                    let name = name.to_string();
                    let executors = executors.clone();
                    // The bridge rides like the delivery arm's: a
                    // hook-spawned executor IS the executor later
                    // deliveries reuse — spawning it host-less would
                    // poison the slot (its bridge would lack every ctx
                    // method the booth's handlers call).
                    // The hook rides the instance's residency on the SAME terms
                    // the calls do (ADR-0046): a booth that declared `unbounded`
                    // serves its hooks from its one shared executor, never from a
                    // second process opened beside it.
                    match concurrency {
                        aura_booth::Concurrency::Unbounded => {
                            let call_host = host.clone();
                            tokio::task::spawn_blocking(move || {
                                executors.with_shared(
                                    &instance_key,
                                    &language,
                                    &source,
                                    &effector_runtime::sandbox::SandboxPolicy::None,
                                    encoding,
                                    host.as_ref(),
                                    move |s| {
                                        // A lifecycle hook is not an instance's work: it
                                        // has no stop slot and takes no handle.
                                        s.call(
                                            &name,
                                            &serde_json::Value::Null,
                                            call_host.as_ref(),
                                            None,
                                        )
                                    },
                                )
                            })
                        }
                        concurrency => tokio::task::spawn_blocking(move || {
                            executors.with_slot_encoded(
                                &instance_key,
                                &language,
                                &source,
                                host.as_ref(),
                                &effector_runtime::sandbox::SandboxPolicy::None,
                                encoding,
                                concurrency.slots(),
                                move |s| s.call(&name, &serde_json::Value::Null),
                            )
                        }),
                    }
                    .await
                    .unwrap_or_else(|e| Err(anyhow::anyhow!("hook task join: {e}")))
                    .map(|_v| ())
                }
                _ => anyhow::bail!(
                    "hook {name}: Declared body but the type carries no script source"
                ),
            },
        }
    }

    pub async fn deliver_timer(realm: SharedRealm, target: InstanceId, tag: String) {
        let job = aura_booth::QueuedJob {
            handler: "__on_timer".into(),
            args: serde_json::json!({ "tag": tag }),
        };
        Self::run_job_queued(realm, &target, job, "__on_timer").await;
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
        let keys: Vec<(String, aura_booth::InstanceKey)> = self
            .instances
            .iter()
            .filter(|(k, inst)| inst.last_activity.elapsed() > ttl_of(&k.0))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            let Some(inst) = self.instances.remove(&key) else {
                continue;
            };
            if let Some(booth) = self.types.get(&key.0) {
                if let Some(hook) = booth.hooks.get("__sleep").cloned() {
                    let body = booth.body.clone();
                    let executors = self.executors.clone();
                    let ctx = Self::ctx_for(
                        self_arc.clone(),
                        self.mq.clone(),
                        self.plan_of(&key.0),
                        self.schema_of(&key.0).cloned().flatten(),
                        &inst.id,
                    );
                    let host = self.delivery_bridge_for(&ctx, &inst.id);
                    if let Err(e) = Self::run_hook(
                        &hook,
                        Some(body),
                        &executors,
                        host,
                        &inst.id,
                        &ctx,
                        "__sleep",
                        booth.concurrency,
                    )
                    .await
                    {
                        // Eviction proceeds regardless: the hook is
                        // advisory; state is already in the store.
                        eprintln!("__sleep failed for {}/{}: {e}", key.0, key.1);
                    }
                }
            }
            // The executor dies WITH the instance: the VM/PTY
            // holds no durable truth (durable state lives in the type's
            // declared collections, ADR-0026 §3), so eviction is a plain
            // drop. The next activation cold-starts a fresh executor and
            // reloads the source.
            self.executors.evict(&format!("{}/{}", key.0, key.1));
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
                let executors;
                {
                    let mut locked = realm.lock().await;
                    locked.sweep_deadlines().await;
                    executors = locked.executors.clone();
                }
                // The exec sweep takes only the registry lock briefly per
                // slot — never held across anything blocking.
                let _dead = executors.sweep_dead();
            }
        });
    }

    pub fn is_resident(&self, id: &InstanceId) -> bool {
        self.instances
            .contains_key(&(id.booth_type.clone(), id.key.clone()))
    }
}

/// Build the executor-facing stream op from a job (ADR-0034, unified
/// seam ADR-0036 — every verb is a stream op now; a plain handler's bare
/// Start reply wraps to `{done:true,value}` at the carrier). Every verb
/// carries handler + args: envelope-mode executors re-invoke the handler
/// each turn (the guard state is theirs, the args are the stream's
/// start args), generator-mode executors use only the stream id.
pub(crate) fn stream_op(job: &Job) -> effector_runtime::carrier::executor::StreamOp {
    use effector_runtime::carrier::executor::StreamOp;
    let stream_id = job.stream.clone();
    match job.kind {
        aura_booth::JobKind::Start => StreamOp::Start {
            stream_id,
            handler: job.handler.clone(),
            args: job.args.clone(),
        },
        aura_booth::JobKind::Next => StreamOp::Next {
            stream_id,
            handler: job.handler.clone(),
            args: job.args.clone(),
        },
        aura_booth::JobKind::Dispose => StreamOp::Dispose {
            stream_id,
            handler: job.handler.clone(),
            args: job.args.clone(),
        },
    }
}
