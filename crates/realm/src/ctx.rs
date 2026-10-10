//! Ctx bridge: per-call ctx assembly and the host-bridge fn table for
//! resident sessions. Split out of lib.rs per ADR-0029.

use super::{dispatch_call, Realm, SharedRealm};
use crate::{mq, store_exec};
use aura_booth::InstanceId;
use std::sync::Arc;

/// Iterate dispatch for the ctx handle (ADR-0034): ride the realm's hot
/// iterate path, then wait the reply — same single-value wait shape as
/// `dispatch_call` for invoke. `IterateStart` answers with the first
/// envelope (stream_id merged by run_job), so the cursor reads both
/// from one reply.
async fn aura_realm_call(
    realm: &SharedRealm,
    op: aura_booth::IterateOp,
) -> anyhow::Result<serde_json::Value> {
    use aura_booth::call::Waited;
    match Realm::iterate(realm, op).await?.wait().await? {
        Waited::Done(result) => result,
        Waited::Pending(_) => {
            Err(anyhow::anyhow!("iterate is hot-tier only (ADR-0034 §5) — no pending slot exists"))
        }
    }
}

impl Realm {
    pub(crate) fn ctx_for(
        self_arc: SharedRealm,
        store_engine: mq::MqStore,
        plan: Option<&store_exec::StorePlan>,
        schema: Option<serde_json::Value>,
        id: &InstanceId,
    ) -> aura_booth::Ctx {
        // The store is already a shared Arc: handlers get a direct handle.
        // (Phase 1 single-node: the store is lock-free per operation. The
        // realm lock only guards registry/instances, never state.)
        // Weak on purpose: the dispatch closure is cloned into resident
        // sessions (steel's register_fn requires 'static) and those live
        // inside the realm's own Sessions map — a strong capture here is a
        // reference cycle (realm → sessions → session → closure → realm)
        // that keeps the fjall Database open forever after engine drop.
        let dispatch_realm = Arc::downgrade(&self_arc);
        let iterate_realm = Arc::downgrade(&self_arc);
        // ctx.emit (ADR-0043 §2): publish rides the SAME Weak-realm
        // discipline as dispatch/iterate; the emitter is captured as the
        // TYPE name (audit; publishing is instance-free — 0011's own
        // observation kept in force under the new placement).
        let emit_realm = Arc::downgrade(&self_arc);
        let emitter = id.booth_type.clone();
        let mut ctx = aura_booth::Ctx::new(
            id.clone(),
            Arc::new(move |target, handler: &str, args| {
                let realm = dispatch_realm.clone();
                let handler = handler.to_string();
                Box::pin(async move {
                    let realm = std::sync::Weak::upgrade(&realm).ok_or_else(|| {
                        anyhow::anyhow!("realm dropped: dispatch after engine shutdown")
                    })?;
                    dispatch_call(realm, target, &handler, args).await
                })
            }),
            // ADR-0034: the iterate dispatch handle rides the same Weak
            // realm discipline — the cursor lives inside the consumer's
            // resident session and may outlive single jobs.
            Arc::new(move |op| {
                let realm = iterate_realm.clone();
                Box::pin(async move {
                    let realm = std::sync::Weak::upgrade(&realm).ok_or_else(|| {
                        anyhow::anyhow!("realm dropped: iterate after engine shutdown")
                    })?;
                    aura_realm_call(&realm, op).await
                })
            }),
        );
        // ctx.emit (ADR-0043 §2): publish into the MQ plane, same Weak
        // discipline; emitter = this ctx's type name (audit rides the
        // Realm::emit signature).
        ctx = ctx.with_emit(Arc::new(move |event: String, data| {
            let realm = emit_realm.clone();
            let emitter = emitter.clone();
            Box::pin(async move {
                let realm = std::sync::Weak::upgrade(&realm).ok_or_else(|| {
                    anyhow::anyhow!("realm dropped: emit after engine shutdown")
                })?;
                Realm::emit(&realm, Some(&emitter), &event, data).await
            })
        }));
        // Type-scoped storage executor (ADR-0026 §3): the handle resolves
        // the type's plan through the realm (Weak, same retain-cycle
        // discipline) and executes the op against the type's own ns. The
        // schema carries the key layout — there is no caller-supplied
        // addressing to bind at this point.
        // Plan + schema are resolved by the CALLER (it already holds the
        // realm lock — ctx_for is called under it everywhere) and captured
        // as DATA: the emit handle clones the plan and the mq handle, so
        // the executor never dereferences the realm at emit time (no
        // blocking_lock in spawn_blocking, no retain cycle).
        if let Some(plan) = plan {
            let store = store_engine.clone();
            let plan = plan.clone();
            ctx = ctx.with_store_emit(Arc::new(move |op: aura_booth::StoreOp| {
                store_exec::execute(&store, &plan, &op)
            }));
        }
        if let Some(schema) = schema {
            ctx = ctx.with_interface_schema(schema);
        }
        ctx
    }

    pub(crate) fn host_bridge_for(
        ctx: &aura_booth::Ctx,
        // The type's ns + a raw ns-bound engine handle (ADR-0026 §4
        // wasm storage): present when the type resolved a store plan.
        // The wasm full-power path needs RAW engine calls, not the
        // Collection-op layer (the module runs the real Collection in
        // itself; the host is its engine).
        wasm_raw: Option<(u16, crate::mq::MqStore)>,
        // The mq store for the queue relief-valve fns (Phase 4.5c):
        // concrete queue identity resolves through the PERSISTED route
        // registry (routes_of_booth — the same source the watermark
        // denominator uses), so the closure needs no realm deref and no
        // subscription list — only the store handle + this instance's id.
        store: &crate::mq::MqStore,
        // The realm's timer handle (ADR-0016 §3b): ctx.timer.* arm
        // Deliver entries directly — the handle is a cheap command-
        // channel clone, no realm deref at call time.
        timers: &crate::timer::TimerHandle,
    ) -> std::collections::BTreeMap<String, effector_runtime::carrier::HostFn> {
        use effector_runtime::carrier::HostFn;

        let dispatch = ctx.invoke_handle();
        let handle = tokio::runtime::Handle::current();

        let mut fns: std::collections::BTreeMap<String, HostFn> = Default::default();
        let invoke_handle = handle.clone();
        fns.insert(
            "invoke".into(),
            Arc::new(move |arg: serde_json::Value| {
                let handle = &invoke_handle;
                // arg: { "type": ..., "key": ..., "args": ... }. Blocks the
                // script thread on the unified call model (Phase 3.5) — the
                // script itself runs in spawn_blocking, so this is bounded
                // by the call's own tier/timeout semantics.
                let obj = arg.as_object().ok_or_else(|| anyhow::anyhow!("ctx.invoke expects an object"))?;
                let ty = obj.get("type").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx.invoke: missing `type`"))?;
                let key = obj.get("key").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx.invoke: missing `key`"))?;
                let handler = obj.get("handler").and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("ctx.invoke: missing `handler` (the function to call)"))?;
                let args = obj.get("args").cloned().unwrap_or(serde_json::Value::Null);
                let target = InstanceId { booth_type: ty.to_string(), key: aura_booth::InstanceKey::parse(key) };
                handle.block_on(dispatch(target, handler, args))
            }) as HostFn,
        );
        // ctx.emit (ADR-0043 §2 — the 0011 half-bridge, landed): the
        // script host fn rides the Ctx's own emit handle (Weak realm +
        // type-name emitter captured at ctx_for — publishing is
        // instance-free). Fire-and-forget beyond route resolution: the
        // reply is always Null on success; a bad shape or a dropped
        // realm is an error value, never a silent no-op.
        if let Some(emit) = ctx.emit_handle() {
            let h = handle.clone();
            fns.insert(
                "emit".into(),
                Arc::new(move |arg: serde_json::Value| {
                    let obj = arg
                        .as_object()
                        .ok_or_else(|| anyhow::anyhow!("ctx.emit expects an object"))?;
                    let event = obj
                        .get("event")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("ctx.emit: missing `event`"))?
                        .to_string();
                    let data = obj.get("data").cloned().unwrap_or(serde_json::Value::Null);
                    let emit = emit.clone();
                    h.block_on(emit(event, data))?;
                    Ok(serde_json::Value::Null)
                }) as HostFn,
            );
        }
        // ADR-0034: the iterate host fns. The cursor loop lives on the
        // script side (the python wrapper is a native generator over
        // these three); each host call is one round trip through the
        // same dispatch machinery invoke rides. Start returns the first
        // envelope with the realm-minted stream_id merged in.
        if let Some(iterate) = ctx.iterate_handle() {
            let h = handle.clone();
            for name in ["iterate.start", "iterate.next", "iterate.dispose"] {
                let iterate = iterate.clone();
                let handle = h.clone();
                let op_kind = match name {
                    _ if name.ends_with("start") => 0u8,
                    _ if name.ends_with("next") => 1u8,
                    _ => 2u8,
                };
                fns.insert(
                    name.into(),
                    Arc::new(move |arg: serde_json::Value| {
                        use aura_booth::IterateOp;
                        let obj = arg
                            .as_object()
                            .ok_or_else(|| anyhow::anyhow!("ctx.{name}: expects an object"))?;
                        let gs = |k: &str| obj.get(k).and_then(|v| v.as_str()).unwrap_or("");
                        let op = match op_kind {
                            0 => IterateOp::Start {
                                target: InstanceId {
                                    booth_type: gs("type").to_string(),
                                    key: aura_booth::InstanceKey::parse(gs("key")),
                                },
                                handler: gs("handler").to_string(),
                                args: obj
                                    .get("args")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null),
                            },
                            // Next/Dispose route by stream id alone —
                            // the realm's registry names the producer.
                            1 => IterateOp::Next { stream_id: gs("stream_id").to_string() },
                            _ => IterateOp::Dispose { stream_id: gs("stream_id").to_string() },
                        };
                        handle.block_on(iterate(op))
                    }) as HostFn,
                );
            }
        }
        // Type-scoped storage (ADR-0026 §3): ONE entry, okm Collection
        // instructions as data. The handle is bound to the owning type's
        // ns at ctx construction — the fn itself carries no addressing.
        if let Some(surface) = ctx.store_emit_handle() {
            let emit_fn = surface.clone();
            fns.insert(
                "store".into(),
                Arc::new(move |arg: serde_json::Value| {
                    let op: aura_booth::StoreOp = serde_json::from_value(arg)
                        .map_err(|e| anyhow::anyhow!("ctx.store: bad op: {e}"))?;
                    (emit_fn)(op).map_err(|e| anyhow::anyhow!(e))
                }) as HostFn,
            );
        }
        if let Some(schema) = ctx.interface_schema() {
            let schema = schema.clone();
            fns.insert(
                "schema".into(),
                Arc::new(move |_arg: serde_json::Value| Ok(schema.clone())) as HostFn,
            );
        }
        if let Some((ns, store)) = wasm_raw {
            // Wasm full-power path (ADR-0026 §4): the guest's in-module
            // Collection emits okm-wire OpFrames; the host answers with
            // OpResponses executed against the type's RAW ns engine
            // plane (no Collection-op layer — the trusted static-mode
            // writer IS the module; the no-bypass-guard ruling covers
            // it). The JSON HostFn seam carries the bytes as number
            // arrays (lossless; storage is not a hot path here).
            let handle = crate::mq::MqStore::ns_raw(&store, ns);
            fns.insert(
                "store.frame".into(),
                Arc::new(move |arg: serde_json::Value| {
                    use okm_wire::{OpFrame, OpResponse};
                    let bytes: Vec<u8> = arg
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("ctx.store.frame: expected byte array"))?
                        .iter()
                        .map(|v| v.as_u64().map(|x| x as u8).ok_or_else(|| anyhow::anyhow!("ctx.store.frame: bad byte")))
                        .collect::<Result<Vec<u8>, _>>()?;
                    let frame = OpFrame::decode(&bytes)
                        .ok_or_else(|| anyhow::anyhow!("ctx.store.frame: malformed op frame"))?;
                    let mut out = OpResponse::default();
                    let s = handle.clone();
                    for (tag, key, value) in &frame.0 {
                        use okm_core::storage::VirtualStorage;
                        match *tag {
                            okm_wire::OP_PUT => s.put(key.clone(), value.clone()),
                            okm_wire::OP_DELETE => s.del(key),
                            okm_wire::OP_GET => out.value = s.get(key),
                            okm_wire::OP_SCAN => out.suffixes = s.scan_range(key, None),
                            other => anyhow::bail!("ctx.store.frame: unsupported op tag {other}"),
                        }
                    }
                    Ok(serde_json::Value::Array(
                        out.encode().into_iter().map(serde_json::Value::from).collect(),
                    ))
                }) as HostFn,
            );
        }
        // Queue relief valve (Phase 4.5c, realm.md retention ruling):
        // `ctx.queue.depth(event)` reads the live backlog count (a point
        // read of the Count reduce — the zero-scan operational surface);
        // `ctx.queue.skip_to_head(event)` jumps THIS instance's cursor to
        // the partition head, discarding the stale backlog. Both resolve
        // the instance's bound queue through the persisted route registry
        // (an unbound event = an error value, never a silent no-op).
        // ADR-0016 §3b, landed: the imperative ctx.timer face. `register`
        // arms a Deliver entry (fire = an `__on_timer` job carrying the
        // tag); `cancel` is by id, idempotent (unknown = already fired/
        // cancelled). Durability is NOT in this surface: the StateStore
        // the ADR's durable-restore clause named was retired by ADR-0026
        // §3 — timers live in the wheel for the process lifetime
        // (recorded residual; a durable path re-enters with a durable
        // registration table, not by reviving flat instance state).
        {
            let timers_register = timers.clone();
            let target = ctx.self_id.clone();
            fns.insert(
                "timer.register".into(),
                Arc::new(move |arg: serde_json::Value| {
                    let obj = arg
                        .as_object()
                        .ok_or_else(|| anyhow::anyhow!("ctx.timer.register expects an object"))?;
                    let at_ms = obj.get("at_ms").and_then(|v| v.as_u64())
                        .ok_or_else(|| anyhow::anyhow!("ctx.timer.register: missing `at_ms` (delay from now, milliseconds)"))?;
                    let tag = obj.get("tag").and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("ctx.timer.register: missing `tag` (delivered to the booth's __on_timer handler)"))?
                        .to_string();
                    let id = timers_register.register_deliver(
                        target.clone(),
                        tag,
                        std::time::Duration::from_millis(at_ms),
                    );
                    Ok(serde_json::json!({ "timer_id": id.0 }))
                }) as HostFn,
            );
            let timers_cancel = timers.clone();
            fns.insert(
                "timer.cancel".into(),
                Arc::new(move |arg: serde_json::Value| {
                    let id = arg
                        .get("timer_id")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| anyhow::anyhow!("ctx.timer.cancel: missing `timer_id`"))?;
                    timers_cancel.cancel(crate::timer::TimerId(id));
                    Ok(serde_json::Value::Null)
                }) as HostFn,
            );
        }
        {
            let store = store.clone();
            let booth_type = ctx.self_id.booth_type.clone();
            let booth_key = ctx.self_id.key.clone();
            for name in ["queue.depth", "queue.skip_to_head"] {
                let store = store.clone();
                let booth_type = booth_type.clone();
                let booth_key = booth_key.clone();
                let skip = name == "queue.skip_to_head";
                fns.insert(
                    name.into(),
                    Arc::new(move |arg: serde_json::Value| {
                        let event = arg.as_str().ok_or_else(|| {
                            anyhow::anyhow!("ctx.{name}: expects the event name (a string)")
                        })?;
                        // ADR-0038 §1: a key-less route binds the singleton
                        // INSTANCE; any other instance of the type has no
                        // queue for it, so its valve answers "no route of
                        // '<type>' binds '<event>'" — the subscription truth,
                        // not a silent no-op.
                        let part = crate::mq::bound_instance_key(&store, &booth_type, &booth_key, event)?
                            .ok_or_else(|| anyhow::anyhow!("ctx.{name}: no route of '{booth_type}' binds '{event}'"))?;
                        if skip {
                            crate::mq::skip_to_head(&store, event, &part, &booth_type)?;
                            Ok(serde_json::Value::Null)
                        } else {
                            Ok(serde_json::Value::from(crate::mq::depth(&store, event, &part)?))
                        }
                    }) as HostFn,
                );
            }
        }
        fns
    }
}