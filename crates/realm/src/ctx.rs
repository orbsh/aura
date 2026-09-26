//! Ctx bridge: per-call ctx assembly and the host-bridge fn table for
//! resident sessions. Split out of lib.rs per ADR-0029.

use super::{dispatch_call, Realm, SharedRealm};
use crate::{mq, store_exec, value};
use aura_actor::{Ctx, InstanceId};
use std::sync::Arc;

impl Realm {
    pub(crate) fn ctx_for(
        self_arc: SharedRealm,
        store_engine: mq::MqStore,
        plan: Option<&store_exec::StorePlan>,
        schema: Option<serde_json::Value>,
        id: &InstanceId,
    ) -> aura_actor::Ctx {
        // The store is already a shared Arc: handlers get a direct handle.
        // (Phase 1 single-node: the store is lock-free per operation. The
        // realm lock only guards registry/instances, never state.)
        // Weak on purpose: the dispatch closure is cloned into resident
        // sessions (steel's register_fn requires 'static) and those live
        // inside the realm's own Sessions map — a strong capture here is a
        // reference cycle (realm → sessions → session → closure → realm)
        // that keeps the fjall Database open forever after engine drop.
        let dispatch_realm = Arc::downgrade(&self_arc);
        let mut ctx = aura_actor::Ctx::new(
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
        );
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
            ctx = ctx.with_store_emit(Arc::new(move |op: aura_actor::StoreOp| {
                store_exec::execute(&store, &plan, &op)
            }));
        }
        if let Some(schema) = schema {
            ctx = ctx.with_interface_schema(schema);
        }
        ctx
    }

    pub(crate) fn host_bridge_for(
        ctx: &aura_actor::Ctx,
        // The type's ns + a raw ns-bound engine handle (ADR-0026 §4
        // wasm storage): present when the type resolved a store plan.
        // The wasm full-power path needs RAW engine calls, not the
        // Collection-op layer (the module runs the real Collection in
        // itself; the host is its engine).
        wasm_raw: Option<(u16, crate::mq::MqStore)>,
    ) -> std::collections::BTreeMap<String, probe_runtime::carrier::HostFn> {
        use probe_runtime::carrier::HostFn;

        let dispatch = ctx.invoke_handle();
        let handle = tokio::runtime::Handle::current();

        let mut fns: std::collections::BTreeMap<String, HostFn> = Default::default();
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
        // Type-scoped storage (ADR-0026 §3): ONE entry, okm Collection
        // instructions as data. The handle is bound to the owning type's
        // ns at ctx construction — the fn itself carries no addressing.
        if let Some(surface) = ctx.store_emit_handle() {
            let emit_fn = surface.clone();
            fns.insert(
                "ctx_store_emit".into(),
                Arc::new(move |arg: serde_json::Value| {
                    let op: aura_actor::StoreOp = serde_json::from_value(arg)
                        .map_err(|e| anyhow::anyhow!("ctx_store_emit: bad op: {e}"))?;
                    (emit_fn)(op).map_err(|e| anyhow::anyhow!(e))
                }) as HostFn,
            );
        }
        if let Some(schema) = ctx.interface_schema() {
            let schema = schema.clone();
            fns.insert(
                "ctx_interface_schema".into(),
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
            let handle = crate::mq::MqStore::ns_raw(&store, ns as u16);
            fns.insert(
                "emit".into(),
                Arc::new(move |arg: serde_json::Value| {
                    use okm_wire::{OpFrame, OpResponse};
                    let bytes: Vec<u8> = arg
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("emit: expected byte array"))?
                        .iter()
                        .map(|v| v.as_u64().map(|x| x as u8).ok_or_else(|| anyhow::anyhow!("emit: bad byte")))
                        .collect::<Result<Vec<u8>, _>>()?;
                    let frame = OpFrame::decode(&bytes)
                        .ok_or_else(|| anyhow::anyhow!("emit: malformed op frame"))?;
                    let mut out = OpResponse::default();
                    let mut s = handle.clone();
                    for (tag, key, value) in &frame.0 {
                        use okm_core::storage::VirtualStorage;
                        match *tag {
                            okm_wire::OP_PUT => s.put(key.clone(), value.clone()),
                            okm_wire::OP_DELETE => s.del(key),
                            okm_wire::OP_GET => out.value = s.get(key),
                            okm_wire::OP_SCAN => out.suffixes = s.scan_range(key, None),
                            other => anyhow::bail!("emit: unsupported op tag {other}"),
                        }
                    }
                    Ok(serde_json::Value::Array(
                        out.encode().into_iter().map(serde_json::Value::from).collect(),
                    ))
                }) as HostFn,
            );
        }
        fns
    }
}
