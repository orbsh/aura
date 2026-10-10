//! Gateway: resolve the effector's host-function calls against the realm.
//! appends to effectors.rs — the reader loop gains a Frame::Host(Call) arm.

use aura_booth::InstanceId;
use aura_booth::call::CallSlot;
use aura_realm::SharedRealm;

/// Execute one host op against the realm, scoped to the instance the
/// enclosing remote call was routed to. Returns the JSON result (or error
/// string — the effector surfaces it as a script error).
pub async fn resolve_host_call(
    realm: &SharedRealm,
    instance: &InstanceId,
    op: &effector_protocol::HostOp,
) -> Result<serde_json::Value, String> {
    use effector_protocol::HostOp;
    match op {
        HostOp::Invoke { target_type, target_key, handler, args } => {
            let target = InstanceId {
                booth_type: target_type.clone(),
                // Wire string → variant (ADR-0042): "" is the singleton.
                key: aura_booth::InstanceKey::parse(target_key),
            };
            // Unified call model: wait hot (script ctx.invoke semantics —
            // the effector blocks until the reply, same as in-process).
            let slot = aura_realm::Realm::call(
                realm,
                Some(&format!("effector/{}", instance.booth_type)),
                target,
                handler,
                args.clone(),
            )
            .await
            .map_err(|e| e.to_string())?;
            match slot {
                CallSlot::Hot { rx, .. } => match rx.await {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(_) => Err("invoke reply dropped".to_string()),
                },
                CallSlot::Cold { .. } => {
                    Err("cold-call (pending) resolution over the wire is not supported yet".into())
                }
            }
        }
        // ADR-0034 consumer legs over the wire: a remote script booth
        // drives a stream through the realm — start routes by the target
        // in the op; Next/Dispose route by the stream id alone (the
        // realm's registry names the producer). Each call parks the
        // effector's host thread on one hot pull, exactly like Invoke.
        HostOp::Iterate { target_type, target_key, handler, args } => {
            let iterate_op = aura_booth::IterateOp::Start {
                target: InstanceId {
                    booth_type: target_type.clone(),
                    key: aura_booth::InstanceKey::parse(target_key),
                },
                handler: handler.clone(),
                args: args.clone(),
            };
            wait_iterate(realm, iterate_op).await
        }
        HostOp::IterateNext { stream_id } => {
            wait_iterate(realm, aura_booth::IterateOp::Next { stream_id: stream_id.clone() }).await
        }
        HostOp::IterateDispose { stream_id } => {
            wait_iterate(realm, aura_booth::IterateOp::Dispose { stream_id: stream_id.clone() }).await
        }
        // Phase 4.14 gate 1 (ADR-0026 §3 over the wire): one okm
        // Collection instruction travels as DATA — the effector side never
        // parses it, the schema lives here with the type registration.
        // The plan + store handle are resolved under the realm lock and
        // CLONED out (the ctx_for discipline: execution never
        // dereferences the realm); no plan = the type declared no
        // storage, a named error value (ADR-0026 Consequences: remote
        // ctx.store requires a resolved plan — the 4.5b upload
        // lifecycle's, not this frame path's).
        HostOp::StoreEmit { instruction } => {
            let store_op: aura_booth::StoreOp = serde_json::from_value(instruction.clone())
                .map_err(|e| format!("ctx.store: malformed op: {e}"))?;
            let (plan, store) = {
                let r = realm.lock().await;
                (r.plan_of(&instance.booth_type).cloned(), r.mq.clone())
            };
            match plan {
                Some(plan) => aura_realm::store_exec::execute(&store, &plan, &store_op),
                None => Err(format!(
                    "ctx.store: type '{}' has no storage plan (no declared \
                     collections) — ctx.store is unavailable",
                    instance.booth_type
                )),
            }
        }
    }
}

/// One hot iterate pull, parked-to-reply — the shape all three stream
/// verbs share over the wire.
async fn wait_iterate(
    realm: &SharedRealm,
    op: aura_booth::IterateOp,
) -> Result<serde_json::Value, String> {
    let slot = aura_realm::Realm::iterate(realm, op)
        .await
        .map_err(|e| e.to_string())?;
    match slot {
        CallSlot::Hot { rx, .. } => match rx.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("iterate reply dropped".to_string()),
        },
        CallSlot::Cold { .. } => {
            Err("iterate is hot-tier only (ADR-0034 §5)".into())
        }
    }
}
