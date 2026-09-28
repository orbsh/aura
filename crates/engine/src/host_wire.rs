//! Gateway: resolve the probe's host-function calls against the realm.
//! appends to probes.rs — the reader loop gains a Frame::Host(Call) arm.

use aura_booth::InstanceId;
use aura_booth::call::CallSlot;
use aura_realm::SharedRealm;

/// Execute one host op against the realm, scoped to the instance the
/// enclosing remote call was routed to. Returns the JSON result (or error
/// string — the probe surfaces it as a script error).
pub async fn resolve_host_call(
    realm: &SharedRealm,
    instance: &InstanceId,
    op: &probe_protocol::HostOp,
) -> Result<serde_json::Value, String> {
    use probe_protocol::HostOp;
    match op {
        HostOp::Invoke { target_type, target_key, handler, args } => {
            let target = InstanceId {
                booth_type: target_type.clone(),
                key: target_key.clone(),
            };
            // Unified call model: wait hot (script ctx_invoke semantics —
            // the probe blocks until the reply, same as in-process).
            let slot = aura_realm::Realm::call(
                realm,
                Some(&format!("probe/{}", instance.booth_type)),
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
        // probe's host thread on one hot pull, exactly like Invoke.
        op @ (HostOp::Iterate { .. } | HostOp::IterateNext { .. } | HostOp::IterateDispose { .. }) => {
            let iterate_op = match op {
                HostOp::Iterate { target_type, target_key, handler, args } => {
                    aura_booth::IterateOp::Start {
                        target: InstanceId {
                            booth_type: target_type.clone(),
                            key: target_key.clone(),
                        },
                        handler: handler.clone(),
                        args: args.clone(),
                    }
                }
                HostOp::IterateNext { stream_id } => {
                    aura_booth::IterateOp::Next { stream_id: stream_id.clone() }
                }
                HostOp::IterateDispose { stream_id } => {
                    aura_booth::IterateOp::Dispose { stream_id: stream_id.clone() }
                }
                _ => unreachable!(),
            };
            let slot = aura_realm::Realm::iterate(realm, iterate_op)
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
    }
}
