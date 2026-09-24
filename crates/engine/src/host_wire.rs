//! Gateway: resolve the probe's host-function calls against the realm.
//! appends to probes.rs — the reader loop gains a Frame::Host(Call) arm.

use aura_actor::InstanceId;
use aura_actor::call::CallSlot;
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
                actor_type: target_type.clone(),
                key: target_key.clone(),
            };
            // Unified call model: wait hot (script ctx_invoke semantics —
            // the probe blocks until the reply, same as in-process).
            let slot = aura_realm::Realm::call(
                realm,
                Some(&format!("probe/{}", instance.actor_type)),
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
    }
}
