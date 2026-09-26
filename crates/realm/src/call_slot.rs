//! Call-slot plane: static call declarations, in-flight calls with
//! deadlines, the deadline sweep. Split out of lib.rs per ADR-0029.

use super::{Realm, SharedRealm};
use aura_actor::call::{CallId, CallSlot, CallSpec, PendingEntry, Tier, Waited};
use aura_actor::{ActorType, InstanceId, Job};
use std::collections::HashMap;
use std::time::{Duration, Instant};

impl Realm {
    pub fn declare_call(&mut self, actor_type: &str, spec: CallSpec) {
        self.call_specs.insert(actor_type.into(), spec);
    }

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

    pub fn pending_calls_len(&self) -> usize {
        self.pending_calls.len()
    }

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
}
