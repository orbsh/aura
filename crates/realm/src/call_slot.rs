//! Call-slot plane: static call declarations, in-flight calls with
//! deadlines, the deadline sweep. Split out of lib.rs per ADR-0029.

use super::{Realm, SharedRealm};
use aura_booth::call::{CallId, CallSlot, CallSpec, PendingEntry, Tier};
use aura_booth::{InstanceId, IterateOp, Job, JobKind};
use std::time::{Duration, Instant};

impl Realm {
    pub fn declare_call(&mut self, booth_type: &str, spec: CallSpec) {
        self.call_specs.insert(booth_type.into(), spec);
    }

    /// Streaming call (ADR-0034): the iterate verbs ride the same hot
    /// call machinery as `call` — queue job + oneshot + deadline — and
    /// the stream registry is frame identity, not new machinery. Hot
    /// tier only (§5): a pull parks the caller briefly; cold streaming
    /// has no known consumer. Start mints the stream id (the same
    /// counter shape pending_remote uses) and registers the producer
    /// binding; Next/Dispose resolve through it and re-address the
    /// handler with the original start args (envelope-mode handlers are
    /// stateless per call — the guard state is theirs).
    pub async fn iterate(
        self_arc: &SharedRealm,
        op: IterateOp,
    ) -> anyhow::Result<CallSlot> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let (kind, stream, handler, args, target) = {
            let mut realm = self_arc.lock().await;
            let (kind, stream, handler, args, target) = match op {
                IterateOp::Start { target, handler, args } => {
                    if !realm.types.contains_key(&target.booth_type) {
                        anyhow::bail!("unknown booth type: {}", target.booth_type);
                    }
                    realm.call_seq += 1;
                    let sid = format!("stream-{}", realm.call_seq);
                    realm.streams.insert(
                        sid.clone(),
                        crate::StreamEntry {
                            target: target.clone(),
                            handler: handler.clone(),
                            args: args.clone(),
                        },
                    );
                    (JobKind::IterateStart, sid, handler, args, target)
                }
                IterateOp::Next { stream_id } => {
                    // The registry is the routing authority: the id
                    // alone names the producer (ADR-0034 §2 — a frame
                    // identity, no re-presented address).
                    let entry = realm.streams.get(&stream_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "iterate: stream '{stream_id}' not live (ended or released by eviction)"
                        )
                    })?;
                    (
                        JobKind::IterateNext,
                        stream_id,
                        entry.handler.clone(),
                        entry.args.clone(),
                        entry.target.clone(),
                    )
                }
                IterateOp::Dispose { stream_id } => match realm.streams.remove(&stream_id) {
                    // Dispose is idempotent (ADR-0034 §3): an unknown
                    // stream answers success without a job.
                    None => {
                        let spec_timeout = realm
                            .call_specs
                            .values()
                            .next()
                            .and_then(|s| s.timeout)
                            .unwrap_or(Duration::from_secs(30));
                        let _ = reply_tx.send(Ok(serde_json::Value::Null));
                        return Ok(CallSlot::Hot {
                            rx: reply_rx,
                            deadline: Some((tokio::time::Instant::now() + spec_timeout, spec_timeout)),
                        });
                    }
                    Some(entry) => (
                        JobKind::IterateDispose,
                        stream_id,
                        entry.handler.clone(),
                        entry.args.clone(),
                        entry.target,
                    ),
                },
            };
            (kind, stream, handler, args, target)
        };
        let deadline = {
            let realm = self_arc.lock().await;
            realm
                .call_specs
                .get(&target.booth_type)
                .and_then(|s| s.timeout)
                .unwrap_or(Duration::from_secs(30))
        };
        {
            let mut realm = self_arc.lock().await;
            let inst = realm.instance(self_arc.clone(), &target).await?;
            let send = inst.queue.tx.try_send(Job {
                kind,
                stream: Some(stream.clone()),
                handler,
                args,
                reply: reply_tx,
            });
            if send.is_err() && kind == JobKind::IterateStart {
                // Roll the registration back: a Start that never made it
                // into the queue must not leave a live-but-dead stream.
                realm.streams.remove(&stream);
            }
            send.map_err(|_| anyhow::anyhow!("queue full: {}/{}", target.booth_type, target.key))?;
        }
        // Per-job consumer, same shape as `call`'s hot arm: drain OUR
        // job from the instance queue and run it.
        let spawn_realm = self_arc.clone();
        let spawn_target = target.clone();
        tokio::spawn(async move {
            let job = {
                let mut r = spawn_realm.lock().await;
                r.instances
                    .get_mut(&(spawn_target.booth_type.clone(), spawn_target.key.clone()))
                    .and_then(|i| i.queue.rx.try_recv().ok())
            };
            if let Some(job) = job {
                Realm::run_job(spawn_realm, &spawn_target, job).await;
            }
        });
        Ok(CallSlot::Hot {
            rx: reply_rx,
            deadline: Some((tokio::time::Instant::now() + deadline, deadline)),
        })
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
            if !realm.types.contains_key(&target.booth_type) {
                anyhow::bail!("unknown booth type: {}", target.booth_type);
            }
            let spec = realm
                .call_specs
                .get(&target.booth_type)
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
                        .try_send(Job {
                            kind: aura_booth::JobKind::Invoke,
                            stream: None,
                            handler: handler.to_string(),
                            args: args.clone(),
                            reply: reply_tx,
                        })
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "queue full: {}/{}",
                                target.booth_type,
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
                                .get_mut(&(target.booth_type.clone(), target.key.clone()))
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
