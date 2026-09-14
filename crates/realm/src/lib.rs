//! Realm: the event/call fabric. Phase 0 scope — instance registry,
//! partition-key routing, and the call path (invoke → mailbox → oneshot
//! reply). Event namespace and emit/on arrive in Phase 3.

use aura_actor::{futures_boxed::BoxFuture, ActorType, InstanceId, Job, Mailbox};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Realm {
    /// Registered actor types by name.
    types: HashMap<String, ActorType>,
    /// Live instance mailboxes by (type, key).
    mailboxes: HashMap<(String, String), Mailbox>,
    /// Mailbox capacity per instance.
    mailbox_capacity: usize,
}

impl Realm {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
            mailboxes: HashMap::new(),
            mailbox_capacity: 64,
        }
    }

    pub fn register_type(&mut self, actor: ActorType) {
        self.types.insert(actor.name.clone(), actor);
    }

    pub fn actor_type(&self, name: &str) -> Option<&ActorType> {
        self.types.get(name)
    }

    /// Resolve (type, key) to a mailbox, activating the instance on first
    /// touch (virtual actor; scale-to-zero mechanics arrive in Phase 1).
    pub fn mailbox(&mut self, id: &InstanceId) -> anyhow::Result<mpsc::Sender<Job>> {
        let entry = self
            .mailboxes
            .entry((id.actor_type.clone(), id.key.clone()))
            .or_insert_with(|| Mailbox::new(id.clone(), self.mailbox_capacity));
        Ok(entry.tx.clone())
    }

    /// The dispatch function handed to every ctx: target resolution is
    /// registry-declared (ADR-0011) — unknown type = error value.
    pub fn dispatch_handle(
        realm: Arc<tokio::sync::Mutex<Realm>>,
    ) -> aura_actor::dispatch_handle::DispatchHandle {
        Arc::new(move |target, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            let realm = realm.clone();
            Box::pin(async move {
                let (tx, handler) = {
                    let mut r = realm.lock().await;
                    let actor = r
                        .actor_type(&target.actor_type)
                        .ok_or_else(|| {
                            anyhow::anyhow!("unknown actor type: {}", target.actor_type)
                        })?
                        .clone();
                    let tx = r.mailbox(&target)?;
                    (tx, actor.handler.clone())
                };
                // Phase 0: the dispatcher drives the handler directly. The
                // mailbox send is kept for routing form (virtual-actor
                // activation, backpressure); a dedicated runtime loop
                // draining mailboxes arrives with Phase 1 (scale-to-zero).
                // The job's reply channel stays unused until then (the
                // receiver end is dropped here — a not-yet-consumed reply).
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                drop(reply_rx);
                tx.send(Job { args: args.clone(), reply: reply_tx })
                    .await
                    .map_err(|_| anyhow::anyhow!("mailbox closed: {}/{}", target.actor_type, target.key))?;

                let ctx = aura_actor::Ctx::new(target.clone(), {
                    let realm2 = realm.clone();
                    Arc::new(move |t, a| {
                        let realm3 = realm2.clone();
                        Box::pin(async move { Self::dispatch_call(realm3, t, a).await })
                    })
                });
                handler(ctx, args).await
            })
        })
    }

    async fn dispatch_call(
        realm: Arc<tokio::sync::Mutex<Realm>>,
        target: InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        Self::dispatch_handle(realm)(target, args).await
    }
}

impl Default for Realm {
    fn default() -> Self {
        Self::new()
    }
}
