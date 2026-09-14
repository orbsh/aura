//! Realm: the event/call fabric. Phase 1 — virtual-actor registry, runtime
//! loop driving mailboxes, idle-TTL eviction (scale-to-zero: on_sleep →
//! drop, on_wake on reactivation). Event namespace and emit/on arrive in
//! Phase 3; the unified CallSlot model in Phase 3.5.

use aura_actor::{ActorType, Instance, InstanceId, Job, SharedStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Shared realm handle: the dispatcher closes over this.
pub type SharedRealm = Arc<tokio::sync::Mutex<Realm>>;

pub struct Realm {
    /// Registered actor types by name.
    types: HashMap<String, ActorType>,
    /// Live instances by (type, key).
    instances: HashMap<(String, String), Instance>,
    /// Mailbox capacity per instance.
    mailbox_capacity: usize,
    /// Instance state store (in-memory now; Fjall in Phase 4).
    pub store: SharedStore,
    /// Idle TTL: an instance with no job for this long is evicted
    /// (scale-to-zero). State survives via the store; hooks run around it.
    pub idle_ttl: Duration,
}

impl Realm {
    pub fn new(store: SharedStore) -> Self {
        Self {
            types: HashMap::new(),
            instances: HashMap::new(),
            mailbox_capacity: 64,
            store,
            idle_ttl: Duration::from_secs(30),
        }
    }

    pub fn register_type(&mut self, actor: ActorType) {
        self.types.insert(actor.name.clone(), actor);
    }

    pub fn actor_type(&self, name: &str) -> Option<&ActorType> {
        self.types.get(name)
    }

    /// Build a ctx for an instance: state backed by the shared store,
    /// invoke routed through the realm's dispatch.
    fn ctx_for(self_arc: SharedRealm, store: aura_actor::SharedStore, id: &InstanceId) -> aura_actor::Ctx {
        // The store is already a shared Arc: handlers get a direct handle.
        // (Phase 1 single-node: the store is lock-free per operation. The
        // realm lock only guards registry/instances, never state.)
        let dispatch_realm = self_arc.clone();
        aura_actor::Ctx::new(
            id.clone(),
            store,
            Arc::new(move |target, args| {
                let realm = dispatch_realm.clone();
                Box::pin(async move { dispatch_call(realm, target, args).await })
            }),
        )
    }

    /// Resolve (type, key) to a live instance, activating on first touch
    /// (virtual actor). Reactivation after eviction runs on_wake.
    async fn instance(&mut self, self_arc: SharedRealm, id: &InstanceId) -> anyhow::Result<&mut Instance> {
        let key = (id.actor_type.clone(), id.key.clone());
        if !self.instances.contains_key(&key) {
            let mut inst = Instance::new(id.clone(), self.mailbox_capacity);
            // on_wake: fresh residency. Runs on first activation too —
            // symmetric with on_sleep; a first-time wake is still a wake.
            if let Some(actor) = self.types.get(&id.actor_type) {
                if let Some(on_wake) = actor.on_wake.clone() {
                    let ctx = Self::ctx_for(self_arc.clone(), self.store.clone(), id);
                    on_wake(ctx, serde_json::Value::Null).await?;
                }
            }
            self.instances.insert(key.clone(), inst);
        } else if let Some(inst) = self.instances.get_mut(&key) {
            inst.last_activity = Instant::now();
        }
        Ok(self.instances.get_mut(&key).expect("just inserted"))
    }

    /// Drive one job through its instance's handler. Called by the runtime
    /// loop; serial per instance (single consumer per mailbox).
    async fn run_job(self_arc: SharedRealm, id: &InstanceId, job: Job) {
        let mut realm = self_arc.lock().await;
        realm.instances.get_mut(&(id.actor_type.clone(), id.key.clone()))
            .map(|i| i.last_activity = Instant::now());
        let Some(actor) = realm.types.get(&id.actor_type) else {
            let _ = job
                .reply
                .send(Err(anyhow::anyhow!("unknown actor type: {}", id.actor_type)));
            return;
        };
        let handler = actor.handler.clone();
        let ctx = Self::ctx_for(self_arc.clone(), realm.store.clone(), id);
        drop(realm);
        let result = handler(ctx, job.args).await;
        let _ = job.reply.send(result);
    }

    /// Submit a job to an instance: activation + mailbox send. The sender
    /// awaits the reply oneshot (CallSlot's hot path; Phase 3.5 formalizes).
    pub async fn submit(
        self_arc: &SharedRealm,
        target: InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<serde_json::Value>>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        {
            let mut realm = self_arc.lock().await;
            if !realm.types.contains_key(&target.actor_type) {
                anyhow::bail!("unknown actor type: {}", target.actor_type);
            }
            let inst = realm.instance(self_arc.clone(), &target).await?;
            inst.mailbox
                .tx
                .try_send(Job { args, reply: reply_tx })
                .map_err(|_| anyhow::anyhow!("mailbox full: {}/{}", target.actor_type, target.key))?;
        }
        // Runtime loop drains the mailbox; spawn a consumer for this job
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
                        .get_mut(&(target2.actor_type.clone(), target2.key.clone()));
                    match inst {
                        Some(i) => i.mailbox.rx.recv().await,
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

    /// Evict instances idle longer than `idle_ttl`: run on_sleep, drop the
    /// instance. State survives in the store — scale-to-zero drops the
    /// resident, not the data.
    pub async fn evict_idle(&mut self, self_arc: SharedRealm) -> Vec<InstanceId> {
        let ttl = self.idle_ttl;
        let mut evicted = Vec::new();
        let keys: Vec<(String, String)> = self
            .instances
            .iter()
            .filter(|(_, inst)| inst.last_activity.elapsed() > ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            let Some(inst) = self.instances.remove(&key) else { continue };
            if let Some(actor) = self.types.get(&key.0) {
                if let Some(on_sleep) = actor.on_sleep.clone() {
                    let ctx = Self::ctx_for(self_arc.clone(), self.store.clone(), &inst.id);
                    if let Err(e) = on_sleep(ctx).await {
                        // Eviction proceeds regardless: the hook is
                        // advisory; state is already in the store.
                        eprintln!("on_sleep failed for {}/{}: {e}", key.0, key.1);
                    }
                }
            }
            evicted.push(inst.id);
        }
        evicted
    }

    /// Periodic eviction tick, spawned once per engine.
    pub fn spawn_evictor(realm: SharedRealm) {
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let mut locked = realm.lock().await;
                locked.evict_idle(realm.clone()).await;
            }
        });
    }
}

async fn dispatch_call(
    realm: SharedRealm,
    target: InstanceId,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    Realm::submit(&realm, target, args).await?.await.map_err(|_| anyhow::anyhow!("call dropped"))?
}

impl Default for Realm {
    fn default() -> Self {
        Self::new(Arc::new(aura_storage::InMemoryStore::default()))
    }
}
