//! Engine assembly: config + realm + runtime. Phase 1 — store-backed ctx
//! state, idle-TTL eviction (scale-to-zero), submit-based call path.

use aura_actor::ActorType;
use aura_realm::{Realm, SharedRealm};
use std::sync::Arc;
use std::time::Duration;

pub struct Engine {
    pub realm: SharedRealm,
}

impl Engine {
    /// Start the engine: realm over the in-memory store (Fjall arrives with
    /// the Phase 4 engine matrix) + the idle evictor task.
    pub fn start(_config: &aura_config::EngineConfig) -> Self {
        let realm: SharedRealm = Arc::new(tokio::sync::Mutex::new(Realm::new(
            Arc::new(aura_storage::InMemoryStore::default()),
        )));
        Realm::spawn_evictor(realm.clone());
        Self { realm }
    }

    /// Override the idle TTL (default 30s).
    pub fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        if let Ok(mut r) = self.realm.try_lock() {
            r.idle_ttl = ttl;
        }
        self
    }

    /// Register an actor type with this engine's realm.
    pub async fn register(&self, actor: ActorType) {
        self.realm.lock().await.register_type(actor);
    }

    /// Invoke a registered actor instance. The call path every surface
    /// (CLI, HTTP, remote Probe) converges on.
    pub async fn invoke(
        &self,
        target: aura_actor::InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        Realm::submit(&self.realm, target, args).await?
            .await
            .map_err(|_| anyhow::anyhow!("call dropped"))?
    }
}
