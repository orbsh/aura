//! Engine assembly: config + realm + actor runtime loop. Phase 0 — single
//! binary start, echo-path validation.

use aura_actor::ActorType;
use aura_realm::Realm;
use std::sync::Arc;

pub struct Engine {
    pub realm: Arc<tokio::sync::Mutex<Realm>>,
}

impl Engine {
    pub fn start(_config: &aura_config::EngineConfig) -> Self {
        Self { realm: Arc::new(tokio::sync::Mutex::new(Realm::new())) }
    }

    /// Register an actor type with this engine's realm.
    pub async fn register(&self, actor: ActorType) {
        self.realm.lock().await.register_type(actor);
    }

    /// Invoke a registered actor instance. This is the call path every
    /// surface (CLI, HTTP, remote Probe) converges on.
    pub async fn invoke(
        &self,
        target: aura_actor::InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        aura_realm::Realm::dispatch_handle(self.realm.clone())(target, args).await
    }
}
