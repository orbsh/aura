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
    /// Start the engine: resolve the state store from config, spawn the
    /// idle evictor. Boot errors on engine/feature mismatches (PLAN Phase 4
    /// matrix: an engine chosen without its feature compiled in fails at
    /// boot, never silently falls back).
    pub fn start(config: &aura_config::EngineConfig) -> anyhow::Result<Self> {
        let store: aura_actor::SharedStore = match config.engine {
            aura_config::Engine::Memory => Arc::new(aura_storage::InMemoryStore::default()),
            aura_config::Engine::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    let path = config.data_dir.clone().unwrap_or_else(|| {
                        std::env::temp_dir().join(format!("aura-{}", config.node_id))
                    });
                    Arc::new(aura_storage::fjall_store::FjallStateStore::open(&path)
                        .map_err(|e| anyhow::anyhow!("fjall open {path:?}: {e}"))?)
                }
                #[cfg(not(feature = "fjall"))]
                {
                    let _ = config;
                    anyhow::bail!(
                        "engine=fjall requires building with the `fjall` feature"
                    );
                }
            }
        };
        let realm: SharedRealm = Arc::new(tokio::sync::Mutex::new(Realm::new(store)));
        Realm::spawn_evictor(&realm);
        Ok(Self { realm })
    }

    /// Override the idle TTL (default 30s).
    pub fn with_idle_ttl(self, ttl: Duration) -> Self {
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
