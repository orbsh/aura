//! Engine assembly: config + realm + runtime. Phase 1 — store-backed ctx
//! state, idle-TTL eviction (scale-to-zero), submit-based call path.

use aura_actor::ActorType;
use aura_realm::{Realm, SharedRealm};
use std::sync::Arc;
use std::time::Duration;

pub struct Engine {
    /// The system/default namespace realm (back-compat: single-node tests,
    /// CLI echo). User-facing surfaces use `namespaces` instead.
    pub realm: SharedRealm,
    /// Per-user namespace map (Phase 3.6): structural isolation — a
    /// NamespacedRealm handle cannot reach another namespace.
    pub namespaces: Arc<aura_realm::namespace::Namespaces>,
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
        let realm: SharedRealm = Arc::new(tokio::sync::Mutex::new(Realm::new(store.clone())));
        Realm::spawn_evictor(&realm);
        // Namespace store factory per engine kind (Phase 4): fjall opens a
        // namespace-scoped keyspace under the engine's data dir; memory
        // hands out a fresh map (isolation by construction).
        let engine_kind = config.engine.clone();
        let data_dir = config.data_dir.clone();
        let node_id = config.node_id.clone();
        let namespaces = Arc::new(aura_realm::namespace::Namespaces::new(move |ns| {
            match &engine_kind {
                aura_config::Engine::Memory => Ok(Arc::new(aura_storage::InMemoryStore::default())),
                aura_config::Engine::Fjall => {
                    #[cfg(feature = "fjall")]
                    {
                        let path = data_dir.clone().unwrap_or_else(|| {
                            std::env::temp_dir().join(format!("aura-{node_id}"))
                        });
                        Ok(Arc::new(
                            aura_storage::fjall_store::FjallStateStore::open_namespaced(&path, ns)
                                .map_err(|e| anyhow::anyhow!("fjall open {path:?}/{ns}: {e}"))?,
                        ))
                    }
                    #[cfg(not(feature = "fjall"))]
                    {
                        let _ = (ns, data_dir, node_id);
                        anyhow::bail!("engine=fjall requires building with the `fjall` feature")
                    }
                }
            }
        }));
        Ok(Self { realm, namespaces })
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

    /// Invoke a registered actor instance (hot path convenience: wait for
    /// the value). The general entry is `call`, returning a CallSlot.
    pub async fn invoke(
        &self,
        target: aura_actor::InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        match self.call(target, args).await? {
            aura_actor::call::Waited::Done(result) => result,
            aura_actor::call::Waited::Pending(id) => {
                anyhow::bail!("cold call returned a Pending slot to a hot caller: {}", id.0)
            }
        }
    }

    /// The unified call (Phase 3.5): every surface — CLI, HTTP, remote
    /// Probe, actor ctx.invoke — converges here. Hot targets return
    /// Done on wait; cold targets return Pending(call_id) and the
    /// result arrives via resolve_call.
    pub async fn call(
        &self,
        target: aura_actor::InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_actor::call::Waited> {
        // Slot construction errors (unknown type/full mailbox) are Err;
        // wait results — including Done(Err(timeout/handler failure)) —
        // travel inside the Waited so callers see failure as a value.
        Realm::call(&self.realm, None, target, args)
            .await?
            .wait()
            .await
    }

    /// Register an actor type into a user namespace (Phase 3.6). The
    /// namespace is derived from the user credential at registration.
    pub async fn register_in(&self, namespace: &str, actor: ActorType) {
        let ns = self.namespaces.realm_of(namespace).await;
        ns.realm().lock().await.register_type(actor);
    }

    /// Namespaced call: target resolution = user namespace + node alias +
    /// operation. A namespace handle never sees another namespace's types
    /// or events — cross-namespace delivery is not expressible.
    pub async fn call_in(
        &self,
        namespace: &str,
        target: aura_actor::InstanceId,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_actor::call::Waited> {
        let ns = self.namespaces.realm_of(namespace).await;
        Realm::call(&ns.realm(), None, target, args)
            .await?
            .wait()
            .await
    }

    /// Namespaced emit: events route only within the namespace.
    pub async fn emit_in(
        &self,
        namespace: &str,
        emitter: Option<&str>,
        event: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let ns = self.namespaces.realm_of(namespace).await;
        Realm::emit(&ns.realm(), emitter, event, data).await
    }

    /// Resolve a pending call (cold re-entry): framework delivers the
    /// result; unknown call_id is a no-op (completed never replay).
    pub async fn resolve_call(
        &self,
        call_id: &aura_actor::call::CallId,
        result: anyhow::Result<serde_json::Value>,
    ) -> bool {
        Realm::resolve_call(&self.realm, call_id, result).await
    }
}
