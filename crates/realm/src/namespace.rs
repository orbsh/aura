//! User namespace isolation (Phase 3.6).
//!
//! Structural, not check-based: each user namespace owns its own Realm
//! (types, instances, router, pending_calls). Cross-namespace delivery is
//! not expressible — a handle is bound to one namespace at construction,
//! the same construction-time isolation pattern as okm's prefix-bound
//! handles (ADR-0010). There is no "send to namespace X" API to misuse.
//!
//! Registration credential = user credential (outbound connection carries
//! "whose machine am I"); the namespace is derived from the credential at
//! registration. Target resolution inside a namespace:
//! `probe:<node_alias>:<operation>` where node_alias is the registration's
//! node alias.
//!
//! Namespaces are created lazily on first use and are cheap (a Realm over
//! the shared store; instances are per-namespace, state keys are
//! namespaced by the store's own `state:{type}:{key}:{field}` scheme —
//! keys carry the namespace as part of actor_type qualification via the
//! per-namespace realm, so no cross-user state is reachable).

use aura_actor::SharedStore;
use std::collections::HashMap;
use std::sync::Arc;

/// A namespace-locked realm handle. Deliberately NOT Clone-into-other-
/// namespace: the namespace is part of the handle, invisible to holders.
#[derive(Clone)]
pub struct NamespacedRealm {
    pub namespace: String,
    realm: crate::SharedRealm,
}

impl NamespacedRealm {
    pub fn realm(&self) -> crate::SharedRealm {
        self.realm.clone()
    }

}

/// The namespace map. Engine holds this; every surface resolves its
/// namespace once and from then on only ever touches that namespace's
/// realm.
pub struct Namespaces {
    map: tokio::sync::Mutex<HashMap<String, crate::SharedRealm>>,
    /// Per-namespace store factory: the engine decides what "a store for
    /// namespace X" means (fjall: a namespace-scoped keyspace; memory: a
    /// fresh map). Structural isolation lives in the factory.
    factory: Box<dyn Fn(&str) -> anyhow::Result<SharedStore> + Send + Sync>,
}

impl Namespaces {
    pub fn new(
        factory: impl Fn(&str) -> anyhow::Result<SharedStore> + Send + Sync + 'static,
    ) -> Self {
        Self { map: tokio::sync::Mutex::new(HashMap::new()), factory: Box::new(factory) }
    }

    /// Get-or-create the namespace's realm. Lazy: namespaces are cheap.
    pub async fn realm_of(&self, namespace: &str) -> NamespacedRealm {
        let mut map = self.map.lock().await;
        let realm = map
            .entry(namespace.to_string())
            .or_insert_with(|| {
                let store = (self.factory)(namespace)
                    .expect("namespace store factory failed");
                Arc::new(tokio::sync::Mutex::new(crate::Realm::new(store)))
            });
        NamespacedRealm { namespace: namespace.to_string(), realm: realm.clone() }
    }

    /// Namespace names currently live (observation).
    pub async fn live(&self) -> Vec<String> {
        self.map.lock().await.keys().cloned().collect()
    }
}
