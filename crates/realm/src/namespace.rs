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

use aura_actor::{InstanceId, SharedStore, StateStore};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A namespace-locked realm handle. Deliberately NOT Clone-into-other-
/// namespace: the namespace is part of the handle, invisible to holders.
#[derive(Clone)]
pub struct NamespacedRealm {
    pub namespace: String,
    realm: crate::SharedRealm,
    store: SharedStore,
}

impl NamespacedRealm {
    pub fn realm(&self) -> crate::SharedRealm {
        self.realm.clone()
    }

    /// The namespace's state store: every key is qualified with the
    /// namespace, so the same (type, key) in two namespaces lands on
    /// different store keys — isolation holds in the durable layer too.
    pub fn store(&self) -> SharedStore {
        self.store.clone()
    }
}

/// Store view that prefixes every instance's actor_type with its
/// namespace: `ns:{namespace}:{type}`. Handlers never see this — the
/// handle is namespace-locked at construction.
pub struct NamespacedStore {
    namespace: String,
    inner: SharedStore,
}

impl NamespacedStore {
    fn qualify(&self, id: &InstanceId) -> InstanceId {
        InstanceId {
            // `u/{namespace}` — the USER namespace qualification (this is
            // the second, meaningful "ns"; the first `state/` segment is
            // the storage-layer prefix in FjallStateStore).
            actor_type: format!("u/{}:{}", self.namespace, id.actor_type),
            key: id.key.clone(),
        }
    }
}

impl StateStore for NamespacedStore {
    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
        self.inner.get(&self.qualify(id), field)
    }
    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
        self.inner.set(&self.qualify(id), field, value)
    }
    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
        self.inner.delete(&self.qualify(id), field)
    }
    fn fields(&self, id: &InstanceId) -> anyhow::Result<Vec<String>> {
        self.inner.fields(&self.qualify(id))
    }
}

/// The namespace map. Engine holds this; every surface resolves its
/// namespace once and from then on only ever touches that namespace's
/// realm.
pub struct Namespaces {
    map: tokio::sync::Mutex<HashMap<String, crate::SharedRealm>>,
    store: SharedStore,
}

impl Namespaces {
    pub fn new(store: SharedStore) -> Self {
        Self { map: tokio::sync::Mutex::new(HashMap::new()), store }
    }

    /// Get-or-create the namespace's realm. Lazy: namespaces are cheap.
    pub async fn realm_of(&self, namespace: &str) -> NamespacedRealm {
        let mut map = self.map.lock().await;
        let realm = map
            .entry(namespace.to_string())
            .or_insert_with(|| {
                let store: SharedStore = Arc::new(NamespacedStore {
                    namespace: namespace.to_string(),
                    inner: self.store.clone(),
                });
                Arc::new(tokio::sync::Mutex::new(crate::Realm::new(store)))
            });
        let store: SharedStore = Arc::new(NamespacedStore {
            namespace: namespace.to_string(),
            inner: self.store.clone(),
        });
        NamespacedRealm { namespace: namespace.to_string(), realm: realm.clone(), store }
    }

    /// Namespace names currently live (observation).
    pub async fn live(&self) -> Vec<String> {
        self.map.lock().await.keys().cloned().collect()
    }
}
