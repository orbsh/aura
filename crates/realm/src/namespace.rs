//! Namespace isolation (Phase 3.6 mechanism) — okm nesting style.
//!
//! The BINDING DIMENSION is an application decision (PLAN 4.10: user,
//! project, or nothing — no credential derivation exists in the frame-
//! work; user separation is the application organizing types/keys). The
//! namespace binds at construction: each namespace's realm rides a
//! prefix-bound `MqStore` (`[u16 BE len][namespace]` prepended inside
//! the engine handle, `MqStore::namespaced`) — every table, mq AND
//! declared collections, lives under the prefix. Prefix escape is not
//! expressible afterwards. (The old `PrefixStore` over the deleted
//! `StateStore` trait died with it: the mq handle IS the nesting
//! layer.)

use std::collections::HashMap;
use std::sync::Arc;

/// The namespace map. Engine holds this; every surface resolves its
/// namespace once and from then on only ever touches that namespace's
/// realm. Realms are per-namespace; the store delegates through PrefixStore
/// over the shared engine.
pub struct Namespaces {
    map: tokio::sync::Mutex<HashMap<String, crate::SharedRealm>>,
    /// The shared okm engine every namespace's realm prefixes into
    /// (ADR-0018 steps 1+2: mq tables AND state documents ride byte-
    /// native tables, never a JSON store).
    mq: crate::mq::MqStore,
}

impl Namespaces {
    /// Full constructor: the shared okm engine (one fjall keyspace).
    pub fn with_mq(mq: crate::mq::MqStore) -> Self {
        Self { map: tokio::sync::Mutex::new(HashMap::new()), mq }
    }

    /// Get-or-create the namespace's realm (lazy; cheap).
    pub async fn realm_of(&self, namespace: &str) -> NamespacedRealm {
        let mut map = self.map.lock().await;
        let realm = map
            .entry(namespace.to_string())
            .or_insert_with(|| {
                // State + mq both ride the namespace-prefixed okm engine
                // (ADR-0018 step 2): the prefix bound at construction is
                // the isolation boundary; no JSON PrefixStore layer.
                let mq = crate::mq::MqStore::namespaced(&self.mq, namespace);
                Arc::new(tokio::sync::Mutex::new(crate::Realm::with_mq(mq)))
            });
        NamespacedRealm { namespace: namespace.to_string(), realm: realm.clone() }
    }

    /// Namespace names currently live (observation).
    pub async fn live(&self) -> Vec<String> {
        self.map.lock().await.keys().cloned().collect()
    }
}

/// A namespace-locked realm handle. Deliberately NOT convertible into
/// another namespace's handle: the namespace is bound at construction,
/// the same construction-time isolation pattern as okm's prefix-bound
/// hosts (ADR-0010).
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
