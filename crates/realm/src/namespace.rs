//! User namespace isolation (Phase 3.6) — okm nesting style.
//!
//! okm's nesting rule (ADR-0010 / remote.rs): the wrapper knows exactly
//! one thing — its declared prefix. Every key enters as
//! `[prefix][inner key]`; the inner engine is untouched and stays key-
//! format-agnostic (the wrapper never parses inner encoding). Prefix
//! escape is not expressible afterwards.
//!
//! Aura applies the same rule at the `StateStore` layer: `PrefixStore`
//! prepends `[u16 BE len(namespace)][namespace]` to `inner.key_for(...)`
//! bytes. Works over ANY engine (memory / fjall / slatedb / okm) because
//! the inner encoding is delegated, not assumed.

use aura_actor::{InstanceId, SharedStore, StateStore};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Namespace-qualified store: prepends `[u16 BE len][namespace]` to every
/// inner key. The prefix is the isolation boundary — structural, bound at
/// construction.
pub struct PrefixStore {
    namespace: String,
    prefix: Vec<u8>,
    inner: SharedStore,
}

impl PrefixStore {
    pub fn new(namespace: &str, inner: SharedStore) -> Self {
        // okm's 2-byte length discipline for the namespace segment.
        let mut prefix = (namespace.len() as u16).to_be_bytes().to_vec();
        prefix.extend_from_slice(namespace.as_bytes());
        Self { namespace: namespace.to_string(), prefix, inner }
    }

    fn qualified_key(&self, id: &InstanceId, field: &str) -> Vec<u8> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&self.inner.key_for(id, field));
        k
    }
}

impl StateStore for PrefixStore {
    fn key_for(&self, id: &InstanceId, field: &str) -> Vec<u8> {
        self.qualified_key(id, field)
    }

    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
        self.inner.get_raw(&self.qualified_key(id, field))
    }

    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
        self.inner.set_raw(self.qualified_key(id, field), value)
    }

    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
        self.inner.del_raw(&self.qualified_key(id, field))
    }

    fn scan_keys(&self, key_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        // The caller's key prefix is already namespaced (built from OUR
        // key_for); scan the inner engine under it, return full keys.
        self.inner.scan_keys(key_prefix)
    }

    /// Raw ops: another nesting layer on top of us composes
    /// `prefix + our key_for(...)` and calls these — we prepend OUR
    /// prefix and delegate. Nesting composes.
    fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Value>> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);
        self.inner.get_raw(&k)
    }
    fn set_raw(&self, key: Vec<u8>, value: Value) -> anyhow::Result<()> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(&key);
        self.inner.set_raw(k, value)
    }
    fn del_raw(&self, key: &[u8]) -> anyhow::Result<()> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);
        self.inner.del_raw(&k)
    }
}

/// The namespace map. Engine holds this; every surface resolves its
/// namespace once and from then on only ever touches that namespace's
/// realm. Realms are per-namespace; the store delegates through PrefixStore
/// over the shared engine.
pub struct Namespaces {
    map: tokio::sync::Mutex<HashMap<String, crate::SharedRealm>>,
    store: SharedStore,
}

impl Namespaces {
    pub fn new(store: SharedStore) -> Self {
        Self { map: tokio::sync::Mutex::new(HashMap::new()), store }
    }

    /// Get-or-create the namespace's realm (lazy; cheap).
    pub async fn realm_of(&self, namespace: &str) -> NamespacedRealm {
        let mut map = self.map.lock().await;
        let realm = map
            .entry(namespace.to_string())
            .or_insert_with(|| {
                let store: SharedStore =
                    Arc::new(PrefixStore::new(namespace, self.store.clone()));
                Arc::new(tokio::sync::Mutex::new(crate::Realm::new(store)))
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
