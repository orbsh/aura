//! Realm-set isolation (Phase 3.6 mechanism; renamed namespace → realm by
//! ADR-0028 — the outer axis IS a Realm, the map is the set of them) —
//! okm nesting style.
//!
//! The BINDING DIMENSION is an application decision (PLAN 4.10: user,
//! project, or nothing — no credential derivation exists in the frame-
//! work; user separation is the application organizing types/keys). The
//! realm binds at construction: each realm's storage handle rides a
//! prefix-bound `MqStore` (`[u16 BE len][realm name]` prepended inside
//! the engine handle, `MqStore::for_realm`) — every table, mq AND
//! declared collections, lives under the prefix. Prefix escape is not
//! expressible afterwards. (The old `PrefixStore` over the deleted
//! `StateStore` trait died with it: the mq handle IS the nesting
//! layer.)

use std::collections::HashMap;
use std::sync::Arc;

/// The realm set: name → live Realm. Engine holds this; every surface
/// resolves its realm once and from then on only ever touches that
/// realm's objects. Realms are per-name; the store delegates through the
/// prefix-bound engine handle.
pub struct RealmSet {
    map: tokio::sync::Mutex<HashMap<String, crate::SharedRealm>>,
    /// The shared okm engine every realm's storage handle prefixes into
    /// (ADR-0018 steps 1+2: mq tables AND state documents ride byte-
    /// native tables, never a JSON store).
    mq: crate::mq::MqStore,
    /// Code-reference prefix carried into every lazily created realm
    /// (ADR-0027; one EngineConfig value, no per-realm choice).
    code_base_url: Option<String>,
}

impl RealmSet {
    /// Full constructor: the shared okm engine (one fjall keyspace).
    pub fn with_mq(mq: crate::mq::MqStore) -> Self {
        Self::with_mq_and_code_base(mq, None)
    }

    /// The shared handle carries the code-reference prefix (ADR-0027)
    /// into every realm lazily created from it.
    pub fn with_mq_and_code_base(mq: crate::mq::MqStore, code_base_url: Option<String>) -> Self {
        Self { map: tokio::sync::Mutex::new(HashMap::new()), mq, code_base_url }
    }

    /// Get-or-create the named realm (lazy; cheap).
    pub async fn realm_of(&self, name: &str) -> NamedRealm {
        let mut map = self.map.lock().await;
        let realm = map
            .entry(name.to_string())
            .or_insert_with(|| {
                // State + mq both ride the realm-prefixed okm engine
                // (ADR-0018 step 2): the prefix bound at construction is
                // the isolation boundary; no JSON PrefixStore layer.
                let mq = crate::mq::MqStore::for_realm(&self.mq, name);
                Arc::new(tokio::sync::Mutex::new(
                    crate::Realm::with_mq(mq).with_code_base_url(self.code_base_url.clone()),
                ))
            });
        NamedRealm { name: name.to_string(), realm: realm.clone() }
    }

    /// Realm names currently live (observation).
    pub async fn live(&self) -> Vec<String> {
        self.map.lock().await.keys().cloned().collect()
    }
}

/// A named realm handle (ADR-0028). Deliberately NOT convertible into
/// another realm's handle: the storage prefix is bound at construction,
/// the same construction-time isolation pattern as okm's prefix-bound
/// hosts (ADR-0010).
#[derive(Clone)]
pub struct NamedRealm {
    pub name: String,
    realm: crate::SharedRealm,
}

impl NamedRealm {
    pub fn realm(&self) -> crate::SharedRealm {
        self.realm.clone()
    }
}
