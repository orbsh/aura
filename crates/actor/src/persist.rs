//! Actor definition persistence (Phase 4.5b): script-actor definitions and
//! their introspected metadata survive node restart.
//!
//! Lifecycle (PLAN 4.5b): UPLOAD is its own lifecycle — `set` persists the
//! definition (source, language, entry) plus the introspected metadata
//! (receives/emits/lifecycle.idle_ttl) into the meta store, keyed by type
//! name, same value for the whole definition (versioning rides the
//! store's LSM snapshot discipline). EXECUTION never reads the schema
//! part — message handling loads source + entry from the record. Boot
//! reloads every persisted script type back into the realm.
//!
//! Rust-closure actors (`Body::Rust`) are compile-time constructs — they
//! are not persistable and not restored; their registration is the host
//! binary itself.

use crate::ActorType;
use crate::SharedStore;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The persistable slice of an `ActorType`: script body + declared
/// metadata. Rust-closure bodies are NOT persistable (compile-time).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistedActor {
    pub name: String,
    pub language: String,
    pub source: String,
    pub entry: Option<String>,
    /// Per-type idle TTL, seconds; absent = realm default.
    pub idle_ttl_secs: Option<u64>,
    /// Metadata extracted from `interface_schema()` introspection at
    /// upload (receives/emits/lifecycle), kept verbatim.
    pub schema: Option<serde_json::Value>,
}

impl PersistedActor {
    pub fn from_type(actor: &ActorType) -> Option<Self> {
        let crate::Body::Script { language, source, entry } = &actor.body else {
            return None; // Rust handlers are compile-time; nothing to persist
        };
        Some(Self {
            name: actor.name.clone(),
            language: language.clone(),
            source: source.clone(),
            entry: entry.clone(),
            idle_ttl_secs: actor.idle_ttl.map(|d| d.as_secs()),
            schema: None,
        })
    }

    /// Rebuild the runtime type from the persisted record. `set_schema`
    /// re-attaches introspection results after the upload-time call.
    pub fn to_type(&self) -> ActorType {
        let mut t = ActorType::script(
            self.name.clone(),
            self.language.clone(),
            self.source.clone(),
            self.entry.clone(),
        );
        t.idle_ttl = self.idle_ttl_secs.map(Duration::from_secs);
        t
    }
}

/// Key encoding for the meta store: `[u32 BE len][type name]` — one
/// record per actor type (latest version wins; LSM keeps history).
pub fn meta_key(type_name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + type_name.len());
    k.extend_from_slice(&(type_name.len() as u32).to_be_bytes());
    k.extend_from_slice(type_name.as_bytes());
    k
}

pub fn persist(store: &SharedStore, actor: &PersistedActor) -> anyhow::Result<()> {
    let value = serde_json::to_value(actor)?;
    store.set_raw(meta_key(&actor.name), value)
}

pub fn load_all(store: &SharedStore) -> anyhow::Result<Vec<PersistedActor>> {
    let mut out = Vec::new();
    // All persisted actor records share the single length-prefixed key
    // space; scan everything and filter by decodability.
    for key in store.scan_keys(&[])? {
        let Some(raw) = store.get_raw(&key)? else { continue };
        if let Ok(def) = serde_json::from_value::<PersistedActor>(raw) {
            out.push(def);
        }
    }
    Ok(out)
}
