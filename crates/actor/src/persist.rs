//! Actor definition persistence (Phase 4.5b): script-actor definitions and
//! their introspected metadata survive node restart.
//!
//! Lifecycle (PLAN 4.5b): UPLOAD is its own lifecycle — `set` persists the
//! definition (source, language) plus the introspected metadata;
//! EXECUTION never reads the schema part — message handling loads source +
//! Boot reloads every persisted script type back
//! into the realm.
//!
//! The PERSISTENCE TABLES live in aura-realm (`realm/src/meta.rs`) over the
//! meta okm instance (ADR-0018 follow-through: no JSON storage anywhere;
//! this struct is the seam type — JSON only as the interface artifact for
//! the introspected schema). Rust-closure actors (`Body::Rust`) are
//! compile-time constructs — they are not persistable and not restored.

use crate::ActorType;
use serde::{Deserialize, Serialize};

/// The persistable slice of an `ActorType`: script body + declared
/// metadata. Rust-closure bodies are NOT persistable (compile-time).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistedActor {
    pub name: String,
    pub language: String,
    pub source: String,
    /// Per-type idle TTL, seconds; absent = realm default.
    pub idle_ttl_secs: Option<u64>,
    /// Metadata extracted from `interface_schema()` introspection at
    /// upload (receives/emits/lifecycle), kept verbatim (interface
    /// artifact — the LLM/script-side JSON contract).
    pub schema: Option<serde_json::Value>,
}

impl PersistedActor {
    pub fn from_type(actor: &ActorType) -> Option<Self> {
        let crate::Body::Script { language, source } = &actor.body else {
            return None; // Rust handlers are compile-time; nothing to persist
        };
        Some(Self {
            name: actor.name.clone(),
            language: language.clone(),
            source: source.clone(),
            idle_ttl_secs: actor.idle_ttl.map(|d| d.as_secs()),
            schema: None,
        })
    }

    /// Rebuild the runtime type from the persisted record.
    pub fn to_type(&self) -> ActorType {
        let mut t = ActorType::script(
            self.name.clone(),
            self.language.clone(),
            self.source.clone(),
        );
        t.idle_ttl = self.idle_ttl_secs.map(std::time::Duration::from_secs);
        t
    }
}
