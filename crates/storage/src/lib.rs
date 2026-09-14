//! State engines. Phase 1: the in-memory `StateStore` implementation.
//! The trait is actor-visible (defined in aura-actor); Fjall arrives with
//! the Phase 4 engine matrix — an engine choice invisible to handlers
//! (`ctx.state` identical either way, wiki §3.5).

use aura_actor::{InstanceId, StateStore};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Single-mutex in-memory implementation. Phase 0/1 correctness baseline;
/// not a throughput statement.
#[derive(Default)]
pub struct InMemoryStore {
    inner: Mutex<HashMap<(String, String, String), Value>>,
}

impl StateStore for InMemoryStore {
    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .get(&(id.actor_type.clone(), id.key.clone(), field.into()))
            .cloned())
    }

    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .insert((id.actor_type.clone(), id.key.clone(), field.into()), value);
        Ok(())
    }

    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .remove(&(id.actor_type.clone(), id.key.clone(), field.into()));
        Ok(())
    }

    fn fields(&self, id: &InstanceId) -> anyhow::Result<Vec<String>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .keys()
            .filter(|(t, k, _)| t == &id.actor_type && k == &id.key)
            .map(|(_, _, f)| f.clone())
            .collect())
    }
}

/// Fjall-backed store (Phase 4): LSM-Tree, WAL-protected, engine-local.
/// The instance's fields live under `state:{actor_type}:{key}:{field}` —
/// per-field writes are native LSM puts (wiki §状态落盘的原子化: Fjall's
/// LSM handles high-frequency small writes).
#[cfg(feature = "fjall")]
pub mod fjall_store {
    use super::*;
    use aura_actor::{InstanceId, StateStore};

    pub struct FjallStateStore {
        db: fjall::Database,
        keyspace: fjall::Keyspace,
        /// Owned temp dir when opened without a path (tests).
        _dir: Option<std::sync::Arc<tempfile::TempDir>>,
    }

    /// Key layout: length-prefixed binary segments —
    /// `[u32 BE len(type)][type][u32 BE len(key)][key][field]`. No textual
    /// separators: okm's key discipline is positional width-delimited
    /// segments (`[ns 2B][slot 1B][fields][pkey]`), so prefix scans match
    /// structural boundaries, never character coincidences. `actor_type`
    /// arrives namespace-qualified from the caller (system realm passes
    /// the bare type; user namespaces pass a two-part qualification — see
    /// NamespacedStore).
    fn state_key(id: &InstanceId, field: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(8 + id.actor_type.len() + id.key.len() + field.len());
        k.extend_from_slice(&(id.actor_type.len() as u32).to_be_bytes());
        k.extend_from_slice(id.actor_type.as_bytes());
        k.extend_from_slice(&(id.key.len() as u32).to_be_bytes());
        k.extend_from_slice(id.key.as_bytes());
        k.extend_from_slice(field.as_bytes());
        k
    }

    impl FjallStateStore {
        /// Open (or create) the engine at a path.
        pub fn open(path: &std::path::Path) -> fjall::Result<Self> {
            let db = fjall::Database::create_or_recover(fjall::Config::new(path))?;
            let keyspace = db.keyspace("aura_state", fjall::KeyspaceCreateOptions::default)?;
            Ok(Self { db, keyspace, _dir: None })
        }

        /// Open a namespace-scoped keyspace: each user namespace gets its
        /// own keyspace inside one engine — structural isolation at the
        /// engine level, no key-encoding tricks.
        pub fn open_namespaced(path: &std::path::Path, namespace: &str) -> fjall::Result<Self> {
            let db = fjall::Database::create_or_recover(fjall::Config::new(path))?;
            let ks_name = format!("state:{namespace}");
            let keyspace = db.keyspace(&ks_name, fjall::KeyspaceCreateOptions::default)?;
            Ok(Self { db, keyspace, _dir: None })
        }

        /// Ephemeral engine over a temp dir (tests).
        pub fn open_tmp() -> fjall::Result<Self> {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let db = fjall::Database::create_or_recover(fjall::Config::new(dir.path()))?;
            let keyspace = db.keyspace("aura_state", fjall::KeyspaceCreateOptions::default)?;
            Ok(Self { db, keyspace, _dir: Some(std::sync::Arc::new(dir)) })
        }

        /// Durable flush (on_sleep may call this; normally the WAL suffices).
        pub fn persist(&self) -> fjall::Result<()> {
            self.db.persist(fjall::PersistMode::SyncData)
        }
    }

    impl StateStore for FjallStateStore {
        fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
            Ok(self
                .keyspace
                .get(state_key(id, field))
                .map_err(|e| anyhow::anyhow!("fjall get: {e}"))?
                .map(|bytes| {
                    serde_json::from_slice(&bytes)
                        .map_err(|e| anyhow::anyhow!("state deserialize: {e}"))
                })
                .transpose()?)
        }

        fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
            let bytes = serde_json::to_vec(&value)
                .map_err(|e| anyhow::anyhow!("state serialize: {e}"))?;
            self.keyspace
                .insert(state_key(id, field), bytes)
                .map_err(|e| anyhow::anyhow!("fjall insert: {e}"))
        }

        fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
            self.keyspace
                .remove(state_key(id, field))
                .map_err(|e| anyhow::anyhow!("fjall remove: {e}"))
        }

        fn fields(&self, id: &InstanceId) -> anyhow::Result<Vec<String>> {
            let mut prefix = Vec::with_capacity(8 + id.actor_type.len() + id.key.len());
            prefix.extend_from_slice(&(id.actor_type.len() as u32).to_be_bytes());
            prefix.extend_from_slice(id.actor_type.as_bytes());
            prefix.extend_from_slice(&(id.key.len() as u32).to_be_bytes());
            prefix.extend_from_slice(id.key.as_bytes());
            let mut out = Vec::new();
            for guard in self.keyspace.prefix(&prefix) {
                let k = match guard.key() {
                    Ok(k) => k,
                    Err(e) => return Err(anyhow::anyhow!("fjall scan: {e}")),
                };
                // Field name = the tail segment after the instance prefix.
                out.push(String::from_utf8_lossy(&k[prefix.len()..]).to_string());
            }
            Ok(out)
        }
    }
}

/// Shared handle used by the runtime and (later) across nodes.
pub type SharedStore = Arc<dyn StateStore>;
