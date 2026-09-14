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
    inner: Mutex<HashMap<Vec<u8>, Value>>,
}

/// In-memory key layout mirrors the fjall one (length-prefixed segments):
/// one key discipline across engines.
fn mem_key(id: &InstanceId, field: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + id.actor_type.len() + id.key.len() + field.len());
    k.extend_from_slice(&(id.actor_type.len() as u32).to_be_bytes());
    k.extend_from_slice(id.actor_type.as_bytes());
    k.extend_from_slice(&(id.key.len() as u32).to_be_bytes());
    k.extend_from_slice(id.key.as_bytes());
    k.extend_from_slice(field.as_bytes());
    k
}

impl StateStore for InMemoryStore {
    fn key_for(&self, id: &InstanceId, field: &str) -> Vec<u8> {
        mem_key(id, field)
    }
    fn scan_keys(&self, key_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.as_slice().starts_with(key_prefix))
            .cloned()
            .collect())
    }
    fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Value>> {
        Ok(self.inner.lock().unwrap().get(key).cloned())
    }
    fn set_raw(&self, key: Vec<u8>, value: Value) -> anyhow::Result<()> {
        self.inner.lock().unwrap().insert(key, value);
        Ok(())
    }
    fn del_raw(&self, key: &[u8]) -> anyhow::Result<()> {
        self.inner.lock().unwrap().remove(key);
        Ok(())
    }
    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
        Ok(self.inner.lock().unwrap().get(&mem_key(id, field)).cloned())
    }

    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
        self.inner
            .lock()
            .unwrap()
            .insert(mem_key(id, field), value);
        Ok(())
    }

    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
        self.inner.lock().unwrap().remove(&mem_key(id, field));
        Ok(())
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
        fn key_for(&self, id: &InstanceId, field: &str) -> Vec<u8> {
            state_key(id, field)
        }
        fn scan_keys(&self, key_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
            let mut out = Vec::new();
            for guard in self.keyspace.prefix(key_prefix) {
                let k = match guard.key() {
                    Ok(k) => k,
                    Err(e) => return Err(anyhow::anyhow!("fjall scan: {e}")),
                };
                out.push(k.to_vec());
            }
            Ok(out)
        }
        fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Value>> {
            Ok(self
                .keyspace
                .get(key)
                .map_err(|e| anyhow::anyhow!("fjall get: {e}"))?
                .map(|b| serde_json::from_slice(&b))
                .transpose()
                .map_err(|e| anyhow::anyhow!("state deserialize: {e}"))?)
        }
        fn set_raw(&self, key: Vec<u8>, value: Value) -> anyhow::Result<()> {
            let bytes =
                serde_json::to_vec(&value).map_err(|e| anyhow::anyhow!("state serialize: {e}"))?;
            self.keyspace
                .insert(key, bytes)
                .map_err(|e| anyhow::anyhow!("fjall insert: {e}"))
        }
        fn del_raw(&self, key: &[u8]) -> anyhow::Result<()> {
            self.keyspace
                .remove(key)
                .map_err(|e| anyhow::anyhow!("fjall remove: {e}"))
        }
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

    }
}

/// Shared handle used by the runtime and (later) across nodes.
pub type SharedStore = Arc<dyn StateStore>;
