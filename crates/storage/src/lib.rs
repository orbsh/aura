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

/// Shared handle used by the runtime and (later) across nodes.
pub type SharedStore = Arc<dyn StateStore>;
