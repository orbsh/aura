//! State engines. Phase 0: in-memory placeholder — `on_sleep` persistence
//! to Fjall arrives in Phase 1, SlateDB + S3 in Phase 4. Engine/consistency
//! matrix (fjall+raft / slate+s3) is validated at boot from Phase 4 on.

use aura_actor::InstanceId;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct InMemoryState {
    /// (actor_type, key, field) → value. Single mutex: Phase 0 is about the
    /// call path, not throughput.
    inner: Mutex<HashMap<(String, String, String), Value>>,
}

impl InMemoryState {
    pub fn get(&self, id: &InstanceId, field: &str) -> Option<Value> {
        self.inner
            .lock()
            .unwrap()
            .get(&(id.actor_type.clone(), id.key.clone(), field.into()))
            .cloned()
    }

    pub fn set(&self, id: &InstanceId, field: &str, value: Value) {
        self.inner
            .lock()
            .unwrap()
            .insert((id.actor_type.clone(), id.key.clone(), field.into()), value);
    }
}
