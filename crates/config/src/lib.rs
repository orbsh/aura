//! Engine configuration. Phase 0: single-node, in-memory — no external
//! dependencies (no Docker / etcd / DB), per Milestone A.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Node identity (goes to partitioning/raft later).
    pub node_id: String,
    /// Realm namespace (Phase 3.6 introduces per-user namespaces).
    pub namespace: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            node_id: "local".into(),
            namespace: "default".into(),
        }
    }
}
