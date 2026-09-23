//! Engine configuration. Phase 0: single-node, in-memory — no external
//! dependencies (no Docker / etcd / DB), per Milestone A.

pub mod kdl;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Node identity (goes to partitioning/raft later).
    pub node_id: String,
    /// Realm namespace (Phase 3.6 introduces per-user namespaces).
    pub namespace: String,
    /// State engine. `memory` = in-process placeholder; `fjall` = local
    /// LSM (requires the `fjall` feature; boot error if absent). The
    /// engine/consistency matrix grows in Phase 4/5 (slate needs raft).
    pub engine: Engine,
    /// Data directory for persistent engines. Ignored by `memory`.
    pub data_dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    /// In-memory placeholder (tests, Phase 0 semantics).
    #[default]
    Memory,
    /// Local LSM-Tree (fjall). Single-node durable.
    Fjall,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            node_id: "local".into(),
            namespace: "default".into(),
            engine: Engine::Memory,
            data_dir: None,
        }
    }
}

impl TryFrom<crate::kdl::RootConfig> for EngineConfig {
    type Error = String;

    fn try_from(root: crate::kdl::RootConfig) -> Result<Self, Self::Error> {
        let parse_engine = |e: &str| match e {
            "fjall" => Ok(Engine::Fjall),
            "memory" => Ok(Engine::Memory),
            other => Err(format!(
                "unknown engine `{other}` (supported: fjall, memory; slate lands with okm adapter)"
            )),
        };
        Ok(Self {
            node_id: root.node.id,
            namespace: root.node.namespace,
            engine: parse_engine(&root.data.engine)?,
            data_dir: Some(root.data.path.into()),
        })
    }
}
