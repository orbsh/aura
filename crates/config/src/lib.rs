//! Engine configuration. Phase 0: single-node, in-memory — no external
//! dependencies (no Docker / etcd / DB), per Milestone A.

pub mod kdl;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Node identity (goes to partitioning/raft later).
    pub node_id: String,
    /// This node's default realm name (Phase 3.6 mechanism, renamed per
    /// ADR-0028; the binding dimension is an application decision).
    pub realm: String,
    /// State engine. `memory` = in-process placeholder; `fjall` = local
    /// LSM (requires the `fjall` feature; boot error if absent). The
    /// engine/consistency matrix grows in Phase 4/5 (slate needs raft).
    pub engine: Engine,
    /// Data directory for persistent engines. Ignored by `memory`.
    pub data_dir: Option<std::path::PathBuf>,
    /// Code reference prefix for remote delivery (ADR-0027). None =
    /// remote types are undeliverable here (error value at dispatch).
    pub code_base_url: Option<String>,
    /// The cursor retention promise in seconds (ADR-0039 §2): a cursor row
    /// that has not advanced for this long leaves the watermark
    /// denominator — its backlog is forfeit. One engine-wide value, never
    /// per-type; default 30 days (finite by design).
    pub cursor_ttl_secs: u64,
    /// What this venue offers as durable data (ADR-0048/0049), one row per kind. Declared at boot
    /// because a grant is a DEPLOYMENT fact: if a script could create one, "a resource is a grant"
    /// would be a sentence about nothing.
    #[serde(default)]
    pub resources: std::collections::BTreeMap<String, ResourceConfig>,
}

/// One grant as configured: where the bytes are, on what terms, and which environment variable holds
/// the credential. The value is deliberately absent — a config that carried the key would be the key
/// in a file that gets copied around.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub base: String,
    /// `ro` or `rw`; anything else is refused at boot rather than silently read-only.
    #[serde(default)]
    pub mode: String,
    /// The environment variable the credential is read from. Empty = this kind needs none.
    #[serde(default)]
    pub secret_env: String,
    #[serde(default)]
    pub desc: String,
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
            realm: "default".into(),
            engine: Engine::Memory,
            data_dir: None,
            code_base_url: None,
            cursor_ttl_secs: 30 * 24 * 3600,
            resources: std::collections::BTreeMap::new(),
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
            realm: root.node.realm,
            engine: parse_engine(&root.data.engine)?,
            data_dir: Some(root.data.path.into()),
            code_base_url: root.node.code_base_url.clone(),
            resources: root
                .resources
                .map(|r| {
                    r.kinds
                        .into_iter()
                        .map(|k| {
                            (
                                k.name,
                                ResourceConfig {
                                    base: k.base,
                                    mode: k.mode.unwrap_or_else(|| "ro".into()),
                                    secret_env: k.secret_env.unwrap_or_default(),
                                    desc: k.desc.unwrap_or_default(),
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            cursor_ttl_secs: match root.mq.as_ref().and_then(|m| m.cursor_ttl.as_deref()) {
                None => 30 * 24 * 3600,
                Some(text) => parse_duration_secs(text)?,
            },
        })
    }
}

/// Parse a human duration with a mandatory unit suffix: "30d" / "12h" /
/// "5m" / "300s". Bare digits are rejected — a unitless retention promise
/// is ambiguous, and ambiguity here silently changes how long data lives.
fn parse_duration_secs(s: &str) -> Result<u64, String> {
    if s.len() < 2 {
        return Err(format!("duration `{s}` needs a unit suffix (s/m/h/d)"));
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("duration `{s}`: `{num}` is not a number"))?;
    match unit {
        "s" => Ok(n),
        "m" => Ok(n * 60),
        "h" => Ok(n * 3600),
        "d" => Ok(n * 24 * 3600),
        other => Err(format!("duration `{s}`: unknown unit `{other}` (s/m/h/d)")),
    }
}
