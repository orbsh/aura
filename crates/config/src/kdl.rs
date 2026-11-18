//! Root configuration, `aura.kdl` (KDL via knus). One top-level node per
//! subsystem; the file never holds secrets — it names environment
//! variables, resolved at call time (krystallizer ADR-0007 pattern).
//!
//! ```kdl
//! node {
//!     id "home-node"
//!     realm "default"
//! }
//!
//! // data plane: booth state
//! data {
//!     engine "fjall"
//!     path "/var/lib/aura/data"
//! }
//!
//! // metadata plane: registry, shard map
//!     engine "fjall"
//! }
//! ```

use knus::Decode;

#[derive(Decode, Debug, Clone)]
pub struct RootConfig {
    /// `node { ... }` — node identity.
    #[knus(child)]
    pub node: NodeConfig,
    /// `data { ... }` — the one storage plane (mq, state, definitions —
    /// ADR-0025 Plan A: no separate meta instance).
    #[knus(child)]
    pub data: StorageConfig,
    /// `mq { ... }` — the event plane's operational knobs (ADR-0039 §2).
    /// Absent = the defaults apply.
    #[knus(child)]
    pub mq: Option<MqConfig>,
    /// `resources { ... }` — what this venue offers as durable data (ADR-0048/0049).
    /// Absent = this venue mints no resources, and `ctx.resource` says so by name.
    #[knus(child)]
    pub resources: Option<ResourcesConfig>,
}

/// `resources { kind "<name>" { ... } }` — one block per kind, the kind named as an argument
/// because that is what the block IS (`git`, `object`, `archive`).
#[derive(Decode, Debug, Clone)]
pub struct ResourcesConfig {
    #[knus(children(name = "kind"))]
    pub kinds: Vec<ResourceKindConfig>,
}

/// One kind's grant, as an operator writes it. The credential is NAMED, never carried: the file names
/// an environment variable and boot reads it (`krystallizer` ADR-0007's rule, which is the same wall
/// as effector's `EFFECTOR_RESOURCE_CREDENTIAL`).
#[derive(Decode, Debug, Clone)]
pub struct ResourceKindConfig {
    #[knus(argument)]
    pub name: String,
    /// Where the bytes are reachable: a host for `git`, a bucket for `object`.
    #[knus(child, unwrap(argument))]
    pub base: String,
    /// `ro` (default) or `rw`.
    #[knus(child, unwrap(argument))]
    pub mode: Option<String>,
    /// The environment variable holding the credential. KDL node name is kebab (`secret-env`):
    /// knus derives node names from field names with `heck`, so an underscore here would be a node
    /// nobody writes.
    #[knus(child, unwrap(argument))]
    pub secret_env: Option<String>,
    /// A human word for the row, so a dumped grant is understandable without the operator.
    #[knus(child, unwrap(argument))]
    pub desc: Option<String>,
}

/// Event-plane configuration. `cursor_ttl` is the retention promise: how
/// long a cursor that has not advanced keeps its backlog (a duration
/// string, e.g. "30d"; a global value, never per-type).
#[derive(Decode, Debug, Clone)]
pub struct MqConfig {
    #[knus(child, unwrap(argument))]
    pub cursor_ttl: Option<String>,
}

#[derive(Decode, Debug, Clone)]
pub struct NodeConfig {
    #[knus(child, unwrap(argument))]
    pub id: String,
    /// This node's default realm (ADR-0028; Phase 3.6 mechanism).
    #[knus(child, unwrap(argument))]
    pub realm: String,
    /// Prefix remote code references are assembled under (ADR-0027):
    /// prism's `/code/` export or a CDN base. Absent = this node never
    /// serves remote effectors (remote execution errors as a value; there is
    /// deliberately no default — pointing code at nobody is a guess).
    #[knus(child, unwrap(argument))]
    pub code_base_url: Option<String>,
}

/// One storage plane. Engine is fjall | slate; slate carries an optional
/// s3 endpoint block (slatedb requires an object store; mem mode is the
/// test shape, not a config shape).
#[derive(Decode, Debug, Clone)]
pub struct StorageConfig {
    #[knus(child, unwrap(argument))]
    pub engine: String,
    #[knus(child, unwrap(argument))]
    pub path: String,
    /// slate only: S3 endpoint configuration. Absent = none.
    #[knus(child)]
    pub s3: Option<S3Config>,
}

#[derive(Decode, Debug, Clone)]
pub struct S3Config {
    #[knus(child, unwrap(argument))]
    pub endpoint: String,
    #[knus(child, unwrap(argument))]
    pub bucket: String,
    /// Environment variable holding the access key (not the key).
    #[knus(child, unwrap(argument))]
    pub key_env: String,
    /// Environment variable holding the secret (not the secret).
    #[knus(child, unwrap(argument))]
    pub secret_env: String,
}

#[derive(Debug)]
pub enum ConfigError {
    Io {
        path: String,
        source: std::io::Error,
    },
    /// miette Report: knus errors are Diagnostics (rich spans), not
    /// std::error::Error — same wrapping as krystallizer's config.
    Parse {
        path: String,
        report: miette::Report,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, .. } => write!(f, "failed to read config file {path}"),
            Self::Parse { path, .. } => write!(f, "failed to parse config file {path}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse { .. } => None, // miette Report is a Diagnostic
        }
    }
}

impl RootConfig {
    /// Load and decode `path` (assembly-site decision, not defaulted here).
    pub fn load(path: &std::path::Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string_lossy().into_owned(),
            source,
        })?;
        knus::parse::<RootConfig>(path.to_string_lossy().as_ref(), &text).map_err(|err| {
            ConfigError::Parse {
                path: path.to_string_lossy().into_owned(),
                report: err.into(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn a_resources_block_names_each_kind_and_its_grant() {
        // One node per LINE: KDL terminates a node at the newline, so `id "n" realm "default"` on one
        // line would be one node with three arguments and no realm.
        let text = r#"
node {
    id "n"
    realm "default"
}
data {
    engine "memory"
    path "/tmp/aura-test"
}
resources {
    kind "git" {
        base "https://gitea.s"
        mode "rw"
        secret-env "AURA_GIT_TOKEN"
        desc "the org's repositories"
    }
    kind "object" {
        base "s3://bucket"
    }
}
"#;
        let root = knus::parse::<RootConfig>("test.kdl", text).expect("parses");
        let cfg = crate::EngineConfig::try_from(root).expect("converts");
        assert_eq!(cfg.resources.len(), 2, "one row per kind");
        let git = &cfg.resources["git"];
        assert_eq!(git.base, "https://gitea.s");
        assert_eq!(git.mode, "rw");
        assert_eq!(git.secret_env, "AURA_GIT_TOKEN");
        assert_eq!(
            cfg.resources["object"].mode, "ro",
            "a mode that is not written is read-only — silence never grants a write"
        );
    }
}
