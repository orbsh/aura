//! Root configuration, `aura.kdl` (KDL via knus). One top-level node per
//! subsystem; the file never holds secrets — it names environment
//! variables, resolved at call time (krystallizer ADR-0007 pattern).
//!
//! ```kdl
//! node {
//!     id "home-node"
//!     namespace "default"
//! }
//!
//! // data plane: actor state
//! data {
//!     engine "fjall"
//!     path "/var/lib/aura/data"
//! }
//!
//! // metadata plane: registry, shard map
//! meta {
//!     engine "fjall"
//!     path "/var/lib/aura/meta"
//! }
//! ```

use knus::Decode;

#[derive(Decode, Debug, Clone)]
pub struct RootConfig {
    /// `node { ... }` — node identity.
    #[knus(child)]
    pub node: NodeConfig,
    /// `data { ... }` — the data plane (actor state).
    #[knus(child)]
    pub data: StorageConfig,
    /// `meta { ... }` — the metadata plane (registry, shard map).
    #[knus(child)]
    pub meta: StorageConfig,
}

#[derive(Decode, Debug, Clone)]
pub struct NodeConfig {
    #[knus(child, unwrap(argument))]
    pub id: String,
    #[knus(child, unwrap(argument))]
    pub namespace: String,
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
    Io { path: String, source: std::io::Error },
    /// miette Report: knus errors are Diagnostics (rich spans), not
    /// std::error::Error — same wrapping as krystallizer's config.
    Parse { path: String, report: miette::Report },
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
