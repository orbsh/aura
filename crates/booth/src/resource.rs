//! The `ctx.resource` protocol (ADR-0048, revised by ADR-0049 — a resource is a GRANT, not a
//! snapshot): durable data asked for at RUN time. A booth's need is not knowable before it runs, so
//! nothing rides the call — the script names a kind and a remote name when it gets there.
//!
//! WHO answers is the only difference between a node and the venue. Over the wire the plane mints the
//! grant and the NODE moves the bytes; in-process the venue is both (`effector` ADR-0003 §1: the node
//! is what contains the code, and the venue contains its own code). So this file declares the ASK and
//! nothing else — no transport, no credential, no path policy. Each host answers with what it is.
//!
//! The credential never crosses this boundary: the script is told a path on the machine that will hold
//! the bytes, never the key that reached them. That is the same rule as `EFFECTOR_RESOURCE_CREDENTIAL`
//! on the node side, and it is why `ask` returns one string rather than a struct.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One ask, as a script makes it: `{op, kind, remote, mount?, ref?}`.
///
/// `op` is one of two verbs and they are not symmetric: `get` brings the data here (`mount` may name
/// where), `publish` sends what is here back and answers with the store's own reference. There is no
/// third verb — a resource is read or published, never "listed" or "deleted", because the registry is
/// not a bucket listing and a store already has its own tools.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ResourceOp {
    /// `get` or `publish`. Defaulted so a missing field is refused by THIS file's words rather than
    /// serde's ("missing field `op`"), which name the wrong party: the script forgot a name, and the
    /// sentence should say which.
    #[serde(default)]
    pub op: String,
    /// The mechanism: `git`, `object` or `archive` (the venue's `KINDS`).
    #[serde(default)]
    pub kind: String,
    /// The caller's own name under the grant's base, the way an S3 client names a key.
    #[serde(default)]
    pub remote: String,
    /// Where to put it locally. `None` = `<root>/<remote>`.
    #[serde(default)]
    pub mount: Option<String>,
    /// For `git`: the ref to land on or start from. Empty and absent mean the same thing.
    #[serde(default, rename = "ref")]
    pub at: Option<String>,
}

impl ResourceOp {
    /// Parse the host-bridge argument. The refusals name the field a script forgot, because the script
    /// is the party that can fix it.
    pub fn parse(arg: &Value) -> anyhow::Result<Self> {
        let op: Self = serde_json::from_value(arg.clone())
            .map_err(|e| anyhow::anyhow!("ctx.resource: malformed op: {e}"))?;
        if op.op.is_empty() || op.kind.is_empty() || op.remote.is_empty() {
            anyhow::bail!("a resource op names `op` (`get` or `publish`), `kind` and `remote`");
        }
        // `write` is answered here so an unknown verb is refused before anything is minted.
        op.write()?;
        Ok(op)
    }

    /// Whether this ask wants to publish. An unknown verb is refused by name: it is not "not a get",
    /// it is a mistake the script should see.
    pub fn write(&self) -> anyhow::Result<bool> {
        match self.op.as_str() {
            "get" => Ok(false),
            "publish" => Ok(true),
            other => anyhow::bail!(
                "`{other}` is not a resource op; a resource is `get` or `publish`"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_verbs_and_nothing_else() {
        let get = ResourceOp::parse(&serde_json::json!({
            "op": "get", "kind": "git", "remote": "demo"
        }))
        .expect("a get parses");
        assert!(!get.write().unwrap());
        assert!(get.mount.is_none() && get.at.is_none());

        let publish = ResourceOp::parse(&serde_json::json!({
            "op": "publish", "kind": "object", "remote": "a/b.txt", "ref": "main"
        }))
        .expect("a publish parses");
        assert!(publish.write().unwrap());
        assert_eq!(publish.at.as_deref(), Some("main"));

        let err = ResourceOp::parse(&serde_json::json!({
            "op": "list", "kind": "object", "remote": "a"
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("not a resource op"), "{err}");
    }

    #[test]
    fn a_missing_name_is_refused_before_anything_is_minted() {
        let err = ResourceOp::parse(&serde_json::json!({ "op": "get", "kind": "git" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("names `op`"), "{err}");
    }
}