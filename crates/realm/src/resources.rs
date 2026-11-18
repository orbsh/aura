//! Resources: the venue's primitive for the durable data an execution needs (ADR-0048, revised by
//! ADR-0049 — a resource is a GRANT, not a snapshot).
//!
//! Two things live here and nothing else: a grant per kind (where the bytes are reachable and on
//! what terms) and the credential that reaches them.
//!
//! What deliberately does NOT live here:
//!
//! - **Any transfer.** Cloning, GET/PUT, `rustic restore/backup` belong to the node that holds the
//!   bytes (effector). The venue hands over an address and a credential; it never moves data.
//! - **Any version.** A store already keeps its own history — a git ref, an object version, a rustic
//!   snapshot — and a pointer kept here would be a second copy of the truth that can disagree with
//!   the first one. The reference a publish produces goes back to the caller, not into a table.
//! - **Any per-resource row.** A script names WHAT it wants (`remote`) under the grant's base, the
//!   way an S3 client names a key. Registering every object would make the registry a slower bucket.
//!
//! The layout is one bucket for everybody, separated by a prefix this code composes:
//! `<base>/<tenant>/<kind>/<remote>`. The prefix is not a security boundary — a credential reaches
//! the whole store — it is what keeps two tenants' files from colliding, and it exists because the
//! node builds the path, not because the caller asked for it.

use crate::mq::MqStore;
use crate::store_exec::{self, StorePlan};
use aura_booth::{StoreOp, StoreOpKind};
use serde_json::{json, Value};

/// ns numbers (ADR-0040's meta band). `37` was the version table and is retired with ADR-0049.
pub const NS_GRANT: u16 = 36;
pub const NS_SECRET: u16 = 38;

/// The discriminator inside a table: a row is `(disc, number)`.
const GRANT: u64 = 1;
const SECRET: u64 = 1;

/// The kinds this venue serves. A kind is an IDENTITY, and it names the MECHANISM the node uses, not
/// the store the bytes happen to sit in: `git` keeps history and answers a `branch@sha`, `object` is
/// one blob and answers an ETag, `archive` is an incremental repository (rustic, or anything else
/// that keeps snapshots in a store) and answers a snapshot id.
///
/// `image` is not here: what a container IS belongs to the environment the node declares, not to the
/// data a script reads. `volume` is not here either: "a directory that outlives the call" is a
/// node-local cache policy, not something a row can describe.
pub const KINDS: [&str; 3] = ["git", "object", "archive"];

/// The registry: two tables in two framework namespaces.
///
/// One table per ns, because okm's structural stamp is one row per ns — sharing a number would let
/// one table's layout change speak for another's. Secrets get their own ns so that "the code that
/// reads a grant cannot read a key" is a schema fact rather than a habit.
#[derive(Clone)]
pub struct Registry {
    store: MqStore,
    grants: StorePlan,
    secrets: StorePlan,
}

/// A grant as an operator declares it: where the bytes are, on what terms, and under which key.
#[derive(Clone, Debug, Default)]
pub struct Grant {
    pub desc: String,
    /// The store root this kind lives under — a host for `git`, a bucket for `object`/`archive`.
    /// NOT the tenant's slice: this code appends `<tenant>/<kind>/`.
    pub base: String,
    pub mode: Mode,
    /// A human word for the credential, so a dumped grant can be understood without a number.
    pub secret_name: String,
}

fn schema(hot: Value, cold: Value) -> Value {
    // Every number is hot and fixed width (`U64`), so the segment's width IS its field count: a
    // table with no numbers has no hot segment at all, and saying 24 for it is a truncated row.
    let hot_width = hot.as_array().map_or(0, |fields| fields.len() as u64 * 8);
    json!({
        "key_len": 16,
        "key_fields": [
            {"name": "kind", "ty": "U64", "width": 8, "offset": 0, "tag": 0},
            {"name": "id", "ty": "U64", "width": 8, "offset": 8, "tag": 0},
        ],
        "layout_version": 1,
        "hot_width": hot_width,
        "payload_header_len": 3,
        "hot_fields": hot,
        "cold_fields": cold,
        "slots": {"primary": 0, "dynamic": 1, "dict_id": 2, "dict_name": 3,
                  "declared_index_base": 16385, "declared_reduce_base": 20481},
    })
}

/// A plain name: something that can sit inside a path without changing where it points. Anything
/// else is refused where it is handed over rather than escaped later.
fn plain_name(what: &str, value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
        || value.contains('\u{1f}')
    {
        anyhow::bail!("{what} `{value}` is not a plain name");
    }
    Ok(())
}

impl Registry {
    pub fn new(store: MqStore) -> anyhow::Result<Self> {
        let grants = json!({ "storage": { "collections": { "grants": { "schema": schema(
            json!([
                {"name": "mode", "ty": "U64", "width": 8, "offset": 0, "tag": 0},
                {"name": "secret", "ty": "U64", "width": 8, "offset": 8, "tag": 0},
            ]),
            json!([
                // Not `kind`: the key field already owns that name, and a cold field with the same
                // name is read as the key's value ("expected string", from a number).
                {"name": "rkind", "ty": "Str", "width": 0, "offset": 0, "tag": 1},
                {"name": "scope", "ty": "Str", "width": 0, "offset": 0, "tag": 2},
                {"name": "desc", "ty": "Str", "width": 0, "offset": 0, "tag": 3},
                {"name": "base", "ty": "Str", "width": 0, "offset": 0, "tag": 4},
                {"name": "secret_name", "ty": "Str", "width": 0, "offset": 0, "tag": 5},
            ]),
        ) } } } });
        let secrets = json!({ "storage": { "collections": { "secrets": { "schema": schema(
            json!([]),
            json!([
                {"name": "rkind", "ty": "Str", "width": 0, "offset": 0, "tag": 1},
                {"name": "value", "ty": "Str", "width": 0, "offset": 0, "tag": 2},
            ]),
        ) } } } });
        Ok(Self {
            store: store.clone(),
            grants: StorePlan::from_schema(NS_GRANT, &grants)?,
            secrets: StorePlan::from_schema(NS_SECRET, &secrets)?,
        })
    }

    fn exec(&self, plan: &StorePlan, collection: &str, op: StoreOpKind) -> anyhow::Result<Value> {
        store_exec::execute(
            &self.store,
            plan,
            &StoreOp { collection: collection.to_string(), op },
        )
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    fn key(disc: u64, id: u64) -> Value {
        json!({ "kind": disc, "id": id })
    }

    /// A kind's number in the key. The list is the vocabulary, so its position IS the number and a
    /// kind that is not in it has no row to look for.
    fn kind_no(kind: &str) -> anyhow::Result<u64> {
        KINDS
            .iter()
            .position(|known| *known == kind)
            .map(|at| at as u64 + 1)
            .ok_or_else(|| {
                anyhow::anyhow!("this venue cannot serve kind `{kind}` (kinds: {KINDS:?})")
            })
    }

    /// The grant as stored, for a management surface that has to show it. The credential is NOT part
    /// of it — a grant names its key, and reading the grant must never be reading the key.
    pub fn grant(&self, kind: &str) -> anyhow::Result<Option<Value>> {
        let row = self.exec(
            &self.grants,
            "grants",
            StoreOpKind::GetDocument { key: Self::key(GRANT, Self::kind_no(kind)?) },
        )?;
        Ok(if row.is_null() { None } else { Some(row) })
    }

    /// Declare (or re-declare) what this venue offers for one kind.
    pub fn declare(&self, kind: &str, grant: &Grant, now_ms: u64) -> anyhow::Result<()> {
        let kind_no = Self::kind_no(kind)?;
        if grant.base.trim().is_empty() {
            // Nothing can be reached through a grant with no root: refused here, at the
            // declaration, rather than as a failed transfer on a machine hours later.
            anyhow::bail!("a `{kind}` grant needs a base");
        }
        if grant.base.contains("..") {
            anyhow::bail!("a `{kind}` base `{}` may not climb", grant.base);
        }
        self.exec(
            &self.grants,
            "grants",
            StoreOpKind::PutDocument {
                key: Self::key(GRANT, kind_no),
                doc: json!({
                    "rkind": kind,
                    // One bucket serves every tenant; the scope says so, and a later per-tenant
                    // override would be a second row with the tenant in this field.
                    "scope": "*",
                    "desc": grant.desc,
                    "base": grant.base.trim_end_matches('/'),
                    "secret": if grant.secret_name.is_empty() { 0 } else { 1 },
                    "secret_name": grant.secret_name,
                    "mode": match grant.mode { Mode::ReadWrite => 1u64, Mode::ReadOnly => 0u64 },
                    "created_at": now_ms,
                }),
            },
        )?;
        Ok(())
    }

    /// Every kind that has a row, in the vocabulary's order. The listing's shape is the store's own
    /// (whatever [`Self::grant`] returns), so a management surface shows exactly what is stored
    /// rather than a re-typed view that can drift from it.
    pub fn declared(&self) -> anyhow::Result<Vec<(String, Value)>> {
        let mut out = Vec::new();
        for kind in KINDS {
            if let Some(row) = self.grant(kind)? {
                out.push((kind.to_string(), row));
            }
        }
        Ok(out)
    }

    /// Withdraw a kind: BOTH rows. Deleting the grant but keeping the credential would leave a key
    /// nobody can reach through — and a key that a later re-declare would silently inherit.
    pub fn revoke(&self, kind: &str) -> anyhow::Result<()> {
        let kind_no = Self::kind_no(kind)?;
        self.exec(
            &self.grants,
            "grants",
            StoreOpKind::DeleteDocument { key: Self::key(GRANT, kind_no) },
        )?;
        self.exec(
            &self.secrets,
            "secrets",
            StoreOpKind::DeleteDocument { key: Self::key(SECRET, kind_no) },
        )?;
        Ok(())
    }

    /// Store the credential for one kind. The value crosses into the store here and is read back
    /// only to mint.
    pub fn secret(&self, kind: &str, value: &str) -> anyhow::Result<()> {
        let kind_no = Self::kind_no(kind)?;
        self.exec(
            &self.secrets,
            "secrets",
            StoreOpKind::PutDocument {
                key: Self::key(SECRET, kind_no),
                doc: json!({ "rkind": kind, "value": value }),
            },
        )?;
        Ok(())
    }

    /// What an execution gets for one kind: the root IT may reach (already separated by tenant),
    /// the credential, and the terms. The tenant comes from the plane, derived from the turn — a
    /// script that could name another tenant could write into somebody else's slice.
    pub fn material(&self, tenant: &str, kind: &str, write: bool) -> anyhow::Result<Material> {
        plain_name("tenant", tenant)?;
        let kind_no = Self::kind_no(kind)?;
        let row = self
            .grant(kind)?
            .ok_or_else(|| anyhow::anyhow!("no `{kind}` grant is declared"))?;
        let text = |name: &str| {
            row.get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let mode = if row.get("mode").and_then(Value::as_u64) == Some(1) {
            Mode::ReadWrite
        } else {
            Mode::ReadOnly
        };
        if write && mode != Mode::ReadWrite {
            // A write against `ro` is an error, not a no-op: "it saved nothing" must not look like
            // it saved something.
            anyhow::bail!("the `{kind}` grant is read-only");
        }
        let mut credential = None;
        if row.get("secret").and_then(Value::as_u64).unwrap_or(0) != 0 {
            let entry = self.exec(
                &self.secrets,
                "secrets",
                StoreOpKind::GetDocument { key: Self::key(SECRET, kind_no) },
            )?;
            credential = entry
                .get("value")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_string);
        }
        Ok(Material {
            base: format!("{}/{}/{}", text("base"), tenant, kind),
            credential,
            mode,
        })
    }
}

/// Whether an execution may publish. `ro` is the default everywhere: omitting a mode never grants a
/// write, and a write against `ro` is an error rather than a silent no-op.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    ReadOnly,
    ReadWrite,
}

impl Mode {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        match text {
            "ro" => Ok(Mode::ReadOnly),
            "rw" => Ok(Mode::ReadWrite),
            other => anyhow::bail!("`{other}` is not a mode (ro or rw)"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::ReadOnly => "ro",
            Mode::ReadWrite => "rw",
        }
    }
}

/// What one transfer is handed: a root to join the caller's own name onto, the credential for it, and
/// the terms. Held by the node for the length of the transfer and never passed to the script.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Material {
    /// The effective root: `<base>/<tenant>/<kind>`. The node appends the caller's `remote` to it.
    pub base: String,
    /// The credential, if this kind needs one.
    pub credential: Option<String>,
    pub mode: Mode,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(base: &str, mode: Mode) -> Grant {
        Grant {
            desc: String::new(),
            base: base.into(),
            mode,
            secret_name: if mode == Mode::ReadWrite { "s3".into() } else { String::new() },
        }
    }

    #[test]
    fn the_kinds_are_three_mechanisms_and_an_unknown_one_is_refused_by_name() {
        assert_eq!(KINDS, ["git", "object", "archive"]);
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let err = registry
            .declare("image", &grant("s3://bucket", Mode::ReadOnly), 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("image"), "the refusal names the kind: {err}");
    }

    #[test]
    fn a_tenant_cannot_climb_out_of_its_own_slice() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .declare("object", &grant("s3://bucket", Mode::ReadWrite), 1)
            .expect("declared");
        for bad in ["", "..", "a/b", "a\\b"] {
            let err = registry.material(bad, "object", false).unwrap_err().to_string();
            assert!(err.contains("plain name"), "tenant `{bad}` is refused: {err}");
        }
    }

    #[test]
    fn the_root_a_caller_reaches_is_its_own_slice_of_the_shared_base() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .secret("object", "s3cr3t")
            .expect("the credential is stored");
        registry
            .declare("object", &grant("s3://bucket/", Mode::ReadWrite), 1)
            .expect("declared");

        let material = registry.material("alice", "object", false).expect("minted");
        assert_eq!(
            material.base, "s3://bucket/alice/object",
            "the prefix is composed by this side, not named by the caller"
        );
        assert_eq!(material.credential.as_deref(), Some("s3cr3t"));
        assert_eq!(material.mode, Mode::ReadWrite);

        let other = registry.material("bob", "object", false).expect("minted");
        assert_ne!(other.base, material.base, "two tenants never share a slice");
    }

    #[test]
    fn a_read_only_grant_refuses_the_write_half_by_name() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .declare("git", &grant("https://gitea.s", Mode::ReadOnly), 1)
            .expect("declared");
        assert!(registry.material("alice", "git", false).is_ok());
        let err = registry.material("alice", "git", true).unwrap_err().to_string();
        assert!(err.contains("read-only"), "{err}");
    }

    #[test]
    fn a_grant_that_reaches_nowhere_is_refused_at_the_declaration() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let err = registry
            .declare("archive", &grant("  ", Mode::ReadOnly), 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("needs a base"), "{err}");
        let err = registry
            .declare("archive", &grant("s3://bucket/../etc", Mode::ReadOnly), 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("climb"), "{err}");
    }

    #[test]
    fn a_kind_with_no_grant_says_so_instead_of_inventing_a_root() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let err = registry.material("alice", "git", false).unwrap_err().to_string();
        assert!(err.contains("no `git` grant"), "{err}");
        assert!(registry.grant("git").unwrap().is_none());
    }

    #[test]
    fn a_mode_is_read_only_unless_it_says_otherwise_and_unknown_modes_are_refused() {
        assert_eq!(Mode::parse("ro").unwrap(), Mode::ReadOnly);
        assert_eq!(Mode::parse("rw").unwrap(), Mode::ReadWrite);
        assert!(Mode::parse("readwrite").is_err());
        assert_eq!(Mode::default(), Mode::ReadOnly);
    }
}