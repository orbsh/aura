//! Resources: the venue's primitive for everything an execution needs besides its arguments
//! (ADR-0048).
//!
//! Three things live here and nothing else: the identity a resource is addressed by, the words a
//! declaration speaks (`kind`, `mode`), and the version arithmetic the registry keys rows with.
//!
//! What deliberately does NOT live here: any transfer. Cloning, fetching, GET/PUT, pulling an image
//! belong to the executor that holds the bytes (effector), and the venue only authorises one
//! transfer at a time — an address plus a one-shot credential, minted per call, read and write
//! separately, so a leaked read credential cannot publish.
//!
//! Keys are NOT here either. A row names the secret it uses (`secret`), and the secret itself sits
//! in a namespace no booth declares, which is why a dumped row hands nobody a key.

use crate::mq::MqStore;
use crate::store_exec::{self, StorePlan};
use aura_booth::{StoreOp, StoreOpKind};
use serde_json::{json, Value};

/// ns numbers (ADR-0040's meta band, extended by ADR-0048).
pub const NS_RESOURCE: u16 = 36;
pub const NS_VERSION: u16 = 37;
pub const NS_SECRET: u16 = 38;

/// The discriminator inside a table: a row is `(disc, number)`.
const RESOURCE: u64 = 1;
const VERSION: u64 = 1;
const SECRET: u64 = 1;

/// The registry: three tables in three framework namespaces.
///
/// One table per ns, because okm's structural stamp is one row per ns — sharing a number would let
/// one table's layout change speak for another's. Secrets get their own ns so that "the code that
/// reads a row cannot read a key" is a schema fact rather than a habit: a booth's plan addresses
/// only its own ns (100+), so none of these tables is reachable from booth code at all.
pub struct Registry {
    store: MqStore,
    resources: StorePlan,
    versions: StorePlan,
    secrets: StorePlan,
}

/// A row as the registry keeps it. `secret` numbers a secret entry; the value never rides a row.
#[derive(Clone, Debug, Default)]
pub struct Row {
    pub desc: String,
    /// The store's address for this resource (a repository URL, a bucket, an image ref).
    pub source: String,
    /// The version to check out by default, when the kind has versions.
    pub at: String,
    /// The secret entry's number, `0` = none.
    pub secret: u64,
    /// A human word for the secret, so a dumped row can be understood without the number.
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
                  "declared_index_base": 4096, "declared_reduce_base": 8192},
    })
}

impl Registry {
    pub fn new(store: MqStore) -> anyhow::Result<Self> {
        let resources = json!({ "storage": { "collections": { "resources": { "schema": schema(
            json!([
                {"name": "mode", "ty": "U64", "width": 8, "offset": 0, "tag": 0},
                {"name": "current", "ty": "U64", "width": 8, "offset": 8, "tag": 0},
                {"name": "secret", "ty": "U64", "width": 8, "offset": 16, "tag": 0},
            ]),
            json!([
                {"name": "owner", "ty": "Str", "width": 0, "offset": 0, "tag": 1},
                {"name": "rkind", "ty": "Str", "width": 0, "offset": 0, "tag": 2},
                {"name": "desc", "ty": "Str", "width": 0, "offset": 0, "tag": 3},
                {"name": "source", "ty": "Str", "width": 0, "offset": 0, "tag": 4},
                {"name": "at", "ty": "Str", "width": 0, "offset": 0, "tag": 5},
                {"name": "secret_name", "ty": "Str", "width": 0, "offset": 0, "tag": 6},
            ]),
        ) } } } });
        let versions = json!({ "storage": { "collections": { "versions": { "schema": schema(
            json!([
                {"name": "seq", "ty": "U64", "width": 8, "offset": 0, "tag": 0},
                {"name": "previous", "ty": "U64", "width": 8, "offset": 8, "tag": 0},
                {"name": "created_at", "ty": "U64", "width": 8, "offset": 16, "tag": 0},
            ]),
            json!([
                {"name": "owner", "ty": "Str", "width": 0, "offset": 0, "tag": 1},
                {"name": "rkind", "ty": "Str", "width": 0, "offset": 0, "tag": 2},
                {"name": "reference", "ty": "Str", "width": 0, "offset": 0, "tag": 3},
                {"name": "call_id", "ty": "Str", "width": 0, "offset": 0, "tag": 4},
            ]),
        ) } } } });
        let secrets = json!({ "storage": { "collections": { "secrets": { "schema": schema(
            json!([]),
            json!([
                {"name": "owner", "ty": "Str", "width": 0, "offset": 0, "tag": 1},
                {"name": "name", "ty": "Str", "width": 0, "offset": 0, "tag": 2},
                {"name": "value", "ty": "Str", "width": 0, "offset": 0, "tag": 3},
            ]),
        ) } } } });
        Ok(Self {
            store: store.clone(),
            resources: StorePlan::from_schema(NS_RESOURCE, &resources)?,
            versions: StorePlan::from_schema(NS_VERSION, &versions)?,
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

    fn row(&self, id: &Id) -> anyhow::Result<Option<Value>> {
        let row = self.exec(
            &self.resources,
            "resources",
            StoreOpKind::GetDocument { key: Self::key(RESOURCE, id.seq) },
        )?;
        Ok(if row.is_null() { None } else { Some(row) })
    }

    /// Declare a resource for an owner. Re-declaring updates the description, never the pointer:
    /// the current version is what publishing writes.
    pub fn declare(&self, id: &Id, row: &Row, mode: Mode, now_ms: u64) -> anyhow::Result<()> {
        let current = self
            .row(id)?
            .and_then(|r| r.get("current").and_then(Value::as_u64))
            .unwrap_or(0);
        self.exec(
            &self.resources,
            "resources",
            StoreOpKind::PutDocument {
                key: Self::key(RESOURCE, id.seq),
                doc: json!({
                    "owner": id.owner, "rkind": id.kind,
                    "desc": row.desc, "source": row.source, "at": row.at,
                    "secret": row.secret, "secret_name": row.secret_name,
                    "mode": match mode { Mode::ReadWrite => 1u64, Mode::ReadOnly => 0u64 },
                    "current": current, "created_at": now_ms,
                }),
            },
        )?;
        Ok(())
    }

    /// Store a key. The value crosses into the store here and is read back only to mint.
    pub fn secret(&self, owner: &str, seq: u64, name: &str, value: &str) -> anyhow::Result<()> {
        self.exec(
            &self.secrets,
            "secrets",
            StoreOpKind::PutDocument {
                key: Self::key(SECRET, seq),
                doc: json!({ "owner": owner, "name": name, "value": value }),
            },
        )?;
        Ok(())
    }

    /// Material for ONE transfer. The owner comparison is the authorization, not a convenience: the
    /// plane passes the owner it derived from the turn, so another account's resource is refused even
    /// if a caller guessed its number.
    pub fn material(&self, id: &Id, write: bool) -> anyhow::Result<Material> {
        let row = self
            .row(id)?
            .ok_or_else(|| anyhow::anyhow!("no resource {id} is declared"))?;
        let text = |name: &str| {
            row.get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        if text("owner") != id.owner {
            anyhow::bail!("resource {id} belongs to another owner");
        }
        let mode = if row.get("mode").and_then(Value::as_u64) == Some(1) {
            Mode::ReadWrite
        } else {
            Mode::ReadOnly
        };
        if write && mode != Mode::ReadWrite {
            // A write against `ro` is an error, not a no-op: "it saved nothing" must not look like
            // it saved something.
            anyhow::bail!("resource {id} is read-only");
        }
        let mut credential = None;
        let secret_seq = row.get("secret").and_then(Value::as_u64).unwrap_or(0);
        if secret_seq != 0 {
            let entry = self.exec(
                &self.secrets,
                "secrets",
                StoreOpKind::GetDocument { key: Self::key(SECRET, secret_seq) },
            )?;
            credential = entry
                .get("value")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .map(str::to_string);
        }
        Ok(Material {
            url: text("source"),
            credential,
            mode,
            at: row
                .get("at")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        })
    }

    /// Record a published version and advance the pointer. The row is written FIRST: a crash between
    /// the two leaves a version nobody points at (harmless) rather than a pointer to a version that
    /// was never recorded.
    pub fn published(
        &self,
        id: &Id,
        call_id: &str,
        reference: &str,
        now_ms: u64,
    ) -> anyhow::Result<(u64, u64)> {
        let row = self
            .row(id)?
            .ok_or_else(|| anyhow::anyhow!("no resource {id} is declared"))?;
        let previous = row.get("current").and_then(Value::as_u64).unwrap_or(0);
        let version = next_version(previous, now_ms);
        self.exec(
            &self.versions,
            "versions",
            StoreOpKind::PutDocument {
                key: Self::key(VERSION, version),
                doc: json!({
                    "seq": id.seq, "previous": previous, "created_at": now_ms,
                    "owner": id.owner, "rkind": id.kind,
                    "reference": reference, "call_id": call_id,
                }),
            },
        )?;
        self.exec(
            &self.resources,
            "resources",
            StoreOpKind::PutFields {
                key: Self::key(RESOURCE, id.seq),
                fields: json!({ "current": version }),
            },
        )?;
        Ok((version, previous))
    }
}

use std::fmt;

/// The kinds the venue knows. A kind is an IDENTITY, not an axis: the same repository delivered by
/// `git` and as a `rustic` snapshot are two resources, because they are two things an execution can
/// ask for and get different answers to.
pub const KINDS: [&str; 5] = ["git", "volume", "s3", "rustic", "image"];

/// A resource's identity: whose it is, what kind it is, which one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Id {
    /// The account that owns it. Stamped by the venue from the turn's envelope — a task never names
    /// its owner, because a task that could would borrow another account's data.
    pub owner: String,
    pub kind: String,
    /// Unique within (owner, kind). The owner's own numbering: a resource outlives every
    /// activation that touches it, so its name cannot come from one.
    pub seq: u64,
}

impl Id {
    /// Validate the parts that are allowed to be text. Both are plain names: a separator would put
    /// the data somewhere nobody asked for, and a resource is identified, never searched.
    pub fn new(owner: &str, kind: &str, seq: u64) -> anyhow::Result<Self> {
        for (what, value) in [("owner", owner), ("kind", kind)] {
            if value.is_empty()
                || value.contains('/')
                || value.contains('\\')
                || value == "."
                || value == ".."
                || value.contains('\u{1f}')
            {
                anyhow::bail!("resource {what} `{value}` is not a plain name");
            }
        }
        if !KINDS.contains(&kind) {
            // Refused by NAME, at the declaration, before any call runs: a venue that cannot serve
            // a kind says so rather than failing halfway through a transfer.
            anyhow::bail!("this venue cannot serve kind `{kind}` (kinds: {KINDS:?})");
        }
        Ok(Self {
            owner: owner.to_string(),
            kind: kind.to_string(),
            seq,
        })
    }

    /// The key a row is stored under. Owner first — "everything I own" is the one query that has
    /// to be a prefix scan, and it is also the only field that carries an authorization boundary.
    pub fn key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        for part in [self.owner.as_str(), self.kind.as_str()] {
            key.extend_from_slice(part.as_bytes());
            key.push(0x1f);
        }
        // Big endian: byte order is numeric order, so a range walk is a sequence walk.
        key.extend_from_slice(&self.seq.to_be_bytes());
        key
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.owner, self.kind, self.seq)
    }
}

/// Whether an execution may publish. `ro` is the default everywhere: omitting a mode never grants a
/// write, and a write against `ro` is an error rather than a silent no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
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

/// What one transfer is authorised with. Minted per call and per half: the read material cannot
/// publish, which is what makes a leaked read credential harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Material {
    /// Where the bytes are, as the executor needs it (a repository URL, a signed URL, an image ref,
    /// a directory). Never a credential-bearing URL unless the mint signed it.
    pub url: String,
    /// The one-shot secret, if this kind needs one. The venue holds the long-lived key; this value
    /// lives in the node's memory for one transfer.
    pub credential: Option<String>,
    pub mode: Mode,
    /// The version to check out, when the caller asked for one.
    pub at: Option<String>,
}

/// The version id a publish gets: 41 bits of milliseconds over 23 of a counter, so byte order is
/// time order, the width is fixed, and it is a number a declared `U64` key can hold.
pub fn next_version(previous: u64, now_ms: u64) -> u64 {
    let counter = (previous & 0x7f_ffff).wrapping_add(1) & 0x7f_ffff;
    (now_ms << 23) | counter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identity_is_a_plain_name_and_a_known_kind() {
        assert!(Id::new("alice", "git", 7).is_ok());
        for bad in ["", "a/b", "..", "a\u{1f}b"] {
            assert!(Id::new("alice", bad, 7).is_err(), "kind `{bad}` must be refused");
            assert!(Id::new(bad, "git", 7).is_err(), "owner `{bad}` must be refused");
        }
        let err = Id::new("alice", "table", 7).unwrap_err().to_string();
        assert!(err.contains("table"), "the refusal names the kind: {err}");
    }

    #[test]
    fn the_key_orders_by_owner_kind_and_then_by_number() {
        let two = Id::new("alice", "git", 2).unwrap();
        let ten = Id::new("alice", "git", 10).unwrap();
        let other = Id::new("bob", "git", 1).unwrap();
        // Big-endian widths: 2 sorts before 10 even though "10" sorts before "2" as text.
        assert!(two.key() < ten.key());
        assert!(ten.key() < other.key(), "owners do not share a prefix");
        let prefix = [&b"alice"[..], &[0x1f], &b"git"[..], &[0x1f]].concat();
        assert!(two.key().starts_with(&prefix));
    }

    #[test]
    fn a_mode_is_read_only_unless_it_says_otherwise_and_unknown_modes_are_refused() {
        assert_eq!(Mode::parse("ro").unwrap(), Mode::ReadOnly);
        assert_eq!(Mode::parse("rw").unwrap(), Mode::ReadWrite);
        assert!(Mode::parse("readwrite").is_err());
    }

    #[test]
    fn a_resource_is_declared_minted_and_published_against_the_store() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let id = Id::new("alice", "git", 7).unwrap();
        registry
            .secret("alice", 1, "gitea-token", "s3cr3t")
            .expect("the key is stored");
        let row = Row {
            desc: "notes".into(),
            source: "https://gitea.s/alice/notes.git".into(),
            at: "main".into(),
            secret: 1,
            secret_name: "gitea-token".into(),
        };
        registry
            .declare(&id, &row, Mode::ReadWrite, 1_700_000_000_000)
            .expect("the row is stored");

        // The read half: address + the key, minted per call.
        let material = registry.material(&id, false).expect("minted");
        assert_eq!(material.url, "https://gitea.s/alice/notes.git");
        assert_eq!(material.credential.as_deref(), Some("s3cr3t"));
        assert_eq!(material.mode, Mode::ReadWrite);

        // Another owner's number is not another owner's resource.
        let thief = Id::new("bob", "git", 7).unwrap();
        assert!(registry.material(&thief, false).is_err());

        // Publishing records a version and moves the pointer; the history is a chain.
        let (first, previous) = registry
            .published(&id, "call-1", "main@abc", 1_700_000_000_500)
            .expect("published");
        assert_eq!(previous, 0);
        let (second, previous) = registry
            .published(&id, "call-2", "main@def", 1_700_000_000_500)
            .expect("published");
        assert_eq!(previous, first);
        assert!(second > first, "the second publish is a later version");
        let row = registry.row(&id).unwrap().expect("the row is still there");
        assert_eq!(row["current"].as_u64(), Some(second));
    }

    #[test]
    fn a_read_only_resource_refuses_a_write_halves_material() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let id = Id::new("alice", "s3", 1).unwrap();
        registry
            .declare(
                &id,
                &Row { desc: "inputs".into(), source: "s3://alice/in".into(), ..Default::default() },
                Mode::ReadOnly,
                0,
            )
            .expect("declared");
        assert!(registry.material(&id, false).is_ok());
        let err = registry.material(&id, true).unwrap_err().to_string();
        assert!(err.contains("read-only"), "{err}");
    }

    #[test]
    fn versions_order_by_time_and_never_repeat_inside_a_millisecond() {
        let ms = 1_700_000_000_000;
        let first = next_version(0, ms);
        let second = next_version(first, ms);
        assert!(second > first, "the counter orders within a millisecond");
        assert!(next_version(second, ms + 1) > second, "the clock orders across them");
        assert_eq!(first >> 23, ms);
    }
}