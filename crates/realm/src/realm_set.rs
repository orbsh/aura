//! Realms: identity, the `realms` table, and the set of live handles
//! (ADR-0050 — a realm is a row: an id in the key, a name in a table).
//!
//! A realm used to BE a prefix: `[u16 BE len][name]` prepended to every key,
//! with the default realm the unprefixed remainder of the keyspace. That made
//! the name the identity AND the location, gave the system realm no row, and
//! left "destroy this realm" with no handle to hold. ADR-0050 settles all
//! three: the id is what lives in the key (`[u32 BE id]`), the name is a
//! column in ONE fixed-root table, and `main` is an ordinary persisted realm
//! in the set — there is no unprefixed tier left to write into.
//!
//! Layout:
//!
//! - `realms` (root ns 256) — `id → (name, created_at)`, with a `by_name`
//!   text index for the direction opening needs (`name → id`) and a
//!   `HighWater(id)` watermark that issues ids. The watermark is the counter,
//!   and it is a PERSISTED ROW of this table: it survives a restart and its
//!   unfold is a no-op, so a destroyed realm's number is retired even though
//!   its row is gone (ADR-0050 §Consequences).
//! - every realm's own tables — `[u32 BE id][ns 2B][slot]…`, so one realm's
//!   whole slice is the contiguous key range under its four id bytes.
//! - TEMPORARY realms are the same kind of thing with two differences
//!   (ADR-0050 §5/§6): their identity is a uuidv7 instead of a table number, and
//!   their store is an in-memory engine of their own instead of a slice of this
//!   keyspace. They hold no row, take no number from the watermark, and are
//!   forgotten by `destroy` without a byte being touched — `create_ephemeral`
//!   and `destroy` are where that asymmetry lives, and it is the only one.
//!
//! Why ns 256 for the root table. Every declared ns is 20–39 plus the booth
//! block above `BOOTH_NS_BASE` (100 + type id), and a booth's ns is only ever
//! bound INSIDE a realm prefix (`MqStore::ns_raw` puts the realm segment
//! first) — so at the root the only table that can appear is the root band's.
//! 256 is above every framework band and above the reserved run (28–99), and
//! its two header bytes `[0x01, 0x00]` cannot be the first two bytes of a
//! realm prefix for any id under 2^24 — a raw dump can therefore tell a root
//! row from a realm row at a glance, which is the currency ADR-0050 pays for
//! a four-byte prefix.

use crate::mq::MqStore;
use crate::realm_id::RealmId;
use okm_core::document::Collection;
use okm_core::storage::VirtualStorage;
use okm_core::{Document, DocumentEncode, KeyEncode, ReduceCodec};
use std::collections::HashMap;

/// The name of the realm a boot opens (ADR-0050 §4): the old default realm,
/// now an ordinary persisted realm with a row.
pub const MAIN_REALM: &str = "main";

/// The `realms` table's ns — the root band, outside every realm prefix.
/// The literal below must equal this constant (a derive cannot name it).
pub const REALMS_NS: u16 = 256;

// ---------------------------------------------------------------------------
// The realms table: id → (name, created_at), the name index, the id watermark.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct RealmRowKey {
    pub id: u32,
}

/// One persisted realm. The row IS the realm's existence: a realm with no row
/// is not openable by name, and `destroy` deletes this row before it deletes
/// the prefix range.
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(RealmRowKey)]
#[ok_index(by_name { fields(name) })]
#[ok_reduce(HighWater(id) { group(global) })]
#[ok_ns(256)]
pub struct RealmRow {
    pub name: String,
    /// Payload mirror of the key's id: index/reduce fields are payload fields
    /// (the key is not one), the same pattern the meta plane's `BoothName`
    /// uses for its own registry.
    pub id: u32,
    /// Wall-clock ms at creation. Observability only — nothing orders by it
    /// and nothing expires on it (ids order, and they never repeat).
    pub created_at: u64,
    /// Single-group discriminator (always 0): the derive rejects an empty
    /// group list, so the table-wide watermark declares a constant group.
    pub global: u32,
}

use __OkmIndex_RealmRow_by_name as RealmRowByName;

type RealmTable = Collection<MqStore, RealmRowKey, RealmRow>;

/// The table over a store. The store is the ROOT handle: this table exists
/// once per engine, not once per realm (ADR-0050 §3 — a fixed root).
fn table(root: &MqStore) -> RealmTable {
    Collection::new(root.clone())
}

/// The id a name already has (`None` = never opened). Exact match = index
/// prefix scan + row verify, the text-first regime's documented cost: no
/// delimiter, so "main" scans "main-old" too, and the row comparison is the
/// exactness.
fn id_of_name(root: &MqStore, name: &str) -> anyhow::Result<Option<u32>> {
    let t = table(root);
    for hit in t.scan::<RealmRowByName>(name.as_bytes()) {
        let (_, pk, row) = hit?;
        if let Some(row) = &row {
            if row.name == name {
                return Ok(Some(pk.decoded.id));
            }
        }
    }
    Ok(None)
}

/// The name an id has (`None` = no such row).
fn name_of_id(root: &MqStore, id: u32) -> anyhow::Result<Option<String>> {
    let t = table(root);
    Ok(t.get_row(&RealmRowKey { id })?.map(|row| row.name))
}

/// The next id to issue: the table's `HighWater(id)` watermark + 1.
///
/// The watermark is where the counter lives, and that is the answer to "a
/// restart must not recycle an id": it is a row in this table (not a process
/// counter), it only ever folds upward, and its unfold is a no-op — deleting
/// the realm with the highest id leaves the watermark exactly where it was.
fn next_id(root: &MqStore) -> anyhow::Result<u32> {
    let t = table(root);
    let watermark = okm_core::reduce_get::<MqStore, __OkmReduce_RealmRow_0>(
        t.store(),
        <RealmRow as Document>::NS_PREFIX,
        &RealmRowKey { id: 0 },
        &RealmRow {
            name: String::new(),
            id: 0,
            created_at: 0,
            global: 0,
        },
    )?
    .unwrap_or(0);
    let next = watermark
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("realm-id watermark is exhausted at {watermark}"))?;
    u32::try_from(next).map_err(|_| {
        anyhow::anyhow!("realm ids are exhausted at {watermark} (u32 space spent) — ids are never reused")
    })
}

/// Wall-clock milliseconds. A clock that cannot be read before 1970 dates the
/// row 0: this field is observability, never a decision input.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// The set.
// ---------------------------------------------------------------------------

/// The realm set: id → live realm, plus the root handle the prefixes are cut
/// from. Engine holds this; every surface resolves its realm once and from
/// then on only ever touches that realm's objects.
pub struct RealmSet {
    /// Live realms by ID (ADR-0050 §3): the memory half of the lookup order —
    /// ephemeral realms are by definition only ever found here.
    live: tokio::sync::Mutex<HashMap<RealmId, Live>>,
    /// The ROOT store every realm's handle prefixes into (ADR-0018 steps 1+2:
    /// mq tables AND state documents ride byte-native tables, never a JSON
    /// store). It also carries this set's own `realms` table.
    root: crate::mq::MqStore,
    /// Code-reference prefix carried into every lazily created realm
    /// (ADR-0027; one EngineConfig value, no per-realm choice).
    code_base_url: Option<String>,
    /// The cursor retention promise carried into every lazily created
    /// realm (ADR-0039 §2; one EngineConfig value, no per-realm choice).
    cursor_ttl: std::time::Duration,
}

/// One live realm: its name (for `NamedRealm`), its prefix-bound store and
/// the handle itself.
struct Live {
    name: String,
    store: crate::mq::MqStore,
    realm: crate::SharedRealm,
}

impl RealmSet {
    /// Full constructor: the shared okm engine (one fjall keyspace).
    pub fn with_mq(mq: crate::mq::MqStore) -> Self {
        Self::with_mq_and_code_base(mq, None)
    }

    /// The shared handle carries the code-reference prefix (ADR-0027)
    /// into every realm lazily created from it.
    pub fn with_mq_and_code_base(mq: crate::mq::MqStore, code_base_url: Option<String>) -> Self {
        Self {
            live: tokio::sync::Mutex::new(HashMap::new()),
            root: mq,
            code_base_url,
            cursor_ttl: crate::DEFAULT_CURSOR_TTL,
        }
    }

    /// Carry the engine-wide cursor retention promise (ADR-0039 §2) into
    /// every realm created from this set.
    pub fn with_cursor_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.cursor_ttl = ttl;
        self
    }

    /// Open the realm called `name`: find its row, else create one with the
    /// next id, and hand back the live handle (lazy — a realm that is in the
    /// table but not in memory is built on first open).
    ///
    /// The id is the identity, so opening the same name twice answers the
    /// same id even across a restart (the row is what carries it), and a name
    /// that was destroyed is free to take — with a NEW id, never the old one.
    pub async fn open(&self, name: &str) -> anyhow::Result<NamedRealm> {
        let mut live = self.live.lock().await;
        let id = match id_of_name(&self.root, name)? {
            Some(id) => RealmId::Persisted(id),
            // Miss: insert the row FIRST, so a realm exists in storage before
            // anyone holds a handle that can write into its prefix — a
            // handle without a row would be a realm nothing could re-open.
            None => {
                let id = next_id(&self.root)?;
                let row = RealmRow {
                    name: name.to_string(),
                    id,
                    created_at: now_ms(),
                    global: 0,
                };
                table(&self.root).put(&RealmRowKey { id }, &row)?;
                RealmId::Persisted(id)
            }
        };
        // `map_entry` is the usual advice here, and it is not available: the body
        // below AWAITS (it builds the realm and its driver), and an entry cannot be
        // held across an await point. The check-then-insert is therefore the shape,
        // and the map is behind this function's own mutex, so nothing can insert
        // between the two.
        #[allow(clippy::map_entry)]
        if !live.contains_key(&id) {
            // State + mq both ride the realm-prefixed okm engine (ADR-0018
            // step 2): the prefix bound at construction is the isolation
            // boundary; no JSON PrefixStore layer.
            let store = crate::mq::MqStore::for_realm(&self.root, id)?;
            let realm = crate::Realm::with_mq(id, store.clone())?
                .with_code_base_url(self.code_base_url.clone())
                .with_cursor_ttl(self.cursor_ttl)
                .shared_async()
                .await;
            live.insert(
                id,
                Live {
                    name: name.to_string(),
                    store,
                    realm,
                },
            );
        }
        let entry = live
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("realm {id} vanished from the set while opening"))?;
        Ok(NamedRealm {
            id,
            name: entry.name.clone(),
            store: entry.store.clone(),
            realm: entry.realm.clone(),
        })
    }

    /// Create a TEMPORARY realm (ADR-0050 §5/§6, ADR-0051).
    ///
    /// Three things, all of them cheap and none of them a write:
    ///
    /// - the identity is a uuidv7 minted in-process (`RealmId::ephemeral_now`),
    ///   so two temporary realms cannot collide without touching a counter the
    ///   persistent half owns;
    /// - the store is `MqStore::mem()` — an in-memory engine of its OWN, not a
    ///   slice of this set's keyspace. `Realm::with_mq` refuses anything else,
    ///   which is what makes "it wrote nothing" a property instead of a promise;
    /// - the name is recorded so `live()` and a log line can say which realm a
    ///   frame came from, and it is an address to nobody: `id_of` answers from
    ///   the `realms` table, which this realm is not in.
    ///
    /// The `realms` table is untouched — no row, and the id watermark does not
    /// step — so the next persisted realm's id is exactly what it would have
    /// been had this call never happened, and `destroy` has nothing to undo.
    ///
    /// A temporary realm cannot create one of these (ADR-0050 §7): the door is
    /// on the SET, and nothing inside a realm holds a set — nesting is not
    /// expressible, which is why it does not need a check.
    pub async fn create_ephemeral(&self) -> anyhow::Result<NamedRealm> {
        let id = RealmId::ephemeral_now();
        // The id's own rendering, tag included ("ephemeral:0197…"): a name that
        // says which kind of realm it labels, so a dump of `live()` cannot read
        // a temporary realm as a durable one.
        let name = id.to_string();
        let store = crate::mq::MqStore::mem();
        let realm = crate::Realm::with_mq(id, store.clone())?
            .with_code_base_url(self.code_base_url.clone())
            .with_cursor_ttl(self.cursor_ttl)
            .shared_async()
            .await;
        self.live.lock().await.insert(
            id,
            Live {
                name: name.clone(),
                store: store.clone(),
                realm: realm.clone(),
            },
        );
        Ok(NamedRealm {
            id,
            name,
            store,
            realm,
        })
    }

    /// The id a name resolves to (`None` = never opened).
    ///
    /// A PERSISTED realm's home is the table, so the table is the answer — and
    /// the live map would be the same answer, because every live persisted
    /// entry was built from its row. A TEMPORARY realm has no row AND no
    /// address: the name it recorded is a label for the logs (ADR-0050 §5), so
    /// this never answers one, which is what makes `open` of that name mint a
    /// persisted realm instead of handing back a realm that cannot survive the
    /// call. `name_of` (id → name) is the direction that does reach one, and
    /// only while it lives.
    pub async fn id_of(&self, name: &str) -> anyhow::Result<Option<RealmId>> {
        Ok(id_of_name(&self.root, name)?.map(RealmId::Persisted))
    }

    /// The name an id resolves to (`None` = no such realm). Memory first, the
    /// table second — the row's own direction, for ops and for rendering a
    /// dump's short prefixes back into words.
    pub async fn name_of(&self, id: RealmId) -> anyhow::Result<Option<String>> {
        if let Some(name) = self.live.lock().await.get(&id).map(|live| live.name.clone()) {
            return Ok(Some(name));
        }
        match id.persisted() {
            Some(number) => name_of_id(&self.root, number),
            // An ephemeral realm lives nowhere else, so memory not knowing it
            // is the whole answer.
            None => Ok(None),
        }
    }

    /// Realm names known to this engine (observation). The persistent half is
    /// a QUERY over the `realms` table — a realm that exists but is not live
    /// in memory still has a name — plus whatever is in memory, which is the
    /// ephemeral kind's only home: a temporary realm is listed WHILE IT LIVES
    /// and gone from here the moment it is destroyed, because nothing else ever
    /// knew its name.
    pub async fn live(&self) -> anyhow::Result<Vec<String>> {
        let mut names: Vec<String> = Vec::new();
        let t = table(&self.root);
        for (_, payload) in t.scan_documents_raw()? {
            // A document with only a dynamic segment is not a realm row (the
            // write path above always lands the row); nothing in aura can
            // create one, so skipping it is not a fallback.
            let Some(payload) = payload else { continue };
            names.push(RealmRow::decode_payload(&payload).name);
        }
        for entry in self.live.lock().await.values() {
            names.push(entry.name.clone());
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// Destroy a realm (ADR-0050 §Consequences): drop the live Realm —
    /// instances, executors, streams, in-flight calls die with the handle —
    /// then, for the persisted kind, delete its row and its whole prefix
    /// range.
    ///
    /// For a TEMPORARY realm the drop IS the whole of it: it wrote nothing, so
    /// there is no row to delete and no prefix range to purge (ADR-0050 §6,
    /// ADR-0051 §5 — the property the register/run/delete sequence could not
    /// promise). Its name leaves `live()` with the entry, because the entry was
    /// the only thing that ever held it.
    ///
    /// The id is retired, not recycled: the watermark that issued it still
    /// stands, so the next realm gets a new number and cannot inherit this
    /// one's leftovers.
    pub async fn destroy(&self, id: RealmId) -> anyhow::Result<()> {
        self.live.lock().await.remove(&id);
        let Some(number) = id.persisted() else {
            // An ephemeral realm wrote nothing, so its destroy is only the
            // drop above — which is the whole of it.
            return Ok(());
        };
        // The row goes first: from this instant the realm is unreachable by
        // name, and a crash between here and the range delete leaves garbage
        // in a slice nothing can open, rather than a row pointing at a realm
        // whose data is half gone.
        table(&self.root).purge(&RealmRowKey { id: number })?;
        self.delete_prefix(number)?;
        Ok(())
    }

    /// Delete every key under `[u32 BE id]` — the realm's whole slice, table
    /// headers and all, because the prefix is outside them.
    fn delete_prefix(&self, number: u32) -> anyhow::Result<()> {
        let begin = number.to_be_bytes();
        // `id + 1` is the exclusive end; the top of the space has no successor
        // and runs to the end of the keyspace.
        let end: Option<Vec<u8>> = number.checked_add(1).map(|n| n.to_be_bytes().to_vec());
        let engine = self.root.root();
        // Materialize the keys before deleting: a delete during a live range
        // scan is a second reader of the same iterator's window.
        let keys: Vec<Vec<u8>> = engine
            .scan_range(&begin, end.as_deref())
            .map(|item| item.map(|(k, _)| k))
            .collect::<okm_core::Result<Vec<_>>>()?;
        for key in keys {
            engine.del(&key)?;
        }
        Ok(())
    }
}

/// A named realm handle (ADR-0028), carrying the id that is its identity
/// (ADR-0050 §1). Deliberately NOT convertible into another realm's handle:
/// the storage prefix is bound at construction, the same construction-time
/// isolation pattern as okm's prefix-bound hosts (ADR-0010).
#[derive(Clone)]
pub struct NamedRealm {
    /// The name this handle was opened under — or, for a temporary realm, the
    /// label it recorded at creation (`create_ephemeral`). A label is not an
    /// address: `RealmSet::open` of it mints a persisted realm, because a
    /// temporary realm has no row to be found by.
    pub name: String,
    /// The identity — what its keys are prefixed with, and what `destroy`
    /// names it by.
    pub id: RealmId,
    /// The realm's own prefix-bound store (a cheap clone of the handle the
    /// Realm holds).
    store: crate::mq::MqStore,
    realm: crate::SharedRealm,
}

impl NamedRealm {
    pub fn realm(&self) -> crate::SharedRealm {
        self.realm.clone()
    }

    /// The realm's store, prefix-bound: what the meta plane, the durable
    /// timer table and every other owner of "this realm's keys" binds to.
    pub fn store(&self) -> crate::mq::MqStore {
        self.store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_is_idempotent_and_the_name_index_answers_both_ways() {
        let root = MqStore::mem();
        let set = RealmSet::with_mq(root.clone());
        let a = set.open("alpha").await.unwrap();
        assert_eq!(a.id, RealmId::Persisted(1));
        // Same name again: the row's id wins, no second allocation.
        assert_eq!(set.open("alpha").await.unwrap().id, a.id);
        // A new name takes the next number.
        assert_eq!(
            set.open("beta").await.unwrap().id,
            RealmId::Persisted(2)
        );
        // Both directions reachable: name → id (the open path) and
        // id → name (the row).
        assert_eq!(id_of_name(&root, "beta").unwrap(), Some(2));
        assert_eq!(name_of_id(&root, 2).unwrap(), Some("beta".to_string()));
        assert_eq!(name_of_id(&root, 99).unwrap(), None);
        assert_eq!(set.id_of("beta").await.unwrap(), Some(RealmId::Persisted(2)));
        assert_eq!(
            set.name_of(RealmId::Persisted(2)).await.unwrap(),
            Some("beta".to_string())
        );
        // An id that was never issued has no name.
        assert_eq!(set.name_of(RealmId::Persisted(99)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn destroy_retires_the_id_instead_of_recycling_it() {
        let root = MqStore::mem();
        let set = RealmSet::with_mq(root.clone());
        let a = set.open("alpha").await.unwrap();
        // Something in its slice, so the range delete has work to do.
        {
            use okm_core::storage::VirtualStorage;
            a.store().put(b"k".to_vec(), b"v".to_vec()).unwrap();
        }
        assert!(!a.store().prefix().is_empty());

        set.destroy(a.id).await.unwrap();
        // The row is gone …
        assert_eq!(id_of_name(&root, "alpha").unwrap(), None);
        assert_eq!(name_of_id(&root, 1).unwrap(), None);
        // … the slice is gone …
        let left = root.root().scan_suffix(&[0, 0, 0, 1]).unwrap();
        assert!(left.is_empty(), "the realm's prefix range still holds {left:?}");
        assert!(!set.live().await.unwrap().contains(&"alpha".to_string()));
        // … and the name re-opens under a NEW id, never the retired one: the
        // watermark that issued 1 still stands (its unfold is a no-op), so
        // the replacement is 2 even though nothing holds 1 any more.
        let again = set.open("alpha").await.unwrap();
        assert_ne!(again.id, a.id, "an id is retired, never recycled");
        assert_eq!(again.id, RealmId::Persisted(2));
    }

    #[tokio::test]
    async fn two_realms_do_not_see_each_others_tables() {
        let root = MqStore::mem();
        let set = RealmSet::with_mq(root);
        let a = set.open("alpha").await.unwrap();
        let b = set.open("beta").await.unwrap();
        let ta = crate::timer_entry::table(a.store());
        crate::timer_entry::put(
            &ta,
            crate::timer_entry::TimerEntryKey { due_ms: 1, id: 7 },
            "turn",
            "device-a",
            "later",
        )
        .unwrap();
        assert_eq!(crate::timer_entry::all(&ta).unwrap().len(), 1);
        let tb = crate::timer_entry::table(b.store());
        assert!(
            crate::timer_entry::all(&tb).unwrap().is_empty(),
            "a write in one realm's table is absent from the other's"
        );
    }

    #[tokio::test]
    async fn an_ephemeral_id_cannot_be_bound_to_the_engines_keyspace() {
        // A temporary realm's store is an engine of its own (ADR-0050 §5/§6);
        // asking this engine for a slice of ITS keyspace under an ephemeral id
        // is refused by name, because there is no prefix to bind — the failure
        // is the isolation, not a missing feature.
        let root = MqStore::mem();
        let err = match crate::mq::MqStore::for_realm(&root, RealmId::Ephemeral(7)) {
            Ok(_) => panic!("an ephemeral realm must not get a slice of the engine's keyspace"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("in-memory engine of its own"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn creating_a_temporary_realm_writes_no_row_and_does_not_step_the_watermark() {
        let root = MqStore::mem();
        let set = RealmSet::with_mq(root.clone());
        let main = set.open(MAIN_REALM).await.unwrap();
        let before: Vec<Vec<u8>> = root
            .root()
            .scan_range(&[], None)
            .map(|item| item.map(|(k, _)| k))
            .collect::<okm_core::Result<Vec<_>>>()
            .unwrap();

        let eph = set.create_ephemeral().await.unwrap();
        assert!(matches!(eph.id, RealmId::Ephemeral(_)));
        assert!(
            eph.store().is_memory() && eph.store().prefix().is_empty(),
            "a temporary realm's store is memory with no prefix"
        );

        // Byte-for-byte the same keyspace: no `realms` row, no watermark step.
        let after: Vec<Vec<u8>> = root
            .root()
            .scan_range(&[], None)
            .map(|item| item.map(|(k, _)| k))
            .collect::<okm_core::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(before, after, "creating a temporary realm wrote something");
        // …so the next persisted realm still gets the number it would have had.
        assert_eq!(main.id, RealmId::Persisted(1));
        assert_eq!(
            set.open("next").await.unwrap().id,
            RealmId::Persisted(2),
            "a temporary realm is not in the id sequence"
        );
    }
}