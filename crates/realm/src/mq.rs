//! Persistent event queues (Phase 4.5c step 2b) as okm tables — per the
//! PLAN ruling and ADR-0002/0005/0006 discipline. Tables in one okm model
//! over the realm's own store (the `MqStore` bridge), keyed per ADR-0040
//! (the event plane's band is ns 20–25):
//!
//! - `EventName` (20) — open-ended event-name vocabulary: proxy id key,
//!   name payload, `by_name` text index (names are runtime data, not ns;
//!   the ns dictionary stays compile-time). Lookup = index scan + row
//!   verify (the text-first regime's documented cost: no delimiter, so
//!   "add" prefix-matches "add_to_cart"; the row comparison is the
//!   exactness).
//! - `InstanceKeyRegistry` (21) — the instance-key registry (the shape settled
//!   in ADR-0039 §1, named in ADR-0041 §1), the same registry pattern:
//!   `by_name` text index + a `HighWater` watermark issues a fixed-width id,
//!   and the reverse direction (id → name) is available for ops. The FNV-1a
//!   hash is retired: an okm primary key is fixed-width by CONSTRUCTION
//!   (`KeyEncode` panics on a `String`), so an instance key had to BE an id —
//!   and `0` (reserved for key-less delivery) is then an id the issuer cannot
//!   produce, instead of a value sharing the space with real keys.
//! - `MqData` (22) — `[event_id][instance_key_id][seq]` → payload. An event
//!   belongs to no booth: one row per emitted event, N subscribers = N
//!   cursors. The sort key is the per-slice SEQUENCE (a counter, not a
//!   timestamp — MqData's own `HighWater(seq)` watermark issues it, ADR-0041
//!   §3): the sort order IS the delivery order,
//!   and a fact's wall-clock time rides a payload field instead.
//! - `MqCursor` (23) — `[event_id][instance_key_id][booth_id]` → last consumed seq
//!   plus `last_active_ms` (ADR-0039 §2: the global `cursor_ttl` predicate
//!   reads it; the value 0 means UNMARKED and never expires). The wall
//!   clock enters retention here, and only here — the sequence never
//!   pretends to be a time.
//! - (24 is vacated — ADR-0041 §3 retired the write-head table.) The head is
//!   now MqData's second reduce, `HighWater(seq)`: append reads the watermark,
//!   assigns `last + 1`, and the row's own put folds it — O(1) append (the old
//!   max-scan over the slice prefix is gone), one writer of the sequence (the
//!   emit path holds the realm lock across append), and issuance lands in the
//!   same batch as the row it numbers.
//! - `EventRoute` (25) — the PERSISTED subscription registry: one row per
//!   (event, booth). The booth id resolves through the meta plane's
//!   ONE booth dictionary (ADR-0038 §2) — this plane keeps no
//!   subscriber-identity dictionary of its own: with wildcard delivery
//!   narrowed to the singleton instance (ADR-0038 §1) there is no
//!   participant-level identity left to issue.
//!
//! Backlog = range scan after the cursor; skip-to-head = cursor write to the
//! slice's write head (cursors are monotonic — advance never rewinds). The
//! slice's DEPTH is a live `Count` reduce over MqData grouped by (event_id,
//! instance_key_id) — the write path folds +1 on append and unfolds −1 on
//! watermark compaction, so `depth()` is one point read, never a scan (the
//! zero-scan operational surface; the skip-to-head decision reads it directly).
//! The WRITE HEAD is the second reduce on the same table, `HighWater(seq)`
//! (ADR-0041 §3).

use okm_core::document::Collection;
use okm_core::{KeyEncode, Document, DocumentEncode, ReduceCodec};
use okm_core::storage::VirtualStorage as _;
use crate::value::{json_to_dyn, dyn_to_json};

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct EventNameKey {
    pub id: u32,
}

/// Name payload + `by_name` text index.
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(EventNameKey)]
#[ok_index(by_name { fields(name) })]
#[ok_ns(20)]
pub struct EventName {
    pub name: String,
}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct InstanceKeyRegistryKey {
    pub id: u32,
}

/// The instance-key registry (the shape settled in ADR-0039 §1, the name in
/// ADR-0041 §1): `by_name` text index + a `HighWater` watermark issuing the
/// proxy id. `SINGLETON_KEY_ID` (0) is never issued, so key-less delivery is
/// structurally unreachable rather than a value that shares the space with
/// real keys.
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(InstanceKeyRegistryKey)]
#[ok_index(by_name { fields(name) })]
#[ok_reduce(HighWater(id) { group(global) })]
#[ok_ns(21)]
pub struct InstanceKeyRegistry {
    pub name: String,
    /// Payload mirror of the proxy id — the MAX reduce folds over payload
    /// fields (okm ADR-0024 gives hooks the key, but the mirror keeps the
    /// fold logic payload-shaped; retire both when 0024's key-field groups
    /// land in the derive).
    pub id: u32,
    /// Single-group discriminator (always 0): the derive rejects empty
    /// group lists, so the registry-wide watermark declares a constant
    /// group instead.
    pub global: u32,
}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct MqDataKey {
    pub event_id: u32,
    /// The instance key's proxy id (ADR-0039 §1 established the shape,
    /// ADR-0041 §1 named it): the dictionary-issued u32 of the routing
    /// value, or `SINGLETON_KEY_ID` for a key-less subscription's queue.
    pub instance_key_id: u32,
    /// The per-slice SEQUENCE: the sort order IS the delivery order. Issued
    /// by the `HighWater(seq)` watermark this table carries (ADR-0041 §3).
    /// A counter, not a timestamp — a fact's wall time rides a payload field
    /// and never sorts.
    pub seq: u64,
}

// The write head has NO table of its own (ADR-0041 §3): it is MqData's
// `HighWater(seq)` watermark, so ns 24 is vacated and issuance folds in the same
// put that writes the row. See MqData below and `head_seq`.

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(MqDataKey)]
#[ok_partition(1)]
#[ok_ns(22)]
// Two live reduces on the data table:
// - `Count` (ADR-0023 preset, ADR-0024 key-field group): append folds +1,
//   watermark compaction's delete unfolds -1 — the write path maintains it,
//   `depth()` reads it as one point get;
// - `HighWater(seq)`: THE WRITE HEAD (ADR-0041 §3). Append reads the watermark
//   (`head_seq`), adds one, and this row's own put folds it — so issuance and
//   the row land in one batch, one physical partition. A watermark never falls
//   (its unfold is a no-op): an issuer's value must outlive its rows, and a
//   view cannot.
#[ok_reduce(Count { group(event_id, instance_key_id) })]
#[ok_reduce(HighWater(seq) { group(event_id, instance_key_id) })]
pub struct MqData {}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct MqCursorKey {
    pub event_id: u32,
    pub instance_key_id: u32,
    /// The booth TYPE id (ADR-0038 §2): one dictionary for the whole
    /// system (the meta plane's, ns 30), not a participant name.
    pub booth_id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(MqCursorKey)]
#[ok_ns(23)]
// No physical partition (ADR-0041 §4): a point table, the same class as booth
// state, which lives in the default keyspace.
// v2: `last_active_ms` appended at the hot tail (append-only rule — a row
// persisted before the field decodes it as 0, which the `cursor_ttl`
// predicate reads as UNMARKED = never expires).
#[ok_layout(version = 2)]
pub struct MqCursor {
    /// Seq of the last CONSUMED event (0 = nothing consumed yet).
    pub cursor: u64,
    /// Wall-clock ms of the last `advance` (skip-to-head counts): the
    /// `cursor_ttl` predicate's input. 0 = unmarked.
    pub last_active_ms: u64,
}

// ---------------------------------------------------------------------------
// EventRoute: the PERSISTED subscription registry. One row per (event,
// booth-type) subscription assembled at registration from the type's @on
// declarations. Replaces the in-memory Route table as the source of truth:
// routes survive restart (no re-introspection to rebuild them), and
// `routes_of` is an index scan. Wildcards ride the same row shape — a
// `pattern` row is matched by prefix at emit (the registry stores the
// declaration, the emit path does the matching, as before). The type id
// comes from the ONE type dictionary (meta's, ns 30): ADR-0038 §2 deleted
// this plane's own subscriber-identity table, which had existed only to
// issue ids for PARTICIPANT names — a need the wildcard narrowing removed.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct EventRouteKey {
    pub event_id: u32,
    pub booth_id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(EventRouteKey)]
#[ok_index(by_booth { fields(booth_id) })]
#[ok_ns(25)]
pub struct EventRoute {
    /// Mirror of the key's type segment — index fields must be payload
    /// fields (the key is not one), so the by_booth scan reads this.
    pub booth_id: u32,
    /// HOW targets resolve (Phase 4.13, event-flow.md §8.2): 0 = singleton,
    /// 1 = payload field, 2 = access-method scan. Three mechanisms,
    /// discriminated at the row-structure layer — never sentinel mixing.
    pub resolution: u8,
    /// `resolution = 1`: the payload field carrying the instance key.
    /// `resolution = 0`: empty.
    pub key_field: String,
    /// `resolution = 2`: the access-method reference, stored as NAMES
    /// (never a slot/ns number — position is not identity, ADR-0038 §3).
    /// `resolution < 2`: empty.
    pub collection: String,
    /// `resolution = 2`: the index name. `resolution < 2`: empty.
    pub index: String,
    /// `resolution = 2`: the payload field supplying the scan effector.
    /// `resolution < 2`: empty.
    pub probe_field: String,
    /// 0 = exact, 1 = wildcard (okm FieldType has no Bool — u8 sentinel).
    /// A wildcard row is always `resolution = 0` (singleton delivery).
    pub wildcard: u8,
}

// ---------------------------------------------------------------------------
// MqStore: the byte engine the mq tables bind to. One handle = optional
// realm prefix + a shared ByteStore (the okm FjallStore keyspace, or
// the in-memory byte stand-in for tests). Values are NATIVE BYTES — the
// base64-in-JSON bridge (`StoreAsVirtual`) is gone (ADR-0018: storage
// values are native, never serialized text; JSON is the API's currency,
// never the store's). The prefix segment is bound at construction:
// realm escape is not expressible (okm nesting rule, aura Phase 3.6).
// ---------------------------------------------------------------------------

/// The engine behind an MqStore. okm picks an engine per assembly site;
/// aura's two assembly points are the fjall keyspace (production) and
/// the in-memory stand-in (tests). An enum, not a trait object: okm's
/// VirtualStorage is not object-safe (&mut self + scan returning owned
/// values is fine, but clones must share the keyspace — enum arms keep
/// the real handle semantics).
#[derive(Clone)]
pub enum MqEngine {
    /// okm's fjall adapter (production): its own keyspace on the engine's
    /// fjall database.
    Fjall(okm_core::FjallStore),
    /// okm's test engine (TestStore): the REAL engine matrix — slatedb
    /// in-memory by default, fjall temp-dir when only the fjall feature
    /// is on. No aura-side stand-in: the test engine belongs to okm.
    Test(okm_core::TestStore),
}

impl okm_core::storage::VirtualStorage for MqEngine {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Fjall(s) => s.put(key, value),
            Self::Test(s) => s.put(key, value),
        }
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Fjall(s) => s.get(key),
            Self::Test(s) => s.get(key),
        }
    }
    fn del(&self, key: &[u8]) {
        match self {
            Self::Fjall(s) => s.del(key),
            Self::Test(s) => s.del(key),
        }
    }
    fn scan_suffix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        match self {
            Self::Fjall(s) => s.scan_suffix(prefix),
            Self::Test(s) => s.scan_suffix(prefix),
        }
    }
    fn scan_range(&self, begin: &[u8], end: Option<&[u8]>) -> Vec<Vec<u8>> {
        match self {
            Self::Fjall(s) => s.scan_range(begin, end),
            Self::Test(s) => s.scan_range(begin, end),
        }
    }
}

impl okm_core::storage::SharedVirtualStorage for MqEngine {
    fn shared_handle(&self) -> Self {
        self.clone()
    }
}

/// The mq store: an okm engine handle + an optional realm prefix,
/// itself an okm VirtualStorage (the tables see a clean key space; the
/// prefix is bound at construction — realm escape is not expressible,
/// the okm nesting rule / aura Phase 3.6). Values are NATIVE BYTES — the
/// base64-in-JSON bridge (`StoreAsVirtual`) is gone (ADR-0018: storage
/// values are native, never serialized text; JSON is the API's currency,
/// never the store's).
#[derive(Clone)]
pub struct MqStore {
    prefix: Vec<u8>,
    inner: MqEngine,
}

impl MqStore {
    /// The mq engine, unqualified (system realm). Fjall: the engine's own
    /// database + keyspace name (okm `FjallStore::open/from_db`).
    pub fn fjall(store: okm_core::FjallStore) -> Self {
        Self::with_engine(MqEngine::Fjall(store))
    }
    /// okm's test engine (tests; real engines, no aura-side double).
    pub fn mem() -> Self {
        Self::with_engine(MqEngine::Test(okm_core::TestStore::default()))
    }
    fn with_engine(engine: MqEngine) -> Self {
        Self { prefix: Vec::new(), inner: engine }
    }
    /// A realm-qualified handle: every key enters as
    /// `[prefix][inner key]`; the inner engine stays untouched.
    /// `PrefixStore`-style 2-byte length discipline for the segment.
    /// A TYPE-NS-raw handle (ADR-0026 §4 wasm storage): prefix = the
    /// type's 2-byte ns. This is the wasm full-power path's engine
    /// plane — the guest's in-module `Collection` emits RAW engine
    /// calls (okm-wire OpFrames) and the host answers with this handle
    /// (the guest code is the trusted static-mode writer; the
    /// no-bypass-guard ruling covers it).
    ///
    /// Handles are CHEAP CLONES sharing the one engine (okm ADR-0026:
    /// `VirtualStorage` writes take `&self` — no outer Mutex, no
    /// per-handle lock boundary; aura ADR-0030 records the ruling).
    pub fn ns_raw(inner: &Self, ns: u16) -> Self {
        let prefix = ns.to_be_bytes().to_vec();
        let mut p = prefix;
        p.extend_from_slice(&inner.prefix);
        Self { prefix: p, inner: inner.inner.clone() }
    }
    /// The realm-prefixed handle (ADR-0028: the outer isolation axis is
    /// a realm, not a "namespace"): `[u16 BE len][realm name]` prepended
    /// inside the engine handle for every stored key.
    pub fn for_realm(inner: &Self, name: &str) -> Self {
        let mut prefix = (name.len() as u16).to_be_bytes().to_vec();
        prefix.extend_from_slice(name.as_bytes());
        let mut p = prefix.clone();
        p.extend_from_slice(&inner.prefix);
        Self { prefix: p, inner: inner.inner.clone() }
    }
    fn qualified(&self, key: &[u8]) -> Vec<u8> {
        let mut k = self.prefix.clone();
        k.extend_from_slice(key);
        k
    }
}

impl okm_core::storage::VirtualStorage for MqStore {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.inner.put(self.qualified(&key), value);
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.get(&self.qualified(key))
    }
    fn del(&self, key: &[u8]) {
        self.inner.del(&self.qualified(key));
    }
    fn scan_suffix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        // okm's suffix contract: the engine scans the QUALIFIED prefix
        // and returns keys minus it — the realm segment never leaks
        // to the caller, and no second strip happens here.
        let full = self.qualified(prefix);
        self.inner.scan_suffix(&full)
    }
    fn scan_range(&self, begin: &[u8], end: Option<&[u8]>) -> Vec<Vec<u8>> {
        // Qualified window over the inner engine; the returned keys are
        // sliced back into the caller's realm-local space.
        let full_begin = self.qualified(begin);
        let full_end = end.map(|e| self.qualified(e));
        self.inner
            .scan_range(&full_begin, full_end.as_deref())
            .into_iter()
            .map(|k| k[self.prefix.len()..].to_vec())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Registry resolve: name → id (assign on first sight). Exact match = index
// prefix scan + row verify ("add" scans "add_to_cart" too — text-first
// regime); miss = append with the next id. Two vocabularies resolve here:
// event names (ns 20) and instance keys (ns 21). The booth-TYPE id is NOT
// resolved here: ADR-0038 §2 moved it to the meta plane's single dictionary
// (ns 30), which the delegating helpers below reach.
// ---------------------------------------------------------------------------

fn resolve_event_id(store: &MqStore, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, EventNameKey, EventName>::new(store.clone());
    // Exact match through the text index: prefix scan by name, then
    // verify (no delimiter in the index bytes).
    for hit in t.scan::<__OkmIndex_EventName_by_name>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(hit.0.decoded.id);
            }
        }
    }
    // Miss: assign the next id (registry vocabularies are tiny; a full
    // scan for the head is honest).
    let mut max_id = 0u32;
    for (pk, _) in t.scan::<__OkmIndex_EventName_by_name>(&[]) {
        if pk.decoded.id > max_id {
            max_id = pk.decoded.id;
        }
    }
    let id = max_id + 1;
    t.put(&EventNameKey { id }, &EventName { name: name.to_string() });
    Ok(id)
}

/// A queue slice: the reserved key-less singleton, or a dictionary-issued
/// instance key.
/// A structural marker, not a magic string (ADR-0039 §1): the singleton
/// never enters the name dictionary, so no payload key can alias into it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum InstanceKey {
    /// Key-less delivery (wildcards and `@on` with no key field).
    Singleton,
    /// The instance key the route resolved.
    Named(String),
}

/// The resolved id for a queue slice: `SINGLETON_KEY_ID` for the singleton,
/// else the dictionary-issued partition id (allocated on first sight).
pub fn instance_key_id(store: &MqStore, part: &InstanceKey) -> anyhow::Result<u32> {
    match part {
        InstanceKey::Singleton => Ok(SINGLETON_KEY_ID),
        InstanceKey::Named(name) => resolve_instance_key_id(store, name),
    }
}

fn resolve_instance_key_id(store: &MqStore, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, InstanceKeyRegistryKey, InstanceKeyRegistry>::new(store.clone());
    for hit in t.scan::<__OkmIndex_InstanceKeyRegistry_by_name>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(hit.0.decoded.id);
            }
        }
    }
    // Miss: next id = the HighWater reduce's value + 1 (no scan; ids are
    // never reused — unfold is a no-op for this watermark). The reserved
    // singleton (0) is therefore unreachable by construction.
    let watermark = okm_core::reduce_get::<MqStore, __OkmReduce_InstanceKeyRegistry_0>(
        t.store(),
        <InstanceKeyRegistry as Document>::NS_PREFIX,
        &InstanceKeyRegistryKey { id: 0 },
        &InstanceKeyRegistry { name: String::new(), id: 0, global: 0 },
    )
    .unwrap_or(0);
    let id = (watermark as u32) + 1;
    t.put(&InstanceKeyRegistryKey { id }, &InstanceKeyRegistry { name: name.to_string(), id, global: 0 });
    Ok(id)
}

/// The id already assigned to a partition name (None = never seen). Peek,
/// never allocate — the ops/observation direction.
pub fn instance_key_id_of(store: &MqStore, name: &str) -> anyhow::Result<Option<u32>> {
    let t = Collection::<MqStore, InstanceKeyRegistryKey, InstanceKeyRegistry>::new(store.clone());
    for hit in t.scan::<__OkmIndex_InstanceKeyRegistry_by_name>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(Some(hit.0.decoded.id));
            }
        }
    }
    Ok(None)
}

/// The registered name for a partition id (None = never issued) — the
/// dictionary's reverse direction, which the retired hash could not offer:
/// it is what lets ops render a queue in human terms.
pub fn instance_key_of(store: &MqStore, id: u32) -> anyhow::Result<Option<String>> {
    let t = Collection::<MqStore, InstanceKeyRegistryKey, InstanceKeyRegistry>::new(store.clone());
    Ok(t.get(&InstanceKeyRegistryKey { id }).map(|row| row.name))
}

/// The booth's identity id (delegated to the meta plane's single dictionary,
/// ns 30 — ADR-0038 §2). Allocation on first sight is the registry pattern;
/// in production `register_type` resolves it before any route row lands.
pub fn booth_id_of(store: &MqStore, booth_type: &str) -> anyhow::Result<u32> {
    crate::meta::resolve_booth_id(store, booth_type)
}

/// Wall-clock ms (the `cursor_ttl` predicate's clock).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Is this cursor row past its retention promise? ADR-0039 §2: `0` is the
/// UNMARKED sentinel (a row written before the field existed) and never
/// expires; otherwise the row expires once it has not advanced for `ttl`.
pub fn cursor_expired(last_active_ms: u64, now: u64, ttl: std::time::Duration) -> bool {
    if last_active_ms == 0 {
        return false;
    }
    now.saturating_sub(last_active_ms) >= ttl.as_millis() as u64
}

/// The slice's write head: the last sequence issued for (event, instance key),
/// read from MqData's `HighWater(seq)` watermark (ADR-0041 §3). One point read
/// of the reduce entry; absent = 0 (nothing issued yet).
fn head_seq(store: &MqStore, event_id: u32, instance_key_id: u32) -> anyhow::Result<u64> {
    let mut header = Vec::with_capacity(4);
    header.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    header.extend_from_slice(<MqData as Document>::NS_PREFIX);
    Ok(okm_core::reduce_get::<MqStore, __OkmReduce_MqData_1>(
        store,
        &header,
        &MqDataKey { event_id, instance_key_id, seq: 0 },
        &MqData {},
    )
    .unwrap_or(0))
}

/// Append one event to a queue slice; returns the assigned sequence number.
/// O(1): MqData's `HighWater(seq)` watermark carries the slice's last issued
/// seq, so append is a point read + `last + 1` + the row's own put (which folds
/// the watermark — same batch, one physical partition; ADR-0041 §3), never a max
/// scan over the slice prefix. The emit path holds the realm lock across this
/// call, which is what makes the sequence single-writer (two emitters cannot
/// mint the same seq). A counter, deliberately: the value carries no wall-clock
/// meaning, so nothing downstream can mistake it for a timestamp.
pub fn append(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    payload: &serde_json::Value,
) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let instance_key_id = instance_key_id(store, part)?;
    let seq = head_seq(store, event_id, instance_key_id)? + 1;
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    t.put(&MqDataKey { event_id, instance_key_id, seq }, &MqData {});
    // Payload → dynamic segment, fully native. The emit chain guarantees
    // an object top (routing reads the instance key from data fields;
    // handlers receive objects) — no wrapping convention exists here or
    // in okm's set_object (its input type IS a map).
    let map = match payload {
        serde_json::Value::Object(map) => map,
        // Unreachable via the emit chain (the emit chain guarantees an
        // object top); an empty map keeps the conversion total without
        // inventing a synthetic field.
        _ => &serde_json::Map::new(),
    };
    let mut obj = std::collections::BTreeMap::new();
    for (k, v) in map {
        obj.insert(k.clone(), json_to_dyn(v));
    }
    t.put_document(&MqDataKey { event_id, instance_key_id, seq }, &obj);
    Ok(seq)
}

/// The reserved id for key-less delivery: wildcards and key-less `@on` slices.
/// The registry issuer never produces 0, so the reservation is structural by
/// construction (ADR-0039 §1). The singleton INSTANCE's own key is the
/// `InstanceKey::Singleton` VARIANT (aura_booth, ADR-0042) — not a string.
pub const SINGLETON_KEY_ID: u32 = 0;

/// The subscriber's cursor (0 = nothing consumed). The subscriber is the
/// booth TYPE (ADR-0038 §2): one dictionary id, no participant name.
pub fn cursor(store: &MqStore, event: &str, part: &InstanceKey, booth_type: &str) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = booth_id_of(store, booth_type)?;
    let t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    Ok(t.get(&MqCursorKey {
        event_id,
        instance_key_id: instance_key_id(store, part)?,
        booth_id,
    })
    .map(|c| c.cursor)
    .unwrap_or(0))
}

/// Advance the cursor after consuming. Monotonic by contract: a lower
/// `seq` never rewinds — that is what makes skip-to-head durable (a
/// skipped backlog must not re-surface on the next drain pass). The same
/// write stamps `last_active_ms`: the `cursor_ttl` predicate's input
/// (ADR-0039 §2), so a consumer that keeps up never expires.
pub fn advance(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    booth_type: &str,
    seq: u64,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = booth_id_of(store, booth_type)?;
    let mut t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    let key = MqCursorKey { event_id, instance_key_id: instance_key_id(store, part)?, booth_id };
    let current = t.get(&key).map(|c| c.cursor).unwrap_or(0);
    if seq <= current {
        return Ok(());
    }
    t.put(&key, &MqCursor { cursor: seq, last_active_ms: now_ms() });
    Ok(())
}

/// The subscriber's backlog: (seq, payload) strictly after `after_seq`,
/// oldest first. Empty = caught up.
pub fn backlog(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    after_seq: u64,
) -> anyhow::Result<Vec<(u64, serde_json::Value)>> {
    let event_id = resolve_event_id(store, event)?;
    let instance_key_id = instance_key_id(store, part)?;
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&instance_key_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 8 {
            continue;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&suffix[suffix.len() - 8..]);
        let seq = u64::from_be_bytes(b);
        if seq > after_seq
            && t.get(&MqDataKey { event_id, instance_key_id, seq }).is_some() {
                // Reconstruct from the dynamic segment (name-keyed).
                let v = match t.get_document(&MqDataKey { event_id, instance_key_id, seq }) {
                    Some(obj) if !obj.contains_key("_root") => {
                        let mut m = serde_json::Map::new();
                        for (k, dv) in &obj {
                            m.insert(k.clone(), dyn_to_json(dv));
                        }
                        serde_json::Value::Object(m)
                    }
                    // Non-object top level came in as a single _root field.
                    Some(obj) => match obj.get("_root") {
                        Some(dv) => dyn_to_json(dv),
                        None => serde_json::Value::Null,
                    },
                    None => serde_json::Value::Null,
                };
                out.push((seq, v));
            }
    }
    out.sort_by_key(|(s, _)| *s);
    Ok(out)
}

/// The queue's backlog depth: rows still stored in (event, instance key).
/// One point read of the live `Count` reduce — the zero-scan operational
/// surface the skip-to-head decision reads. Absent group (no rows ever,
/// or everything compacted away with the count at its zero state) is 0.
pub fn depth(store: &MqStore, event: &str, part: &InstanceKey) -> anyhow::Result<u64> {
    let Some(event_id) = event_id_of(store, event)? else {
        return Ok(0);
    };
    let instance_key_id = instance_key_id(store, part)?;
    let mut header = Vec::with_capacity(4);
    header.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    header.extend_from_slice(<MqData as Document>::NS_PREFIX);
    Ok(
        okm_core::reduce_get::<MqStore, __OkmReduce_MqData_0>(
            store,
            &header,
            &MqDataKey { event_id, instance_key_id, seq: 0 },
            &MqData {},
        )
        .unwrap_or(0),
    )
}

/// Rewind a cursor BELOW its current value (test-support only — the
/// production path never does this; `advance` is monotonic). Used to pin
/// the retention watermark at an old logical time in compaction tests: the
/// stamp is NOW, so the row does not additionally expire (combine with
/// `age_cursor` to test the `cursor_ttl` path).
pub fn rewind_cursor(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    booth_type: &str,
    seq: u64,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = booth_id_of(store, booth_type)?;
    let mut t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    t.put(
        &MqCursorKey { event_id, instance_key_id: instance_key_id(store, part)?, booth_id },
        &MqCursor { cursor: seq, last_active_ms: now_ms() },
    );
    Ok(())
}

/// Backdate a cursor row's activity stamp (test-support only): with a small
/// `cursor_ttl` the row is then past its retention promise, which is what
/// the expiry tests need to exercise (ADR-0039 §2). 0 would mean UNMARKED
/// (never expires), so the caller passes an explicit millisecond stamp.
pub fn age_cursor(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    booth_type: &str,
    last_active_ms: u64,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = booth_id_of(store, booth_type)?;
    let mut t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    let key = MqCursorKey { event_id, instance_key_id: instance_key_id(store, part)?, booth_id };
    let cursor = t.get(&key).map(|c| c.cursor).unwrap_or(0);
    t.put(&key, &MqCursor { cursor, last_active_ms });
    Ok(())
}

/// skip-to-head: jump the cursor to the slice's write head (MqData's
/// `HighWater(seq)` watermark), discarding the stale backlog (the relief valve
/// per the ruling).
pub fn skip_to_head(
    store: &MqStore,
    event: &str,
    part: &InstanceKey,
    booth_type: &str,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let instance_key_id = instance_key_id(store, part)?;
    let head = head_seq(store, event_id, instance_key_id)?;
    advance(store, event, part, booth_type, head)
}

// ---------------------------------------------------------------------------
// EventRoute registry ops: the persisted subscription set. Writers are the
// realm registration path (register/deregister); readers are emit matching,
// `routes_of` (activation binding) and the watermark denominator.
// ---------------------------------------------------------------------------

/// Persist one subscription: (event, booth TYPE) → resolution declaration.
/// Idempotent (a re-register overwrites the same row).
pub fn route_put(
    store: &MqStore,
    event: &str,
    booth_type: &str,
    resolution: &aura_booth::RouteResolution,
    is_wildcard: bool,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = booth_id_of(store, booth_type)?;
    let (tag, key_field, collection, index, probe_field) = match resolution {
        aura_booth::RouteResolution::Singleton => (0u8, String::new(), String::new(), String::new(), String::new()),
        aura_booth::RouteResolution::Field(f) => (1u8, f.clone(), String::new(), String::new(), String::new()),
        aura_booth::RouteResolution::Scan { collection, index, probe_field } => {
            (2u8, String::new(), collection.clone(), index.clone(), probe_field.clone())
        }
    };
    let mut t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    t.put(
        &EventRouteKey { event_id, booth_id },
        &EventRoute { booth_id, resolution: tag, key_field, collection, index, probe_field, wildcard: u8::from(is_wildcard) },
    );
    Ok(())
}

/// Drop every subscription row for one booth type (deregistration /
/// hot-swap): its cursors then fall out of the watermark denominator.
/// The scan rides the by_booth index — the primary key is
/// `[event_id][booth_id]`, so a type-prefixed primary-slot scan would
/// delete rows belonging to whoever's event id matched the type id.
pub fn routes_drop_booth(store: &MqStore, booth_type: &str) -> anyhow::Result<()> {
    let booth_id = booth_id_of(store, booth_type)?;
    let mut t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    let stale: Vec<u32> = t
        .scan::<__OkmIndex_EventRoute_by_booth>(&booth_id.to_be_bytes())
        .into_iter()
        .map(|(pk, _row)| pk.decoded.event_id)
        .collect();
    for event_id in stale {
        t.delete_by_pkey(&EventRouteKey { event_id, booth_id });
    }
    Ok(())
}

/// Every subscription row for one event: (booth_id, resolution, is_wildcard).
pub fn routes_of_event(
    store: &MqStore,
    event: &str,
) -> anyhow::Result<Vec<(u32, aura_booth::RouteResolution, bool)>> {
    let event_id = resolve_event_id(store, event)?;
    let t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<EventRoute as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<EventRoute as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 4 {
            continue;
        }
        // suffix = [booth_id 4B] (the rest of the primary key)
        let mut b = [0u8; 4];
        b.copy_from_slice(&suffix[suffix.len() - 4..]);
        let booth_id = u32::from_be_bytes(b);
        if let Some(row) = t.get(&EventRouteKey { event_id, booth_id }) {
            out.push((booth_id, row_resolution(&row), row.wildcard != 0));
        }
    }
    Ok(out)
}

/// Every subscription row for one booth type: (event_id, resolution,
/// is_wildcard). The activation binding's persistent `routes_of`.
pub fn routes_of_booth(
    store: &MqStore,
    booth_type: &str,
) -> anyhow::Result<Vec<(u32, aura_booth::RouteResolution, bool)>> {
    let booth_id = booth_id_of(store, booth_type)?;
    let t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    let mut out = Vec::new();
    for hit in t.scan::<__OkmIndex_EventRoute_by_booth>(&booth_id.to_be_bytes()) {
        if let Some(row) = &hit.1 {
            out.push((hit.0.decoded.event_id, row_resolution(row), row.wildcard != 0));
        }
    }
    Ok(out)
}

/// The row's resolution columns decoded back into the declaration enum
/// (the inverse of `route_put`'s encoding; the u8 tag is the row-structure
/// discriminator, the names ride their own columns).
fn row_resolution(row: &EventRoute) -> aura_booth::RouteResolution {
    match row.resolution {
        1 => aura_booth::RouteResolution::Field(row.key_field.clone()),
        2 => aura_booth::RouteResolution::Scan {
            collection: row.collection.clone(),
            index: row.index.clone(),
            probe_field: row.probe_field.clone(),
        },
        _ => aura_booth::RouteResolution::Singleton,
    }
}

// ---------------------------------------------------------------------------
// Retention (min-watermark over registered subscribers). The watermark's
// denominator comes from the ROUTE REGISTRY (the persisted @on metadata),
// never from the raw cursor keys: a cursor row whose booth no longer has a
// route for this event must not pin the watermark. Eviction (instance
// scale-to-zero) does NOT deregister — the type's route remains, the
// instance replays its backlog on re-activation; deregistration (type
// hot-swap / booth deletion) drops the route, and the stale cursor row
// falls out of the denominator (its row is removable by prefix scan).
// ---------------------------------------------------------------------------

/// Delete mq-data rows in a slice with seq < `min_seq`. Returns the
/// number of rows removed.
pub fn delete_before(
    store: &MqStore,
    event_id: u32,
    instance_key_id: u32,
    min_seq: u64,
) -> anyhow::Result<usize> {
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&instance_key_id.to_be_bytes());
    let mut removed = 0;
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 8 {
            continue;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&suffix[suffix.len() - 8..]);
        let seq = u64::from_be_bytes(b);
        if seq < min_seq {
            t.delete_by_pkey(&MqDataKey { event_id, instance_key_id, seq });
            removed += 1;
        }
    }
    Ok(removed)
}

/// Every cursor row in a partition: (booth_id, cursor, last_active_ms). The
/// caller filters against the route registry and the `cursor_ttl`
/// predicate (ADR-0039 §2).
pub fn cursor_rows(
    store: &MqStore,
    event_id: u32,
    instance_key_id: u32,
) -> anyhow::Result<Vec<(u32, u64, u64)>> {
    let t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqCursor as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqCursor as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&instance_key_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 4 {
            continue;
        }
        // suffix = [booth_id 4B]
        let mut b = [0u8; 4];
        b.copy_from_slice(&suffix[suffix.len() - 4..]);
        let booth_id = u32::from_be_bytes(b);
        let row = t.get(&MqCursorKey { event_id, instance_key_id, booth_id });
        out.push((
            booth_id,
            row.as_ref().map(|c| c.cursor).unwrap_or(0),
            row.as_ref().map(|c| c.last_active_ms).unwrap_or(0),
        ));
    }
    Ok(out)
}

/// Drop one cursor row whose backlog no longer exists (its cursor sits at
/// or below the partition's watermark). NOTE the asymmetry with expiry:
/// expiry (ADR-0039 §2) only removes a row from the denominator — deleting
/// an EXPIRED row while it still holds a position ABOVE the watermark would
/// read the cursor back as 0 and re-deliver the surviving rows. This call
/// is therefore only for rows already below the watermark (proof that
/// nothing can replay).
pub fn drop_cursor(store: &MqStore, event_id: u32, instance_key_id: u32, booth_id: u32) -> anyhow::Result<()> {
    let mut t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    t.delete_by_pkey(&MqCursorKey { event_id, instance_key_id, booth_id });
    Ok(())
}

/// The registered name for an event id (None = never registered).
pub fn event_name_of(store: &MqStore, event_id: u32) -> anyhow::Result<Option<String>> {
    let t = Collection::<MqStore, EventNameKey, EventName>::new(store.clone());
    Ok(t.get(&EventNameKey { id: event_id }).map(|a| a.name))
}

/// The queue THIS booth instance subscribes to for one concrete event,
/// resolved through the PERSISTED route registry — the same source the
/// watermark denominator and the consumer loop's binding use. An exact
/// row matches by id; a wildcard row (the registry stores the PREFIX as
/// its event name) matches when the concrete name starts with it. The
/// returned slice is `Singleton` for a key-less route and
/// `Named(instance_key)` for a keyed one (the emit path derives the
/// partition from the same field, so subscriber and queue agree by
/// construction). None = no route of this booth type binds the event.
pub fn bound_instance_key(
    store: &MqStore,
    booth_type: &str,
    instance_key: &aura_booth::InstanceKey,
    event: &str,
) -> anyhow::Result<Option<InstanceKey>> {
    let Some(event_id) = event_id_of(store, event)? else {
        return Ok(None);
    };
    for (rid, resolution, wildcard) in routes_of_booth(store, booth_type)? {
        let matched = if rid == event_id {
            true
        } else if wildcard {
            match event_name_of(store, rid)? {
                // The registry stores the PATTERN ("order.*"); the
                // consumer loop expands it by trimming the trailing
                // '*' — the same rule applies to the relief valve.
                Some(pattern) => event.starts_with(pattern.trim_end_matches('*')),
                None => false,
            }
        } else {
            false
        };
        if matched {
            // ADR-0038 §1: a key-less route's queue is consumed by the
            // type's SINGLETON instance only — any other instance of the
            // type has no bound queue for it (the valve reports "no route"
            // rather than aiming at a queue it does not consume). Variant
            // matching, not a string comparison (ADR-0042).
            match resolution {
                aura_booth::RouteResolution::Singleton => {
                    return Ok(matches!(instance_key, aura_booth::InstanceKey::Singleton)
                        .then_some(InstanceKey::Singleton));
                }
                aura_booth::RouteResolution::Field(_) | aura_booth::RouteResolution::Scan { .. } => {
                    return Ok(Some(InstanceKey::Named(instance_key.render().to_string())));
                }
            }
        }
    }
    Ok(None)
}

/// Does this booth type currently register a route that MATCHES a concrete
/// event (an exact row, or a wildcard row whose prefix matches)? This is
/// the retention denominator's test (ADR-0038 §2, ADR-0039 §2): a cursor
/// row whose type has no matching route must not pin the watermark, while
/// a wildcard subscriber's cursor must (the registry stores the PATTERN, so
/// an exact-id lookup alone would silently drop wildcard consumers out of
/// the denominator and let compaction eat their backlog).
pub fn booth_subscribes(store: &MqStore, booth_id: u32, event: &str) -> anyhow::Result<bool> {
    let Some(event_id) = event_id_of(store, event)? else {
        return Ok(false);
    };
    let t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    for hit in t.scan::<__OkmIndex_EventRoute_by_booth>(&booth_id.to_be_bytes()) {
        let Some(row) = &hit.1 else { continue };
        let rid = hit.0.decoded.event_id;
        if rid == event_id {
            return Ok(true);
        }
        if row.wildcard != 0 {
            if let Some(pattern) = event_name_of(store, rid)? {
                if event.starts_with(pattern.trim_end_matches('*')) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// Every registered event name matching a wildcard PREFIX (the pattern's
/// concrete instantiations the registry has seen). Prefix scan over the
/// by_name index + row verify.
pub fn events_matching(store: &MqStore, prefix: &str) -> anyhow::Result<Vec<String>> {
    let t = Collection::<MqStore, EventNameKey, EventName>::new(store.clone());
    let mut out = Vec::new();
    for hit in t.scan::<__OkmIndex_EventName_by_name>(prefix.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name.starts_with(prefix) {
                out.push(row.name.clone());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// The registered id for an event name (None = never emitted/registered).
pub fn event_id_of(store: &MqStore, event: &str) -> anyhow::Result<Option<u32>> {
    let t = Collection::<MqStore, EventNameKey, EventName>::new(store.clone());
    for hit in t.scan::<__OkmIndex_EventName_by_name>(event.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == event {
                return Ok(Some(hit.0.decoded.id));
            }
        }
    }
    Ok(None)
}
