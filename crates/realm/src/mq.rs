//! Persistent event queues (Phase 4.5c step 2b) as okm tables — per the
//! PLAN ruling and ADR-0002/0005/0006 discipline. Tables in one okm model
//! over the realm's own store (the `MqStore` bridge):
//!
//! - `EventName` — open-ended event-name vocabulary: proxy id key, name
//!   payload, `by_name` text index (names are runtime data, not ns; the
//!   ns dictionary stays compile-time). Lookup = index scan + row verify
//!   (the text-first regime's documented cost: no delimiter, so "add"
//!   prefix-matches "add_to_cart"; the row comparison is the exactness).
//! - `BoothName` — same registry pattern for subscriber identity.
//! - `MqData` — `[event_id][part_id][time]` → payload. An event belongs to
//!   no booth: one row per emitted event, N subscribers = N cursors. The
//!   sort key is the LOGICAL time (ms, monotonic via MqHead — not wall
//!   truth; the event's real timestamp rides the payload fields).
//! - `MqHead` — `[event_id][part_id]` → last assigned logical time. The
//!   per-partition write head: append reads it, assigns
//!   `max(now_ms, last+1)`, writes it back. O(1) append (the old max-scan
//!   over the partition prefix is gone) and cross-emitter monotonicity
//!   (same-ms emits from concurrent emitters fold +1 into the sequence).
//! - `MqCursor` — `[event_id][part_id][booth_id]` → last consumed seq.
//!
//! Backlog = range scan after the cursor; skip-to-now = cursor write to
//! the partition head. Min-watermark retention compaction and reduce-based
//! depth counts are follow-ups (PLAN), not implemented here.

use okm_core::document::Collection;
use okm_core::{KeyEncode, Document, DocumentEncode};
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
#[ok_ns(30)]
pub struct EventName {
    pub name: String,
}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct MqDataKey {
    pub event_id: u32,
    pub part_id: u64,
    /// Logical time (ms), monotonic per partition via MqHead — the sort
    /// order IS the delivery order; wall truth rides the payload.
    pub time: u64,
}

/// Per-partition write head: the last logical time assigned by append.
/// One row per partition (bounded — same cardinality as the partitions
/// themselves); the O(1) alternative to scanning the data prefix for max.
#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct MqHeadKey {
    pub event_id: u32,
    pub part_id: u64,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(MqHeadKey)]
#[ok_ns(34)]
pub struct MqHead {
    pub last_time: u64,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(MqDataKey)]
#[ok_partition(1)]
#[ok_ns(31)]
pub struct MqData {}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct MqCursorKey {
    pub event_id: u32,
    pub part_id: u64,
    pub booth_id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(MqCursorKey)]
#[ok_partition(2)]
#[ok_ns(32)]
pub struct MqCursor {
    /// Seq of the last CONSUMED event (0 = nothing consumed yet).
    pub cursor: u64,
}

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct BoothNameKey {
    pub id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(BoothNameKey)]
#[ok_index(by_name { fields(name) })]
#[ok_ns(33)]
pub struct BoothName {
    pub name: String,
}

// ---------------------------------------------------------------------------
// EventRoute: the PERSISTED subscription registry. One row per (event,
// booth-type) subscription assembled at registration from the type's @on
// declarations. Replaces the in-memory Route table as the source of truth:
// routes survive restart (no re-introspection to rebuild them), and
// `routes_of` is an index scan. Wildcards ride the same row shape — a
// `pattern` row is matched by prefix at emit (the registry stores the
// declaration, the emit path does the matching, as before).
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct EventRouteKey {
    pub event_id: u32,
    pub booth_id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(EventRouteKey)]
#[ok_index(by_booth { fields(booth_id) })]
#[ok_ns(35)]
pub struct EventRoute {
    /// Mirror of the key's booth segment — index fields must be payload
    /// fields (the key is not one), so the by_booth scan reads this.
    pub booth_id: u32,
    /// Empty = singleton subscription (no instance key); a wildcard
    /// subscription carries the PREFIX here (matching is the emit path's
    /// job) and `wildcard` is set.
    pub key_field: String,
    /// 0 = exact, 1 = wildcard (okm FieldType has no Bool — u8 sentinel).
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
// regime); miss = append with the next id.
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

fn resolve_booth_id(store: &MqStore, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, BoothNameKey, BoothName>::new(store.clone());
    for hit in t.scan::<__OkmIndex_BoothName_by_name>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(hit.0.decoded.id);
            }
        }
    }
    let mut max_id = 0u32;
    for (pk, _) in t.scan::<__OkmIndex_BoothName_by_name>(&[]) {
        if pk.decoded.id > max_id {
            max_id = pk.decoded.id;
        }
    }
    let id = max_id + 1;
    t.put(&BoothNameKey { id }, &BoothName { name: name.to_string() });
    Ok(id)
}

/// Append one event to a partition; returns the assigned logical time.
/// O(1): the head row (MqHead) carries the partition's last assigned
/// time — `max(now_ms, last+1)` keeps the sequence monotonic across
/// concurrent emitters whose wall clocks agree to the millisecond or not.
pub fn append(
    store: &MqStore,
    event: &str,
    part: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_id_of(part);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut head_t = Collection::<MqStore, MqHeadKey, MqHead>::new(store.clone());
    let head_key = MqHeadKey { event_id, part_id };
    let time = {
        let last = head_t.get(&head_key).map(|h| h.last_time).unwrap_or(0);
        if now_ms > last { now_ms } else { last + 1 }
    };
    head_t.put(&head_key, &MqHead { last_time: time });
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    let seq = time;
    t.put(&MqDataKey { event_id, part_id, time }, &MqData {});
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
    t.put_document(&MqDataKey { event_id, part_id, time }, &obj);
    Ok(seq)
}

/// The reserved singleton partition id: key-less routes (wildcards and
/// key-less @on) bind here. 0 is never produced by `part_hash` (mapped to
/// 1), so the reserved value is structural, not a hash coincidence.
pub const SINGLETON_PART: u64 = 0;

/// Partition id: open-ended string → u64. FNV-1a — a key FIELD hash, not
/// an ns (ADR-0002's hash rejection is about the ns dictionary, not
/// payload-level discriminators); collisions only merge two partitions'
/// backlogs, never lose events, and the consumer's handler re-checks
/// nothing (partitioning is a delivery fan-out key, not an address). 0 is
/// reserved for the singleton partition (mapped to 1 on collision).
fn part_hash(part: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in part.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    if h == 0 { 1 } else { h }
}

/// Partition id for a partition name: key-less partitions (the
/// `__singleton__` sentinel the routing layer passes) map to the
/// reserved id; everything else hashes.
pub fn part_id_of(part: &str) -> u64 {
    if part == SINGLETON {
        SINGLETON_PART
    } else {
        part_hash(part)
    }
}

/// The singleton partition sentinel the routing layer uses.
pub const SINGLETON: &str = "__singleton__";

/// The subscriber's cursor (0 = nothing consumed).
pub fn cursor(store: &MqStore, event: &str, part: &str, booth: &str) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = resolve_booth_id(store, booth)?;
    let t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    Ok(t.get(&MqCursorKey {
        event_id,
        part_id: part_id_of(part),
        booth_id,
    })
    .map(|c| c.cursor)
    .unwrap_or(0))
}

/// Advance the cursor after consuming.
pub fn advance(
    store: &MqStore,
    event: &str,
    part: &str,
    booth: &str,
    seq: u64,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = resolve_booth_id(store, booth)?;
    let mut t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    t.put(
        &MqCursorKey { event_id, part_id: part_id_of(part), booth_id },
        &MqCursor { cursor: seq },
    );
    Ok(())
}

/// The subscriber's backlog: (seq, payload) strictly after `after_seq`,
/// oldest first. Empty = caught up.
pub fn backlog(
    store: &MqStore,
    event: &str,
    part: &str,
    after_seq: u64,
) -> anyhow::Result<Vec<(u64, serde_json::Value)>> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_id_of(part);
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&part_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 8 {
            continue;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&suffix[suffix.len() - 8..]);
        let seq = u64::from_be_bytes(b);
        if seq > after_seq
            && t.get(&MqDataKey { event_id, part_id, time: seq }).is_some() {
                // Reconstruct from the dynamic segment (name-keyed).
                let v = match t.get_document(&MqDataKey { event_id, part_id, time: seq }) {
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

/// skip-to-now: jump the cursor to the partition head, discarding the
/// stale backlog (the relief valve per the ruling).
pub fn skip_to_now(
    store: &MqStore,
    event: &str,
    part: &str,
    booth: &str,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_id_of(part);
    let head = Collection::<MqStore, MqHeadKey, MqHead>::new(store.clone())
        .get(&MqHeadKey { event_id, part_id })
        .map(|h| h.last_time)
        .unwrap_or(0);
    advance(store, event, part, booth, head)
}

// ---------------------------------------------------------------------------
// EventRoute registry ops: the persisted subscription set. Writers are the
// realm registration path (register/deregister); readers are emit matching,
// `routes_of` (activation binding) and the watermark denominator.
// ---------------------------------------------------------------------------

/// Persist one subscription: (event, booth type) → key field declaration.
/// Idempotent (a re-register overwrites the same row).
pub fn route_put(
    store: &MqStore,
    event: &str,
    booth_type: &str,
    key_field: &str,
    is_wildcard: bool,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let booth_id = resolve_booth_id(store, booth_type)?;
    let mut t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    t.put(
        &EventRouteKey { event_id, booth_id },
        &EventRoute { booth_id, key_field: key_field.to_string(), wildcard: u8::from(is_wildcard) },
    );
    Ok(())
}

/// Drop every subscription row for one booth type (deregistration /
/// hot-swap): its cursors then fall out of the watermark denominator.
/// The scan rides the by_booth index — the primary key is
/// `[event_id][booth_id]`, so an booth-prefixed primary-slot scan would
/// delete rows belonging to whoever's event id matched the booth id.
pub fn routes_drop_booth(store: &MqStore, booth_type: &str) -> anyhow::Result<()> {
    let booth_id = resolve_booth_id(store, booth_type)?;
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

/// Every subscription row for one event: (booth_id, key_field, is_wildcard).
pub fn routes_of_event(
    store: &MqStore,
    event: &str,
) -> anyhow::Result<Vec<(u32, String, bool)>> {
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
            out.push((booth_id, row.key_field, row.wildcard != 0));
        }
    }
    Ok(out)
}

/// Every subscription row for one booth type: (event_id, key_field,
/// is_wildcard). The activation binding's persistent `routes_of`.
pub fn routes_of_booth(
    store: &MqStore,
    booth_type: &str,
) -> anyhow::Result<Vec<(u32, String, bool)>> {
    let booth_id = resolve_booth_id(store, booth_type)?;
    let t = Collection::<MqStore, EventRouteKey, EventRoute>::new(store.clone());
    let mut out = Vec::new();
    for hit in t.scan::<__OkmIndex_EventRoute_by_booth>(&booth_id.to_be_bytes()) {
        if let Some(row) = &hit.1 {
            out.push((hit.0.decoded.event_id, row.key_field.clone(), row.wildcard != 0));
        }
    }
    Ok(out)
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

/// Delete mq-data rows in a partition with seq < `min_seq`. Returns the
/// number of rows removed.
pub fn delete_before(
    store: &MqStore,
    event_id: u32,
    part_id: u64,
    min_seq: u64,
) -> anyhow::Result<usize> {
    let mut t = Collection::<MqStore, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&part_id.to_be_bytes());
    let mut removed = 0;
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 8 {
            continue;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&suffix[suffix.len() - 8..]);
        let seq = u64::from_be_bytes(b);
        if seq < min_seq {
            t.delete_by_pkey(&MqDataKey { event_id, part_id, time: seq });
            removed += 1;
        }
    }
    Ok(removed)
}

/// Every cursor row in a partition: (booth_id, cursor). The caller filters
/// against the route registry.
pub fn cursor_rows(
    store: &MqStore,
    event_id: u32,
    part_id: u64,
) -> anyhow::Result<Vec<(u32, u64)>> {
    let t = Collection::<MqStore, MqCursorKey, MqCursor>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqCursor as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqCursor as Document>::NS_PREFIX);
    prefix.extend_from_slice(&okm_core::index::PRIMARY_SLOT.to_be_bytes());
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&part_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 4 {
            continue;
        }
        // suffix = [booth_id 4B]
        let mut b = [0u8; 4];
        b.copy_from_slice(&suffix[suffix.len() - 4..]);
        let booth_id = u32::from_be_bytes(b);
        let cursor = t
            .get(&MqCursorKey { event_id, part_id, booth_id })
            .map(|c| c.cursor)
            .unwrap_or(0);
        out.push((booth_id, cursor));
    }
    Ok(out)
}

/// The booth_id for a cursor name ("type/key") — the registry resolve the
/// consumer path uses; callers need it to map cursor rows back to routes.
pub fn booth_id_of(store: &MqStore, booth: &str) -> anyhow::Result<u32> {
    resolve_booth_id(store, booth)
}

/// The registered name for an booth id (None = never registered).
pub fn booth_name_of(store: &MqStore, booth_id: u32) -> anyhow::Result<Option<String>> {
    let t = Collection::<MqStore, BoothNameKey, BoothName>::new(store.clone());
    Ok(t.get(&BoothNameKey { id: booth_id }).map(|a| a.name))
}

/// The partition hash (exposed for realm-side watermark computation).
pub fn part_hash_of(part: &str) -> u64 {
    part_hash(part)
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
