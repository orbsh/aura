//! Persistent event queues (Phase 4.5c step 2b) as okm tables — per the
//! PLAN ruling and ADR-0002/0005/0006 discipline. Tables in one okm model
//! over the realm's own store (the `StoreAsVirtual` bridge):
//!
//! - `EventName` — open-ended event-name vocabulary: proxy id key, name
//!   payload, `by_name` text index (names are runtime data, not ns; the
//!   ns dictionary stays compile-time). Lookup = index scan + row verify
//!   (the text-first regime's documented cost: no delimiter, so "add"
//!   prefix-matches "add_to_cart"; the row comparison is the exactness).
//! - `ActorName` — same registry pattern for subscriber identity.
//! - `MqData` — `[event_id][part_id][seq]` → payload. An event belongs to
//!   no actor: one row per emitted event, N subscribers = N cursors.
//! - `MqCursor` — `[event_id][part_id][actor_id]` → last consumed seq.
//!
//! Backlog = range scan after the cursor; skip-to-now = cursor write to
//! the partition head. Min-watermark retention compaction and reduce-based
//! depth counts are follow-ups (PLAN), not implemented here.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use okm_core::document::Collection;
use okm_core::{KeyEncode, Document, DocumentEncode};
use okm_core::storage::VirtualStorage as _;
/// serde_json::Value -> okm DynamicValue (the dynamic-segment currency).
/// Numbers widen to i64/f64; the dynamic reader narrows on consumption.
fn json_to_dyn(v: &serde_json::Value) -> okm_core::obj_dynamic::DynamicValue {
    use okm_core::obj_dynamic::DynamicValue;
    match v {
        serde_json::Value::Null => DynamicValue::Null,
        serde_json::Value::Bool(b) => DynamicValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DynamicValue::Int(i)
            } else {
                DynamicValue::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => DynamicValue::Str(s.clone()),
        serde_json::Value::Array(items) => {
            DynamicValue::Array(items.iter().map(json_to_dyn).collect())
        }
        serde_json::Value::Object(map) => DynamicValue::Obj(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_dyn(v)))
                .collect(),
        ),
    }
}

fn dyn_to_json(v: &okm_core::obj_dynamic::DynamicValue) -> serde_json::Value {
    use okm_core::obj_dynamic::DynamicValue;
    match v {
        DynamicValue::Null => serde_json::Value::Null,
        DynamicValue::Bool(b) => serde_json::Value::Bool(*b),
        DynamicValue::Int(i) => serde_json::Value::Number((*i).into()),
        DynamicValue::UInt(u) => serde_json::Value::Number((*u).into()),
        DynamicValue::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        DynamicValue::Str(s) => serde_json::Value::String(s.clone()),
        DynamicValue::Bytes(b) => {
            // Opaque bytes: try UTF-8 text, else base64 (JSON has no binary).
            std::str::from_utf8(b)
                .map(|s| serde_json::Value::String(s.to_string()))
                .unwrap_or_else(|_| {
                    serde_json::Value::String(BASE64.encode(b))
                })
        }
        DynamicValue::Array(items) => {
            serde_json::Value::Array(items.iter().map(dyn_to_json).collect())
        }
        DynamicValue::Obj(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), dyn_to_json(v)))
                .collect(),
        ),
    }
}


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
    pub seq: u64,
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
    pub actor_id: u32,
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
pub struct ActorNameKey {
    pub id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(ActorNameKey)]
#[ok_index(by_name { fields(name) })]
#[ok_ns(33)]
pub struct ActorName {
    pub name: String,
}

// ---------------------------------------------------------------------------
// Store bridge: realm's StateStore raw ops as okm VirtualStorage. Raw-key
// ops map 1:1 (values ride base64 inside the JSON store values — the
// StateStore's value type is serde_json::Value; okm wants raw bytes).
// The mq tables ride the SAME engine instance as actor state.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct StoreAsVirtual(pub aura_actor::SharedStore);

impl okm_core::storage::VirtualStorage for StoreAsVirtual {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let _ = self
            .0
            .set_raw(key, serde_json::Value::String(BASE64.encode(&value)));
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0
            .get_raw(key)
            .ok()
            .flatten()
            .and_then(|v| v.as_str().map(str::to_owned))
            .and_then(|s| BASE64.decode(s).ok())
    }
    fn del(&mut self, key: &[u8]) {
        let _ = self.0.del_raw(key);
    }
    fn scan_suffix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.0
            .scan_keys(prefix)
            .unwrap_or_default()
            .into_iter()
            .map(|k| k[prefix.len()..].to_vec())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Registry resolve: name → id (assign on first sight). Exact match = index
// prefix scan + row verify ("add" scans "add_to_cart" too — text-first
// regime); miss = append with the next id.
// ---------------------------------------------------------------------------

fn resolve_event_id(store: &mut StoreAsVirtual, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<StoreAsVirtual, EventNameKey, EventName>::new(store.clone());
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

fn resolve_actor_id(store: &mut StoreAsVirtual, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<StoreAsVirtual, ActorNameKey, ActorName>::new(store.clone());
    for hit in t.scan::<__OkmIndex_ActorName_by_name>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(hit.0.decoded.id);
            }
        }
    }
    let mut max_id = 0u32;
    for (pk, _) in t.scan::<__OkmIndex_ActorName_by_name>(&[]) {
        if pk.decoded.id > max_id {
            max_id = pk.decoded.id;
        }
    }
    let id = max_id + 1;
    t.put(&ActorNameKey { id }, &ActorName { name: name.to_string() });
    Ok(id)
}

/// Append one event to a partition; returns the assigned seq (the
/// partition's max seq + 1 — a full-scan head derivation on the mq-data
/// prefix; watermark compaction will revisit this).
pub fn append(
    store: &mut StoreAsVirtual,
    event: &str,
    part: &str,
    payload: &serde_json::Value,
) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_hash(part);
    let mut t = Collection::<StoreAsVirtual, MqDataKey, MqData>::new(store.clone());
    let seq = {
        let mut max_seq = 0u64;
        // Range scan the partition's primary-key segment: keys sort by seq
        // (BE suffix), so the head is the max over the scan.
        let mut prefix = Vec::new();
        prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
        prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
        prefix.push(okm_core::index::PRIMARY_SLOT);
        prefix.extend_from_slice(&event_id.to_be_bytes());
        prefix.extend_from_slice(&part_id.to_be_bytes());
        for suffix in store.scan_suffix(&prefix) {
            // suffix = [seq 8B] (the rest of the primary key)
            if suffix.len() >= 8 {
                let mut b = [0u8; 8];
                b.copy_from_slice(&suffix[suffix.len() - 8..]);
                let s = u64::from_be_bytes(b);
                if s > max_seq {
                    max_seq = s;
                }
            }
        }
        max_seq + 1
    };
    t.put(&MqDataKey { event_id, part_id, seq }, &MqData {});
    // Payload → dynamic segment, fully native. The emit chain guarantees
    // an object top (routing reads the partition key from data fields;
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
    t.put_document(&MqDataKey { event_id, part_id, seq }, &obj);
    Ok(seq)
}

/// Partition id: open-ended string → u64. FNV-1a — a key FIELD hash, not a
/// namespace (ADR-0002's hash rejection is about the ns dictionary, not
/// payload-level discriminators); collisions only merge two partitions'
/// backlogs, never lose events, and the consumer's handler re-checks
/// nothing (partitioning is a delivery fan-out key, not an address).
fn part_hash(part: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in part.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

/// The subscriber's cursor (0 = nothing consumed).
pub fn cursor(store: &mut StoreAsVirtual, event: &str, part: &str, actor: &str) -> anyhow::Result<u64> {
    let event_id = resolve_event_id(store, event)?;
    let actor_id = resolve_actor_id(store, actor)?;
    let mut t = Collection::<StoreAsVirtual, MqCursorKey, MqCursor>::new(store.clone());
    Ok(t.get(&MqCursorKey {
        event_id,
        part_id: part_hash(part),
        actor_id,
    })
    .map(|c| c.cursor)
    .unwrap_or(0))
}

/// Advance the cursor after consuming.
pub fn advance(
    store: &mut StoreAsVirtual,
    event: &str,
    part: &str,
    actor: &str,
    seq: u64,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let actor_id = resolve_actor_id(store, actor)?;
    let mut t = Collection::<StoreAsVirtual, MqCursorKey, MqCursor>::new(store.clone());
    t.put(
        &MqCursorKey { event_id, part_id: part_hash(part), actor_id },
        &MqCursor { cursor: seq },
    );
    Ok(())
}

/// The subscriber's backlog: (seq, payload) strictly after `after_seq`,
/// oldest first. Empty = caught up.
pub fn backlog(
    store: &mut StoreAsVirtual,
    event: &str,
    part: &str,
    after_seq: u64,
) -> anyhow::Result<Vec<(u64, serde_json::Value)>> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_hash(part);
    let mut t = Collection::<StoreAsVirtual, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.push(okm_core::index::PRIMARY_SLOT);
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
        if seq > after_seq {
            if let Some(row) = t.get(&MqDataKey { event_id, part_id, seq }) {
                // Reconstruct from the dynamic segment (name-keyed).
                let v = match t.get_document(&MqDataKey { event_id, part_id, seq }) {
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
    }
    out.sort_by_key(|(s, _)| *s);
    Ok(out)
}

/// skip-to-now: jump the cursor to the partition head, discarding the
/// stale backlog (the relief valve per the ruling).
pub fn skip_to_now(
    store: &mut StoreAsVirtual,
    event: &str,
    part: &str,
    actor: &str,
) -> anyhow::Result<()> {
    let event_id = resolve_event_id(store, event)?;
    let part_id = part_hash(part);
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.push(okm_core::index::PRIMARY_SLOT);
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&part_id.to_be_bytes());
    let mut head = 0u64;
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() >= 8 {
            let mut b = [0u8; 8];
            b.copy_from_slice(&suffix[suffix.len() - 8..]);
            head = head.max(u64::from_be_bytes(b));
        }
    }
    advance(store, event, part, actor, head)
}

// ---------------------------------------------------------------------------
// Retention (min-watermark over registered subscribers). The watermark's
// denominator comes from the ROUTE REGISTRY (the persisted @on metadata),
// never from the raw cursor keys: a cursor row whose actor no longer has a
// route for this event must not pin the watermark. Eviction (instance
// scale-to-zero) does NOT deregister — the type's route remains, the
// instance replays its backlog on re-activation; deregistration (type
// hot-swap / actor deletion) drops the route, and the stale cursor row
// falls out of the denominator (its row is removable by prefix scan).
// ---------------------------------------------------------------------------

/// Delete mq-data rows in a partition with seq < `min_seq`. Returns the
/// number of rows removed.
pub fn delete_before(
    store: &mut StoreAsVirtual,
    event_id: u32,
    part_id: u64,
    min_seq: u64,
) -> anyhow::Result<usize> {
    let mut t = Collection::<StoreAsVirtual, MqDataKey, MqData>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqData as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqData as Document>::NS_PREFIX);
    prefix.push(okm_core::index::PRIMARY_SLOT);
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
            t.delete_by_pkey(&MqDataKey { event_id, part_id, seq });
            removed += 1;
        }
    }
    Ok(removed)
}

/// Every cursor row in a partition: (actor_id, cursor). The caller filters
/// against the route registry.
pub fn cursor_rows(
    store: &mut StoreAsVirtual,
    event_id: u32,
    part_id: u64,
) -> anyhow::Result<Vec<(u32, u64)>> {
    let t = Collection::<StoreAsVirtual, MqCursorKey, MqCursor>::new(store.clone());
    let mut prefix = Vec::new();
    prefix.extend_from_slice(<MqCursor as Document>::PARTITION_PREFIX);
    prefix.extend_from_slice(<MqCursor as Document>::NS_PREFIX);
    prefix.push(okm_core::index::PRIMARY_SLOT);
    prefix.extend_from_slice(&event_id.to_be_bytes());
    prefix.extend_from_slice(&part_id.to_be_bytes());
    let mut out = Vec::new();
    for suffix in store.scan_suffix(&prefix) {
        if suffix.len() < 4 {
            continue;
        }
        // suffix = [actor_id 4B]
        let mut b = [0u8; 4];
        b.copy_from_slice(&suffix[suffix.len() - 4..]);
        let actor_id = u32::from_be_bytes(b);
        let cursor = t
            .get(&MqCursorKey { event_id, part_id, actor_id })
            .map(|c| c.cursor)
            .unwrap_or(0);
        out.push((actor_id, cursor));
    }
    Ok(out)
}

/// The actor_id for a cursor name ("type/key") — the registry resolve the
/// consumer path uses; callers need it to map cursor rows back to routes.
pub fn actor_id_of(store: &mut StoreAsVirtual, actor: &str) -> anyhow::Result<u32> {
    resolve_actor_id(store, actor)
}

/// The registered name for an actor id (None = never registered).
pub fn actor_name_of(store: &mut StoreAsVirtual, actor_id: u32) -> anyhow::Result<Option<String>> {
    let t = Collection::<StoreAsVirtual, ActorNameKey, ActorName>::new(store.clone());
    Ok(t.get(&ActorNameKey { id: actor_id }).map(|a| a.name))
}

/// The partition hash (exposed for realm-side watermark computation).
pub fn part_hash_of(part: &str) -> u64 {
    part_hash(part)
}

/// The registered id for an event name (None = never emitted/registered).
pub fn event_id_of(store: &mut StoreAsVirtual, event: &str) -> anyhow::Result<Option<u32>> {
    let t = Collection::<StoreAsVirtual, EventNameKey, EventName>::new(store.clone());
    for hit in t.scan::<__OkmIndex_EventName_by_name>(event.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == event {
                return Ok(Some(hit.0.decoded.id));
            }
        }
    }
    Ok(None)
}
