//! The `ctx.store.emit(op)` executor (ADR-0026 §3): one host-bridge entry
//! carrying okm Collection operations as data. The executor resolves the
//! actor type's registry-allocated ns (`meta::ns_of`), rebuilds the type's
//! declared collection from the schema persisted at upload, and executes
//! the op through okm-dynamic `DynamicCollection` — the schema-driven
//! arm of the two-mode storage model (okm ADR-0025). The op protocol
//! types live in aura-actor (`store_emit.rs`); JSON is the wire currency,
//! converted to okm-dynamic `Value` at this seam.
//!
//! wasm (Rust source) actors do NOT pass through here: they compile the
//! static mode (derive + okm-core `Collection`) into the module and talk
//! engine calls over the host bridge — same bytes, compiled schema.

use crate::mq::MqStore;
use aura_actor::{StoreOp, StoreOpKind};
use okm_dynamic::{AccessMethod, DynamicCollection, ReduceSpec, Value, ValueMap};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// ReduceSpec holds a host-object logic (not cloneable) — declared specs
/// register here at plan parse; execution looks them up. Single-process
/// single-writer discipline (embedded mode) makes the global registry
/// safe; the plan is the only writer.
static REDUCE_SPECS: std::sync::LazyLock<Mutex<BTreeMap<(String, String), (u16, Vec<String>, PresetKind)>>> =
    std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));
static INDEX_SPECS: std::sync::LazyLock<Mutex<BTreeMap<(String, String), IndexSpecData>>> =
    std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// The cloneable schema data of one declared index — a fresh
/// `AccessMethod` is constructed per execution (the non-cloneable kinds
/// carry host objects; schema data is data).
#[derive(Clone, Debug)]
struct IndexSpecData {
    slot: u16,
    fields: Vec<String>,
    includes: Vec<String>,
}

impl IndexSpecData {
    fn access_method(&self) -> AccessMethod {
        AccessMethod { slot: self.slot, fields: self.fields.clone(), includes: self.includes.clone(), kind: okm_dynamic::AccessMethodKind::Plain }
    }
}

/// The type's storage plan: ns + per-collection schemas (parsed from the
/// persisted interface_schema's `storage` block at upload).
#[derive(Clone, Debug)]
pub struct StorePlan {
    pub ns: u16,
    /// collection name → schema (serde form carried in interface_schema).
    pub collections: BTreeMap<String, okm_core::schema::CollectionSchema>,
    /// collection name → (index name → slot) — the spec data lives in
    /// the INDEX_SPECS registry.
    pub indexes: BTreeMap<String, BTreeMap<String, u16>>,
    /// collection name → (reduce name → declared preset spec).
    pub reduces: BTreeMap<String, BTreeMap<String, (u16, Vec<String>)>>,
}

impl StorePlan {
    /// Parse the `storage` block of an interface_schema value:
    /// `{ "storage": { "collections": { "<name>": <CollectionSchema serde>, ... } } }`.
    pub fn from_schema(ns: u16, schema: &serde_json::Value) -> anyhow::Result<Self> {
        let mut collections = BTreeMap::new();
        let mut indexes: BTreeMap<String, BTreeMap<String, u16>> = BTreeMap::new();
        let mut reduces: BTreeMap<String, BTreeMap<String, (u16, Vec<String>)>> = BTreeMap::new();
        if let Some(block) = schema.get("storage").and_then(|s| s.get("collections")) {
            let map = block.as_object().ok_or_else(|| anyhow::anyhow!("interface_schema.storage.collections must be an object"))?;
            for (name, raw) in map {
                let cs: okm_core::schema::CollectionSchema = serde_json::from_value(raw.get("schema").cloned().unwrap_or(raw.clone()))
                    .map_err(|e| anyhow::anyhow!("collection `{name}` schema parse: {e}"))?;
                collections.insert(name.clone(), cs);
                let mut idx = BTreeMap::new();
                if let Some(list) = raw.get("indexes").and_then(|x| x.as_array()) {
                    let mut specs = INDEX_SPECS.lock().unwrap();
                    for e in list {
                        let (n, am) = build_access_method(e).map_err(|e| anyhow::anyhow!("collection `{name}` index: {e}"))?;
                        idx.insert(n.clone(), am.slot);
                        specs.insert((name.clone(), n), IndexSpecData { slot: am.slot, fields: am.fields, includes: am.includes });
                    }
                }
                indexes.insert(name.clone(), idx);
                let mut red = BTreeMap::new();
                if let Some(list) = raw.get("reduces").and_then(|x| x.as_array()) {
                    for e in list {
                        let (n, spec, kind) = build_reduce(e).map_err(|e| anyhow::anyhow!("collection `{name}` reduce: {e}"))?;
                        red.insert(n.clone(), (spec.slot, spec.group_fields.clone()));
                        REDUCE_SPECS.lock().unwrap().insert((name.clone(), n.clone()), (spec.slot, spec.group_fields.clone(), kind));
                    }
                }
                reduces.insert(name.clone(), red);
            }
        }
        Ok(Self { ns, collections, indexes, reduces })
    }
}

/// json → okm-dynamic Value (the executor seam; realm/value.rs is the
/// okm-core DynamicValue seam — two modes, two currencies).
fn json_to_value(v: &serde_json::Value) -> Result<Value, String> {
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Value::U64(u)
            } else if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else {
                Value::F64(n.as_f64().ok_or("non-finite number")?)
            }
        }
        serde_json::Value::String(s) => Value::Str(s.clone()),
        serde_json::Value::Array(items) => {
            // Bytes arrive as arrays of small ints — the JSON view of
            // FixedBytes key fields.
            let bytes: Result<Vec<u8>, _> = items.iter().map(|i| i.as_u64().map(|x| x as u8).ok_or_else(|| "array element not a byte".to_string())).collect();
            match bytes {
                Ok(b) => Value::Bytes(b),
                Err(_) => return Err("nested arrays are not storage values".into()),
            }
        }
        serde_json::Value::Object(_) => return Err("objects are documents, not field values".into()),
    })
}

fn json_map(v: &serde_json::Value, what: &str) -> Result<ValueMap, String> {
    let obj = v.as_object().ok_or_else(|| format!("{what} must be an object"))?;
    let mut m = ValueMap::new();
    for (k, v) in obj {
        m.insert(k.clone(), json_to_value(v)?);
    }
    Ok(m)
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::U8(x) => serde_json::json!(x),
        Value::U16(x) => serde_json::json!(x),
        Value::U32(x) => serde_json::json!(x),
        Value::U64(x) => serde_json::json!(x),
        Value::I64(x) => serde_json::json!(x),
        Value::F64(x) => serde_json::json!(x),
        Value::Bool(x) => serde_json::json!(x),
        Value::Null => serde_json::Value::Null,
        Value::Bytes(b) => serde_json::Value::Array(b.iter().map(|&x| serde_json::json!(x)).collect()),
        Value::Str(s) => serde_json::json!(s),
    }
}

fn map_to_json(m: &ValueMap) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (k, v) in m {
        out.insert(k.clone(), value_to_json(v));
    }
    serde_json::Value::Object(out)
}

/// Execute one op against the type's declared collections.
pub fn execute(
    store: &MqStore,
    plan: &StorePlan,
    op: &StoreOp,
) -> Result<serde_json::Value, String> {
    let schema = plan.collections.get(&op.collection).ok_or_else(|| {
        format!("collection `{}` is not declared by this type (declared: {:?})", op.collection, plan.collections.keys().collect::<Vec<_>>())
    })?;
    let indexes: Vec<AccessMethod> = plan
        .indexes
        .get(&op.collection)
        .map(|m| {
            let mut specs = INDEX_SPECS.lock().unwrap();
            m.keys()
                .filter_map(|n| specs.get(&(op.collection.clone(), n.clone())).map(IndexSpecData::access_method))
                .collect()
        })
        .unwrap_or_default();
    let reduces: Vec<ReduceSpec> = plan
        .reduces
        .get(&op.collection)
        .map(|m| {
            let specs = REDUCE_SPECS.lock().unwrap();
            m.keys()
                .filter_map(|n| {
                    specs.get(&(op.collection.clone(), n.clone()))
                        .map(|(slot, group, kind)| ReduceSpec { slot: *slot, group_fields: group.clone(), logic: Box::new(PresetLogic { kind: kind.clone() }) })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut coll = DynamicCollection::with_reduces(store.clone(), plan.ns, schema.clone(), indexes, reduces);
    match &op.op {
        StoreOpKind::PutDocument { key, doc } => {
            let k = okm_dynamic::encode_key(schema, &json_map(key, "key")?).map_err(|e| e.to_string())?;
            let d = json_map(doc, "doc")?;
            coll.put(&k, &d)?;
            Ok(serde_json::json!({ "ok": true }))
        }
        StoreOpKind::GetDocument { key } => {
            let k = okm_dynamic::encode_key(schema, &json_map(key, "key")?).map_err(|e| e.to_string())?;
            match coll.get(&k)? {
                Some(m) => Ok(map_to_json(&m)),
                None => Ok(serde_json::Value::Null),
            }
        }
        StoreOpKind::DeleteDocument { key } => {
            let k = okm_dynamic::encode_key(schema, &json_map(key, "key")?).map_err(|e| e.to_string())?;
            coll.delete(&k)?;
            Ok(serde_json::json!({ "ok": true }))
        }
        StoreOpKind::PutFields { .. } | StoreOpKind::GetFields { .. } | StoreOpKind::DeleteFields { .. } => {
            Err("field-level ops land with the dynamic-segment bridge on DynamicCollection (pending)".into())
        }
        StoreOpKind::Scan { index, value, limit } => {
            let slots = plan.indexes.get(&op.collection).ok_or_else(|| format!("collection `{}` declares no indexes", op.collection))?;
            let slot = *slots.get(index).ok_or_else(|| format!("index `{index}` is not declared (declared: {:?})", slots.keys().collect::<Vec<_>>()))?;
            // The encoded prefix: the indexed fields' values, in the
            // access method's field order, encoded by the schema
            // (fixed-width fields only — key or hot).
            let vals = value.as_array().ok_or("scan value must be an array (one value per indexed field, in order)")?;
            let (index_fields, index_slot) = {
                let specs = INDEX_SPECS.lock().unwrap();
                let am = specs.get(&(op.collection.clone(), index.clone())).ok_or_else(|| format!("index `{index}` spec missing"))?;
                (am.fields.clone(), am.slot)
            };
            let mut key_values = ValueMap::new();
            for (fname, v) in index_fields.iter().zip(vals.iter()) {
                key_values.insert(fname.clone(), json_to_value(v)?);
            }
            let encoded = okm_dynamic::encode_fields(schema, &index_fields, &key_values)
                .map_err(|e| format!("scan prefix encode: {e}"))?;
            let ns_prefix = plan.ns.to_be_bytes().to_vec();
            let am = INDEX_SPECS.lock().unwrap().get(&(op.collection.clone(), index.clone())).ok_or("index spec missing")?.access_method();
            let mut rows = okm_dynamic::scan_access_method(store, schema, &ns_prefix, &am, &encoded)?;
            if let Some(n) = limit {
                rows.truncate(limit.unwrap_or(u64::MAX) as usize);
            }
            Ok(serde_json::Value::Array(rows.iter().map(map_to_json).collect()))
        }
        StoreOpKind::ReduceGet { reduce, group } => {
            let slots = plan.reduces.get(&op.collection).ok_or_else(|| format!("collection `{}` declares no reduces", op.collection))?;
            let (slot, spec_group) = slots.get(reduce).ok_or_else(|| format!("reduce `{reduce}` is not declared (declared: {:?})", slots.keys().collect::<Vec<_>>()))?.clone();
            let g = json_map(group, "group")?;
            // Entry key: [ns 2B][slot u16 BE][group fields in declaration order].

            let mut key_values = ValueMap::new();
            for fname in &spec_group {
                let v = g.get(fname.as_str()).ok_or_else(|| format!("group value `{fname}` missing"))?;
                key_values.insert(fname.clone(), v.clone());
            }
            let mut entry = plan.ns.to_be_bytes().to_vec();
            entry.extend_from_slice(&slot.to_be_bytes());
            entry.extend_from_slice(&okm_dynamic::encode_fields(schema, &spec_group, &key_values).map_err(|e| e.to_string())?);
            match okm_dynamic::reduce_get(store, &entry) {
                Some(bytes) => {
                    if bytes.len() == 8 {
                        Ok(serde_json::json!(u64::from_be_bytes(bytes.try_into().unwrap())))
                    } else {
                        Ok(serde_json::Value::Array(bytes.iter().map(|&b| serde_json::json!(b)).collect()))
                    }
                }
                None => Ok(serde_json::Value::Null),
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mq::MqStore;

    /// A minimal valid CollectionSchema: key = one U64 field, one hot U64
    /// payload field. Built as serde JSON (the interface_schema's cargo).
    pub(super) fn schema_json() -> serde_json::Value {
        serde_json::json!({
            "key_len": 8,
            "key_fields": [
                { "name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": null }
            ],
            "layout_version": 1,
            "hot_width": 8,
            "payload_header_len": 3,
            "hot_fields": [
                { "name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": null }
            ],
            "cold_fields": [],
            "slots": { "primary": 0, "dynamic": 1, "dict_id": 2, "dict_name": 3, "declared_index_base": 4096, "declared_reduce_base": 8192, "junction_base": 12288 }
        })
    }

    #[test]
    fn put_get_roundtrip_through_emit() {
        let store = MqStore::mem();
        let plan = StorePlan::from_schema(700, &serde_json::json!({
            "storage": { "collections": { "counters": schema_json() } }
        }))
        .unwrap();
        let op = StoreOp {
            collection: "counters".into(),
            op: StoreOpKind::PutDocument {
                key: serde_json::json!({ "id": 7 }),
                doc: serde_json::json!({ "count": 42 }),
            },
        };
        execute(&store, &plan, &op).unwrap();
        let get = StoreOp {
            collection: "counters".into(),
            op: StoreOpKind::GetDocument { key: serde_json::json!({ "id": 7 }) },
        };
        let out = execute(&store, &plan, &get).unwrap();
        assert_eq!(out["count"], 42);
        // Distinct ns = no cross-talk.
        let other = StorePlan::from_schema(701, &serde_json::json!({
            "storage": { "collections": { "counters": schema_json() } }
        }))
        .unwrap();
        let out = execute(&store, &other, &get).unwrap();
        assert!(out.is_null());
    }

    #[test]
    fn undeclared_collection_is_an_error() {
        let store = MqStore::mem();
        let plan = StorePlan::from_schema(700, &serde_json::json!({})).unwrap();
        let op = StoreOp {
            collection: "ghost".into(),
            op: StoreOpKind::GetDocument { key: serde_json::json!({ "id": 1 }) },
        };
        assert!(execute(&store, &plan, &op).is_err());
    }
}

// ---------------------------------------------------------------------
// Schema-declared access methods and preset reduces (from the
// interface_schema's storage block, as data):
//   "indexes":  [ { "name": "by_user", "slot": 4097, "fields": [...],
//                   "includes": [...], "kind": "plain" } ]
//   "reduces":  [ { "name": "total", "slot": 8193, "group": [...],
//                   "kind": "count" | {"high_water": "field"} | {"low_water": "field"} } ]
// Preset logics implement okm-dynamic's ReduceLogic in Rust (the acc is
// u64 BE — the same layout okm-core's presets use); host-language logic
// objects (python/steel callables) register through the bindings, not
// through schema data.

use okm_dynamic::ReduceLogicObj;

fn build_access_method(v: &serde_json::Value) -> Result<(String, AccessMethod), String> {
    let obj = v.as_object().ok_or("index entry must be an object")?;
    let name = obj.get("name").and_then(|x| x.as_str()).ok_or("index missing name")?.to_string();
    let slot = obj.get("slot").and_then(|x| x.as_u64()).ok_or("index missing slot")? as u16;
    let fields = obj.get("fields").and_then(|x| x.as_array()).ok_or("index missing fields")?
        .iter().map(|x| x.as_str().map(String::from).ok_or_else(|| "index field not a string".to_string())).collect::<Result<Vec<_>, _>>()?;
    let includes = match obj.get("includes").and_then(|x| x.as_array()) {
        Some(a) => a.iter().map(|x| x.as_str().map(String::from).ok_or_else(|| "includes field not a string".to_string())).collect::<Result<Vec<_>, _>>()?,
        None => Vec::new(),
    };
    let kind = match obj.get("kind").and_then(|x| x.as_str()).unwrap_or("plain") {
        "plain" => okm_dynamic::AccessMethodKind::Plain,
        other => return Err(format!("unsupported index kind `{other}`")),
    };
    Ok((name, AccessMethod { slot, fields, includes, kind }))
}

/// Preset reduce logics over the u64 BE accumulator.
#[derive(Clone)]
enum PresetKind { Count, HighWater(String), LowWater(String) }

struct PresetLogic { kind: PresetKind }

impl PresetLogic {
    fn field_u64(doc: &ValueMap, field: &str) -> Result<u64, String> {
        match doc.get(field) {
            Some(Value::U64(v)) => Ok(*v),
            Some(other) => Err(format!("reduce field `{field}` is not U64: {other:?}")),
            None => Err(format!("reduce field `{field}` missing from document")),
        }
    }
}

impl okm_dynamic::ReduceLogic for PresetLogic {
    fn seed(&self) -> Vec<u8> {
        match self.kind {
            PresetKind::Count | PresetKind::HighWater(_) => 0u64.to_be_bytes().to_vec(),
            PresetKind::LowWater(_) => u64::MAX.to_be_bytes().to_vec(),
        }
    }
    fn fold(&self, acc: &mut Vec<u8>, _key: &ValueMap, document: &ValueMap) -> Result<(), String> {
        let mut cur = u64::from_be_bytes(acc.as_slice().try_into().unwrap_or([0u8; 8]));
        match &self.kind {
            PresetKind::Count => cur += 1,
            PresetKind::HighWater(f) => cur = cur.max(Self::field_u64(document, f)?),
            PresetKind::LowWater(f) => cur = cur.min(Self::field_u64(document, f)?),
        }
        *acc = cur.to_be_bytes().to_vec();
        Ok(())
    }
    fn unfold(&self, acc: &mut Vec<u8>, _key: &ValueMap, document: &ValueMap) -> Result<(), String> {
        match &self.kind {
            PresetKind::Count => {
                let mut cur = u64::from_be_bytes(acc.as_slice().try_into().unwrap_or([0u8; 8]));
                cur = cur.saturating_sub(1);
                *acc = cur.to_be_bytes().to_vec();
                Ok(())
            }
            // Watermark: never falls.
            PresetKind::HighWater(_) | PresetKind::LowWater(_) => Ok(()),
        }
    }
}

fn build_reduce(v: &serde_json::Value) -> Result<(String, ReduceSpec, PresetKind), String> {
    let obj = v.as_object().ok_or("reduce entry must be an object")?;
    let name = obj.get("name").and_then(|x| x.as_str()).ok_or("reduce missing name")?.to_string();
    let slot = obj.get("slot").and_then(|x| x.as_u64()).ok_or("reduce missing slot")? as u16;
    let group_fields = match obj.get("group").and_then(|x| x.as_array()) {
        Some(a) => a.iter().map(|x| x.as_str().map(String::from).ok_or_else(|| "group field not a string".to_string())).collect::<Result<Vec<_>, _>>()?,
        None => Vec::new(),
    };
    let kind_v = obj.get("kind").ok_or("reduce missing kind")?;
    let kind_name = kind_v.as_str().map(String::from).unwrap_or_default();
    let (logic, _field) = if kind_name == "count" {
        (PresetKind::Count, None)
    } else if let Some(f) = kind_v.get("high_water").and_then(|x| x.as_str()) {
        (PresetKind::HighWater(f.to_string()), Some(f.to_string()))
    } else if let Some(f) = kind_v.get("low_water").and_then(|x| x.as_str()) {
        (PresetKind::LowWater(f.to_string()), Some(f.to_string()))
    } else {
        return Err(format!("unsupported reduce kind `{kind_name}`"));
    };
    let spec = ReduceSpec { slot, group_fields: group_fields.clone(), logic: Box::new(PresetLogic { kind: logic.clone() }) };
    Ok((name, spec, logic))
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    use okm_core::storage::VirtualStorage;
    use super::tests::schema_json;
    #[test]
    fn scan_and_reduce_through_emit() {
        let store = MqStore::mem();
        let plan = StorePlan::from_schema(710, &serde_json::json!({
            "storage": { "collections": { "orders": {
                "schema": schema_json(),
                "indexes": [ { "name": "by_count", "slot": 4097, "fields": ["count"], "kind": "plain" } ],
                "reduces": [ { "name": "n", "slot": 8193, "group": ["id"], "kind": "count" },
                             { "name": "peak", "slot": 8194, "group": ["id"], "kind": { "high_water": "count" } } ]
            } } }
        }))
        .unwrap();
        for (id, count) in [(1u64, 10u64), (2, 20)] {
            execute(&store, &plan, &StoreOp {
                collection: "orders".into(),
                op: StoreOpKind::PutDocument {
                    key: serde_json::json!({ "id": id }),
                    doc: serde_json::json!({ "count": count }),
                },
            }).unwrap();
        }
        // Index scan by count value.
        let scan = execute(&store, &plan, &StoreOp {
            collection: "orders".into(),
            op: StoreOpKind::Scan { index: "by_count".into(), value: serde_json::json!([20]), limit: None },
        }).unwrap();
        assert_eq!(scan.as_array().unwrap().len(), 1, "one order with count=20");
        assert_eq!(scan[0]["id"], 2, "scan results are KEY maps (the id of the count=20 order)");
        // Preset reduce: count = 1 per group; high_water = the count.
        let rg = |name: &str, id: u64| execute(&store, &plan, &StoreOp {
            collection: "orders".into(),
            op: StoreOpKind::ReduceGet { reduce: name.into(), group: serde_json::json!({ "id": id }) },
        }).unwrap();
        assert_eq!(rg("n", 1), serde_json::json!(1u64));
        assert_eq!(rg("peak", 2), serde_json::json!(20u64));
        // Absent group → null.
        assert!(rg("n", 99).is_null());
    }
}

