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
use okm_dynamic::{DynamicCollection, Value, ValueMap};
use std::collections::BTreeMap;

/// The type's storage plan: ns + per-collection schemas (parsed from the
/// persisted interface_schema's `storage` block at upload).
#[derive(Clone, Debug)]
pub struct StorePlan {
    pub ns: u16,
    /// collection name → schema (serde form carried in interface_schema).
    pub collections: BTreeMap<String, okm_core::schema::CollectionSchema>,
}

impl StorePlan {
    /// Parse the `storage` block of an interface_schema value:
    /// `{ "storage": { "collections": { "<name>": <CollectionSchema serde>, ... } } }`.
    pub fn from_schema(ns: u16, schema: &serde_json::Value) -> anyhow::Result<Self> {
        let mut collections = BTreeMap::new();
        if let Some(block) = schema.get("storage").and_then(|s| s.get("collections")) {
            let map = block.as_object().ok_or_else(|| anyhow::anyhow!("interface_schema.storage.collections must be an object"))?;
            for (name, raw) in map {
                let cs: okm_core::schema::CollectionSchema = serde_json::from_value(raw.clone())
                    .map_err(|e| anyhow::anyhow!("collection `{name}` schema parse: {e}"))?;
                collections.insert(name.clone(), cs);
            }
        }
        Ok(Self { ns, collections })
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
    let mut coll = DynamicCollection::new(store.clone(), plan.ns, schema.clone(), Vec::new());
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
        StoreOpKind::Scan { .. } => {
            Err("scan rides the access-method declaration (pending — index slots from the schema's AccessMethod list)".into())
        }
        StoreOpKind::ReduceGet { .. } => {
            Err("reduce reads ride the declared BoundReduce set (pending)".into())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mq::MqStore;

    /// A minimal valid CollectionSchema: key = one U64 field, one hot U64
    /// payload field. Built as serde JSON (the interface_schema's cargo).
    fn schema_json() -> serde_json::Value {
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
