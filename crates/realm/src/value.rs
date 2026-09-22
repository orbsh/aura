//! The JSON ↔ DynamicValue seam (ADR-0018): serde_json::Value is the
//! actor-API's currency; okm DynamicValue is the storage currency. The
//! conversion happens EXACTLY here, once — no storage path may carry
//! JSON, no API path may carry DynamicValue.
//!
//! Numbers widen to i64/f64 on the way in; the dynamic reader narrows on
//! consumption. Opaque bytes map to UTF-8 text when possible, else
//! base64 (JSON has no binary — that encoding is a display decision at
//! the seam, never a storage shape).

use okm_core::obj_dynamic::DynamicValue;

pub fn json_to_dyn(v: &serde_json::Value) -> DynamicValue {
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

pub fn dyn_to_json(v: &DynamicValue) -> serde_json::Value {
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
                    use base64::Engine as _;
                    use base64::engine::general_purpose::STANDARD as BASE64;
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
