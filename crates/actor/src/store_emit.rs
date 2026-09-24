//! The `ctx.store.emit(op)` protocol (ADR-0026 §3): one host-bridge entry
//! carrying okm Collection operations as data. aura-actor declares the
//! protocol; the realm side translates each op onto okm's dynamic-document
//! Collection API. The op set stays at the COLLECTION semantic layer —
//! raw VirtualStorage primitives are not exposed (a raw put would skip
//! index/reduce compensation). No bypass guard is built: the developer
//! holds full control of the type's ns; primitive misuse is self-sabotage
//! only (ADR-0026 Honest semantic cost).
//!
//! JSON is the wire encoding here (the host bridge's currency, matching
//! `ctx_state_*` / `ctx_invoke`); the realm converts to `DynamicValue` at
//! its seam (`realm/src/value.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One storage instruction: collection name + operation + arguments.
/// Addressing is the TYPE'S ns (bound at registration); `collection`
/// selects among the type's declared collections.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StoreOp {
    /// The type-declared collection this op addresses.
    pub collection: String,
    /// The operation: a [StoreOpKind] name plus its argument fields.
    #[serde(flatten)]
    pub op: StoreOpKind,
}

/// The Collection-semantic-layer op set over dynamic documents. Field
/// names inside `args` are the okm document map's names (declared fields
/// go through the typed path, the rest to the dynamic segment).
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum StoreOpKind {
    /// Whole-document write (replaces the dynamic segment; declared
    /// fields RMW per okm's put_document). args: { key: <map>, doc: <map> }.
    PutDocument { key: Value, doc: Value },
    /// Whole-document read: declared + dynamic fields merged.
    /// args: { key: <map> }. Missing document → `null` result.
    GetDocument { key: Value },
    /// Drop the whole document (both segments). args: { key: <map> }.
    DeleteDocument { key: Value },
    /// Field write (RMW of the whole field map — the per-field unit).
    /// args: { key: <map>, fields: <map> }.
    PutFields { key: Value, fields: Value },
    /// Field read. args: { key: <map>, fields: [names] }.
    GetFields { key: Value, fields: Vec<String> },
    /// Drop the dynamic segment. args: { key: <map> }.
    DeleteFields { key: Value },
    /// Index scan by encoded index value. args: { index: <name>,
    /// value: <encoding-compatible scalar or map> }, limit optional.
    Scan { index: String, value: Value, limit: Option<u64> },
    /// Group accumulator read (declared reduce). args: { reduce: <name>,
    /// group: <map> }. Missing group → `null`.
    ReduceGet { reduce: String, group: Value },
}

/// The result of one op: document ops return maps, scans return arrays,
/// deletes return booleans, absence returns null.
pub type StoreOpResult = Value;

impl StoreOp {
    pub fn parse(arg: &Value) -> anyhow::Result<Self> {
        serde_json::from_value(arg.clone())
            .map_err(|e| anyhow::anyhow!("ctx.store.emit: malformed op: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_roundtrip() {
        let arg = serde_json::json!({
            "collection": "carts",
            "op": "put_document",
            "key": {"user": "alice"},
            "doc": {"items": 3}
        });
        let op = StoreOp::parse(&arg).unwrap();
        assert_eq!(op.collection, "carts");
        match op.op {
            StoreOpKind::PutDocument { ref key, ref doc } => {
                assert_eq!(key["user"], "alice");
                assert_eq!(doc["items"], 3);
            }
            other => panic!("wrong op: {other:?}"),
        }
    }

    #[test]
    fn unknown_op_is_rejected() {
        let arg = serde_json::json!({ "collection": "c", "op": "drop_table" });
        assert!(StoreOp::parse(&arg).is_err());
    }
}
