//! Actor state as documents (ADR-0018 step 2): one document per actor
//! instance in an okm collection — `ctx.state.set(field, v)` writes one
//! dynamic-segment field of THAT document, `get` reads one field back.
//! The state store's JSON currency (`serde_json::Value`) stops at the
//! `StateDocumentStore` seam: inside, everything is `DynamicValue` via
//! `put_document` / `get_document` (okm's public document API — the
//! per-field hand-encoded frame form is rejected by ADR-0018).
//!
//! Key model: the instance identity is `(type name, instance key)` — two
//! open-ended runtime strings. The okm key discipline rejects `String`
//! key fields outright (fixed-width BE segments, pure pointer slicing),
//! so the identity resolves to a fixed-width surrogate, the registry
//! pattern:
//!
//! - type name → `ActorName` registry id (mq.rs — the same registry the
//!   mq tables share);
//! - instance key → `InstanceStateKey { instance_id: u32 }` proxy key.
//!   The (type_id, key) → id assignment rides the document's `by_key`
//!   text index (`fields(type_id, instance_key)` — fixed-width leading,
//!   variable-length terminal, ADR-0005's allowed shape; exact match =
//!   prefix scan + row verify, the text-first regime's accepted price).
//!   The next id comes from a MAX reduce over `type_id` — okm's
//!   cross-document precomputation, the fold running inside the same
//!   engine write as the document put (no scan, no separate counter
//!   entry to keep in sync).
//!
//! The document declares `type_id`, `instance_key` and `instance_id`
//! (the payload mirror of the proxy key — the reduce folds over payload
//! fields by contract, so the id rides there too; both copies are
//! written on every create path and read paths use the key). Every
//! OTHER ctx field lands in the dynamic segment. A ctx field named like
//! a declared field would route to the typed path and be shadowed on
//! read — the three names are reserved at the seam.
//!
//! Namespace isolation rides the MqStore prefix the same way the mq
//! tables do: a namespaced `MqStore` makes cross-namespace state
//! unreachable (structural, bound at construction).

use crate::mq::resolve_actor_type_id;
use crate::mq::MqStore;
use crate::value::{json_to_dyn, dyn_to_json};
use aura_actor::{InstanceId, StateStore};
use okm_core::document::Collection;
use okm_core::storage::VirtualStorage as _;
use okm_core::{Document, DocumentEncode, KeyEncode, ReduceLogic, ReduceCodec};
use serde_json::Value;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// The table: proxy id key + identity fields + one document per instance.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct InstanceStateKey {
    /// Registry-resolved actor type id — the proxy id's owner segment:
    /// each type's ids are assigned from its own watermark, so the
    /// primary key MUST carry the type (two types' id=1 are different
    /// rows, not collisions).
    pub type_id: u32,
    /// Proxy id within the type: assigned on first sight of (type_id,
    /// key), fixed width.
    pub instance_id: u32,
}

/// One document per actor instance. The declared identity triple
/// (`type_id`, `instance_key`, `instance_id` payload mirror) addresses
/// and observes; every OTHER ctx field lands in the dynamic segment via
/// put_document.
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(InstanceStateKey)]
#[ok_index(by_key { fields(type_id, instance_key) })]
#[ok_reduce(HighWater(instance_id) { group(type_id) })]
#[ok_ns(34)]
pub struct InstanceState {
    /// Registry-resolved actor type id (fixed width, index-leading).
    pub type_id: u32,
    /// The raw instance key (variable-length, terminal in the index —
    /// dictionary order, exact match via row verify).
    pub instance_key: String,
    /// Payload mirror of the proxy key: the reduce hook folds over
    /// payload fields, so the id rides here for `MaxInstanceId`. Written
    /// on every create path; addressing always goes through the KEY.
    pub instance_id: u32,
}

// ---------------------------------------------------------------------------
// Identity resolve: (type_id, key) → proxy id, assign on first sight.
// ---------------------------------------------------------------------------

/// The generated index struct the derive emits per `#[ok_index]`.
use __OkmIndex_InstanceState_by_key as ByKey;

fn resolve_instance_id(
    store: &MqStore,
    type_id: u32,
    key: &str,
) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, InstanceStateKey, InstanceState>::new(store.clone());
    // Exact match through the text index: prefix scan by the fixed-width
    // type segment + the raw key bytes, then verify (no delimiter in the
    // index bytes — "a" scans "ab" too; the row comparison is the
    // exactness).
    let mut probe = Vec::with_capacity(4 + key.len());
    probe.extend_from_slice(&type_id.to_be_bytes());
    probe.extend_from_slice(key.as_bytes());
    for hit in t.scan::<ByKey>(&probe) {
        if let Some(row) = &hit.1 {
            if row.type_id == type_id && row.instance_key == key {
                return Ok(hit.0.decoded.instance_id);
            }
        }
    }
    // Miss: next id = the type's MAX reduce watermark + 1 (no scan; the
    // fold runs inside the same engine write as the put below).
    let doc = InstanceState {
        type_id,
        instance_key: key.to_string(),
        instance_id: 0, // placeholder; the real id is set just below
    };
    // reduce_get(store, ns, key, document): the group segment comes from
    // the named fields (type_id only) — key/other fields are irrelevant.
    let watermark: u64 = okm_core::reduce_get::<MqStore, __OkmReduce_InstanceState_0>(
        t.store(),
        <InstanceState as Document>::NS_PREFIX,
        &InstanceStateKey { type_id, instance_id: 0 },
        &InstanceState {
            type_id,
            instance_key: String::new(),
            instance_id: 0,
        },
    )
    .unwrap_or(0);
    let id = (watermark as u32) + 1;
    let doc = InstanceState {
        type_id,
        instance_key: key.to_string(),
        instance_id: id,
    };
    t.put(&InstanceStateKey { type_id, instance_id: id }, &doc);
    Ok(id)
}

// ---------------------------------------------------------------------------
// The StateStore adapter: JSON at the seam, documents inside.
// ---------------------------------------------------------------------------

pub struct StateDocumentStore {
    mq: MqStore,
}

impl StateDocumentStore {
    pub fn new(mq: MqStore) -> Self {
        Self { mq }
    }

    fn collection(&self) -> Collection<MqStore, InstanceStateKey, InstanceState> {
        Collection::new(self.mq.clone())
    }

    /// The full document of one instance (None = never stored).
    fn document(
        &self,
        id: &InstanceId,
    ) -> anyhow::Result<Option<BTreeMap<String, okm_core::obj_dynamic::DynamicValue>>> {
        let type_id = resolve_actor_type_id(&self.mq.clone(), &id.actor_type)?;
        let instance_id = resolve_instance_id(&self.mq.clone(), type_id, &id.key)?;
        let mut t = self.collection();
        Ok(t.get_document(&InstanceStateKey { type_id, instance_id }))
    }

    fn key_of(&self, id: &InstanceId) -> anyhow::Result<InstanceStateKey> {
        let type_id = resolve_actor_type_id(&self.mq.clone(), &id.actor_type)?;
        let instance_id = resolve_instance_id(&self.mq.clone(), type_id, &id.key)?;
        Ok(InstanceStateKey { type_id, instance_id })
    }
}

impl StateStore for StateDocumentStore {
    fn key_for(&self, _id: &InstanceId, _field: &str) -> Vec<u8> {
        // The document model owns addressing; raw-key views of actor
        // state are not expressible (that opacity is the point of
        // ADR-0018). Callers needing a key prefix for scans use the
        // field-scan surface below.
        Vec::new()
    }

    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>> {
        Ok(self
            .document(id)?
            .and_then(|doc| doc.get(field).map(dyn_to_json)))
    }

    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()> {
        if matches!(field, "type_id" | "instance_key" | "instance_id") {
            anyhow::bail!("field name `{field}` is reserved (the document's declared identity fields)");
        }
        let key = self.key_of(id)?;
        let mut t = self.collection();
        // put_document REPLACES the dynamic segment with the given map's
        // undeclared fields (declared names RMW, dynamic names swap) —
        // one document per instance means a per-field write must RMW the
        // WHOLE field map or the second field write would drop the
        // first's. Read the document, merge the field, write it all back.
        let mut obj = t.get_document(&key).unwrap_or_default();
        obj.insert(field.to_string(), json_to_dyn(&value));
        t.put_document(&key, &obj);
        Ok(())
    }

    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()> {
        let key = self.key_of(id)?;
        let mut t = self.collection();
        // okm's delete is per-SEGMENT (the whole dynamic entry), not per
        // field — one document = one instance, so dropping one field is
        // a read-modify-write: the surviving map goes back through
        // put_document. Absent document = nothing to do.
        let Some(mut doc) = t.get_document(&key) else {
            return Ok(());
        };
        doc.remove(field);
        t.put_document(&key, &doc);
        Ok(())
    }

    fn scan_keys(&self, _key_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
        // Structural: the document model has no raw-key space to scan.
        // (The old per-field key bytes were the JSON container's
        // addressing — ADR-0018 removes that container.) Field-level
        // listing, if a consumer appears, rides get_document.
        Ok(Vec::new())
    }

    fn get_raw(&self, _key: &[u8]) -> anyhow::Result<Option<Value>> {
        anyhow::bail!("raw-key state ops are not expressible over the document model (ADR-0018)")
    }

    fn set_raw(&self, _key: Vec<u8>, _value: Value) -> anyhow::Result<()> {
        anyhow::bail!("raw-key state ops are not expressible over the document model (ADR-0018)")
    }

    fn del_raw(&self, _key: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("raw-key state ops are not expressible over the document model (ADR-0018)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_roundtrip() {
        let st = StateDocumentStore::new(crate::mq::MqStore::mem());
        let id = InstanceId { actor_type: "cart".into(), key: "alice".into() };
        st.set(&id, "events", serde_json::json!(1)).unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), Some(serde_json::json!(1)));
        st.set(&id, "events", serde_json::json!(2)).unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), Some(serde_json::json!(2)));
        st.delete(&id, "events").unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), None);
        // Different instances are independent.
        let bob = InstanceId { actor_type: "cart".into(), key: "bob".into() };
        st.set(&bob, "events", serde_json::json!(9)).unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), None);
        assert_eq!(st.get(&bob, "events").unwrap(), Some(serde_json::json!(9)));
        // Field names live in one dynamic segment: two fields coexist.
        st.set(&bob, "other", serde_json::json!("x")).unwrap();
        assert_eq!(st.get(&bob, "events").unwrap(), Some(serde_json::json!(9)));
        // Two DIFFERENT types with the same instance key are independent
        // (type_id leads the index; ids resolve per type).
        let other = InstanceId { actor_type: "stats".into(), key: "alice".into() };
        st.set(&other, "events", serde_json::json!(5)).unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), None);
        assert_eq!(st.get(&other, "events").unwrap(), Some(serde_json::json!(5)));
        // Repeated resolve of an existing identity is stable (no new id).
        st.set(&id, "events", serde_json::json!(7)).unwrap();
        assert_eq!(st.get(&id, "events").unwrap(), Some(serde_json::json!(7)));
        assert_eq!(st.get(&other, "events").unwrap(), Some(serde_json::json!(5)));
        // Concurrent first-touch of DIFFERENT identities: the
        // check-then-act resolve may hand two racers the same id —
        // documented hazard; single-threaded here it cannot fire, but the
        // serial path must stay correct.
        for i in 0..8 {
            let iid = InstanceId { actor_type: "bulk".into(), key: format!("k{i}") };
            st.set(&iid, "n", serde_json::json!(i)).unwrap();
        }
        for i in 0..8 {
            let iid = InstanceId { actor_type: "bulk".into(), key: format!("k{i}") };
            assert_eq!(st.get(&iid, "n").unwrap(), Some(serde_json::json!(i)), "k{i}");
        }
        // Reserved identity names are rejected at the seam.
        assert!(st.set(&bob, "instance_key", serde_json::json!("x")).is_err());
        assert!(st.set(&bob, "type_id", serde_json::json!(1)).is_err());
        assert!(st.set(&bob, "instance_id", serde_json::json!(1)).is_err());
    }
}
