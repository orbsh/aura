//! The meta plane as okm documents (ADR-0018 follow-through: NO
//! exceptions — actor definitions are documents over a SEPARATE okm
//! instance, exactly like the data plane; serialized JSON is never a
//! storage representation).
//!
//! Identity model (option A, the registry pattern): the type name is
//! open-ended runtime data, so it resolves through a `TypeName` registry
//! INSIDE the meta instance (a separate okm engine with its own
//! directory — ids are assigned from the instance's own watermark;
//! cross-instance lookups do not exist). The definition's proxy key is
//! that id; the raw name rides a declared field for observability.
//!
//! The introspected schema rides as its verbatim JSON text: it IS an
//! interface artifact (the LLM/script-side contract consumed by
//! introspection surfaces), carried through the seam like any other
//! interface currency — never a storage encoding of structured data.
//! The `PersistedActor` struct (aura-actor) is the seam type.

use crate::mq::MqStore;
use aura_actor::PersistedActor;
use okm_core::document::Collection;
use okm_core::{Document, DocumentEncode, KeyEncode, ReduceLogic, ReduceCodec};

// ---------------------------------------------------------------------------
// The registry: open-ended type name → id.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct TypeIdKey {
    pub id: u32,
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(TypeIdKey)]
#[ok_index(by_name { fields(name) })]
#[ok_reduce(MaxTypeId { group(global) })]
#[ok_ns(40)]
pub struct TypeName {
    pub name: String,
    /// Payload mirror of the proxy id — the MAX reduce folds over payload
    /// fields (okm ADR-0024 gives hooks the key, but the mirror keeps the
    /// fold logic payload-shaped; retire both when 0024's key-field
    /// groups land in the derive).
    pub id: u32,
    /// Single-group discriminator (always 0): the derive rejects empty
    /// group lists, so the registry-wide watermark declares a constant
    /// group instead.
    pub global: u32,
}

use __OkmIndex_TypeName_by_name as TypeNameByName;

fn resolve_type_id(meta: &MqStore, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, TypeIdKey, TypeName>::new(meta.clone());
    for hit in t.scan::<TypeNameByName>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(hit.0.decoded.id);
            }
        }
    }
    // Miss: next id = MAX reduce watermark + 1 (no scan; ids never
    // reused — unfold is a no-op for this watermark).
    let watermark = okm_core::reduce_get::<MqStore, MaxTypeId>(
        t.store(),
        <TypeName as Document>::NS_PREFIX,
        &TypeIdKey { id: 0 },
        &TypeName { name: String::new(), id: 0, global: 0 },
    )
    .unwrap_or(0);
    let id = (watermark as u32) + 1;
    t.put(
        &TypeIdKey { id },
        &TypeName { name: name.to_string(), id, global: 0 },
    );
    Ok(id)
}

/// MAX reduce over the whole registry (a single group — the group
/// segment is empty): the highest assigned type id. Unfold = keep (ids
/// retired, never reclaimed).
pub struct MaxTypeId;

impl ReduceLogic for MaxTypeId {
    type Document = TypeName;
    type Acc = u64;
    fn fold(acc: &mut u64, item: &TypeName) {
        *acc = (*acc).max(item.id as u64);
    }
    fn unfold(_acc: &mut u64, _item: &TypeName) {}
}

// ---------------------------------------------------------------------------
// The definition document: one per actor type.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct ActorDefKey {
    pub type_id: u32,
}

/// One document per actor type. `Option` fields map to sentinel
/// encodings: `entry` empty string = none; `idle_ttl_secs` 0 = realm
/// default (a zero TTL is meaningless — it would evict on arrival). The
/// introspected schema rides verbatim as a JSON string (interface
/// artifact; empty = none).
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(ActorDefKey)]
#[ok_ns(41)]
pub struct ActorDef {
    /// The raw type name (observability; the id is the addressing).
    pub name: String,
    pub language: String,
    pub source: String,
    pub entry: String,
    pub idle_ttl_secs: u64,
    pub schema: String,
}

impl ActorDef {
    fn of(def: &PersistedActor) -> Self {
        Self {
            name: def.name.clone(),
            language: def.language.clone(),
            source: def.source.clone(),
            entry: def.entry.clone().unwrap_or_default(),
            idle_ttl_secs: def.idle_ttl_secs.unwrap_or(0),
            schema: def
                .schema
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default(),
        }
    }

    fn into_persisted(self) -> PersistedActor {
        PersistedActor {
            name: self.name,
            language: self.language,
            source: self.source,
            entry: (!self.entry.is_empty()).then_some(self.entry),
            idle_ttl_secs: (self.idle_ttl_secs > 0).then_some(self.idle_ttl_secs),
            schema: (!self.schema.is_empty())
                .then(|| serde_json::from_str(&self.schema).ok())
                .flatten(),
        }
    }
}

// ---------------------------------------------------------------------------
// The public surface: persist / load_all (JSON only at the struct seam).
// ---------------------------------------------------------------------------

/// Persist one definition (latest version wins per type name; the id is
/// stable across versions — the registry resolve).
pub fn persist(meta: &MqStore, actor: &PersistedActor) -> anyhow::Result<()> {
    let type_id = resolve_type_id(meta, &actor.name)?;
    let mut t = Collection::<MqStore, ActorDefKey, ActorDef>::new(meta.clone());
    t.put(&ActorDefKey { type_id }, &ActorDef::of(actor));
    Ok(())
}

/// Every persisted definition (boot reload). The raw document scan is
/// the export surface (slot-0 primary entries only); each payload
/// materializes through the row's own typed decoder — the same codec
/// put wrote, no second one.
pub fn load_all(meta: &MqStore) -> anyhow::Result<Vec<PersistedActor>> {
    let t = Collection::<MqStore, ActorDefKey, ActorDef>::new(meta.clone());
    let mut out = Vec::new();
    for (_, payload) in t.scan_documents_raw() {
        out.push(ActorDef::decode_payload(&payload).into_persisted());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str) -> PersistedActor {
        PersistedActor {
            name: name.into(),
            language: "steel".into(),
            source: "(define (execute args) args)".into(),
            entry: Some("execute".into()),
            idle_ttl_secs: Some(300),
            schema: Some(serde_json::json!({"lifecycle": {"idle_ttl": "300s"}})),
        }
    }

    #[test]
    fn definition_roundtrip_and_registry_stability() {
        let meta = MqStore::mem();
        persist(&meta, &sample("cart")).unwrap();
        // Same name again: the id is STABLE (registry resolve, not a new
        // row) and the record updates in place (latest version wins).
        let mut v2 = sample("cart");
        v2.idle_ttl_secs = Some(600);
        persist(&meta, &v2).unwrap();
        let all = load_all(&meta).unwrap();
        assert_eq!(all.len(), 1, "one type = one document: {:?}", all);
        assert_eq!(all[0].name, "cart");
        assert_eq!(all[0].idle_ttl_secs, Some(600));
        assert_eq!(
            all[0].schema.as_ref().unwrap()["lifecycle"]["idle_ttl"],
            "300s"
        );
        // Option sentinels survive the round trip.
        let mut v3 = sample("cart");
        v3.entry = None;
        v3.idle_ttl_secs = None;
        v3.schema = None;
        persist(&meta, &v3).unwrap();
        let all = load_all(&meta).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].entry, None);
        assert_eq!(all[0].idle_ttl_secs, None);
        assert_eq!(all[0].schema, None);
        // A second type gets its own document and its own id.
        persist(&meta, &sample("stats")).unwrap();
        assert_eq!(load_all(&meta).unwrap().len(), 2);
    }
}
