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
//! The introspected schema rides the DYNAMIC segment as structured nTLV
//! (one `schema` entry, nested objects resolved through the field-name
//! dictionary): the interface-artifact attribute (the LLM/script-side
//! contract consumed by introspection surfaces) is unchanged, but the
//! storage shape is a first-class dynamic document — no JSON-text detour,
//! no opaque blob. The `PersistedActor` struct (aura-actor) is the seam
//! type; JSON exists only at that seam (script/LLM currency).

use crate::mq::MqStore;
use aura_actor::PersistedActor;
use okm_core::document::Collection;
use okm_core::{Bytes, Document, DocumentEncode, KeyEncode, ReduceLogic, ReduceCodec};

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
#[ok_reduce(HighWater(id) { group(global) })]
#[ok_ns(40)]
pub struct TypeName {
    pub name: String,
    /// Payload mirror of the proxy id — the MAX reduce folds over payload
    /// fields (okm ADR-0024 gives hooks the key, but the mirror keeps the
    /// fold logic payload-shaped; retire both when 0024's key-field
    /// groups land in the derive).
    pub id: u32,
    /// The type's own storage ns (ADR-0026): allocated at first
    /// registration from the actor base block, never reused.
    pub ns: u32,
    /// Single-group discriminator (always 0): the derive rejects empty
    /// group lists, so the registry-wide watermark declares a constant
    /// group instead.
    pub global: u32,
}

use __OkmIndex_TypeName_by_name as TypeNameByName;

/// The first ns a registered actor type receives (ADR-0026 §1): the low
/// block is aura's own (mq 30–35, meta/state 40–41); actor types allocate
/// from a fixed base above it, and ids are never reused within the node.
pub const ACTOR_NS_BASE: u32 = 100;

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
    // reused — unfold is a no-op for this watermark). The type's storage
    // ns rides the same registration: base + id (one allocation per
    // type, monotonic with the id, never reclaimed).
    let watermark = okm_core::reduce_get::<MqStore, __OkmReduce_TypeName_0>(
        t.store(),
        <TypeName as Document>::NS_PREFIX,
        &TypeIdKey { id: 0 },
        &TypeName { name: String::new(), id: 0, ns: 0, global: 0 },
    )
    .unwrap_or(0);
    let id = (watermark as u32) + 1;
    t.put(
        &TypeIdKey { id },
        &TypeName {
            name: name.to_string(),
            id,
            ns: ACTOR_NS_BASE + id,
            global: 0,
        },
    );
    Ok(id)
}

// ---------------------------------------------------------------------------
// The definition document: one per actor type.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct ActorDefKey {
    pub type_id: u32,
}

/// One document per actor type. `Option` maps to a sentinel encoding:
/// `idle_ttl_secs` 0 = realm default (a zero TTL is meaningless — it
/// would evict on arrival).
///
/// The introspected schema is NOT a declared field: it rides the DYNAMIC
/// segment (one `schema` entry, `DynamicValue::Obj`) — structured nTLV
/// encoding, no JSON-text detour, readable field-wise. The interface-
/// artifact attribute (the LLM/script-side contract) is unchanged; only
/// the storage shape stopped being an opaque text blob.
#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(ActorDefKey)]
#[ok_ns(41)]
pub struct ActorDef {
    /// The raw type name (observability; the id is the addressing).
    pub name: String,
    pub language: String,
    /// Content address of the code (ADR-0027): the definition points at
    /// its bytes, it no longer carries them. The hash IS the version
    /// identity — re-registering unchanged code dedups to the same blob;
    /// changed code is a new hash the new definition version points at.
    pub code_sha256: [u8; 32],
    pub idle_ttl_secs: u64,
}

// ---------------------------------------------------------------------------
// CodeBlob (ADR-0027): content-addressed code bytes, meta plane ns 42.
// Pure content rows: key = the sha256, value = the bytes. No name, no
// version, no foreign key — every relational fact lives in ActorDef, the
// single source of reference. Immutable by construction: a "different
// content at the same key" is a hash collision, not a state.
// ---------------------------------------------------------------------------

#[derive(KeyEncode, Clone, PartialEq, Debug, Default)]
pub struct CodeBlobKey {
    pub sha256: [u8; 32],
}

#[derive(DocumentEncode, Clone, PartialEq, Debug)]
#[ok_ref(CodeBlobKey)]
#[ok_ns(42)]
pub struct CodeBlob {
    /// Mirrors the key segment (index fields must be payload fields; and
    /// observability: a raw scan sees its own content address).
    pub sha256: [u8; 32],
    pub data: Bytes,
}

/// sha256 of bytes as fixed-size array (the key form).
pub fn code_hash(source: &str) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(source.as_bytes());
    hasher.finalize().into()
}

/// Lowercase hex of a code hash (the wire/URL form).
pub fn code_hex(sha: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in sha {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Store the blob if absent (content addressing makes the pre-check a
/// dedup, never a correctness requirement — same bytes, same row).
pub fn put_blob(meta: &MqStore, sha: [u8; 32], bytes: &[u8]) -> anyhow::Result<()> {
    let mut t = Collection::<MqStore, CodeBlobKey, CodeBlob>::new(meta.clone());
    if t.get(&CodeBlobKey { sha256: sha }).is_some() {
        return Ok(());
    }
    t.put(&CodeBlobKey { sha256: sha }, &CodeBlob { sha256: sha, data: Bytes(bytes.to_vec()) });
    Ok(())
}

/// Read the blob bytes (None = never stored under this hash).
pub fn get_blob(meta: &MqStore, sha: &[u8; 32]) -> Option<Vec<u8>> {
    let mut t = Collection::<MqStore, CodeBlobKey, CodeBlob>::new(meta.clone());
    t.get(&CodeBlobKey { sha256: *sha }).map(|row| row.data.0)
}

/// The dynamic-segment key the schema rides under ("schema" as a dynamic
/// name — the dictionary assigns it an id disjoint from the declared
/// fields; a ctx/registry field of the same name cannot collide because
/// this table's declared set is fixed above).
const SCHEMA_FIELD: &str = "schema";

impl ActorDef {
    fn of(def: &PersistedActor) -> (Self, Option<okm_core::obj_dynamic::DynamicValue>) {
        (
            Self {
                name: def.name.clone(),
                language: def.language.clone(),
                code_sha256: code_hash(&def.source),
                idle_ttl_secs: def.idle_ttl_secs.unwrap_or(0),
            },
            def.schema.as_ref().map(crate::value::json_to_dyn),
        )
    }

    /// The row + its dynamic schema segment. `source` is NOT here — the
    /// bytes are content-addressed (ADR-0027); the loader hydrates them
    /// from the blob store by `code_sha256`.
    fn into_persisted(
        self,
        schema: Option<okm_core::obj_dynamic::DynamicValue>,
    ) -> (PersistedActor, [u8; 32]) {
        (
            PersistedActor {
                name: self.name,
                language: self.language,
                source: String::new(), // filled by the blob hydrate below
                idle_ttl_secs: (self.idle_ttl_secs > 0).then_some(self.idle_ttl_secs),
                schema: schema.map(|v| crate::value::dyn_to_json(&v)),
            },
            self.code_sha256,
        )
    }
}

// ---------------------------------------------------------------------------
// The public surface: persist / load_all (JSON only at the struct seam).
// ---------------------------------------------------------------------------

/// The type's storage ns (ADR-0026): registry resolve (no allocation —
/// unregistered types have no ns; the caller registers first).
pub fn ns_of(meta: &MqStore, name: &str) -> anyhow::Result<u32> {
    let mut t = Collection::<MqStore, TypeIdKey, TypeName>::new(meta.clone());
    for hit in t.scan::<TypeNameByName>(name.as_bytes()) {
        if let Some(row) = &hit.1 {
            if row.name == name {
                return Ok(row.ns);
            }
        }
    }
    anyhow::bail!("no storage ns for unregistered actor type `{name}`")
}

/// Persist one definition (latest version wins per type name; the id is
/// stable across versions — the registry resolve).
pub fn persist(meta: &MqStore, actor: &PersistedActor) -> anyhow::Result<()> {
    let type_id = resolve_type_id(meta, &actor.name)?;
    let mut t = Collection::<MqStore, ActorDefKey, ActorDef>::new(meta.clone());
    let (row, schema) = ActorDef::of(actor);
    // The bytes must exist before the pointer to them is published: a
    // definition whose blob is missing is an unloadable actor (boot
    // reload errors instead of resurrecting a hash with no content).
    put_blob(meta, row.code_sha256, actor.source.as_bytes())?;
    t.put(&ActorDefKey { type_id }, &row);
    // Schema rides the dynamic segment (structured nTLV, no JSON text);
    // absent schema = the field is absent (sentinel by absence).
    let dynamic = schema
        .map(|v| {
            let mut m = std::collections::BTreeMap::new();
            m.insert(SCHEMA_FIELD.to_string(), v);
            m
        })
        .unwrap_or_default();
    t.put_fields(&ActorDefKey { type_id }, &dynamic);
    Ok(())
}

/// The type's ns + persisted storage schema in one resolve: the actor
/// ctx bridge's store-executor assembly input (ns from the type registry,
/// schema from the ActorDef's dynamic segment — the uploaded copy).
/// `schema: None` = the type declared no storage (no ctx.store surface).
pub fn ns_and_schema_of(meta: &MqStore, name: &str) -> anyhow::Result<(u32, Option<serde_json::Value>)> {
    let ns = ns_of(meta, name)?;
    let type_id = resolve_type_id(meta, name)?;
    let mut t = Collection::<MqStore, ActorDefKey, ActorDef>::new(meta.clone());
    let schema = t
        .get_fields(&ActorDefKey { type_id })
        .and_then(|f| f.get(SCHEMA_FIELD).cloned())
        .map(|v| crate::value::dyn_to_json(&v));
    Ok((ns, schema))
}

/// Every persisted definition (boot reload). The raw document scan is
/// the export surface (slot-0 primary entries only); each payload
/// materializes through the row's own typed decoder — the same codec
/// put wrote, no second one.
pub fn load_all(meta: &MqStore) -> anyhow::Result<Vec<PersistedActor>> {
    let mut t = Collection::<MqStore, ActorDefKey, ActorDef>::new(meta.clone());
    let mut out = Vec::new();
    for (suffix, payload) in t.scan_documents_raw() {
        // suffix = [type_id 4B]: the primary key of the row (u32 BE).
        if suffix.len() < 4 {
            continue;
        }
        let mut b = [0u8; 4];
        b.copy_from_slice(&suffix[suffix.len() - 4..]);
        let type_id = u32::from_be_bytes(b);
        let key = ActorDefKey { type_id };
        let schema = t
            .get_fields(&key)
            .and_then(|f| f.get(SCHEMA_FIELD).cloned());
        let row = ActorDef::decode_payload(&payload);
        let (mut def, sha) = row.into_persisted(schema);
        // Hydrate the source through the row's own content address —
        // definitions outlive the process, and so does their blob (same
        // engine). A missing blob is corruption, not an empty program.
        match get_blob(meta, &sha) {
            Some(bytes) => {
                def.source = String::from_utf8(bytes)
                    .map_err(|_| anyhow::anyhow!(
                        "actor `{}`: code blob is not valid UTF-8", def.name
                    ))?;
            }
            None => anyhow::bail!(
                "actor `{}`: no code blob under sha256 {} (definition without content)",
                def.name, code_hex(&sha)
            ),
        }
        out.push(def);
    }
    Ok(out)
}

#[cfg(test)]
mod ns_schema_tests {
    use super::*;

    #[test]
    fn ns_and_schema_resolves_after_persist() {
        let meta = MqStore::mem();
        let def = PersistedActor {
            name: "sc".into(),
            language: "steel".into(),
            source: "x".into(),
            idle_ttl_secs: None,
            schema: Some(serde_json::json!({"storage": {"collections": {"notes": {"schema": {"key_len": 8}}}}})),
        };
        persist(&meta, &def).unwrap();
        let (ns, schema) = ns_and_schema_of(&meta, "sc").unwrap();
        assert!(ns > 0);
        assert!(schema.is_some(), "schema must come back from the dynamic segment");
        assert_eq!(schema.unwrap()["storage"]["collections"]["notes"]["schema"]["key_len"], 8);
    }

    #[test]
    fn big_schema_roundtrip() {
        let meta = MqStore::mem();
        // Shape matrix over the dynamic-segment codec: the nested-composite
        // shapes (Obj inside Array etc.) are exactly what broke the persisted
        // interface_schema before okm 0c2a354 threaded the name resolver.
        for probe in [
            serde_json::json!({"a": [{"c": 0}]}),
            serde_json::json!({"a": [[1]]}),
            serde_json::json!({"a": {"b": [1]}}),
            serde_json::json!({"a": {"b": [{"c": 0}]}}),
            serde_json::json!({"a": {"b": {"c": [{"d": "x"}]}}}),
            serde_json::json!({"a": 4096}),
            serde_json::json!({"a": [1, 2, 3]}),
            serde_json::json!({"a": {"b": 0}}),
        ] {
            let def = PersistedActor { name: "p".into(), language: "steel".into(), source: "x".into(), idle_ttl_secs: None, schema: Some(probe.clone()) };
            let m2 = MqStore::mem();
            persist(&m2, &def).unwrap();
            let all = load_all(&m2).unwrap();
            assert!(all[0].schema.is_some(), "roundtrip failed for {probe}");
        }
        let full = serde_json::json!({
            "storage": {"collections": {"notes": {"schema": {
                "key_len": 8,
                "key_fields": [{"name":"id","ty":"U64","width":8,"offset":0,"tag":0}],
                "layout_version": 1,
                "hot_width": 8,
                "payload_header_len": 3,
                "hot_fields": [{"name":"count","ty":"U64","width":8,"offset":0,"tag":0}],
                "cold_fields": [],
                "slots": {"primary":0,"dynamic":1,"dict_id":2,"dict_name":3,"declared_index_base":4096,"declared_reduce_base":8192,"junction_base":12288}
            }}}}
        });
        let def = PersistedActor {
            name: "big".into(),
            language: "steel".into(),
            source: "(define (execute a) a)".into(),
            idle_ttl_secs: None,
            schema: Some(full),
        };
        persist(&meta, &def).unwrap();
        let all = load_all(&meta).unwrap();
        assert_eq!(all.len(), 1);
        assert!(all[0].schema.is_some(), "big schema must round-trip");
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str) -> PersistedActor {
        PersistedActor {
            name: name.into(),
            language: "steel".into(),
            source: "(define (execute args) args)".into(),
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
        v3.idle_ttl_secs = None;
        v3.schema = None;
        persist(&meta, &v3).unwrap();
        let all = load_all(&meta).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].idle_ttl_secs, None);
        assert_eq!(all[0].schema, None);
        // A second type gets its own document and its own id.
        persist(&meta, &sample("stats")).unwrap();
        assert_eq!(load_all(&meta).unwrap().len(), 2);
    }

    #[test]
    fn storage_ns_allocation_is_stable_base_offset_and_distinct() {
        let meta = MqStore::mem();
        persist(&meta, &sample("cart")).unwrap();
        persist(&meta, &sample("stats")).unwrap();
        let ns_cart = ns_of(&meta, "cart").unwrap();
        let ns_stats = ns_of(&meta, "stats").unwrap();
        // Each type owns one real ns from the actor base block.
        assert!(ns_cart >= ACTOR_NS_BASE && ns_stats >= ACTOR_NS_BASE);
        assert_ne!(ns_cart, ns_stats, "two types never share a ns");
        // Re-resolve (re-registration path) is stable — no reallocation.
        assert_eq!(ns_cart, ns_of(&meta, "cart").unwrap());
        // Unregistered types have none.
        assert!(ns_of(&meta, "ghost").is_err());
    }
}
