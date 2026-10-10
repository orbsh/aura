//! ADR-0037 4.16a python injection face e2e: the script's declared
//! `@DocumentEncode` classes come back as bound `Collection` objects
//! (the host-injected face — built over the realm's live engine at
//! session load), and writes through the BINDING face must be readable
//! through `ctx.store` and vice versa — same bytes, one engine
//! (the acceptance lock: two paths, one keyspace). Cross-type ns is
//! structurally unexpressible here: the slot only builds the plan's
//! declared collections, ns bound at construction (DSL rule).
#![cfg(feature = "python")]

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;

const SCRIPT: &str = r##"
import json

@KeyEncode
class CounterKey:
    id: u64

@DocumentEncode
@ok_ref(CounterKey)
@ok_index("by_count", fields=("count",))
class Counters:
    count: u64

# The key is the raw primary-key bytes the Collection face takes:
# U64 id, big-endian (schema key_len = 8).
def _k(i):
    return i.to_bytes(8, "big")

@on("bind_write_emit_read", key="user_id")
def bind_write_emit_read(args):
    Counters.put(_k(1), {"count": 41})
    cur = ctx.store(json.dumps({"collection": "Counters", "op": "get_document",
                                     "key": {"id": 1}}))
    return {"count": cur["count"]}

@on("emit_write_bind_read", key="user_id")
def emit_write_bind_read(args):
    ctx.store(json.dumps({"collection": "Counters", "op": "put_document",
                               "key": {"id": 2}, "doc": {"count": 42}}))
    cur = Counters.get(_k(2))
    return {"count": cur["count"]}

@on("bind_scan", key="user_id")
def bind_scan(args):
    Counters.put(_k(3), {"count": 43})
    # Declared-index scan through the binding face: slot 4097 is the
    # DSL-assigned first index slot (INDEX_BASE = 4096 | 1); count=43
    # BE is the prefix.
    hits = Counters.scan(4097, (43).to_bytes(8, "big"))
    return {"hits": len(hits)}
"##;

#[tokio::test]
async fn binding_face_and_ctx_store_share_one_keyspace() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("py-inject", "python", SCRIPT))
        .await
        .unwrap();

    let target = |key: &str| InstanceId { booth_type: "py-inject".into(), key: aura_booth::InstanceKey::Named(key.into()) };

    // Binding write → ctx.store read (same session instance).
    let out = engine
        .invoke(target("k"), "bind_write_emit_read", serde_json::json!({"user_id": "a"}))
        .await
        .expect("bind_write_emit_read");
    assert_eq!(out, serde_json::json!({ "count": 41 }));

    // ctx.store write → binding read.
    let out = engine
        .invoke(target("k"), "emit_write_bind_read", serde_json::json!({"user_id": "a"}))
        .await
        .expect("emit_write_bind_read");
    assert_eq!(out, serde_json::json!({ "count": 42 }));

    // Evict + re-activate: the bound Collection is rebuilt over the same
    // realm engine — the durable rows survive the ephemeral bindings.
    aura_realm::Realm::evict_instance(engine.realm.clone(), &target("k")).await;
    let out = engine
        .invoke(target("k"), "bind_scan", serde_json::json!({"user_id": "a"}))
        .await
        .expect("bind_scan after eviction — rows survived");
    assert_eq!(out, serde_json::json!({ "hits": 1 }));
}

#[tokio::test]
async fn interface_schema_still_assembles_after_injection() {
    // The ordering trap lock: injection registers pyclass instances under
    // the collection names in the SAME module namespace; introspection
    // must still report the storage block (load-time capture, not a late
    // re-assembly that would see instances instead of classes).
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("py-inject2", "python", SCRIPT))
        .await
        .unwrap();
    let schema = engine
        .realm
        .lock()
        .await
        .schema_of("py-inject2")
        .cloned()
        .flatten()
        .expect("persisted schema");
    assert_eq!(schema["storage"]["collections"]["Counters"]["schema"]["key_len"], 8);
    assert!(schema["receives"]["bind_scan"].is_object());
}
