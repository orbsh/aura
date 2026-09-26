//! ADR-0026 §4 per-language schema declaration, python path: the script
//! declares its collections with `@DocumentEncode` classes (the python
//! mirror of the Rust derive); introspection assembles the serde form of
//! okm's CollectionSchema; `engine.register` persists it with the
//! definition; the ctx.store plan resolves from it — `ctx_store_emit`
//! round-trips through the type's own ns. Requires the `python` feature.

#[cfg(feature = "python")]
mod py_schema_tests {
    use aura_booth::{BoothType, InstanceId};
    use aura_engine::Engine;

    // Storage handlers receive host fns taking ONE JSON string; the
    // collection is the decorator-declared class. Assertion is the booth's
    // observable output (invoke read-back), never a store poke.
    const RMW: &str = r##"
import json

@KeyEncode
class CounterKey:
    id: u64

@DocumentEncode
@ok_ref(CounterKey)
class Counters:
    count: u64

@on("bump", key="user_id")
def bump(args):
    cur = ctx_store_emit(json.dumps({"collection": "Counters", "op": "get_document", "key": {"id": 1}}))
    c = 0 if (cur is None or "count" not in cur) else cur["count"]
    ctx_store_emit(json.dumps({"collection": "Counters", "op": "put_document",
                               "key": {"id": 1}, "doc": {"count": c + 1}}))
    return {"count": c + 1}

@on("read", key="user_id")
def read(args):
    cur = ctx_store_emit(json.dumps({"collection": "Counters", "op": "get_document", "key": {"id": 1}}))
    return {"count": 0 if (cur is None or "count" not in cur) else cur["count"]}
"##;

    #[tokio::test]
    async fn decorator_schema_resolves_the_store_plan() {
        let engine = Engine::start(&Default::default()).await.expect("engine boot");
        engine.register(BoothType::script("py-counter", "python", RMW)).await.unwrap();

        // The plan resolved from the decorator-assembled schema: put/get
        // round trip through the type's declared collection.
        let out = engine
            .invoke(
                InstanceId { booth_type: "py-counter".into(), key: "k".into() },
                "bump",
                serde_json::json!({"user_id": "alice"}),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"count": 1}));

        // Read back through the reader handler — the booth's observable
        // output, not a store poke.
        let read = engine
            .invoke(
                InstanceId { booth_type: "py-counter".into(), key: "k".into() },
                "read",
                serde_json::json!({"user_id": "alice"}),
            )
            .await
            .unwrap();
        assert_eq!(read, serde_json::json!({"count": 1}));
    }

    #[tokio::test]
    async fn decorator_schema_is_persisted_with_the_definition() {
        // The assembled schema rides BoothDef (ADR-0025 Plan A) — the
        // persisted copy is what ctx.interface_schema reflects and what
        // StorePlan::from_schema consumes at plan resolution.
        let engine = Engine::start(&Default::default()).await.expect("engine boot");
        engine.register(BoothType::script("py-counter2", "python", RMW)).await.unwrap();
        let schema = engine
            .realm
            .lock()
            .await
            .schema_of("py-counter2")
            .cloned()
            .flatten()
            .expect("persisted schema");
        assert_eq!(
            schema["storage"]["collections"]["Counters"]["schema"]["key_len"],
            8
        );
    }
}
