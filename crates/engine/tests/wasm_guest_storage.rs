//! Wasm guest storage e2e (ADR-0026 §4 full-power path, PLAN 4.9 wasm
//! item): a REAL rustc-compiled wasm module registers as an booth type,
//! its `interface_schema` export carries the compiled `storage.collections`
//! block, and the handlers run the in-module Collection over the ctx
//! store bridge. The full upload → introspect → persist → emit-executor
//! path is exercised with the artifact built by the probe workspace
//! (`cargo build -p actor-guest --example counter_actor --target
//! wasm32-unknown-unknown`); the test fails loudly when the artifact is
//! missing (a stale build is a CI recipe error, not a skip condition).
//
//! Whole-crate gate: this e2e needs the wasmtime carrier — without the
//! feature it fails fast with "language not resident-carried by this probe
//! build" (and imports/helper would dangle unused). Gate the crate, not
//! the fn.
#![cfg(feature = "wasmtime")]

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;
use aura_realm::Realm;
use base64::Engine as _;

fn wasm_booth() -> BoothType {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../probe/target/wasm32-unknown-unknown/debug/examples/counter_actor.wasm"
    );
    let bytes = std::fs::read(path).expect(
        "counter_actor.wasm missing — build it in ~/world/probe: cargo build -p actor-guest --example counter_actor --target wasm32-unknown-unknown",
    );
    let source = base64::engine::general_purpose::STANDARD.encode(&bytes);
    BoothType::script("wasm-guest-counter", "wasmtime", source)
}

/// Full path: register → introspection persists the module's compiled
/// schema onto BoothDef → instance handler emits Collection ops through
/// the ctx store bridge → the realm executor resolves them against the
/// type's ns → the value reads back through the module's own reader.
#[tokio::test]
async fn wasm_booth_storage_end_to_end() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");

    engine.register(wasm_booth()).await.unwrap();

    // The persisted definition carries the module's compiled schema.
    // Verified indirectly: the type resolved a store plan (a handler
    // that emits succeeds) and `ctx.interface_schema` returns the block.
    let target = InstanceId { booth_type: "wasm-guest-counter".into(), key: "u1".into() };

    // bump via invoke: the handler does an in-module Collection RMW —
    // put_document/get_document cross the ctx store bridge.
    let v = engine
        .invoke(target.clone(), "bump", serde_json::json!(null))
        .await
        .expect("bump 1");
    assert_eq!(v, serde_json::json!(1), "first RMW round trip");
    let v = engine
        .invoke(target.clone(), "bump", serde_json::json!(null))
        .await
        .expect("bump 2");
    assert_eq!(v, serde_json::json!(2), "second RMW: read-modify-write across the bridge");

    // Reader handler: the persisted state reads back (documents live in
    // the type's ns — the same data the RMW wrote).
    let v = engine
        .invoke(target, "peek", serde_json::json!(null))
        .await
        .expect("peek");
    assert_eq!(v, serde_json::json!(2));

    // Evict + re-activate: the document survives (scale-to-zero keeps
    // the collections; re-activation reads on demand).
    Realm::evict_instance(
        engine.realm.clone(),
        &InstanceId { booth_type: "wasm-guest-counter".into(), key: "u1".into() },
    )
    .await;
    let v = engine
        .invoke(
            InstanceId { booth_type: "wasm-guest-counter".into(), key: "u1".into() },
            "peek",
            serde_json::json!(null),
        )
        .await
        .expect("peek after evict");
    assert_eq!(v, serde_json::json!(2), "guest-written document survives eviction");
}

// ---- ADR-0034 consumer side: wasm pulls a python generator's stream ----

/// The consumer fixture module (probe `cargo build -p actor-guest
/// --example stream_puller --target wasm32-unknown-unknown`): its exports
/// drive the ctx_iter_start/next/dispose host imports to completion.
fn puller_booth() -> BoothType {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../probe/target/wasm32-unknown-unknown/debug/examples/stream_puller.wasm"
    );
    let bytes = std::fs::read(path).expect(
        "stream_puller.wasm missing — build it in ~/world/probe: cargo build -p actor-guest --example stream_puller --target wasm32-unknown-unknown",
    );
    let source = base64::engine::general_purpose::STANDARD.encode(&bytes);
    BoothType::script("wasm-puller", "wasmtime", source)
}

#[cfg(feature = "python")]
fn producer_booth() -> BoothType {
    BoothType::script(
        "py-prod",
        "python",
        r#"
def tokens(args):
    for t in args["list"]:
        yield t
"#,
    )
}

/// Full drain: the wasm loop terminates on the typed `done` field —
/// structural, not a sentinel. Three yields, three items across.
#[cfg(feature = "python")]
#[tokio::test]
async fn wasm_consumer_pulls_python_generator_to_done() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(puller_booth()).await.unwrap();
    #[cfg(feature = "python")]
    engine.register(producer_booth()).await.unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "wasm-puller".into(), key: "w1".into() },
            "pull_all",
            serde_json::json!({
                "type": "py-prod", "key": "p1", "handler": "tokens",
                "args": { "list": ["a", "b", "c"] },
            }),
        )
        .await
        .expect("wasm pull_all");
    assert_eq!(out, serde_json::json!({ "got": ["a", "b", "c"] }));

    // The producer exhausted itself: the realm's registry is empty.
    assert_eq!(engine.realm.lock().await.streams.len(), 0, "done releases the entry");
}

/// Mid-stream break: the wasm consumer calls dispose explicitly (no
/// destructor hook exists across the import seam) — the registry drains
/// and the producer's generator sees GeneratorExit on close().
#[cfg(feature = "python")]
#[tokio::test]
async fn wasm_consumer_break_disposes() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(puller_booth()).await.unwrap();
    #[cfg(feature = "python")]
    engine.register(producer_booth()).await.unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "wasm-puller".into(), key: "w1".into() },
            "pull_break",
            serde_json::json!({
                "type": "py-prod", "key": "p1", "handler": "tokens",
                "args": { "list": ["a", "b", "c"] },
            }),
        )
        .await
        .expect("wasm pull_break");
    assert_eq!(out, serde_json::json!({ "got": ["a", "b"] }), "abandoned after two of three");

    assert_eq!(
        engine.realm.lock().await.streams.len(),
        0,
        "explicit dispose must drain the registry (no leak through the import seam)"
    );
}
