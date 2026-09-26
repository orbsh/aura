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
