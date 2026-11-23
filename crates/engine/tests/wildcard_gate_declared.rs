//! A WILDCARD that declares a gate (aura PLAN's last schema-surface item).
//!
//! `wildcard_receives` used to be read as a plain string array, so a script's
//! `(on "ui:*" (hash "auth" #t) h)` arrived at the host as a bare pattern: the
//! declaration said `auth`, the schema said nothing, and a gated wildcard was
//! unreachable from any script. The entry carries its knobs now
//! (`{"pattern": "ui:*", "auth": true}`), and the bare string still reads — a
//! hand-written declaration spells the pattern and nothing else.
//!
//! What is pinned here is the ROUTE TABLE, not the schema: the gate has to land
//! where the venue's check reads it (`Route.auth`), because a gate that stops at
//! the declaration is a gate nobody consults.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::BoothType;
use aura_engine::Engine;

const NOBODY: &str = r#"(define (execute args) args)"#;

/// Register a type whose schema is HANDED IN (ADR-0051 §1): the declaration is
/// data, and this test is about reading it.
async fn register_with(engine: &Engine, name: &str, schema: serde_json::Value) {
    engine
        .register_into_with(
            &engine.realm,
            BoothType::script(name, "steel", NOBODY),
            Some(schema),
        )
        .await
        .unwrap();
}

async fn gate_of(engine: &Engine, event: &str) -> Vec<bool> {
    let realm = engine.realm.lock().await;
    realm
        .router
        .matches(event)
        .into_iter()
        .map(|route| route.auth)
        .collect()
}

#[tokio::test]
async fn a_wildcard_entry_carries_its_gate() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    register_with(
        &engine,
        "gated",
        serde_json::json!({
            "wildcard_receives": [{ "pattern": "ui:*", "auth": true }]
        }),
    )
    .await;

    assert_eq!(
        gate_of(&engine, "ui:draw").await,
        vec![true],
        "the declared gate reached the route the venue checks"
    );
}

#[tokio::test]
async fn a_bare_pattern_is_still_a_public_wildcard() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    register_with(
        &engine,
        "open",
        serde_json::json!({ "wildcard_receives": ["ui:*"] }),
    )
    .await;

    assert_eq!(
        gate_of(&engine, "ui:draw").await,
        vec![false],
        "a pattern with no knobs is public, as it always was"
    );
}

#[tokio::test]
async fn an_object_without_a_pattern_is_refused_by_index() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let err = engine
        .register_into_with(
            &engine.realm,
            BoothType::script("nameless", "steel", NOBODY),
            Some(serde_json::json!({ "wildcard_receives": [{ "auth": true }] })),
        )
        .await
        .expect_err("an entry that names no pattern is not a wildcard");
    let text = format!("{err:#}");
    assert!(
        text.contains("wildcard_receives[0]") && text.contains("pattern"),
        "the error names the entry and the missing field: {text}"
    );
}