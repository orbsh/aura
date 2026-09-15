//! Phase 3 acceptance, as executable documentation: emit routing (the
//! event name IS the reference), partition key from event data, wildcard
//! singleton routing, emits whitelist as the Realm boundary, dead events.

use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use aura_engine::Engine;
use aura_realm::Realm;
use std::sync::Arc;

fn counter_of(name: &'static str) -> ActorType {
    ActorType::simple(
        name,
        Arc::new(|ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move {
                // Record the event payload under the event name; the test
                // reads it back after a yield.
                let n = ctx.state.get("events")?.and_then(|v| v.as_i64()).unwrap_or(0);
                ctx.state.set("events", serde_json::json!(n + 1))?;
                ctx.state.set(&format!("last:{}", args["event"].as_str().unwrap_or("?")), args)?;
                Ok(serde_json::Value::Null)
            })
        }),
    )
}

#[tokio::test]
async fn exact_route_partition_key_from_event_data() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart")).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("add_to_cart", "cart", "user_id");
    }

    // The event name references the actor; the payload's user_id picks the
    // instance. Two users → two instances, independent state.
    Realm::emit(&engine.realm, None, "add_to_cart", serde_json::json!({
        "event": "add_to_cart", "user_id": "alice", "item": "book"
    })).await.unwrap();
    Realm::emit(&engine.realm, None, "add_to_cart", serde_json::json!({
        "event": "add_to_cart", "user_id": "bob", "item": "pen"
    })).await.unwrap();

    // Fire-and-forget: yield until handlers drain.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let read = |k: &str| {
        let realm = engine.realm.try_lock().unwrap();
        realm.store.get(
            &InstanceId { actor_type: "cart".into(), key: k.into() }, "events"
        ).unwrap()
    };
    assert_eq!(read("alice"), Some(serde_json::json!(1)));
    assert_eq!(read("bob"), Some(serde_json::json!(1)));
}

#[tokio::test]
async fn wildcard_route_goes_to_singleton() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("audit")).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on_wildcard("order.*", "audit");
    }

    for name in ["order.created", "order.cancelled"] {
        Realm::emit(&engine.realm, None, name, serde_json::json!({
            "event": name, "user_id": "u1"
        })).await.unwrap();
    }
    // "order" alone does not match the "order." prefix (wiki: no dot = no match).
    Realm::emit(&engine.realm, None, "order", serde_json::json!({"event": "order"})).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let realm = engine.realm.try_lock().unwrap();
    let events = realm.store.get(
        &InstanceId { actor_type: "audit".into(), key: "__singleton__".into() }, "events"
    ).unwrap();
    assert_eq!(events, Some(serde_json::json!(2)));
    // Unmatched event landed in dead letters.
    assert_eq!(realm.dead_events.len(), 1);
}

#[tokio::test]
async fn emits_need_no_declaration_dead_ring_is_the_boundary() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart")).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("cart_updated", "cart", "user_id");
    }

    // ADR-0012: emits are never declared or validated — the receiver set is
    // a runtime fact. A matching subscriber receives.
    assert!(Realm::emit(&engine.realm, Some("cart"), "cart_updated", serde_json::json!({
        "event": "cart_updated", "user_id": "u1"
    })).await.is_ok());
    // No subscriber: the event lands in the dead ring — the observable
    // boundary, not a registration error.
    assert!(Realm::emit(&engine.realm, Some("cart"), "ghost_event", serde_json::json!({
        "event": "ghost_event"
    })).await.is_ok());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let realm = engine.realm.try_lock().unwrap();
    assert_eq!(realm.dead_events.len(), 1);
    assert_eq!(realm.dead_events.snapshot()[0].0, "ghost_event");
}

#[tokio::test]
async fn unmatched_events_land_in_dead_ring() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    Realm::emit(&engine.realm, None, "nobody_listens", serde_json::json!({"x": 1}))
        .await.unwrap();
    let realm = engine.realm.try_lock().unwrap();
    assert_eq!(realm.dead_events.len(), 1);
    assert_eq!(realm.dead_events.snapshot()[0].0, "nobody_listens");
}

#[tokio::test]
async fn exact_and_wildcard_both_match_deliver_independently() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart")).await;
    engine.register(counter_of("stats")).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("order.created", "cart", "user_id");
        r.router.on_wildcard("order.*", "stats");
    }

    Realm::emit(&engine.realm, None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "alice"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let realm = engine.realm.try_lock().unwrap();
    // Exact: keyed instance got it.
    assert_eq!(
        realm.store.get(&InstanceId { actor_type: "cart".into(), key: "alice".into() }, "events").unwrap(),
        Some(serde_json::json!(1))
    );
    // Wildcard: singleton got it too.
    assert_eq!(
        realm.store.get(&InstanceId { actor_type: "stats".into(), key: "__singleton__".into() }, "events").unwrap(),
        Some(serde_json::json!(1))
    );
}

// Regression: direct invoke still works alongside the event namespace.
#[tokio::test]
async fn invoke_path_unaffected() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(
        ActorType::simple(
            "echo",
            Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move { Ok(args) })
            }),
        )
    ).await;
    let out = engine
        .invoke(InstanceId { actor_type: "echo".into(), key: "a".into() }, "execute", serde_json::json!({"x": 1}))
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"x": 1}));
    let _ = InstanceId { actor_type: String::new(), key: String::new() }; // silence unused if refactors
}

// ------------------------------------- Phase 4.5c (step 2: event queues) --
//
// One-to-many is structural: two actor types subscribed to the same event
// each get the message through their own private queue Receiver. The
// per-subscription cursor keeps each instance's consumption serial.
#[tokio::test]
async fn one_event_multiple_subscriber_types() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart")).await;
    engine.register(counter_of("stats")).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("order.created", "cart", "user_id");
        r.router.on("order.created", "stats", "user_id");
    }

    Realm::emit(&engine.realm, None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "alice"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let realm = engine.realm.try_lock().unwrap();
    // Both subscriber types received the same event, independently.
    assert_eq!(
        realm.store.get(&InstanceId { actor_type: "cart".into(), key: "alice".into() }, "events").unwrap(),
        Some(serde_json::json!(1))
    );
    assert_eq!(
        realm.store.get(&InstanceId { actor_type: "stats".into(), key: "alice".into() }, "events").unwrap(),
        Some(serde_json::json!(1))
    );
}
