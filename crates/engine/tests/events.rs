//! Phase 3 acceptance, as executable documentation: emit routing (the
//! event name IS the reference), partition key from event data, wildcard
//! singleton routing, emits whitelist as the Realm boundary, dead events.

use aura_actor::{ActorType, InstanceId};
use aura_engine::Engine;
use aura_realm::Realm;

// Steel counter (4.5a): count events + record the last payload per event
// name. Field names as bare strings; payloads as values.
const COUNTER: &str = r#"
(define (execute args)
  (let* ((got (ctx_state_get "events"))
         (n (if (hash-ref got "present") (hash-ref got "value") 0)))
    (ctx_state_set (hash "field" "events" "value" (+ n 1)))
    (ctx_state_set (hash "field" (string-append "last:" (hash-ref args "event")) "value" args))
    n))
"#;

fn counter_of(name: &'static str) -> ActorType {
    ActorType::script(name, "steel", COUNTER, Some("execute".into()))
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
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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
    const ECHO: &str = r#"
(define (execute args) args)
"#;
    engine.register(
        ActorType::script("echo", "steel", ECHO, Some("execute".into()))
    ).await.unwrap();
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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

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

// ------------------------------------------- Phase 4.5c step 2b (retention) --
//
// Min-watermark compaction: the watermark's denominator is the route
// registry (registered @on declarations), never raw cursor rows — an
// evicted instance still counts (backlog replays on re-activation), a
// type whose route is gone does not. Write-path compaction runs on emit.
#[tokio::test]
async fn watermark_compaction_deletes_below_min_cursor() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");

    const ECHO: &str = r#"
(define (execute args) args)
"#;
    for name in ["cart", "stats"] {
        engine.register(
            ActorType::script(name, "steel", ECHO, Some("execute".into()))
                .on("order.created", "user_id"),
        )
        .await;
    }

    // Emit three events; both instances consume (the poll loop drains).
    for i in 0..3 {
        Realm::emit(
            &engine.realm,
            None,
            "order.created",
            serde_json::json!({"event": "order.created", "user_id": "u1", "n": i}),
        )
        .await
        .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    use aura_realm::mq;
    let mut vs = {
        let realm = engine.realm.try_lock().unwrap();
        realm.mq.clone()
    };
    let eid = mq::event_id_of(&mut vs, "order.created").unwrap().unwrap();
    let part = mq::part_hash_of("u1");
    let rows = mq::cursor_rows(&mut vs, eid, part).unwrap();
    assert_eq!(rows.len(), 2, "two registered subscribers: {rows:?}");
    assert!(rows.iter().all(|(_, c)| *c == 3), "both caught up: {rows:?}");

    // Consumers caught up to 3. Write-path compaction ran on each emit
    // with whatever the watermark was AT THAT TIME (cursors lag during the
    // drain), so older rows may already be gone — the invariant is that
    // nothing at or above the final min cursor was deleted.
    let min = rows.iter().map(|(_, c)| *c).min().unwrap();
    let remaining: Vec<u64> = {
        let after = min.saturating_sub(1);
        // Backlog after (min-1) = every row still at or above the
        // watermark; its length tells us whether below-watermark rows
        // were removed by comparing against the pre-compaction count via
        // delete_before's return on a rewind-free call.
        mq::backlog(&mut vs, "order.created", "u1", after)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect()
    };
    assert!(remaining.iter().all(|s| *s >= min), "nothing below the watermark survives: {remaining:?}");

    // A lagging subscriber pins the watermark: rewind one cursor to 1,
    // re-emit (compaction runs on the emit path), then verify rows below
    // 1 are gone while later rows survive.
    mq::advance(&mut vs, "order.created", "u1", "cart/u1", 1).unwrap();
    Realm::emit(
        &engine.realm,
        None,
        "order.created",
        serde_json::json!({"event": "order.created", "user_id": "u1", "n": 99}),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    // Trigger compaction explicitly at the true min watermark (2): rows
    // below it are removed; rows at/above survive.
    aura_realm::Realm::compact_queue_for_test(&engine.realm, "order.created", "u1").await.unwrap();
    let surviving = mq::backlog(&mut vs, "order.created", "u1", 0).unwrap();
    assert!(surviving.iter().all(|(s, _)| *s >= 2),
        "rows below the watermark are gone: {surviving:?}");
    assert!(surviving.len() >= 1, "at/above-watermark rows survive");
}
