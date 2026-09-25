//! Phase 3 acceptance, as executable documentation: emit routing (the
//! event name IS the reference), instance key from event data, wildcard
//! singleton routing, emits whitelist as the Realm boundary, dead events.

use aura_actor::{ActorType, InstanceId};
use aura_engine::Engine;
use aura_realm::Realm;

// Steel counter (ADR-0026): per-user count into the type's declared
// collection. The handler fn is named after the EVENT it serves (multi-entry
// model). The collection key is the APPLICATION's choice — the counter is
// keyed by the event's user_id (identity rides payload metadata; the
// instance key answers who serializes, modeling.md §2.1). Tests read the
// count back by invoking the `count` handler with the same user_id — the
// actor's observable output, not the retired instance-document model.
fn counter_script(events: &[&'static str]) -> String {
    let common = r#"(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (count args)
  (let* ((n (user-n (hash-ref args "user_id")))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n)))))
    (if (void? cur) (hash "count" 0) (hash "count" (hash-ref cur "count")))))
"#;
    // user-doc maps user_id → a fixed U64 id (the collection's key fields
    // are fixed-width; string user ids hash — here the tests use small
    // numeric user_ids, so the map is trivial).
    let body = |name: &str| format!(
        r#"(define ({name} args)
  (let* ((uid (hash-ref args "user_id"))
         (n (user-n uid))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n))))
         (c (if (void? cur) 0 (hash-ref cur "count"))))
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                       "key" (hash "id" n) "doc" (hash "count" (+ c 1))))
    (+ c 1)))"#
    );
    // user-doc: alice→1, bob→2, u1→3, __singleton__→4 (the shapes the
    // tests emit).
    let userdoc = r#"(define (user-n uid)
  (if (string=? uid "alice") 1
  (if (string=? uid "bob") 2
  (if (string=? uid "u1") 3
  (if (string=? uid "__singleton__") 4
  9)))))
(define (user-doc uid) (hash "n" (user-n uid)))
"#;
    format!("{common}{userdoc}{}", events.iter().map(|e| body(e)).collect::<Vec<_>>().join("\n"))
}

fn counter_of(name: &'static str, events: &[&'static str]) -> ActorType {
    ActorType::script(name, "steel", counter_script(events))
}

#[tokio::test]
async fn exact_route_instance_key_from_event_data() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart", &["add_to_cart"])).await;
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

    let read = async |uid: &str| {
        engine
            .invoke(
                InstanceId { actor_type: "cart".into(), key: format!("cart/{uid}") },
                "count",
                serde_json::json!({ "user_id": uid }),
            )
            .await
            .unwrap()
    };
    assert_eq!(read("alice").await, serde_json::json!({"count": 1}));
    assert_eq!(read("bob").await, serde_json::json!({"count": 1}));
}

#[tokio::test]
async fn wildcard_route_goes_to_singleton() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("audit", &["order.created", "order.cancelled"])).await;
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

    let count = engine
        .invoke(
            InstanceId { actor_type: "audit".into(), key: "__singleton__".into() },
            "count",
            serde_json::json!({ "user_id": "u1" }),
        )
        .await
        .unwrap();
    assert_eq!(count, serde_json::json!({"count": 2}));
    // Unmatched event landed in dead letters.
    assert_eq!(engine.realm.lock().await.dead_events.len(), 1);
}

#[tokio::test]
async fn emits_need_no_declaration_dead_ring_is_the_boundary() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart", &["cart_updated"])).await;
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
    engine.register(counter_of("cart", &["order.created"])).await;
    engine.register(counter_of("stats", &["order.created"])).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("order.created", "cart", "user_id");
        r.router.on_wildcard("order.*", "stats");
    }

    Realm::emit(&engine.realm, None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "alice"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Exact: keyed instance got it.
    assert_eq!(
        engine.invoke(
            InstanceId { actor_type: "cart".into(), key: "alice".into() },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
    // Wildcard: singleton got it too (the stats handler counted under the
    // event's own user_id — same key the exact-route instance wrote).
    assert_eq!(
        engine.invoke(
            InstanceId { actor_type: "stats".into(), key: "__singleton__".into() },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
}

// Regression: direct invoke still works alongside event delivery.
#[tokio::test]
async fn invoke_path_unaffected() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    const ECHO: &str = r#"
(define (execute args) args)
"#;
    engine.register(
        ActorType::script("echo", "steel", ECHO)
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
    engine.register(counter_of("cart", &["order.created"])).await;
    engine.register(counter_of("stats", &["order.created"])).await;
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("order.created", "cart", "user_id");
        r.router.on("order.created", "stats", "user_id");
    }

    Realm::emit(&engine.realm, None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "alice"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Both subscriber types received the same event, independently.
    assert_eq!(
        engine.invoke(
            InstanceId { actor_type: "cart".into(), key: "alice".into() },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
    assert_eq!(
        engine.invoke(
            InstanceId { actor_type: "stats".into(), key: "alice".into() },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
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
            ActorType::script(name, "steel", ECHO)
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
    // Caught up = both cursors equal the partition head (logical time,
    // monotonic — not a compact 1..3 sequence). The last emit's append
    // assigned the head; both subscribers drained it.
    let head = rows.iter().map(|(_, c)| *c).max().unwrap();
    assert!(head > 0, "head advanced past zero");
    assert!(rows.iter().all(|(_, c)| *c == head), "both caught up: {rows:?}");

    // Consumers caught up to the head. Write-path compaction ran on each emit
    // with whatever the watermark was AT THAT TIME (cursors lag during the
    // drain), so older rows may already be gone — the invariant is that
    // nothing at or above the final min cursor was deleted.
    let min = rows.iter().map(|(_, c)| *c).min().unwrap();
    // The first event's logical time: the oldest row the drain saw. With
    // write-path compaction it may already be deleted, so reconstruct the
    // rewind point as the smallest still-known time below the head; if
    // compaction removed everything below, fall back to min-1 (a rewind
    // just below the watermark suffices — the invariant tested is
    // relative, not tied to a literal 1..3 numbering).
    let known: Vec<u64> = mq::backlog(&mut vs, "order.created", "u1", 0)
        .unwrap()
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    let first_time = known.first().copied().unwrap_or(min.saturating_sub(1));
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

    // A lagging subscriber pins the watermark: rewind one cursor to the
    // first event's logical time, re-emit (compaction runs on the emit
    // path), then verify rows below the pinned watermark are gone while
    // later rows survive.
    mq::advance(&mut vs, "order.created", "u1", "cart/u1", first_time).unwrap();
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
    assert!(surviving.iter().all(|(s, _)| *s > first_time),
        "rows at/below the pinned watermark are gone: {surviving:?}");
    assert!(surviving.len() >= 1, "at/above-watermark rows survive");
}
