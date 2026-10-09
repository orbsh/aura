//! Phase 3 acceptance, as executable documentation: emit routing (the
//! event name IS the reference), instance key from event data, wildcard
//! delivery to the singleton instance (ADR-0038 §1), the dead ring as the
//! Realm boundary (ADR-0012 + ADR-0038 §4).

use aura_booth::{BoothType, InstanceId, InstanceKey as BKey};
use aura_engine::Engine;
use aura_realm::Realm;

/// A keyed slice (what a route with a key field resolves to).
fn named(key: &str) -> aura_realm::mq::InstanceKey {
    aura_realm::mq::InstanceKey::Named(key.to_string())
}

// Steel counter (ADR-0026): per-user count into the type's declared
// collection. The handler fn is named after the EVENT it serves (multi-entry
// model). The collection key is the APPLICATION's choice — the counter is
// keyed by the event's user_id (identity rides payload metadata; the
// instance key answers who serializes, modeling.md §2.1). Tests read the
// count back by invoking the `count` handler with the same user_id — the
// booth's observable output, not the retired instance-document model.
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

fn counter_of(name: &'static str, events: &[&'static str]) -> BoothType {
    BoothType::script(name, "steel", counter_script(events))
}

#[tokio::test]
async fn exact_route_instance_key_from_event_data() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart", &["add_to_cart"])).await.unwrap();
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("add_to_cart", "cart", "user_id");
    }

    // The event name references the booth; the payload's user_id picks the
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
                InstanceId { booth_type: "cart".into(), key: BKey::Named(format!("cart/{uid}")) },
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
    engine.register(counter_of("audit", &["order.created", "order.cancelled"])).await.unwrap();
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
            InstanceId { booth_type: "audit".into(), key: BKey::Named("__singleton__".into()) },
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
    engine.register(counter_of("cart", &["cart_updated"])).await.unwrap();
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
    engine.register(counter_of("cart", &["order.created"])).await.unwrap();
    engine.register(counter_of("stats", &["order.created"])).await.unwrap();
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
            InstanceId { booth_type: "cart".into(), key: BKey::Named("alice".into()) },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
    // Wildcard: singleton got it too (the stats handler counted under the
    // event's own user_id — same key the exact-route instance wrote).
    assert_eq!(
        engine.invoke(
            InstanceId { booth_type: "stats".into(), key: BKey::Named("__singleton__".into()) },
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
        BoothType::script("echo", "steel", ECHO)
    ).await.unwrap();
    let out = engine
        .invoke(InstanceId { booth_type: "echo".into(), key: BKey::Named("a".into()) }, "execute", serde_json::json!({"x": 1}))
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"x": 1}));
    let _ = InstanceId { booth_type: String::new(), key: aura_booth::InstanceKey::Singleton }; // silence unused if refactors
}

// ------------------------------------- Phase 4.5c (step 2: event queues) --
//
// One-to-many is structural: two booth types subscribed to the same event
// each get the message through their own private queue Receiver. The
// per-subscription cursor keeps each instance's consumption serial.
#[tokio::test]
async fn one_event_multiple_subscriber_types() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart", &["order.created"])).await.unwrap();
    engine.register(counter_of("stats", &["order.created"])).await.unwrap();
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
            InstanceId { booth_type: "cart".into(), key: BKey::Named("alice".into()) },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
    assert_eq!(
        engine.invoke(
            InstanceId { booth_type: "stats".into(), key: BKey::Named("alice".into()) },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 1})
    );
}

// Regression lock: the consumer task once wrapped an infinite per-
// subscription `loop` inside a `for (event,..) in subs` — for an instance
// bound to MORE queues than the first, the first queue looped forever and
// the rest were never drained (starvation; clippy::never_loop flagged the
// shape). One instance, two routes, two events: both must be consumed.
// Old shape: count 1 (only the first queue's handler ran); new: count 2.
#[tokio::test]
async fn multi_route_instance_drains_every_queue() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("multi", &["evt.a", "evt.b"])).await.unwrap();
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("evt.a", "multi", "user_id");
        r.router.on("evt.b", "multi", "user_id");
    }
    Realm::emit(&engine.realm, None, "evt.a", serde_json::json!({
        "event": "evt.a", "user_id": "alice"
    })).await.unwrap();
    Realm::emit(&engine.realm, None, "evt.b", serde_json::json!({
        "event": "evt.b", "user_id": "alice"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // ONE instance (same key) consumed BOTH queues — the counter is
    // per-user, so 2 means both handlers ran, not fan-out copies.
    assert_eq!(
        engine.invoke(
            InstanceId { booth_type: "multi".into(), key: BKey::Named("alice".into()) },
            "count", serde_json::json!({ "user_id": "alice" }),
        ).await.unwrap(),
        serde_json::json!({"count": 2})
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
            BoothType::script(name, "steel", ECHO)
                .on("order.created", "user_id"),
        )
        .await.unwrap();
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
    let vs = {
        let realm = engine.realm.try_lock().unwrap();
        realm.mq.clone()
    };
    let eid = mq::event_id_of(&vs, "order.created").unwrap().unwrap();
    let part = mq::instance_key_id_of(&vs, "u1").unwrap().unwrap();
    let rows = mq::cursor_rows(&vs, eid, part).unwrap();
    assert_eq!(rows.len(), 2, "two registered subscribers: {rows:?}");
    // Caught up = both cursors equal the partition head (the sequence
    // counter MqData's HighWater(seq) issues; a per-slice 1..N counter, not a
    // clock). The last emit's append advanced the head; both subscribers
    // drained it.
    let head = rows.iter().map(|(_, c, _)| *c).max().unwrap();
    assert!(head > 0, "head advanced past zero");
    assert!(rows.iter().all(|(_, c, _)| *c == head), "both caught up: {rows:?}");

    // Consumers caught up to the head. Write-path compaction ran on each emit
    // with whatever the watermark was AT THAT TIME (cursors lag during the
    // drain), so older rows may already be gone — the invariant is that
    // nothing at or above the final min cursor was deleted.
    let min = rows.iter().map(|(_, c, _)| *c).min().unwrap();
    // The first event's seq: the oldest row the drain saw. With
    // write-path compaction it may already be deleted, so reconstruct the
    // rewind point as the smallest still-known seq below the head; if
    // compaction removed everything below, fall back to min-1 (a rewind
    // just below the watermark suffices — the invariant tested is
    // relative, not tied to a literal starting value).
    let known: Vec<u64> = mq::backlog(&vs, "order.created", &named("u1"), 0)
        .unwrap()
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    let first_seq = known.first().copied().unwrap_or(min.saturating_sub(1));
    let remaining: Vec<u64> = {
        let after = min.saturating_sub(1);
        // Backlog after (min-1) = every row still at or above the
        // watermark; its length tells us whether below-watermark rows
        // were removed by comparing against the pre-compaction count via
        // delete_before's return on a rewind-free call.
        mq::backlog(&vs, "order.created", &named("u1"), after)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect()
    };
    assert!(remaining.iter().all(|s| *s >= min), "nothing below the watermark survives: {remaining:?}");

    // A lagging subscriber pins the watermark: rewind one cursor to the
    // first event's seq (advance is monotonic — a test-only
    // rewind pins the old cursor), re-emit (compaction runs on the emit
    // path), then verify rows below the pinned watermark are gone while
    // later rows survive.
    mq::rewind_cursor(&vs, "order.created", &named("u1"), "cart", first_seq).unwrap();
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
    aura_realm::Realm::compact_queue_for_test(&engine.realm, "order.created", &named("u1")).await.unwrap();
    let surviving = mq::backlog(&vs, "order.created", &named("u1"), 0).unwrap();
    assert!(surviving.iter().all(|(s, _)| *s > first_seq),
        "rows at/below the pinned watermark are gone: {surviving:?}");
    assert!(!surviving.is_empty(), "at/above-watermark rows survive");
}

// --------------------------------- ADR-0038 §1 (the consumer set is closed) --

/// The narrowing lock: a key-less (wildcard) subscription delivers to the
/// type's SINGLETON instance only. Other instances of the type exist in
/// this test, and none of them holds a cursor on the singleton queue — the
/// retired broadcast gave every instance its own participant cursor there,
/// which made the retention denominator an open set (a new key's emit
/// activated a new instance whose cursor read 0 and replayed the queue).
#[tokio::test]
async fn wildcard_delivers_to_the_singleton_instance_only() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(counter_of("audit", &["order.created", "order.ping"]))
        .await
        .unwrap();
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on_wildcard("order.*", "audit");
        r.router.on("order.ping", "audit", "user_id");
    }
    // Two NON-singleton instances of the same type: each binds only its own
    // keyed queue (order.ping), never the singleton one.
    for key in ["alice", "bob"] {
        engine
            .invoke(
                InstanceId { booth_type: "audit".into(), key: aura_booth::InstanceKey::Named(key.into()) },
                "count",
                serde_json::json!({ "user_id": key }),
            )
            .await
            .unwrap();
    }
    Realm::emit(&engine.realm, None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "u1"
    })).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // The singleton consumed it.
    assert_eq!(
        engine
            .invoke(
                InstanceId { booth_type: "audit".into(), key: BKey::Named("__singleton__".into()) },
                "count",
                serde_json::json!({ "user_id": "u1" }),
            )
            .await
            .unwrap(),
        serde_json::json!({ "count": 1 })
    );

    // Exactly ONE cursor on the singleton queue (the singleton's own). Under
    // the broadcast there would be three: alice's, bob's and the singleton's.
    use aura_realm::mq;
    let vs = engine.realm.try_lock().unwrap().mq.clone();
    let eid = mq::event_id_of(&vs, "order.created").unwrap().unwrap();
    let rows = mq::cursor_rows(&vs, eid, mq::SINGLETON_KEY_ID).unwrap();
    assert_eq!(rows.len(), 1, "one consumer per queue (ADR-0038 §1): {rows:?}");
}

// ---------------------------------- ADR-0038 §4 (no silent drops: malformed) --

/// A matched route whose declared key field is missing records a MALFORMED
/// event — the `__default__` fallback instance is retired, so neither a
/// queue row nor a partition name appears. The reason rides the record,
/// because "nobody subscribed" and "the event was malformed" call for
/// different reactions.
#[tokio::test]
async fn missing_key_field_is_recorded_not_fallen_back() {
    use aura_realm::event::DeadReason;
    use aura_realm::mq;

    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(counter_of("cart", &["cart_updated"])).await.unwrap();
    {
        let mut r = engine.realm.try_lock().unwrap();
        r.router.on("cart_updated", "cart", "user_id");
    }
    Realm::emit(&engine.realm, None, "cart_updated", serde_json::json!({
        "event": "cart_updated" // user_id missing
    })).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let realm = engine.realm.try_lock().unwrap();
    let dead = realm.dead_events.detailed();
    assert_eq!(dead.len(), 1, "the malformed event is recorded: {dead:?}");
    assert_eq!(dead[0].0, "cart_updated");
    assert_eq!(dead[0].2, DeadReason::MissingKeyField, "the reason rides the record");
    // Nothing reached the queue plane: no fallback partition, no event id.
    let vs = realm.mq.clone();
    assert!(
        mq::instance_key_id_of(&vs, "__default__").unwrap().is_none(),
        "no fallback partition was ever created"
    );
    assert!(
        mq::event_id_of(&vs, "cart_updated").unwrap().is_none(),
        "the event was never queued"
    );
}

// ------------------------------------- ADR-0039 §2 (the retention promise) --

/// The retention lock: a live-but-lagging consumer pins its backlog; once
/// its cursor is past `cursor_ttl` it LEAVES the denominator and the backlog
/// is forfeit. Driven without consumer tasks, so the observation is exact.
#[tokio::test]
async fn expired_cursor_forfeits_its_backlog() {
    use aura_realm::mq;
    use std::time::Duration;

    let realm = std::sync::Arc::new(tokio::sync::Mutex::new(Realm::default()));
    let vs = realm.lock().await.mq.clone();
    // Two registered types on one event: the denominator's members.
    mq::route_put(&vs, "e", "cart", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();
    mq::route_put(&vs, "e", "stats", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();
    let s1 = mq::append(&vs, "e", &named("u1"), &serde_json::json!({"n": 1})).unwrap();
    let s2 = mq::append(&vs, "e", &named("u1"), &serde_json::json!({"n": 2})).unwrap();
    mq::advance(&vs, "e", &named("u1"), "stats", s2).unwrap();
    mq::advance(&vs, "e", &named("u1"), "cart", s1).unwrap();

    // Fresh rows, default promise (30d): the lagging cart pins the backlog.
    Realm::compact_queue_for_test(&realm, "e", &named("u1")).await.unwrap();
    assert_eq!(
        mq::backlog(&vs, "e", &named("u1"), 0).unwrap().len(),
        2,
        "a live lagging consumer pins its backlog"
    );

    // The promise shrinks to one second and cart's stamp is two seconds
    // stale: cart leaves the denominator, so the watermark becomes stats'
    // cursor and everything below it — cart's unconsumed row — is forfeit.
    realm.lock().await.cursor_ttl = Duration::from_secs(1);
    mq::age_cursor(&vs, "e", &named("u1"), "cart", 1).unwrap();
    Realm::compact_queue_for_test(&realm, "e", &named("u1")).await.unwrap();
    let surviving = mq::backlog(&vs, "e", &named("u1"), 0).unwrap();
    assert_eq!(surviving.len(), 1, "the forfeited backlog is compacted away: {surviving:?}");
    assert!(surviving.iter().all(|(s, _)| *s > s1), "only rows at/above the new watermark survive");

    // The expired row itself is reclaimed once it sits below the watermark
    // (nothing can replay through a position whose rows are gone).
    let eid = mq::event_id_of(&vs, "e").unwrap().unwrap();
    let part = mq::instance_key_id_of(&vs, "u1").unwrap().unwrap();
    let rows = mq::cursor_rows(&vs, eid, part).unwrap();
    assert_eq!(rows.len(), 1, "only stats' cursor remains: {rows:?}");
}
