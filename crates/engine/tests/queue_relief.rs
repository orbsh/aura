//! Phase 4.5c close-out: the queue relief valve as observable surface.
//! `ctx_queue_depth` / `ctx_skip_to_now` resolve the instance's bound
//! queue through the PERSISTED route registry — routing here comes from
//! the script's own `(on ...)` declarations (the registration path's
//! EventRoute rows), not a manual `router.on`, which is exactly the
//! source of truth the valve reads.
//!
//! Depth semantics (realm.md retention ruling): the live Count reduce
//! over STORED rows (append +1, watermark compaction -1) — a point read,
//! never a scan. Skip-to-now's cursor mechanics (jump to head, monotonic
//! advance, backlog never re-surfaces) are locked in `realm/tests/mq_okm.rs`;
//! this test proves the bridge reaches them from a script and that the
//! handler's read agrees with the store's.

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;
use aura_realm::{mq, Realm};

fn script() -> &'static str {
    r#"
(on "tick" "user_id" (lambda (args) (hash "got" (hash-ref args "n"))))
(define (depth args) (ctx_queue_depth "tick"))
(define (skip args) (ctx_skip_to_now "tick") 1)
(define (peek args) (ctx_queue_depth (hash-ref args "ev")))
"#
}

fn valve() -> InstanceId {
    InstanceId { booth_type: "valve".into(), key: "k1".into() }
}

#[tokio::test]
async fn relief_valve_bridges_reaches_the_store_and_skip_moves_the_cursor() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("valve", "steel", script()))
        .await
        .unwrap();

    // Warm the resident session.
    Realm::emit(&engine.realm, None, "tick", serde_json::json!({
        "event": "tick", "user_id": "k1", "n": 0
    })).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    for n in 1..=3 {
        Realm::emit(&engine.realm, None, "tick", serde_json::json!({
            "event": "tick", "user_id": "k1", "n": n
        })).await.unwrap();
    }

    // Handler-side depth == store-side depth (same point read), and the
    // route resolution through the persisted registry worked at all.
    let d = engine.invoke(valve(), "depth", serde_json::json!({ "user_id": "k1" })).await.unwrap();
    let vs = engine.realm.try_lock().unwrap().mq.clone();
    let stored = mq::depth(&vs, "tick", "k1").unwrap();
    assert_eq!(d.as_u64(), Some(stored), "handler read == store read: {d} vs {stored}");

    // An event this booth type has no route for = an error value, never
    // a silent no-op (the valve answers to the subscription truth).
    let err = engine.invoke(valve(), "peek", serde_json::json!({ "ev": "other.event" })).await;
    assert!(err.is_err(), "unbound event surfaces as a failure value");

    // Skip-to-now: the cursor jumps to the head — the stored backlog is
    // ahead of no consumer anymore...
    engine.invoke(valve(), "skip", serde_json::json!({ "user_id": "k1" })).await.unwrap();
    let cur = mq::cursor(&vs, "tick", "k1", "valve/k1").unwrap();
    let pending = mq::backlog(&vs, "tick", "k1", cur).unwrap();
    assert!(pending.is_empty(), "skipped: nothing pending after the head cursor");

    // ...and a new emit past the skip drains normally — the valve does
    // not kill the subscription.
    Realm::emit(&engine.realm, None, "tick", serde_json::json!({
        "event": "tick", "user_id": "k1", "n": 99
    })).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let cur = mq::cursor(&vs, "tick", "k1", "valve/k1").unwrap();
    assert!(mq::backlog(&vs, "tick", "k1", cur).unwrap().is_empty(),
        "post-skip events still flow to the cursor");
}
