//! Phase 3.6 acceptance, as executable documentation: per-user namespace
//! isolation. A NamespacedRealm handle is bound at construction —
//! cross-namespace delivery is not expressible, not merely checked.

use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use aura_engine::Engine;
use std::sync::Arc;

const COUNTER: &str = r#"
(define (execute args)
  (let* ((got (ctx_state_get "count"))
         (n (if (hash-ref got "present") (hash-ref got "value") 0)))
    (ctx_state_set (hash "field" "count" "value" (+ n 1)))
    (hash "count" (+ n 1))))
"#;
const LISTENER: &str = r#"
(define (order.created args)
  (let* ((got (ctx_state_get "seen"))
         (n (if (hash-ref got "present") (hash-ref got "value") 0)))
    (ctx_state_set (hash "field" "seen" "value" (+ n 1)))
    n))
"#;

fn counter() -> ActorType {
    ActorType::script("counter", "steel", COUNTER)
}

#[tokio::test]
async fn same_type_key_isolated_per_namespace() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    // Same type, same key, two users: independent state.
    engine.register_in("alice", counter()).await;
    engine.register_in("bob", counter()).await;

    let target = InstanceId { actor_type: "counter".into(), key: "k".into() };
    engine.call_in("alice", target.clone(), "execute", serde_json::json!(null)).await.unwrap();
    engine.call_in("alice", target.clone(), "execute", serde_json::json!(null)).await.unwrap();
    engine.call_in("bob", target.clone(), "execute", serde_json::json!(null)).await.unwrap();

    // alice's count is 2, bob's is 1 — the namespaces never mixed.
    let alice = engine.namespaces.realm_of("alice").await;
    let bob = engine.namespaces.realm_of("bob").await;
    assert_eq!(
        alice.realm().lock().await.store.get(&target, "count").unwrap(),
        Some(serde_json::json!(2))
    );
    assert_eq!(
        bob.realm().lock().await.store.get(&target, "count").unwrap(),
        Some(serde_json::json!(1))
    );
}

#[tokio::test]
async fn events_do_not_cross_namespaces() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    // Same event subscription in two namespaces; the emit goes to one.
    let listener = || {
        ActorType::script(
            "listener",
            "steel",
            LISTENER,
        )
    };
    engine.register_in("alice", listener()).await;
    engine.register_in("bob", listener()).await;

    for ns in ["alice", "bob"] {
        let ns_realm = engine.namespaces.realm_of(ns).await;
        ns_realm.realm().lock().await.router.on("order.created", "listener", "user_id");
    }

    // Alice emits; only alice's listener may see it.
    engine.emit_in("alice", None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "u1"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let alice = engine.namespaces.realm_of("alice").await;
    let bob = engine.namespaces.realm_of("bob").await;
    let target = InstanceId { actor_type: "listener".into(), key: "u1".into() };
    assert_eq!(
        alice.realm().lock().await.store.get(&target, "seen").unwrap(),
        Some(serde_json::json!(1)),
        "alice's listener must have seen the event"
    );
    assert_eq!(
        bob.realm().lock().await.store.get(&target, "seen").unwrap(),
        None,
        "bob's listener must NOT see alice's event"
    );
}

#[tokio::test]
async fn type_registered_in_one_namespace_is_unknown_in_another() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    engine.register_in("alice", counter()).await;

    // Bob's namespace has no "counter" type: target resolution fails —
    // this is the isolation expressed as a call error.
    let err = engine
        .call_in(
            "bob",
            InstanceId { actor_type: "counter".into(), key: "k".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown actor type"));
}

#[tokio::test]
async fn namespaces_are_lazy_and_observable() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    assert!(engine.namespaces.live().await.is_empty());
    engine.register_in("alice", counter()).await;
    let _ = engine.namespaces.realm_of("bob").await;
    let mut live = engine.namespaces.live().await;
    live.sort();
    assert_eq!(live, vec!["alice".to_string(), "bob".to_string()]);
}
