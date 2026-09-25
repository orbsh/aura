//! Phase 3.6 acceptance, as executable documentation: realm isolation
//! (the axis ADR-0028 renamed namespace → realm; the 3.6 demo shape binds
//! per user — an application choice, not the mechanism's meaning). A
//! NamedRealm handle is bound at construction — cross-realm delivery is
//! not expressible, not merely checked.

use aura_actor::{ActorType, InstanceId};
use aura_engine::Engine;
use aura_realm::Realm;

// Counter into the type's declared collection (ADR-0026 §3). Keyed by the
// payload's user_id (identity rides payload metadata, modeling.md §2.1);
// tests read the count back by INVOKING the `count` handler — the actor's
// observable output, not the retired instance-document model.
const COUNTER: &str = r#"
(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (user-n uid)
  (if (string=? uid "alice") 1
  (if (string=? uid "bob") 2
  9)))
(define (count args)
  (let* ((n (user-n (hash-ref args "user_id")))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n)))))
    (hash "count" (if (void? cur) 0 (if (hash-contains? cur "count") (hash-ref cur "count") 0)))))
(define (execute args)
  (let* ((n (user-n (hash-ref args "user_id")))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n))))
         (c (if (void? cur) 0 (if (hash-contains? cur "count") (hash-ref cur "count") 0))))
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                          "key" (hash "id" n) "doc" (hash "count" (+ c 1))))
    (hash "count" (+ c 1))))
"#;
const LISTENER: &str = r#"
(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "seen" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (user-n uid)
  (if (string=? uid "u1") 3
  9))
(define (count args)
  (let* ((n (user-n (hash-ref args "user_id")))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n)))))
    (hash "seen" (if (void? cur) 0 (if (hash-contains? cur "seen") (hash-ref cur "seen") 0)))))
(define (order.created args)
  (let* ((n (user-n (hash-ref args "user_id")))
         (cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" n))))
         (c (if (void? cur) 0 (if (hash-contains? cur "seen") (hash-ref cur "seen") 0))))
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                          "key" (hash "id" n) "doc" (hash "seen" (+ c 1))))
    (+ c 1)))
"#;

fn counter() -> ActorType {
    ActorType::script("counter", "steel", COUNTER)
}

#[tokio::test]
async fn same_type_key_isolated_per_realm() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    // Same type, same key, two users: independent state.
    engine.register_in("alice", counter()).await;
    engine.register_in("bob", counter()).await;

    let target = InstanceId { actor_type: "counter".into(), key: "k".into() };
    engine.call_in("alice", target.clone(), "execute", serde_json::json!({"user_id": "alice"})).await.unwrap();
    engine.call_in("alice", target.clone(), "execute", serde_json::json!({"user_id": "alice"})).await.unwrap();
    engine.call_in("bob", target.clone(), "execute", serde_json::json!({"user_id": "bob"})).await.unwrap();

    // alice's count is 2, bob's is 1 — the realms never mixed. Read
    // back through the `count` handler (the actor's observable output).
    let read = |ns: String, uid: String| {
        let engine_ns = engine.realm_set.clone();
        let target = target.clone();
        async move {
            let realm = engine_ns.realm_of(&ns).await;
            let waited = Realm::call(&realm.realm(), None, target, "count", serde_json::json!({"user_id": uid})).await.unwrap().wait().await.unwrap();
            match waited {
                aura_actor::call::Waited::Done(v) => v.unwrap(),
                aura_actor::call::Waited::Pending(_) => panic!("count read went cold"),
            }
        }
    };
    assert_eq!(
        read("alice".into(), "alice".into()).await,
        serde_json::json!({"count": 2})
    );
    assert_eq!(
        read("bob".into(), "bob".into()).await,
        serde_json::json!({"count": 1})
    );
}

#[tokio::test]
async fn events_do_not_cross_realms() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    // Same event subscription in two realms; the emit goes to one.
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
        let ns_realm = engine.realm_set.realm_of(ns).await;
        ns_realm.realm().lock().await.router.on("order.created", "listener", "user_id");
    }

    // Alice emits; only alice's listener may see it.
    engine.emit_in("alice", None, "order.created", serde_json::json!({
        "event": "order.created", "user_id": "u1"
    })).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let target = InstanceId { actor_type: "listener".into(), key: "u1".into() };
    // Read back through the `count` handler (the actor's observable output).
    let read = |ns: String| {
        let engine_ns = engine.realm_set.clone();
        let target = target.clone();
        async move {
            let realm = engine_ns.realm_of(&ns).await;
            let waited = Realm::call(&realm.realm(), None, target, "count", serde_json::json!({"user_id": "u1"})).await.unwrap().wait().await.unwrap();
            match waited {
                aura_actor::call::Waited::Done(v) => v.unwrap(),
                aura_actor::call::Waited::Pending(_) => panic!("count read went cold"),
            }
        }
    };
    assert_eq!(
        read("alice".into()).await,
        serde_json::json!({"seen": 1}),
        "alice's listener must have seen the event"
    );
    assert_eq!(
        read("bob".into()).await,
        serde_json::json!({"seen": 0}),
        "bob's listener must NOT see alice's event"
    );
}

#[tokio::test]
async fn type_registered_in_one_realm_is_unknown_in_another() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    engine.register_in("alice", counter()).await;

    // Bob's realm has no "counter" type: target resolution fails —
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
async fn realms_are_lazy_and_observable() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    assert!(engine.realm_set.live().await.is_empty());
    engine.register_in("alice", counter()).await;
    let _ = engine.realm_set.realm_of("bob").await;
    let mut live = engine.realm_set.live().await;
    live.sort();
    assert_eq!(live, vec!["alice".to_string(), "bob".to_string()]);
}
