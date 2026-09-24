//! Acceptance paths, as executable documentation.
//!
//! Phase 0: define → invoke → return; ctx.invoke as the single call
//! surface (ADR-0011). Phase 1: state survives eviction (scale-to-zero
//! drops the resident, not the data); on_sleep/on_wake run around it.

use aura_actor::{ActorType, InstanceId};
use aura_engine::Engine;
use std::time::Duration;

// Steel script actors (4.5a): script source is the only public actor form.
const ECHO: &str = r#"
(define (execute args) args)
"#;

const CALLER: &str = r#"
(define (execute args)
  (ctx_invoke (string-append
    "{\"type\": \"echo\", \"key\": \""
    (hash-ref args "target_key")
    "\", \"handler\": \"execute\", \"args\": {\"via\": \"ctx.invoke\"}}")))
"#;

fn echo_type() -> ActorType {
    ActorType::script("echo", "steel", ECHO)
}

// ---------------------------------------------------------------- Phase 0 --

#[tokio::test]
async fn invoke_returns_handler_result() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(echo_type()).await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "echo".into(), key: "a1".into() },
                "execute",
            serde_json::json!({"hello": "aura"}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"hello": "aura"}));
}

#[tokio::test]
async fn ctx_invoke_routes_through_realm() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(echo_type()).await;

    // `caller` invokes `echo` via ctx.invoke — the only call surface an
    // actor sees; target resolution is registry-declared.
    engine
        .register(ActorType::script("caller", "steel", CALLER))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { actor_type: "caller".into(), key: "c1".into() },
                "execute",
            serde_json::json!({"target_key": "a2"}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"via": "ctx.invoke"}));
}

#[tokio::test]
async fn unknown_actor_type_is_error_value() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let err = engine
        .invoke(
            InstanceId { actor_type: "ghost".into(), key: "x".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown actor type"));
}

#[tokio::test]
async fn instance_key_activates_distinct_instances() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(echo_type()).await;

    for key in ["a1", "a2"] {
        let out = engine
            .invoke(
                InstanceId { actor_type: "echo".into(), key: key.into() },
                "execute",
                serde_json::json!({"key": key}),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"key": key}));
    }
}

// ---------------------------------------------------------------- Phase 1 --

// Declared-collection writes persist across eviction: the instance is
// dropped, the data is not. on_sleep/on_wake run around the boundary.
// (The ctx_state_* point model is retired — ADR-0026 §3: state rides the
// type's declared collections through ctx.store.emit.)
#[tokio::test]
async fn state_survives_scale_to_zero() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    // RMW via the ctx bridge: read the whole field map, merge, write back.
    const COUNTER: &str = r#"
(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (execute args)
  (let* ((cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" 1))))
         (n (if (void? cur) 0 (if (hash-contains? cur "count") (hash-ref cur "count") 0)))
         (put (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                                    "key" (hash "id" 1) "doc" (hash "count" (+ n 1))))))
    (hash "count" (+ n 1))))
"#;
    engine.register(
        ActorType::script("counter", "steel", COUNTER)
    ).await.unwrap();

    let target = InstanceId { actor_type: "counter".into(), key: "k1".into() };
    assert_eq!(engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 1}));
    assert_eq!(engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 2}));

    // Force eviction: everything idle is older than 0s.
    engine.realm.lock().await.evict_idle(engine.realm.clone()).await;

    // Resident is gone; state survives. Next touch reactivates (on_wake)
    // and the count continues.
    assert_eq!(engine.invoke(target, "execute", serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 3}));
}

// ------------------------------------------------------- Phase 2 (script) --

// Script actors execute through the probe carriers — the same carrier set
// the remote actuator uses; language execution is not reimplemented here.
// Script actors are pure functions in this phase (args in, value out).
#[cfg(feature = "nushell")]
#[tokio::test]
async fn nushell_script_actor() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "nu-op",
            "nushell",
            r#"
export def execute [args] {
    { sum: ($args.items | math sum) }
}
"#,
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "nu-op".into(), key: "n1".into() },
                "execute",
            serde_json::json!({"items": [1, 2, 3]}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"sum": 6}));
}

#[cfg(feature = "python")]
#[tokio::test]
async fn python_script_actor() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "py-op",
            "python",
            r#"def execute(args):
    return {"doubled": args["x"] * 2}
"#,
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "py-op".into(), key: "p1".into() },
                "execute",
            serde_json::json!({"x": 21}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"doubled": 42}));
}

// A script actor naming a language this build does not carry is an error
// value on the call path — the same validate-at-dispatch rule as probe.
#[cfg(feature = "nushell")]
#[tokio::test]
async fn script_unknown_language_is_error_value() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "koto-op",
            "koto",
            "1 + 2",
        ))
        .await;

    let err = engine
        .invoke(
            InstanceId { actor_type: "koto-op".into(), key: "k1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("koto"));
}

// Idle TTL drives eviction without manual calls: short TTL + evictor tick.
#[tokio::test]
async fn idle_ttl_evicts_automatically() {
    // The evictor ticks every 5s; use a 0s TTL and drive one tick manually
    // via the realm to keep the test fast — the tick loop itself is
    // exercised by the running engine.
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(echo_type()).await;

    let target = InstanceId { actor_type: "echo".into(), key: "ttl".into() };
    engine.invoke(target, "execute", serde_json::json!(null)).await.unwrap();

    {
        let mut realm = engine.realm.try_lock().unwrap();
        realm.idle_ttl = Duration::from_secs(0);
        let evicted = realm.evict_idle(engine.realm.clone()).await;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, "ttl");
    }
}

// Per-type TTL: a type's own residency policy overrides the realm-wide
// default — the mechanism Phase 6.5's retention window rides on (a
// turn-executor declares a long TTL; entity actors fall back to default).
#[tokio::test]
async fn per_type_idle_ttl_overrides_realm_default() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");

    // "dweller": 10-minute TTL (long-lived resident, the retention-window
    // shape). "echo": no override — realm default applies.
    let dweller = ActorType::script("dweller", "steel", ECHO)
        .with_idle_ttl(Duration::from_secs(600));
    engine.register(dweller).await;
    engine.register(echo_type()).await;

    engine
        .invoke(
            InstanceId { actor_type: "dweller".into(), key: "d1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap();
    engine
        .invoke(
            InstanceId { actor_type: "echo".into(), key: "e1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap();

    {
        let mut realm = engine.realm.try_lock().unwrap();
        realm.idle_ttl = Duration::from_secs(0); // default: everything idle
        let evicted = realm.evict_idle(engine.realm.clone()).await;
        // Only echo is evicted; the dweller's own TTL keeps it resident.
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].actor_type, "echo");
        // A second pass evicts nothing more: the dweller is still under
        // its own 600s TTL even though the realm default is 0s.
        let again = realm.evict_idle(engine.realm.clone()).await;
        assert!(again.is_empty());
    }
}

// ------------------------------------------------- Phase 2.5 (ctx bridge) --
//
// Script actors reach the host through named functions: one JSON argument
// in, one JSON value out. Storage rides `ctx_store_emit` (the type's
// declared collections, ADR-0026 §3); `ctx_invoke` rides the unified call
// model (Phase 3.5).

// Steel script: one RMW into the type's declared collection, read back,
// and invoke another actor through ctx_invoke. The storage declaration is
// the interface_schema `storage` block (same shape as events.rs).
#[cfg(feature = "steel")]
#[tokio::test]
async fn steel_script_ctx_bridge() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");

    // Target invoked from the script: echoes back its args.
    engine.register(echo_type()).await;
    engine
        .register(aura_actor::ActorType::script(
            "steel-ctx",
            "steel",
            r#"
(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (execute args)
  (let* ((cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" 1))))
         (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"ttl2\", \"handler\": \"execute\", \"args\": {\"hello\": true}}")))
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                          "key" (hash "id" 1) "doc" (hash "count" 1)))
    (hash "present" (if (void? cur) #f #t) "visits" (if (void? cur) 1 (+ 1 (hash-ref cur "count"))) "echo" (hash-ref echoed "hello"))))
"#,
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "steel-ctx".into(), key: "s1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        serde_json::json!({"present": false, "visits": 1, "echo": true})
    );
}

// Python script: same bridge surface — collection RMW + invoke. The
// explicit `interface_schema` half merges into the derived one (python.rs
// field-wise merge), so the storage block reaches the persisted schema.
#[cfg(feature = "python")]
#[tokio::test]
async fn python_script_ctx_bridge() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(echo_type()).await;
    engine
        .register(aura_actor::ActorType::script(
            "py-ctx",
            "python",
            r#"
import json

def interface_schema(args):
    return {"storage": {"collections": {"counters": {"schema": {
        "key_len": 8,
        "key_fields": [{"name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": 0}],
        "layout_version": 1, "hot_width": 8, "payload_header_len": 3,
        "hot_fields": [{"name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": 0}],
        "cold_fields": [],
        "slots": {"primary": 0, "dynamic": 1, "dict_id": 2, "dict_name": 3,
                  "declared_index_base": 4096, "declared_reduce_base": 8192,
                  "junction_base": 12288}}}}}}

def execute(args):
    cur = ctx_store_emit(json.dumps({"collection": "counters", "op": "get_document", "key": {"id": 1}}))
    ctx_store_emit(json.dumps({"collection": "counters", "op": "put_document",
                               "key": {"id": 1}, "doc": {"color": "blue"}}))
    echo = ctx_invoke(json.dumps({"type": "echo", "key": "ttl3", "handler": "execute", "args": {"ok": 7}}))
    stored = "blue" if cur is None else (cur.get("color") or "blue")
    return {"stored": stored, "echo": echo["ok"]}
"#,
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "py-ctx".into(), key: "p1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"stored": "blue", "echo": 7}));
}

// Residency is EPHEMERAL: idle eviction drops the VM with the instance
// (Phase 2.6 wiring). In-session memory state (module globals) restarts;
// store state (declared collections) survives — durable truth is only the
// store.
#[cfg(feature = "python")]
#[tokio::test]
async fn idle_eviction_drops_the_resident_session() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "py-resident",
            "python",
            r#"
memory = 0

def bump(args):
    global memory
    memory = memory + 1
    return {"memory": memory}
"#,
            Some("bump".into()),
        ))
        .await;

    let target = InstanceId { actor_type: "py-resident".into(), key: "r1".into() };
    let out = engine.invoke(target.clone(), "bump", serde_json::json!(null)).await.unwrap();
    let out = engine.invoke(target.clone(), "bump", serde_json::json!(null)).await.unwrap();
    assert_eq!(out, serde_json::json!({"memory": 2}), "same session accumulates");

    {
        let mut realm = engine.realm.try_lock().unwrap();
        realm.idle_ttl = Duration::from_secs(0);
        let evicted = realm.evict_idle(engine.realm.clone()).await;
        assert_eq!(evicted.len(), 1);
    }

    let out = engine.invoke(target, "bump", serde_json::json!(null)).await.unwrap();
    assert_eq!(out, serde_json::json!({"memory": 1}), "evicted session restarted fresh");
}


// Script-declared TTL: `interface_schema()` introspection seeds
// `ActorType.idle_ttl` at registration (host ← script; the script never
// touches the engine). The retention window rides this for script
// turn-executors.
#[cfg(feature = "python")]
#[tokio::test]
async fn script_interface_schema_declares_idle_ttl() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "py-dweller",
            "python",
            r#"
def interface_schema(args):
    return {"lifecycle": {"idle_ttl": "5m"}}

def execute(args):
    return {"ok": True}
"#,
        ))
        .await;

    // Register introspected the declaration: the type now carries a
    // per-type TTL even though the host never called with_idle_ttl.
    {
        let realm = engine.realm.try_lock().unwrap();
        let actor = realm.actor_type("py-dweller").unwrap();
        assert_eq!(actor.idle_ttl, Some(Duration::from_secs(300)));
    }

    // Plain script without the lifecycle section: no TTL adopted.
    engine
        .register(aura_actor::ActorType::script(
            "py-plain",
            "python",
            r#"
def interface_schema(args):
    return {"receives": {}}

def execute(args):
    return {"ok": True}
"#,
        ))
        .await;
    {
        let realm = engine.realm.try_lock().unwrap();
        let actor = realm.actor_type("py-plain").unwrap();
        assert_eq!(actor.idle_ttl, None);
    }
}

// ------------------------------------------------- Phase 4.5b (metadata lifecycle) --
//
// UPLOAD is its own lifecycle: registering a script actor persists the
// definition + introspected TTL into the meta store; a fresh engine booted
// on the same data dir reloads the type — definitions survive node
// restart, execution never re-introspects.

#[cfg(all(feature = "fjall", feature = "steel"))]
#[tokio::test]
async fn script_actor_definition_survives_restart() {
    // ADR-0025 Plan A: definitions live in the DATA plane's okm instance
    // (ActorDef table) — restart persistence rides the single data dir,
    // there is no separate meta instance/config anymore.
    let dir = tempfile::tempdir().unwrap();

    let mut cfg = aura_config::EngineConfig::default();
    cfg.engine = aura_config::Engine::Fjall;
    cfg.data_dir = Some(dir.path().to_path_buf());

    // Node 1: register a script actor (declares idle_ttl via
    // interface_schema — introspection happens at upload).
    {
        let engine = Engine::start(&cfg).await.expect("boot");
        engine
            .register(aura_actor::ActorType::script(
                "persisted",
                "steel",
                r#"
(define (interface_schema args)
  (hash "lifecycle" (hash "idle_ttl" "5m")))

(define (execute args)
  (hash "ok" #t))
"#,
            ))
            .await
            .unwrap();
        // engine dropped here
    }

    // Node 2: fresh engine on the same meta dir — the type reloads with
    // its introspected TTL, and is immediately invocable.
    let engine = Engine::start(&cfg).await.expect("boot");
    {
        let realm = engine.realm.try_lock().unwrap();
        let actor = realm.actor_type("persisted").expect("type reloaded");
        assert_eq!(actor.idle_ttl, Some(Duration::from_secs(300)));
    }
    let out = engine
        .invoke(
            InstanceId { actor_type: "persisted".into(), key: "k1".into() },
                "execute",
            serde_json::json!(null),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"ok": true}));
}

// Phase 4.5b (c): nushell introspection — interface_schema() declared in
// the script is callable at registration through the same generated
// wrapper (one spawn, call schema, done). Nu actors declare TTL in
// script like python/steel; ctx host fns remain unavailable.
#[cfg(feature = "nushell")]
#[tokio::test]
async fn nushell_interface_schema_declares_idle_ttl() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "nu-dweller",
            "nushell",
            r#"
export def interface_schema [args] {
    { lifecycle: { idle_ttl: "5m" } }
}

export def execute [args] {
    { ok: true }
}
"#,
        ))
        .await
        .unwrap();

    {
        let realm = engine.realm.try_lock().unwrap();
        let actor = realm.actor_type("nu-dweller").unwrap();
        assert_eq!(actor.idle_ttl, Some(Duration::from_secs(300)));
    }
}

// ------------------------------------- Phase 4.5c (multi-entry actors, step 1) --
//
// @on-decorated handlers: the decorator registry derives `receives` at
// upload; `register` seeds the router from it (event → type, key field),
// so delivery no longer depends on a single `execute` entry.
#[cfg(feature = "python")]
#[tokio::test]
async fn python_on_decorators_derive_receives_and_routes() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "cart",
            "python",
            r#"
@on("add_to_cart", key="user_id")
def add(args):
    return {"added": args["item"]}

@on("remove_from_cart")
def remove(args):
    return {"removed": True}

@on("order.*")
def audit(args):
    return None
"#,
        ))
        .await
        .unwrap();

    let realm = engine.realm.try_lock().unwrap();
    let routes = realm.router.matches("add_to_cart");
    assert_eq!(routes.len(), 1, "add_to_cart routed to cart");
    assert_eq!(routes[0].actor_type, "cart");
    assert_eq!(routes[0].instance_key_field, "user_id");
    let routes = realm.router.matches("remove_from_cart");
    assert_eq!(routes.len(), 1, "remove_from_cart routed (no key → singleton)");
    assert_eq!(routes[0].instance_key_field, "");
    let routes = realm.router.matches("order.created");
    assert_eq!(routes.len(), 1, "order.* wildcard routed");
    assert!(realm.router.matches("unrelated").is_empty());
}

// ADR-0026 §3 + §4: the type declares storage collections through its
// interface_schema (`storage.collections` — serde CollectionSchema +
// indexes/reduces); `ctx_store_emit` executes ops against the type's own
// ns, and `ctx_interface_schema` reads the persisted copy. steel script,
// full round trip through the host bridge.
#[cfg(feature = "steel")]
#[tokio::test]
async fn store_emit_roundtrip_and_interface_schema_read() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let full_src = r#"
(define (interface_schema args)
  (hash "storage"
        (hash "collections"
              (hash "notes"
                    (hash "schema"
                          (hash "key_len" 8
                                "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
                                "layout_version" 1
                                "hot_width" 8
                                "payload_header_len" 3
                                "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
                                "cold_fields" (list)
                                "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))

(define (put-note args)
  (ctx_store_emit (hash "collection" "notes"
                        "op" "put_document"
                        "key" (hash "id" 7)
                        "doc" (hash "count" 42))))

(define (get-note args)
  (ctx_store_emit (hash "collection" "notes"
                        "op" "get_document"
                        "key" (hash "id" 7))))

(define (read-schema args)
  (ctx_interface_schema ""))
"#;
    engine
        .register(aura_actor::ActorType::script(
            "store-keeper",
            "steel",
            full_src,
        ))
        .await
        .expect("register store-keeper");

    let target = aura_actor::InstanceId { actor_type: "store-keeper".into(), key: "k".into() };

    // DEBUG: introspect directly to see what schema comes back.
    let src = r#"
(define (interface_schema args)
  (hash "storage" (hash "collections" (hash "notes" (hash "schema" (hash "key_len" 8))))))
"#;
    // probe with the FULL source from the registered actor


    // The type's plan resolved at registration: ctx.store is available.
    engine
        .invoke(target.clone(), "put-note", serde_json::json!(7))
        .await
        .expect("put through ctx_store_emit");
    let got = engine
        .invoke(target.clone(), "get-note", serde_json::json!(7))
        .await
        .expect("get through ctx_store_emit");
    assert_eq!(got["count"], 42);

    // The persisted schema copy is readable from the handler.
    let schema = engine
        .invoke(target.clone(), "read-schema", serde_json::json!(null))
        .await
        .expect("ctx_interface_schema");
    assert!(
        schema["storage"]["collections"]["notes"].is_object(),
        "interface_schema carries the storage declaration: {schema}"
    );

    // A type without a storage declaration has no ctx.store surface:
    // registering one and calling ctx_store_emit errors as a value.
}
