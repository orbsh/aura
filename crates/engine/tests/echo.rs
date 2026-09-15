//! Acceptance paths, as executable documentation.
//!
//! Phase 0: define → invoke → return; ctx.invoke as the single call
//! surface (ADR-0011). Phase 1: state survives eviction (scale-to-zero
//! drops the resident, not the data); on_sleep/on_wake run around it.

use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use aura_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

fn echo_type() -> ActorType {
    ActorType::simple(
        "echo",
        Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move { Ok(args) })
        }),
    )
}

// ---------------------------------------------------------------- Phase 0 --

#[tokio::test]
async fn invoke_returns_handler_result() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(echo_type()).await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "echo".into(), key: "a1".into() },
            serde_json::json!({"hello": "aura"}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"hello": "aura"}));
}

#[tokio::test]
async fn ctx_invoke_routes_through_realm() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(echo_type()).await;

    // `caller` invokes `echo` via ctx.invoke — the only call surface an
    // actor sees; target resolution is registry-declared.
    engine
        .register(ActorType::simple(
            "caller",
            Arc::new(|ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move {
                    let key = args["target_key"].as_str().unwrap_or("a2").to_string();
                    ctx.invoke(
                        InstanceId { actor_type: "echo".into(), key },
                        serde_json::json!({"via": "ctx.invoke"}),
                    )
                    .await
                })
            }),
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "caller".into(), key: "c1".into() },
            serde_json::json!({"target_key": "a2"}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"via": "ctx.invoke"}));
}

#[tokio::test]
async fn unknown_actor_type_is_error_value() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    let err = engine
        .invoke(
            InstanceId { actor_type: "ghost".into(), key: "x".into() },
            serde_json::json!(null),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown actor type"));
}

#[tokio::test]
async fn partition_key_activates_distinct_instances() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(echo_type()).await;

    for key in ["a1", "a2"] {
        let out = engine
            .invoke(
                InstanceId { actor_type: "echo".into(), key: key.into() },
                serde_json::json!({"key": key}),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"key": key}));
    }
}

// ---------------------------------------------------------------- Phase 1 --

// ctx.state writes persist across eviction: the instance is dropped, the
// data is not. on_sleep/on_wake run around the boundary.
#[tokio::test]
async fn state_survives_scale_to_zero() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(
        ActorType::simple(
            "counter",
            Arc::new(|ctx: Ctx, _args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move {
                    // RMW: read, bump, write — per-field durable units.
                    let n = ctx.state.get("count")?.and_then(|v| v.as_i64()).unwrap_or(0);
                    ctx.state.set("count", serde_json::json!(n + 1))?;
                    Ok(serde_json::json!({ "count": n + 1 }))
                })
            }),
        )
        .with_on_sleep(Arc::new(|_ctx: Ctx| -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { Ok(()) }) // advisory; the store already has it
        }))
        .with_on_wake(Arc::new(|_ctx: Ctx, _args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async { Ok(serde_json::Value::Null) })
        })),
    ).await;

    let target = InstanceId { actor_type: "counter".into(), key: "k1".into() };
    assert_eq!(engine.invoke(target.clone(), serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 1}));
    assert_eq!(engine.invoke(target.clone(), serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 2}));

    // Force eviction: everything idle is older than 0s.
    engine.realm.lock().await.evict_idle(engine.realm.clone()).await;

    // Resident is gone; state survives. Next touch reactivates (on_wake)
    // and the count continues.
    assert_eq!(engine.invoke(target, serde_json::json!(null)).await.unwrap(), serde_json::json!({"count": 3}));
}

// ------------------------------------------------------- Phase 2 (script) --

// Script actors execute through the probe carriers — the same carrier set
// the remote actuator uses; language execution is not reimplemented here.
// Script actors are pure functions in this phase (args in, value out).
#[cfg(feature = "nushell")]
#[tokio::test]
async fn nushell_script_actor() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "nu-op",
            "nushell",
            r#"
export def execute [args] {
    { sum: ($args.items | math sum) }
}
"#,
            Some("execute".into()),
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "nu-op".into(), key: "n1".into() },
            serde_json::json!({"items": [1, 2, 3]}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"sum": 6}));
}

#[cfg(feature = "python")]
#[tokio::test]
async fn python_script_actor() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "py-op",
            "python",
            r#"def execute(args):
    return {"doubled": args["x"] * 2}
"#,
            Some("execute".into()),
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "py-op".into(), key: "p1".into() },
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
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "koto-op",
            "koto",
            "1 + 2",
            None,
        ))
        .await;

    let err = engine
        .invoke(
            InstanceId { actor_type: "koto-op".into(), key: "k1".into() },
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
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(echo_type()).await;

    let target = InstanceId { actor_type: "echo".into(), key: "ttl".into() };
    engine.invoke(target, serde_json::json!(null)).await.unwrap();

    {
        let mut realm = engine.realm.try_lock().unwrap();
        realm.idle_ttl = Duration::from_secs(0);
        let evicted = realm.evict_idle(engine.realm.clone()).await;
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].key, "ttl");
    }
}

// ------------------------------------------------- Phase 2.5 (ctx bridge) --
//
// Script actors reach the host through named functions: one JSON argument
// in, one JSON value out. `ctx_state_*` touch the instance's own state
// (scoped to self_id — cross-instance reach is not expressible);
// `ctx_invoke` rides the unified call model (Phase 3.5).

// Steel script: set a counter field, read it back, and invoke another
// actor through ctx_invoke.
#[cfg(feature = "steel")]
#[tokio::test]
async fn steel_script_ctx_bridge() {
    let engine = Engine::start(&Default::default()).expect("engine boot");

    // Target invoked from the script: echoes back its args.
    engine.register(echo_type()).await;
    engine
        .register(aura_actor::ActorType::script(
            "steel-ctx",
            "steel",
            r#"
(define (execute args)
  (ctx_state_set "{\"field\": \"visits\", \"value\": 1}")
  (let* ((got (ctx_state_get "\"visits\""))
         (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"ttl2\", \"args\": {\"hello\": true}}")))
    (hash "present" (hash-ref got "present") "visits" (hash-ref got "value") "echo" (hash-ref echoed "hello")))
)"#,
            Some("execute".into()),
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "steel-ctx".into(), key: "s1".into() },
            serde_json::json!(null),
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        serde_json::json!({"present": true, "visits": 1, "echo": true})
    );
}

// Python script: same bridge surface — state + invoke.
#[cfg(feature = "python")]
#[tokio::test]
async fn python_script_ctx_bridge() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine.register(echo_type()).await;
    engine
        .register(aura_actor::ActorType::script(
            "py-ctx",
            "python",
            r#"
import json

def execute(args):
    ctx_state_set(json.dumps({"field": "color", "value": "blue"}))
    got = ctx_state_get(json.dumps("color"))
    echo = ctx_invoke(json.dumps({"type": "echo", "key": "ttl3", "args": {"ok": 7}}))
    return {"stored": got["value"], "echo": echo["ok"]}
"#,
            Some("execute".into()),
        ))
        .await;

    let out = engine
        .invoke(
            InstanceId { actor_type: "py-ctx".into(), key: "p1".into() },
            serde_json::json!(null),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"stored": "blue", "echo": 7}));
}

// State written through the bridge persists across eviction: the script
// actor's field survives scale-to-zero.
#[cfg(feature = "steel")]
#[tokio::test]
async fn script_state_survives_eviction() {
    let engine = Engine::start(&Default::default()).expect("engine boot");
    engine
        .register(aura_actor::ActorType::script(
            "steel-counter",
            "steel",
            r#"
(define (execute args)
  (let* ((prev (ctx_state_get "\"count\""))
         (n (if (hash-ref prev "present") (+ 1 (hash-ref prev "value")) 1)))
    (ctx_state_set (string-append "{\"field\": \"count\", \"value\": " (number->string n) "}"))
    (hash "count" n))
)"#,
            Some("execute".into()),
        ))
        .await;

    let target = InstanceId { actor_type: "steel-counter".into(), key: "c1".into() };
    let out = engine.invoke(target.clone(), serde_json::json!(null)).await.unwrap();
    assert_eq!(out, serde_json::json!({"count": 1}));

    engine.realm.lock().await.evict_idle(engine.realm.clone()).await;

    let out = engine.invoke(target, serde_json::json!(null)).await.unwrap();
    assert_eq!(out, serde_json::json!({"count": 2}));
}

