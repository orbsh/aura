//! Phase 0 acceptance, as executable documentation: define → invoke →
//! return. The call path every surface converges on is `engine.invoke`;
//! actor-to-actor calls go through `ctx.invoke` — the single controlled
//! call surface (ADR-0011).

use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use aura_engine::Engine;
use std::sync::Arc;

fn echo_type() -> ActorType {
    ActorType {
        name: "echo".into(),
        handler: Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move { Ok(args) })
        }),
    }
}

#[tokio::test]
async fn invoke_returns_handler_result() {
    let engine = Engine::start(&Default::default());
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
    let engine = Engine::start(&Default::default());
    engine.register(echo_type()).await;

    // `caller` invokes `echo` via ctx.invoke — the only call surface an
    // actor sees; target resolution is registry-declared.
    engine
        .register(ActorType {
            name: "caller".into(),
            handler: Arc::new(|ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move {
                    let key = args["target_key"].as_str().unwrap_or("a2").to_string();
                    ctx.invoke(
                        InstanceId { actor_type: "echo".into(), key },
                        serde_json::json!({"via": "ctx.invoke"}),
                    )
                    .await
                })
            }),
        })
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
    let engine = Engine::start(&Default::default());
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
    let engine = Engine::start(&Default::default());
    engine.register(echo_type()).await;

    // Same type, two keys: virtual-actor activation resolves each key to
    // its own instance mailbox.
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
