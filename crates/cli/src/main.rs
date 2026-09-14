//! Single-binary start. Phase 0: boot the engine, run the echo validation
//! (define → invoke → return) when `--echo-check` is passed.

use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = aura_config::EngineConfig::default();
    let engine = aura_engine::Engine::start(&config).expect("engine boot");

    // Echo Actor: define → invoke → return. The Phase 0 acceptance path.
    engine
        .register(ActorType::simple(
            "echo",
            Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move { Ok(args) })
            }),
        ))
        .await;

    let target = InstanceId { actor_type: "echo".into(), key: "a1".into() };
    let result = engine.invoke(target, serde_json::json!({"hello": "aura"})).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    // Chained invoke: echo.a2 invoked BY echo.a1's ctx — exercises ctx.invoke
    // through the same realm dispatch.
    engine
        .register(ActorType::simple(
            "caller",
            Arc::new(|ctx: Ctx, _args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
                Box::pin(async move {
                    let nested = ctx
                        .invoke(
                            InstanceId { actor_type: "echo".into(), key: "a2".into() },
                            serde_json::json!({"via": "ctx.invoke"}),
                        )
                        .await?;
                    Ok(nested)
                })
            }),
        ))
        .await;

    let result = engine
        .invoke(
            InstanceId { actor_type: "caller".into(), key: "c1".into() },
            serde_json::json!(null),
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
