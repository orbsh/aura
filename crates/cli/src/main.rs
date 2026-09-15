//! Single-binary start. Phase 0: boot the engine, run the echo validation
//! (define → invoke → return) when `--echo-check` is passed.
//!
//! 4.5a: the demo is a steel script actor — script source is the only
//! public actor form; the Rust-closure form is gone.

use aura_actor::{ActorType, InstanceId};

const ECHO: &str = r#"
(define (execute args)
  args)
"#;

const CALLER: &str = r#"
(define (execute args)
  (ctx_invoke "{\"type\": \"echo\", \"key\": \"a2\", \"handler\": \"execute\", \"args\": {\"via\": \"ctx.invoke\"}}"))
"#;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = aura_config::EngineConfig::default();
    let engine = aura_engine::Engine::start(&config).await.expect("engine boot");

    // Echo Actor: define → invoke → return. The Phase 0 acceptance path,
    // now as a script actor.
    engine
        .register(ActorType::script("echo", "steel", ECHO, Some("execute".into())))
        .await?;

    let target = InstanceId { actor_type: "echo".into(), key: "a1".into() };
    let result = engine.invoke(target, "execute", serde_json::json!({"hello": "aura"})).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    // Chained invoke: echo.a2 invoked BY caller.c1's ctx — exercises ctx.invoke
    // through the same realm dispatch.
    engine
        .register(ActorType::script("caller", "steel", CALLER, Some("execute".into())))
        .await?;

    let result = engine
        .invoke(
            InstanceId { actor_type: "caller".into(), key: "c1".into() },
            "execute",
            serde_json::json!(null),
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
