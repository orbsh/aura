//! Single-binary start. Phase 0: boot the engine, run the echo validation
//! (define → invoke → return) when `--echo-check` is passed.
//!
//! 4.5a: the demo is a steel script booth — script source is the only
//! public booth form; the Rust-closure form is gone.

use aura_booth::{BoothType, InstanceId};

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

    // Echo Booth: define → invoke → return. The Phase 0 acceptance path,
    // now as a script booth.
    engine
        .register(BoothType::script("echo", "steel", ECHO))
        .await?;

    let target = InstanceId { booth_type: "echo".into(), key: "a1".into() };
    let result = engine.invoke(target, "execute", serde_json::json!({"hello": "aura"})).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    // Chained invoke: echo.a2 invoked BY caller.c1's ctx — exercises ctx.invoke
    // through the same realm dispatch.
    engine
        .register(BoothType::script("caller", "steel", CALLER))
        .await?;

    let result = engine
        .invoke(
            InstanceId { booth_type: "caller".into(), key: "c1".into() },
            "execute",
            serde_json::json!(null),
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
