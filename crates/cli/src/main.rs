//! Single-binary start. Phase 0: boot the engine, run the echo validation
//! (define → invoke → return) when `--echo-check` is passed.
//!
//! 4.5a: the demo is a steel script booth — script source is the only
//! public booth form; the Rust-closure form is gone.

use aura_booth::{BoothType, InstanceId, InstanceKey};

const ECHO: &str = r#"
(define (execute args)
  args)
"#;

const CALLER: &str = r#"
(define (execute args)
  (ctx.invoke "{\"type\": \"echo\", \"key\": \"a2\", \"handler\": \"execute\", \"args\": {\"via\": \"ctx.invoke\"}}"))
"#;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Aura's background loops (the realm's consumer poll) have no caller to
    // return a failure to, so the log is where their warnings land. `warn` by
    // default; `AURA_LOG=debug` (or any EnvFilter directive) widens it.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let config = aura_config::EngineConfig::default();
    // Boot failure is a Result, not a panic: the CLI reports it and exits
    // non-zero like every other step of this program.
    let engine = aura_engine::Engine::start(&config)
        .await
        .map_err(|e| anyhow::anyhow!("engine boot: {e}"))?;

    // Echo Booth: define → invoke → return. The Phase 0 acceptance path,
    // now as a script booth.
    engine
        .register(BoothType::script("echo", "steel", ECHO))
        .await?;

    let target = InstanceId {
        booth_type: "echo".into(),
        key: InstanceKey::Named("a1".into()),
    };
    let result = engine
        .invoke(target, "execute", serde_json::json!({"hello": "aura"}))
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    // Chained invoke: echo.a2 invoked BY caller.c1's ctx — exercises ctx.invoke
    // through the same realm dispatch.
    engine
        .register(BoothType::script("caller", "steel", CALLER))
        .await?;

    let result = engine
        .invoke(
            InstanceId {
                booth_type: "caller".into(),
                key: InstanceKey::Named("c1".into()),
            },
            "execute",
            serde_json::json!(null),
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
