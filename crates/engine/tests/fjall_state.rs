//! Phase 4 acceptance: engine=fjall — state is durable across engine
//! restart (the Phase 1 test proved eviction survival; this proves
//! process-restart survival, which is what a real storage engine adds).
//!
//! Requires the `fjall` feature.

#[cfg(feature = "fjall")]
mod fjall_tests {
    use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
    use aura_config::{Engine as EngineKind, EngineConfig};
    use aura_engine::Engine;
    use std::sync::Arc;

    fn fjall_config(data_dir: &std::path::Path) -> EngineConfig {
        EngineConfig {
            engine: EngineKind::Fjall,
            data_dir: Some(data_dir.to_path_buf()),
            ..Default::default()
        }
    }

    const COUNTER: &str = r#"
(define (execute args)
  (let* ((got (ctx_state_get "count"))
         (n (if (hash-ref got "present") (hash-ref got "value") 0)))
    (ctx_state_set (hash "field" "count" "value" (+ n 1)))
    (hash "count" (+ n 1))))
"#;
    fn counter() -> ActorType {
        ActorType::script("counter", "steel", COUNTER, Some("execute".into()))
    }

    #[test]
    fn boot_error_without_fjall_feature() {
        // engine=fjall without the feature compiled in = boot error, never
        // a silent fallback (PLAN Phase 4 matrix rule). This test runs in
        // the default (feature-less) build only.
        #[cfg(not(feature = "fjall"))]
        {
            let config = EngineConfig {
                engine: EngineKind::Fjall,
                data_dir: None,
                ..Default::default()
            };
            let err = Engine::start(&config).await.unwrap_err();
            assert!(err.to_string().contains("fjall feature"));
        }
    }

    #[cfg(feature = "fjall")]
    #[tokio::test]
    async fn state_survives_engine_restart() {
        let dir = tempfile::tempdir().unwrap();

        // Engine #1: two calls → count = 2, persisted in fjall (WAL).
        {
            let engine = Engine::start(&fjall_config(dir.path())).await.unwrap();
            engine.register(counter()).await;
            let target = InstanceId { actor_type: "counter".into(), key: "k".into() };
            engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap();
            engine.invoke(target, "execute", serde_json::json!(null)).await.unwrap();
        } // Engine dropped — process-restart semantics.

        // Engine #2: fresh engine over the same data dir; state restored
        // lazily from the store on first touch → count continues at 3.
        let engine = Engine::start(&fjall_config(dir.path())).await.unwrap();
        engine.register(counter()).await;
        let out = engine
            .invoke(
                InstanceId { actor_type: "counter".into(), key: "k".into() },
                "execute",
                serde_json::json!(null),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"count": 3}));
    }
}
