//! Phase 4 acceptance: engine=fjall — state is durable across engine
//! restart (the Phase 1 test proved eviction survival; this proves
//! process-restart survival, which is what a real storage engine adds).
//!
//! Requires the `fjall` feature.

#[cfg(feature = "fjall")]
mod fjall_tests {
    use aura_booth::{BoothType, Ctx, InstanceId, futures_boxed::BoxFuture};
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

    // Counter into the type's declared collection (ADR-0026 §3). Reads go
    // through the `count` handler (the booth's observable output).
    const COUNTER: &str = r#"
(define (schema) (hash "storage" (hash "collections" (hash "counters" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 4096 "declared_reduce_base" 8192 "junction_base" 12288)))))))
(define (interface_schema args) (schema))
(define (count args)
  (let* ((cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" 1)))))
    (hash "count" (if (void? cur) 0 (hash-ref cur "count" 0)))))
(define (execute args)
  (let* ((cur (ctx_store_emit (hash "collection" "counters" "op" "get_document" "key" (hash "id" 1))))
         (c (if (void? cur) 0 (hash-ref cur "count"))))
    (ctx_store_emit (hash "collection" "counters" "op" "put_document"
                          "key" (hash "id" 1) "doc" (hash "count" (+ c 1))))
    (hash "count" (+ c 1))))
"#;
    fn counter() -> BoothType {
        BoothType::script("counter", "steel", COUNTER)
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
            let target = InstanceId { booth_type: "counter".into(), key: "k".into() };
            engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap();
            engine.invoke(target, "execute", serde_json::json!(null)).await.unwrap();
        } // Engine dropped — process-restart semantics.

        // Engine #2: fresh engine over the same data dir; state restored
        // lazily from the store on first touch → count continues at 3.
        let engine = Engine::start(&fjall_config(dir.path())).await.unwrap();
        engine.register(counter()).await;
        let out = engine
            .invoke(
                InstanceId { booth_type: "counter".into(), key: "k".into() },
                "execute",
                serde_json::json!(null),
            )
            .await
            .unwrap();
        assert_eq!(out, serde_json::json!({"count": 3}));
    }
}
