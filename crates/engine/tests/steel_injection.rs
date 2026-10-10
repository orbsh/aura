//! ADR-0037 4.16b steel injection face e2e: the type's declared
//! `interface_schema` storage collections come back bound IN THE VM
//! (the host-injected face — built over the realm's live engine at
//! session load, addressed by NAME through the six `collection-*!`
//! fns), and writes through the BINDING face must be readable through
//! `ctx.store` and vice versa — same bytes, one engine (the
//! acceptance lock, the shape py_injection.rs established). The steel
//! mirror also locks the INTROSPECTION side: the handler bodies
//! reference the collection fns at define-compile time — the stub arms
//! must let the script load for schema introspection without shadowing
//! the real fns in the resident session.
#![cfg(feature = "steel")]

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;

const SCRIPT: &str = r#"
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

;; The raw primary-key bytes the Collection face takes: U64 id,
;; big-endian (schema key_len = 8) — the same hand-encoding the python
;; fixture's `_k` does.
(define (bind_write_emit_read args)
  (collection-put! "notes"
    (list->vector (list 0 0 0 0 0 0 0 1))
    (hash "id" 1 "count" 41))
  (ctx.store (hash "collection" "notes" "op" "get_document" "key" (hash "id" 1))))

(define (emit_write_bind_read args)
  (ctx.store (hash "collection" "notes" "op" "put_document"
                        "key" (hash "id" 2) "doc" (hash "count" 42)))
  (collection-get! "notes" (list->vector (list 0 0 0 0 0 0 0 2))))

;; Post-eviction effector: reads id=1 THROUGH the rebuilt binding — the
;; durable rows survive the ephemeral per-VM registry.
(define (bind_get args)
  (collection-get! "notes" (list->vector (list 0 0 0 0 0 0 0 1))))
"#;

#[tokio::test]
async fn steel_binding_face_and_ctx_store_share_one_keyspace() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("steel-inject", "steel", SCRIPT))
        .await
        .unwrap();

    let target = |key: &str| InstanceId { booth_type: "steel-inject".into(), key: key.into() };

    // Binding write → ctx.store read (same session instance).
    let out = engine
        .invoke(target("k"), "bind_write_emit_read", serde_json::json!({"user_id": "a"}))
        .await
        .expect("bind_write_emit_read");
    assert_eq!(out["count"], 41);

    // ctx.store write → binding read.
    let out = engine
        .invoke(target("k"), "emit_write_bind_read", serde_json::json!({"user_id": "a"}))
        .await
        .expect("emit_write_bind_read");
    assert_eq!(out["count"], 42);

    // Evict + re-activate: the per-VM registry is rebuilt over the same
    // realm engine — the durable rows survive the ephemeral bindings.
    aura_realm::Realm::evict_instance(engine.realm.clone(), &target("k")).await;
    let out = engine
        .invoke(target("k"), "bind_get", serde_json::json!({"user_id": "a"}))
        .await
        .expect("bind_get after eviction — the injected binding came back");
    assert_eq!(out["count"], 41);
}
