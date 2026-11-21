//! ADR-0016 §3, the DECLARATIVE half: a script declares `lifecycle.cron` in its
//! `interface_schema`, the host translates it into durable one-shot timers AT
//! REGISTRATION, and the wheel delivers `__on_timer` with the declared tag —
//! nothing about the schedule is read again at runtime.
//!
//! Three facts are pinned here, and each one is a rule rather than an
//! observation:
//!
//! - a declared schedule fires without any call ever arming it (the declaration
//!   IS the arming — that is what "no turn asked for it" means for a booth with
//!   no turn);
//! - registering the same type twice does not double the schedule (a
//!   re-declaration replaces its chain; a doubling wake would be invisible
//!   until it burned a node);
//! - a type that routes per KEY cannot use the form at all: a type-level
//!   schedule has no instance to wake, and the error says where the wake really
//!   belongs.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::BoothType;
use aura_engine::Engine;

/// The counter every variant of this booth writes: one row, `count` advanced by
/// one per delivery, `tag` the last tag seen. `id` is always 7 so the whole test
/// is a read-back, not a scan.
fn storage() -> &'static str {
    r#"(hash "collections" (hash "ticks" (hash "schema"
  (hash "key_len" 8
        "key_fields" (list (hash "name" "id" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "layout_version" 1 "hot_width" 8 "payload_header_len" 3
        "hot_fields" (list (hash "name" "count" "ty" "U64" "width" 8 "offset" 0 "tag" 0))
        "cold_fields" (list)
        "slots" (hash "primary" 0 "dynamic" 1 "dict_id" 2 "dict_name" 3 "declared_index_base" 16385 "declared_reduce_base" 20481 "junction_base" 12288)))))"#
}

/// The declaring booth: `lifecycle.cron` names ONE tag, `beat`, every second.
/// `__on_timer` counts the deliveries (read-modify-write, so two chains in one
/// period would show as 2 and one chain as 1); `check` reports the count.
///
/// `extra` is the REST of the declaration — a test that wants a keyed route
/// passes its `receives` here, because the point of that test is what the
/// declaration says, not what the handler does.
fn declaring(extra: &str) -> String {
    format!(
        r#"(define (schema) {storage})
(define (interface_schema args)
  (hash "lifecycle" (hash "cron" (list (hash "tag" "beat" "every" "1s")))
        "storage" {storage} {extra}))
(define (__on_timer args)
  (let ((cur (ctx.store (hash "collection" "ticks" "op" "get_document" "key" (hash "id" 7)))))
    (ctx.store (hash "collection" "ticks" "op" "put_document"
                     "key" (hash "id" 7)
                     "doc" (hash "count" (if (void? cur) 1 (+ 1 (hash-ref cur "count")))
                                 "tag" (hash-ref args "tag")))))
  (hash "woke" 1))
(define (check args)
  (hash "fired" (ctx.store (hash "collection" "ticks" "op" "get_document" "key" (hash "id" 7)))))"#,
        storage = storage(),
        extra = extra
    )
}

fn singleton() -> aura_booth::InstanceId {
    aura_booth::InstanceId {
        booth_type: "declaring".into(),
        key: aura_booth::InstanceKey::Singleton,
    }
}

/// `check` answers a stored row (not a scalar): pull the count out of it.
async fn fired(engine: &Engine) -> u64 {
    let out = engine
        .invoke(singleton(), "check", serde_json::json!({}))
        .await
        .unwrap();
    out.get("fired")
        .and_then(|row| row.get("count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn a_declared_cron_fires_without_anyone_arming_it() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("declaring", "steel", declaring("")))
        .await
        .unwrap();

    // NOTHING invokes `arm`: the declaration was the arming.
    tokio::time::sleep(std::time::Duration::from_millis(1_400)).await;
    assert_eq!(
        fired(&engine).await,
        1,
        "the declared schedule delivered exactly one wake"
    );
}

#[tokio::test]
async fn registering_twice_does_not_double_the_schedule() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("declaring", "steel", declaring("")))
        .await
        .unwrap();
    // The same declaration again, before the first fire: a chain that was
    // appended instead of replaced would be two rows for one schedule, and the
    // booth would wake twice for one period.
    engine
        .register(BoothType::script("declaring", "steel", declaring("")))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1_400)).await;
    assert_eq!(
        fired(&engine).await,
        1,
        "a re-registration replaces its chain, it does not add one"
    );
}

#[tokio::test]
async fn a_keyed_type_cannot_declare_a_schedule() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    // `check` routes per key: there is no singleton instance for a type-level
    // schedule to wake, so registration refuses the declaration by name.
    let err = engine
        .register(BoothType::script(
            "declaring",
            "steel",
            declaring(r#""receives" (hash "check" (hash "key" "id"))"#),
        ))
        .await
        .expect_err("a keyed type cannot carry a declared schedule");
    let text = format!("{err:#}");
    assert!(
        text.contains("no instance to wake"),
        "the error says why, not just that: {text}"
    );
    assert!(
        text.contains("ctx.timer.register"),
        "the error names the door the wake belongs in: {text}"
    );
}