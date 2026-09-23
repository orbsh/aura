//! Timer wheel acceptance (ADR-0016 revised 2026-09-23): reclaim entries —
//! idle measured from job COMPLETION (a long job cannot expire
//! mid-execution; the idle entry only exists while the instance is idle),
//! and the execution watchdog (max_exec budget: expiry = evict). Rust
//! bodies keep the tests carrier-independent (exact sleep control).

use aura_actor::{ActorType, Body, InstanceId};
use aura_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

fn actor(name: &str, idle_ttl: Duration, max_exec: Option<Duration>) -> ActorType {
    ActorType {
        name: name.into(),
        body: Body::Rust(Arc::new(|_ctx, args| {
            Box::pin(async move { Ok(args) })
        })),
        idle_ttl: Some(idle_ttl),
        max_exec,
        on_sleep: None,
        on_wake: None,
        receives: Vec::new(),
    }
}

fn sleeper(name: &str, idle_ttl: Duration, max_exec: Option<Duration>, secs: u64) -> ActorType {
    ActorType {
        name: name.into(),
        body: Body::Rust(Arc::new(move |_ctx, args| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(secs)).await;
                Ok(args)
            })
        })),
        idle_ttl: Some(idle_ttl),
        max_exec,
        on_sleep: None,
        on_wake: None,
        receives: Vec::new(),
    }
}

fn id(name: &str) -> InstanceId {
    InstanceId { actor_type: name.into(), key: "k".into() }
}

// Idle entry arms at job COMPLETION: after a job whose execution alone
// exceeds the TTL, the instance is still resident; it is evicted only
// after the TTL elapses from completion.
#[tokio::test]
async fn idle_is_measured_from_completion() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    // TTL (500ms) shorter than this job's own execution (2s): the
    // pre-revision evictor would have expired the instance mid-job.
    engine.register(sleeper("slow", Duration::from_millis(500), None, 2)).await;

    let target = id("slow");
    engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap();

    // Immediately after completion: the idle entry (500ms) is pending —
    // the instance must still be resident.
    assert!(engine.realm.lock().await.is_resident(&target));

    // After TTL from completion: evicted by the timer driver.
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(!engine.realm.lock().await.is_resident(&target));
}

// Watchdog: a job exceeding max_exec gets its instance evicted.
#[tokio::test]
async fn watchdog_evicts_on_budget_exceeded() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(sleeper("hog", Duration::from_secs(60), Some(Duration::from_millis(300)), 2)).await;

    let target = id("hog");
    let _ = engine.invoke(target.clone(), "execute", serde_json::json!(null)).await;

    // Budget (300ms) far below execution (2s): the watchdog fired.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!engine.realm.lock().await.is_resident(&target));
}

// Cancel-after-register is deterministic: a short job completes, the
// completion path rearms idle from completion, and the instance survives
// at least until the new TTL elapses.
#[tokio::test]
async fn completion_rearm_survives_first_ttl_half() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine.register(actor("echo", Duration::from_millis(400), None)).await;

    let target = id("echo");
    engine.invoke(target.clone(), "execute", serde_json::json!(null)).await.unwrap();

    // Half the TTL after completion: must still be resident (the entry
    // was armed AT completion, not at job start).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(engine.realm.lock().await.is_resident(&target));

    // Past the TTL: evicted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!engine.realm.lock().await.is_resident(&target));
}
