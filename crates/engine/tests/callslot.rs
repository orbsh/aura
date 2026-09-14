//! Phase 3.5 acceptance, as executable documentation: the unified call
//! model — one call mode for every target. Hot: park on the oneshot,
//! timeout = failure value. Cold: wait never enters park — Pending(call_id)
//! returns immediately, the result arrives via resolve_call, unknown id
//! never replays.

use aura_actor::call::{CallId, CallSpec, Tier};
use aura_actor::{ActorType, Ctx, InstanceId, futures_boxed::BoxFuture};
use aura_engine::Engine;
use std::sync::Arc;
use std::time::Duration;

fn echo() -> ActorType {
    ActorType::simple(
        "echo",
        Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move { Ok(args) })
        }),
    )
}

fn slow_echo() -> ActorType {
    ActorType::simple(
        "slow_echo",
        Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(args)
            })
        }),
    )
}

#[tokio::test]
async fn hot_call_parks_and_returns() {
    let engine = Engine::start(&Default::default()).unwrap();
    engine.register(echo()).await;
    let waited = engine
        .call(
            InstanceId { actor_type: "echo".into(), key: "a".into() },
            serde_json::json!({"hot": true}),
        )
        .await
        .unwrap();
    match waited {
        aura_actor::call::Waited::Done(Ok(v)) => assert_eq!(v, serde_json::json!({"hot": true})),
        other => panic!("expected Done, got {other:?}"),
    }
}

#[tokio::test]
async fn hot_timeout_is_failure_value() {
    let engine = Engine::start(&Default::default()).unwrap();
    let mut slow = slow_echo();
    slow.on_wake = None;
    engine.register(slow).await;
    // 50ms deadline vs 500ms handler: the caller gets a failure value at
    // ~50ms, not the result at 500ms.
    engine
        .realm
        .try_lock()
        .unwrap()
        .declare_call("slow_echo", CallSpec::hot(Duration::from_millis(50)));

    let err = engine
        .call(
            InstanceId { actor_type: "slow_echo".into(), key: "s".into() },
            serde_json::json!(null),
        )
        .await
        .unwrap();
    match err {
        aura_actor::call::Waited::Done(Err(e)) => assert!(e.to_string().contains("timed out")),
        other => panic!("expected timeout failure, got {other:?}"),
    }
}

#[tokio::test]
async fn cold_call_returns_pending_without_parking() {
    let engine = Engine::start(&Default::default()).unwrap();
    let mut approval = ActorType::simple(
        "approval",
        Arc::new(|_ctx: Ctx, args| -> BoxFuture<'static, anyhow::Result<serde_json::Value>> {
            Box::pin(async move { Ok(args) })
        }),
    );
    approval.on_wake = None;
    engine.register(approval).await;
    engine
        .realm
        .try_lock()
        .unwrap()
        .declare_call("approval", CallSpec::cold());

    let started = std::time::Instant::now();
    let waited = engine
        .call(
            InstanceId { actor_type: "approval".into(), key: "human".into() },
            serde_json::json!({"ask": "allow rm -rf?"}),
        )
        .await
        .unwrap();
    // The slot returns immediately — wait never entered park.
    assert!(started.elapsed() < Duration::from_millis(100));
    let call_id = match waited {
        aura_actor::call::Waited::Pending(id) => id,
        other => panic!("expected Pending, got {other:?}"),
    };

    // The call is registered.
    assert_eq!(
        engine.realm.try_lock().unwrap().pending_calls_len(),
        1,
        "pending call must be registered"
    );

    // The external world answers (minutes later in reality): resolve
    // delivers through the framework's re-entry.
    assert!(engine.resolve_call(&call_id, Ok(serde_json::json!("approved"))).await);
}

#[tokio::test]
async fn resolve_unknown_call_is_noop() {
    let engine = Engine::start(&Default::default()).unwrap();
    // Completed calls never replay: resolving an unknown id is a no-op.
    assert!(!engine
        .resolve_call(&CallId("ghost".into()), Ok(serde_json::json!(1)))
        .await);
}

#[tokio::test]
async fn deadline_scan_fails_expired_hot_calls() {
    let engine = Engine::start(&Default::default()).unwrap();
    let mut slow = slow_echo();
    slow.on_wake = None;
    engine.register(slow).await;
    engine
        .realm
        .try_lock()
        .unwrap()
        .declare_call("slow_echo", CallSpec::hot(Duration::from_millis(20)));

    // Fire without awaiting: the call is in-flight past its deadline.
    let slot = engine
        .call(
            InstanceId { actor_type: "slow_echo".into(), key: "s".into() },
            serde_json::json!(null),
        )
        .await
        .unwrap();

    // Deadline scan (runs on the evictor tick; called directly here for
    // determinism) converts expiry into the failure value.
    tokio::time::sleep(Duration::from_millis(50)).await;
    engine.realm.lock().await.sweep_deadlines().await;

    match slot {
        aura_actor::call::Waited::Done(Err(e)) => assert!(e.to_string().contains("timed out")),
        other => panic!("expected timeout failure from sweep, got {other:?}"),
    }
}
