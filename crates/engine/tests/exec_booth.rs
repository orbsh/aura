//! ADR-0035 exec carrier e2e (aura side): a native binary speaking the
//! line protocol registers as an booth type (`language = "exec"`, source
//! = argv) and runs through the realm's resident-session path — plain
//! invoke, and the ctx seam crossing back into the realm (the child's
//! host frame answers as a real ctx_invoke against a sibling booth).
//! The child's residency is its process; idle_ttl eviction closes stdin
//! and reaps (the same session lifecycle as every embedded carrier).
//!
//! Mode B (one-shot SKILL) is the same spec minus the loop — a spawn
//! declaration's business, not a second code path; this file locks mode A.

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;

fn exec_bin() -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../probe/target/debug/examples/exec_loop"
    );
    assert!(
        std::path::Path::new(path).exists(),
        "exec_loop missing — build it in ~/world/probe: cargo build -p actor-guest --example exec_loop"
    );
    path.to_string()
}

/// Plain call: the realm's script arm dispatches into the exec session
/// like any carrier — the child answers the request frame with a result.
#[tokio::test]
async fn exec_booth_invoke() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-echo", "exec", exec_bin()))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "bin-echo".into(), key: "k1".into() },
            "echo",
            serde_json::json!({ "x": 1 }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": { "x": 1 } }));

    // Same child across calls: residency is the process (the second
    // call must not cold-start — the registry key is type/kind identity).
    let out = engine
        .invoke(
            InstanceId { booth_type: "bin-echo".into(), key: "k1".into() },
            "echo",
            serde_json::json!({ "y": 2 }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": { "y": 2 } }));
}

/// The ctx seam across the process boundary, into the REALM: the exec
/// booth's `ctx_round_trip` sends a host frame (ctx_invoke) which the
/// host bridge answers against a sibling python booth — child → pipes →
/// realm → child, one full round.
#[tokio::test]
async fn exec_booth_ctx_invoke_to_sibling() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-echo", "exec", exec_bin()))
        .await
        .unwrap();
    engine
        .register(BoothType::script(
            "py-target",
            "python",
            r#"
def handle(args):
    return {"from": "python", "got": args["q"]}
"#,
        ))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "bin-echo".into(), key: "k2".into() },
            "ctx_round_trip",
            serde_json::json!({
                "type": "py-target", "key": "p1", "handler": "handle",
                "args": { "q": 42 },
            }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "from": "python", "got": 42 }));
}

/// ADR-0034 over the exec seam through the realm: the iterate jobs drive
/// the child's guard state; done drains the realm registry (the same
/// bookkeeping the script carriers get — carrier-independent by design).
#[tokio::test]
async fn exec_booth_iterate_through_realm() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-prod", "exec", exec_bin()))
        .await
        .unwrap();

    let realm = engine.realm.clone();
    let target = InstanceId { booth_type: "bin-prod".into(), key: "p1".into() };
    let mut items = Vec::new();
    let mut sid: Option<String> = None;
    loop {
        let op = match &sid {
            None => aura_booth::IterateOp::Start {
                target: target.clone(),
                handler: "count".into(),
                args: serde_json::json!({ "total": 3 }),
            },
            Some(s) => aura_booth::IterateOp::Next { stream_id: s.clone() },
        };
        let slot = aura_realm::Realm::iterate(&realm, op).await.unwrap();
        let aura_booth::call::Waited::Done(r) = slot.wait().await.unwrap() else {
            panic!("hot only");
        };
        let env = r.unwrap();
        if let Some(s) = env.get("stream_id").and_then(|v| v.as_str()) {
            sid = Some(s.to_string());
        }
        let e = aura_booth::Envelope::from_value(&env).unwrap();
        if e.done {
            break;
        }
        items.push(e.item.unwrap());
        if items.len() > 5 {
            panic!("stream must terminate: {items:?}");
        }
    }
    assert_eq!(items, vec!["i0", "i1", "i2"]);
    assert_eq!(realm.lock().await.streams.len(), 0, "done drains the registry");
}

/// Eviction ends the residency: the child is closed + reaped (the
/// session's Drop), the registry slot gone. Re-invocation cold-starts.
#[tokio::test]
async fn exec_booth_eviction_reaps_the_child() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(
            BoothType::script("bin-echo", "exec", exec_bin()).with_idle_ttl(std::time::Duration::from_millis(200)),
        )
        .await
        .unwrap();
    let target = InstanceId { booth_type: "bin-echo".into(), key: "k3".into() };
    engine.invoke(target.clone(), "echo", serde_json::json!(1)).await.unwrap();
    assert!(engine.realm.lock().await.is_resident(&target));

    // Drive eviction directly (not the 5s tick): the instance evict runs
    // sessions.evict — the exec slot's Drop closes stdin + reaps.
    aura_realm::Realm::evict_instance(engine.realm.clone(), &target).await;

    // Cold start: the next call spawns a fresh child and works.
    let out = engine.invoke(target.clone(), "echo", serde_json::json!("post-evict")).await.unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": "post-evict" }));
}
