//! ADR-0035 exec/BGI carriers through the realm path: a framed resident
//! booth registers with `language = "bgi"` (source = the spawn spec —
//! the `bgi_loop` fixture speaking the line protocol), and a bare
//! one-shot booth registers with `language = "exec"` (the `one_shot`
//! fixture: one JSON in, one JSON out, nothing survives). Both run
//! through the realm's resident-session dispatch — the shapes differ,
//! the machinery does not.
//!
//! exec (the bare cgi shape) has no protocol and no residency — handled
//! by the spawn declaration, not by new realm machinery; this file locks
//! both citizenships plus the statelessness boundary.

use aura_booth::{BoothType, InstanceId};
use aura_engine::Engine;

fn bin(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../probe/target/debug/examples/");
    let full = format!("{path}{name}");
    assert!(
        std::path::Path::new(&full).exists(),
        "{name} missing — build it in ~/world/probe: cargo build -p actor-guest --examples"
    );
    full
}

/// Plain call: the realm's script arm dispatches into the bgi session
/// like any carrier — the child answers the request frame with a result.
#[tokio::test]
async fn bgi_booth_invoke() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-echo", "bgi", bin("bgi_loop")))
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
    // call must not cold-start — the registry key is type/key identity).
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

/// The ctx seam across the process boundary, into the REALM: the bgi
/// booth's `ctx_round_trip` sends a host frame (ctx_invoke) which the
/// host bridge answers against a sibling python booth — child → pipes →
/// realm → child, one full round.
#[tokio::test]
async fn bgi_booth_ctx_invoke_to_sibling() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-echo", "bgi", bin("bgi_loop")))
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

/// ADR-0034 over the bgi seam through the realm: the iterate jobs drive
/// the child's guard state; done drains the realm registry (the same
/// bookkeeping the script carriers get — carrier-independent by design).
#[tokio::test]
async fn bgi_booth_iterate_through_realm() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-prod", "bgi", bin("bgi_loop")))
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

/// BGI residency: eviction reaps the child, re-invocation cold-starts
/// (the same session lifecycle as every embedded carrier).
#[tokio::test]
async fn bgi_booth_eviction_reaps_the_child() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(
            BoothType::script("bin-echo", "bgi", bin("bgi_loop"))
                .with_idle_ttl(std::time::Duration::from_millis(200)),
        )
        .await
        .unwrap();
    let target = InstanceId { booth_type: "bin-echo".into(), key: "k3".into() };
    engine.invoke(target.clone(), "echo", serde_json::json!(1)).await.unwrap();
    assert!(engine.realm.lock().await.is_resident(&target));

    // Drive eviction directly (not the 5s tick): the instance evict runs
    // sessions.evict — the bgi slot's Drop closes stdin + reaps.
    aura_realm::Realm::evict_instance(engine.realm.clone(), &target).await;

    // Cold start: the next call spawns a fresh child and works.
    let out = engine.invoke(target.clone(), "echo", serde_json::json!("post-evict")).await.unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": "post-evict" }));
}

/// exec (the bare cgi shape) through the realm: invoke works like any
/// carrier (one JSON in, one JSON out per call); iterate is a NAMED
/// error — statelessness by definition, the fix is bgi. The counter
/// handler proves it: every call counts its own args, never a previous
/// call's (a one-shot guard could not exist even if asked).
#[tokio::test]
async fn exec_oneshot_booth_through_realm() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-shot", "exec", bin("one_shot")))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "bin-shot".into(), key: "s1".into() },
            "echo",
            serde_json::json!({ "now": 1 }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "echoed": { "now": 1 } }));

    let realm = engine.realm.clone();
    let slot = aura_realm::Realm::iterate(
        &realm,
        aura_booth::IterateOp::Start {
            target: InstanceId { booth_type: "bin-shot".into(), key: "s1".into() },
            handler: "count".into(),
            args: serde_json::json!({ "total": 2 }),
        },
    )
    .await
    .unwrap();
    let aura_booth::call::Waited::Done(result) = slot.wait().await.unwrap() else {
        panic!("hot only");
    };
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("stateless by definition") && err.contains("bgi"),
        "one-shot iterate names the design: {err}"
    );
    // A failed Start must not leak a registry entry (the rollback
    // discipline of every failed send).
    assert_eq!(realm.lock().await.streams.len(), 0, "no orphaned stream entry");
}

/// Phase 4.14 gate 1: `ctx_store_emit` over the bgi seam into the REALM
/// store. The fixture's `store_round_trip` forwards two okm Collection
/// instructions (put, read-back) as host frames — pure transport, the
/// child never parses them — and the parent answers from the type's own
/// plan (the bgi fixture's hand-written `interface_schema` declares the
/// `counters` collection; the plan resolves at registration through the
/// same 4.5b introspection path the script carriers use). This is the
/// gate for the nushell PTY retirement: the in-process bridge's storage
/// capability now provably crosses the process boundary.
#[tokio::test]
async fn bgi_booth_store_emit_roundtrip() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("bin-store", "bgi", bin("bgi_loop")))
        .await
        .unwrap();

    // The type's schema introspected at upload: a plan resolved.
    assert!(
        engine.realm.lock().await.plan_of("bin-store").is_some(),
        "the bgi fixture's declared storage resolves a plan"
    );

    let out = engine
        .invoke(
            InstanceId { booth_type: "bin-store".into(), key: "s1".into() },
            "store_round_trip",
            serde_json::json!({
                "put": { "collection": "counters", "op": "put_document",
                         "key": { "id": 1 }, "doc": { "count": 42 } },
                "get": { "collection": "counters", "op": "get_document",
                         "key": { "id": 1 } },
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        out["read_back"]["count"], 42,
        "the put landed in the type's store and the get read it back through the wire"
    );
}

/// Phase 4.14 gate 2: the same store-emit round trip over the NUSHELL
/// bgi adapter (the two-fifo shape — spawn spec `nu <author.nu>`, the
/// author's `def main` loop). This is the live acceptance for the PTY
/// retirement: every ctx capability the nushell PTY bridge carried
/// (schema upload-introspection + store emit) provably crosses the
/// boundary on the framed resident shape, so the PTY machinery
/// (NushellResident, bridge.nu, pump_quiet) retires without a gap.
#[tokio::test]
async fn bgi_nu_booth_store_emit_roundtrip() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../probe/crates/actor-guest/examples/bgi_nu.nu"
    );
    assert!(std::path::Path::new(fixture).exists(), "nu bgi fixture missing: {fixture}");
    engine
        .register(BoothType::script("nu-store", "bgi", format!("nu {fixture}")))
        .await
        .unwrap();

    // Upload introspection ran through the fifo seam: the schema frame
    // round-tripped and its storage block resolved a plan.
    assert!(
        engine.realm.lock().await.plan_of("nu-store").is_some(),
        "the nu fixture's declared storage resolves a plan over the fifo shape"
    );

    let out = engine
        .invoke(
            InstanceId { booth_type: "nu-store".into(), key: "s1".into() },
            "store_round_trip",
            serde_json::json!({
                "put": { "collection": "counters", "op": "put_document",
                         "key": { "id": 7 }, "doc": { "count": 99 } },
                "get": { "collection": "counters", "op": "get_document",
                         "key": { "id": 7 } },
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        out["read_back"]["count"], 99,
        "two host frames answered over the rep fifo; the realm store round-trips on the nu shape"
    );

    // Residency on the fifo shape: the second call rides the SAME child
    // — the count guard lives in the child's $env (the PTY carrier's
    // $env rule, now without the PTY).
    let c1 = engine
        .invoke(InstanceId { booth_type: "nu-store".into(), key: "s2".into() }, "count", serde_json::json!({}))
        .await
        .unwrap();
    let c2 = engine
        .invoke(InstanceId { booth_type: "nu-store".into(), key: "s2".into() }, "count", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!((c1["count"].as_u64(), c2["count"].as_u64()), (Some(1), Some(2)),
        "one child per instance; $env state persists across calls");
}
