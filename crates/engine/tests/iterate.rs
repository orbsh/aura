//! ADR-0034 iterate e2e: generator-mode producer (python `yield`),
//! native-iterable consumption (`ctx_iterate` + `for`), the mandatory
//! dispose dual, the typed envelope, and mid-stream failure as a value.
//! Every pull is one hot call riding the existing machinery — these
//! tests assert the delivery shape, not a new queue.

use aura_booth::{BoothType, Body, InstanceId};
use aura_engine::Engine;
use std::sync::Arc;

// Producer: a python generator handler. The framework drives it — the
// handler never sees the wire protocol (no stream_id, no envelope).
#[cfg(feature = "python")]
const PRODUCER: &str = r#"
def tokens(args):
    for t in args["list"]:
        yield t
"#;

// Consumer: pulls via the loaded ctx_iterate wrapper (a native
// generator over ctx_iter_start/next/dispose) and returns the items
// gathered. `limit` breaks mid-stream — GeneratorExit through the
// wrapper's finally must send dispose to the producer.
#[cfg(feature = "python")]
fn consumer(limit: Option<usize>) -> String {
    let take = match limit {
        Some(n) => format!("        if len(got) >= {n}:\n            break\n"),
        None => String::new(),
    };
    format!(
        r#"
def consume(args):
    got = []
    for tok in ctx_iterate("py-prod", "p1", "tokens", {{"list": args["list"]}}):
        got.append(tok)
{take}    return {{"got": got}}
"#
    )
}

#[cfg(all(feature = "python", feature = "nushell"))]
async fn boot() -> Engine {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    engine
        .register(BoothType::script("py-prod", "python", PRODUCER))
        .await
        .unwrap();
    engine
}

// Full drain: for-loop over a sibling's generator, envelope ends with
// done:true; the realm's stream registry is empty afterwards.
#[cfg(all(feature = "python", feature = "nushell"))]
#[tokio::test]
async fn python_generator_to_python_consumer() {
    let engine = boot().await;
    engine
        .register(BoothType::script("py-cons", "python", consumer(None)))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "py-cons".into(), key: "c1".into() },
            "consume",
            serde_json::json!({ "list": ["a", "b", "c"] }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "got": ["a", "b", "c"] }));

    // Exhaustion is terminal and structural: the routing entry is gone.
    let live = engine.realm.lock().await.streams.len();
    assert_eq!(live, 0, "done:true must release the stream registry entry");
}

// Mid-stream break: the wrapper's GeneratorExit path MUST deliver
// dispose — observable as the registry entry leaving the live set.
#[cfg(all(feature = "python", feature = "nushell"))]
#[tokio::test]
async fn break_sends_dispose() {
    let engine = boot().await;
    engine
        .register(BoothType::script("py-cons", "python", consumer(Some(1))))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "py-cons".into(), key: "c1".into() },
            "consume",
            serde_json::json!({ "list": ["a", "b", "c"] }),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({ "got": ["a"] }));

    // The consumer broke after one item; dispose must have removed the
    // realm's routing entry (the session-side generator got
    // GeneratorExit via close()).
    let live = engine.realm.lock().await.streams.len();
    assert_eq!(live, 0, "break must dispose the stream");
}

// A pull naming an unknown stream id fails eagerly with an error value
// (ADR-0012) — streams are not durable and never replay. Dispose is
// the idempotent dual: an unknown id answers success, no job, no error.
#[cfg(all(feature = "python", feature = "nushell"))]
#[tokio::test]
async fn unknown_stream_and_idempotent_dispose() {
    let engine = boot().await;
    let err = aura_realm::Realm::iterate(
        &engine.realm,
        aura_booth::IterateOp::Next { stream_id: "stream-ghost".into() },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not live"), "{err}");

    let slot = aura_realm::Realm::iterate(
        &engine.realm,
        aura_booth::IterateOp::Dispose { stream_id: "stream-ghost".into() },
    )
    .await
    .unwrap();
    let aura_booth::call::Waited::Done(result) = slot.wait().await.unwrap() else {
        panic!("iterate is hot-only");
    };
    assert_eq!(result.unwrap(), serde_json::Value::Null);
}

// Mid-stream producer failure: a raising generator fails the pull —
// error values, never a second channel. The routing entry drops with
// the failure (the producer side is dead).
#[cfg(all(feature = "python", feature = "nushell"))]
#[tokio::test]
async fn mid_stream_failure_is_error_value() {
    let engine = Engine::start(&Default::default())
        .await
        .expect("engine boot");
    engine
        .register(BoothType::script(
            "py-prod",
            "python",
            r#"
def bad(args):
    yield "ok"
    raise RuntimeError("stream broke")
"#,
        ))
        .await
        .unwrap();
    engine
        .register(BoothType::script(
            "py-cons",
            "python",
            r#"
def consume(args):
    out = []
    err = None
    try:
        for tok in ctx_iterate("py-prod", "p1", "bad", {}):
            out.append(tok)
    except Exception as e:
        err = str(e)
    return {"got": out, "err": err}
"#,
        ))
        .await
        .unwrap();

    let out = engine
        .invoke(
            InstanceId { booth_type: "py-cons".into(), key: "c1".into() },
            "consume",
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert_eq!(out["got"], serde_json::json!(["ok"]));
    assert!(out["err"].as_str().unwrap().contains("stream broke"));

    let live = engine.realm.lock().await.streams.len();
    assert_eq!(live, 0, "a failed stream must leave the registry");
}

// Envelope mode (nushell): the handler is a repeatedly callable def
// writing `done: true` explicitly — no generator protocol, no magic.
// The framework injects {stream_id, op}; the guard counter rides $env.
// The cursor is driven Rust-side: the envelope contract is
// carrier-independent.
#[cfg(feature = "nushell")]
#[tokio::test]
async fn nushell_envelope_producer() {
    let engine = Engine::start(&Default::default())
        .await
        .expect("engine boot");
    engine
        .register(BoothType::script(
            "nu-prod",
            "nushell",
            r#"export def --env "count3" [args: record] {
    let op = $args.iterate.op
    if $op == "start" {
        $env.AURA_NU_TEST_COUNT = 0
        ({ item: 0, done: false })
    } else if $op == "next" {
        let cur = ($env | get AURA_NU_TEST_COUNT? | default 0) + 1
        if $cur > 2 {
            ({ done: true })
        } else {
            $env.AURA_NU_TEST_COUNT = $cur
            ({ item: $cur, done: false })
        }
    } else {
        ({ done: true })
    }
}
"#,
        ))
        .await
        .unwrap();

    let realm = engine.realm.clone();
    let target = InstanceId { booth_type: "nu-prod".into(), key: "n1".into() };
    let mut items = Vec::new();
    let mut stream_id: Option<String> = None;
    loop {
        let op = match &stream_id {
            None => aura_booth::IterateOp::Start {
                target: target.clone(),
                handler: "count3".into(),
                args: serde_json::json!({}),
            },
            Some(sid) => aura_booth::IterateOp::Next { stream_id: sid.clone() },
        };
        let slot = aura_realm::Realm::iterate(&realm, op).await.unwrap();
        let aura_booth::call::Waited::Done(result) = slot.wait().await.unwrap() else {
            panic!("iterate is hot-only");
        };
        let waited = result.unwrap();
        if let Some(sid) = waited.get("stream_id").and_then(|s| s.as_str()) {
            stream_id = Some(sid.to_string());
        }
        let env = aura_booth::Envelope::from_value(&waited).unwrap();
        if env.done {
            break;
        }
        items.push(env.item.unwrap());
        if items.len() > 5 {
            panic!("envelope stream must terminate: {items:?}");
        }
    }
    assert_eq!(
        items,
        vec![serde_json::json!(0), serde_json::json!(1), serde_json::json!(2)]
    );
    assert_eq!(realm.lock().await.streams.len(), 0, "done releases the entry");
}

// Rust closure bodies carry no resident stream state (ADR-0034: the
// producer shape lives in the session — recorded residual). The
// Start round trip succeeds but the job fails with an error value —
// never a silent single-shot fallback.
#[tokio::test]
async fn rust_body_iterate_is_error_value() {
    let engine = Engine::start(&Default::default())
        .await
        .expect("engine boot");
    engine
        .register(BoothType {
            name: "rust-prod".into(),
            body: Body::Rust(Arc::new(|_ctx, args| Box::pin(async move { Ok(args) }))),
            idle_ttl: None,
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: Vec::new(),
        })
        .await
        .unwrap();
    let realm = engine.realm.clone();
    let slot = aura_realm::Realm::iterate(
        &realm,
        aura_booth::IterateOp::Start {
            target: InstanceId { booth_type: "rust-prod".into(), key: "r1".into() },
            handler: "whatever".into(),
            args: serde_json::json!({}),
        },
    )
    .await
    .unwrap();
    let aura_booth::call::Waited::Done(result) = slot.wait().await.unwrap() else {
        panic!("iterate is hot-only");
    };
    let err = result.unwrap_err();
    assert!(err.to_string().contains("ADR-0034"), "{err}");
}
