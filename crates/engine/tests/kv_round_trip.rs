//! Phase 6.6 sender side (ADR-0010): the realm pushes a raw okm-wire op
//! frame to a probe's declared KV executor and resolves the correlated
//! answer — `Frame::Kv` (executor bytes back) or `Frame::KvRefused` (the
//! reason back). A probe that answers neither must become a failure value,
//! never a hang.
//!
//! The peer here is a hand-rolled WS client on purpose: what is under test
//! is AURA's half of the wire — correlation by `kv_id`, refusal as an
//! answer, deadline withdrawal. The probe runtime covers the same wire from
//! its own side (`probe/crates/runtime/tests/kv_over_wire.rs`).

use aura_engine::{Engine, probes};
use futures_util::stream::SplitStream;
use futures_util::{SinkExt, StreamExt};
use probe_protocol::Frame;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const ALIAS: &str = "kv-node";

#[tokio::test]
async fn kv_round_trip_over_a_probe_connection() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), listener));
    tokio::spawn(fake_probe(port));
    wait_for_probe(&engine).await;

    // An answer: the executor's bytes come back byte-for-byte — the realm
    // parses nothing on this path.
    let answer = aura_realm::Realm::kv_round_trip(
        &engine.realm,
        ALIAS,
        "app-a",
        b"op-frame".to_vec(),
    )
    .await
    .expect("executor answered");
    assert_eq!(answer, b"resp:op-frame");

    // A refusal is an ANSWER, not a dropped frame: the stated reason comes
    // back as the failure value (an empty Ok would have hidden the cause).
    let err = aura_realm::Realm::kv_round_trip(
        &engine.realm,
        ALIAS,
        "app-c",
        b"op-frame".to_vec(),
    )
    .await
    .expect_err("undeclared executor is refused");
    assert!(
        err.contains("app-c") && err.contains("no such declared executor"),
        "the refusal names the executor and its cause: {err}"
    );

    // Silence times out: the caller gets a failure value instead of waiting
    // forever, and the correlation is withdrawn with it.
    let err = aura_realm::Realm::kv_round_trip_within(
        &engine.realm,
        ALIAS,
        "app-a",
        b"op-frame".to_vec(),
        Duration::from_millis(200),
    )
    .await
    .expect_err("an unanswered frame is a timeout");
    assert!(err.contains("timed out"), "{err}");
    assert!(
        engine.realm.lock().await.kv_pending.is_empty(),
        "a timed-out correlation is withdrawn, so a late answer is discarded"
    );

    // No connection by that alias: refused locally, without touching the wire.
    let err = aura_realm::Realm::kv_round_trip(&engine.realm, "absent-node", "app-a", vec![])
        .await
        .expect_err("an unconnected probe is an error");
    assert!(err.contains("not connected"), "{err}");
}

/// Wait for the fake probe's registration to land in the realm.
async fn wait_for_probe(engine: &Engine) {
    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key(ALIAS) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("probe registration never landed");
}

/// A probe's connection end: register, then serve three requests — answer,
/// refuse, and stay silent.
async fn fake_probe(port: u16) {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}"))
        .await
        .expect("dial the probe gateway");
    let (mut sink, mut stream) = ws.split();
    let register = Frame::Register {
        node_alias: ALIAS.into(),
        credential: "tok".into(),
        carriers: vec!["steel".into()],
    };
    sink.send(Message::Text(serde_json::to_string(&register).unwrap()))
        .await
        .unwrap();
    assert!(
        matches!(next(&mut stream).await, Frame::Registered),
        "the gateway acks registration before any traffic"
    );

    // 1. Answer: the request carries the addressed executor + the caller's
    //    raw bytes, and the answer echoes the same kv_id.
    let kv = expect_kv(&mut stream).await;
    assert_eq!(kv.executor, "app-a");
    assert_eq!(kv.frame, b"op-frame", "the op frame rides unparsed");
    let kv_id = kv.kv_id;
    let answer = Frame::Kv(probe_protocol::KvFrame {
        executor: kv.executor,
        kv_id: kv_id.clone(),
        frame: b"resp:op-frame".to_vec(),
    });
    sink.send(Message::Text(serde_json::to_string(&answer).unwrap()))
        .await
        .unwrap();

    // 2. Refuse: same correlation slot, different frame type.
    let kv = expect_kv(&mut stream).await;
    assert_eq!(kv.executor, "app-c");
    assert_ne!(kv.kv_id, kv_id, "each round trip gets its own correlation id");
    let refused = Frame::KvRefused {
        executor: kv.executor,
        kv_id: kv.kv_id,
        reason: "no such declared executor".into(),
    };
    sink.send(Message::Text(serde_json::to_string(&refused).unwrap()))
        .await
        .unwrap();

    // 3. Silence: read the request and answer nothing — the realm's deadline
    //    is the only thing that ends this one.
    let _ = expect_kv(&mut stream).await;
}

async fn next(stream: &mut SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>) -> Frame {
    let Message::Text(text) = stream.next().await.unwrap().unwrap() else {
        panic!("expected a text frame");
    };
    serde_json::from_str(&text).unwrap()
}

async fn expect_kv(
    stream: &mut SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
) -> probe_protocol::KvFrame {
    match next(stream).await {
        Frame::Kv(kv) => kv,
        other => panic!("expected a Kv request, got {other:?}"),
    }
}