//! Alias takeover is allowed but never silent (ADR-0015 step ①'s aura
//! residue: the replacement discipline). Locks the identity-checked
//! presence: after a second connection takes the alias, the FIRST
//! connection ending must NOT unregister the alias (the guard's
//! same_channel check), and the routing channel is the NEW one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_engine::{effectors, Engine};
use std::time::Duration;

/// A node key on disk: the handshake proves possession, so every dial needs one (the two
/// dials use DIFFERENT keys on purpose — local mode has no registry, so neither is enrolled).
fn node_key(tag: &str) -> effector_protocol::SigningKey {
    let mut path = std::env::temp_dir();
    path.push(format!("aura-alias-{tag}-{}.key", std::process::id()));
    let _ = std::fs::remove_file(&path);
    effector_protocol::write_keypair(&path).unwrap();
    effector_protocol::read_signing_key(&path).unwrap()
}

fn dial(port: u16, tag: &str) -> tokio::task::JoinHandle<()> {
    let key = node_key(tag);
    // A minimal effector-shaped client: register a WS handshake manually, answering the
    // challenge the host always speaks first.
    tokio::spawn(async move {
        use effector_protocol::Frame;
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        let url = format!("ws://127.0.0.1:{port}");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let nonce = match stream.next().await.unwrap().unwrap() {
            Message::Text(text) => match serde_json::from_str::<Frame>(&text).unwrap() {
                Frame::Challenge { nonce } => nonce,
                other => panic!("expected Challenge, got {other:?}"),
            },
            other => panic!("expected text frame, got {other:?}"),
        };
        let node_alias = "shared".to_string();
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Register {
                public_key: effector_protocol::public_key_b64(&key),
                signature: effector_protocol::sign_register(&key, &nonce, &node_alias),
                node_alias,
                carriers: vec!["steel".into()],
            })
            .unwrap(),
        ))
        .await
        .unwrap();
        // Keep the connection open until cancelled.
        while stream.next().await.is_some() {}
    })
}

#[tokio::test]
async fn alias_takeover_routes_to_the_new_peer() {
    let engine = Engine::start(&Default::default())
        .await
        .expect("engine boot");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(effectors::serve_effectors_listener(
        engine.realm.clone(),
        listener,
    ));

    let first = dial(port, "first");
    for _ in 0..50 {
        if engine
            .realm
            .try_lock()
            .unwrap()
            .effectors
            .contains_key("shared")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let first_peer = engine.realm.try_lock().unwrap().effectors["shared"].peer;

    let second = dial(port, "second");
    // Wait for the takeover: the stored peer address must change.
    let mut replaced = false;
    for _ in 0..50 {
        let r = engine.realm.try_lock().unwrap();
        if let Some(conn) = r.effectors.get("shared") {
            if conn.peer != first_peer {
                replaced = true;
                break;
            }
        }
        drop(r);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        replaced,
        "second connection takes over the alias, routing to the new peer"
    );

    // The OLD connection ending must NOT unregister the new holder
    // (presence guard identity check): kill the first, alias survives.
    first.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        engine
            .realm
            .try_lock()
            .unwrap()
            .effectors
            .contains_key("shared"),
        "the displaced connection's guard must not remove the new registration"
    );
    second.abort();
}
