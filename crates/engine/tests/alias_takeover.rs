//! Alias takeover is allowed but never silent (ADR-0015 step ①'s aura
//! residue: the replacement discipline). Locks the identity-checked
//! presence: after a second connection takes the alias, the FIRST
//! connection ending must NOT unregister the alias (the guard's
//! same_channel check), and the routing channel is the NEW one.

use aura_engine::{Engine, probes};
use std::time::Duration;

fn dial(port: u16) -> tokio::task::JoinHandle<()> {
    // A minimal probe-shaped client: register a WS handshake manually.
    tokio::spawn(async move {
        use futures_util::{SinkExt, StreamExt};
        use probe_protocol::Frame;
        use tokio_tungstenite::tungstenite::Message;
        let url = format!("ws://127.0.0.1:{port}");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        sink.send(Message::Text(
            serde_json::to_string(&Frame::Register {
                node_alias: "shared".into(),
                credential: String::new(),
                carriers: vec!["steel".into()],
            })
            .unwrap(),
        ))
        .await
        .unwrap();
        // Keep the connection open until cancelled.
        while let Some(_) = stream.next().await {}
    })
}

#[tokio::test]
async fn alias_takeover_routes_to_the_new_peer() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), listener));

    let first = dial(port);
    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key("shared") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let first_peer = engine.realm.try_lock().unwrap().probes["shared"].peer;

    let second = dial(port);
    // Wait for the takeover: the stored peer address must change.
    let mut replaced = false;
    for _ in 0..50 {
        let r = engine.realm.try_lock().unwrap();
        if let Some(conn) = r.probes.get("shared") {
            if conn.peer != first_peer {
                replaced = true;
                break;
            }
        }
        drop(r);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replaced, "second connection takes over the alias, routing to the new peer");

    // The OLD connection ending must NOT unregister the new holder
    // (presence guard identity check): kill the first, alias survives.
    first.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        engine.realm.try_lock().unwrap().probes.contains_key("shared"),
        "the displaced connection's guard must not remove the new registration"
    );
    second.abort();
}
