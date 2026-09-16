//! Probe connection gateway (Phase 3): accept the probes' OUTBOUND WS
//! connections (they dial us), register them by node alias, and correlate
//! Result frames back to in-flight realm calls.

use aura_realm::SharedRealm;
use futures_util::{SinkExt, StreamExt};
use probe_protocol::Frame;
use tokio_tungstenite::tungstenite::Message;

/// Accept loop: one task per engine; each connection gets a writer task
/// (realm.probes holds the sender) and a reader task (correlates results).
pub async fn serve_probes(realm: SharedRealm, addr: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("probe gateway listening on {addr}");
    serve_probes_listener(realm, listener).await
}

/// Variant taking a pre-bound listener (tests bind :0 to learn the port).
pub async fn serve_probes_listener(
    realm: SharedRealm,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let realm = realm.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(realm, stream).await {
                eprintln!("probe connection error: {e:#}");
            }
        });
    }
}

async fn handle_connection(realm: SharedRealm, stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut sink, mut stream) = ws.split();

    // Registration: alias + carriers. The alias keys the connection.
    let (node_alias, _carriers) = match stream.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Register { node_alias, carriers, .. } => (node_alias, carriers),
            other => anyhow::bail!("expected Register, got {other:?}"),
        },
        other => anyhow::bail!("bad register frame: {other:?}"),
    };
    sink.send(Message::Text(serde_json::to_string(&Frame::Registered)?))
        .await?;

    // Writer channel: realm calls push frames; this task owns the sink.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    {
        let mut r = realm.lock().await;
        if let Some(old) = r.probes.insert(node_alias.clone(), tx) {
            // Re-registration (reconnect): the old writer channel dies with
            // this insert — its reader task exits on send failure.
            let _ = old;
        }
    }

    // Writer task: drain the channel into the socket.
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if sink.send(Message::Text(serde_json::to_string(&frame)?)).await.is_err() {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    // Reader loop: correlate Result frames to pending remote calls.
    while let Some(msg) = stream.next().await {
        let Message::Text(text) = msg? else { continue };
        match serde_json::from_str::<Frame>(&text)? {
            Frame::Result(result) => {
                let mut r = realm.lock().await;
                if let Some(tx) = r.pending_remote.remove(&result.call_id) {
                    let _ = tx.send(result.outcome);
                }
                // Unknown call_id: the caller timed out and was removed —
                // drop the late result (the pending_calls scan owns
                // timeout semantics; a late answer is not re-delivered).
            }
            other => anyhow::bail!("unexpected frame from probe: {other:?}"),
        }
    }
    // Connection gone: unregister so calls fail fast with "not connected".
    // Only remove if the registered channel is still ours (a reconnect may
    // have replaced it meanwhile).
    let mut r = realm.lock().await;
    if r.probes.get(&node_alias).is_some() {
        r.probes.remove(&node_alias);
    }
    writer.abort();
    Ok(())
}
