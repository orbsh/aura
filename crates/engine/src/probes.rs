//! Probe connection gateway (Phase 3): accept the probes' OUTBOUND WS
//! connections (they dial us), register them by node alias, and correlate
//! Result frames back to in-flight realm calls.

use aura_realm::SharedRealm;
use futures_util::{SinkExt, StreamExt};
use probe_protocol::{Frame, HostFrame};
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
    // A clone stays with the reader so it can answer host calls.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    {
        let mut r = realm.lock().await;
        if let Some(old) = r.probes.insert(node_alias.clone(), tx.clone()) {
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
                if let Some(pending) = r.pending_remote.remove(&result.call_id) {
                    let _ = pending.reply.send(result.outcome);
                }
                // Unknown call_id: the caller timed out and was removed —
                // drop the late result (the pending_calls scan owns
                // timeout semantics; a late answer is not re-delivered).
            }
            Frame::Kv(kv) => {
                // KV executor reply (Phase 4.5): resolve the pending
                // request. Unknown kv_id = caller timed out — drop.
                let mut r = realm.lock().await;
                if let Some(pending) = r.kv_pending.remove(&kv.kv_id) {
                    let _ = pending.send(Ok(kv.frame));
                }
            }
            Frame::KvRefused { executor, kv_id, reason } => {
                // A refusal is the executor's answer, not a dropped frame:
                // resolve the pending request with its stated cause (the
                // frame names both the executor and why it did not run).
                // Unknown kv_id = caller timed out — drop.
                let mut r = realm.lock().await;
                if let Some(pending) = r.kv_pending.remove(&kv_id) {
                    let _ = pending.send(Err(format!("kv executor `{executor}` refused: {reason}")));
                }
            }
            Frame::Host(HostFrame::Call(call)) => {
                // Ctx bridge over the wire: resolve against the instance
                // the enclosing remote call was routed to (looked up from
                // pending_remote by the call_id the probe carries), then
                // reply on this connection's writer.
                let instance = {
                    let r = realm.lock().await;
                    r.pending_remote.get(&call.call_id).map(|p| p.instance.clone())
                };
                let outcome = match instance {
                    Some(inst) => {
                        crate::host_wire::resolve_host_call(&realm, &inst, &call.op).await
                    }
                    None => Err("unknown call_id: the enclosing remote call is not in flight".into()),
                };
                tx.send(Frame::Host(HostFrame::Result(probe_protocol::HostResult {
                    host_call_id: call.host_call_id,
                    outcome,
                })))
                .ok();
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
