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
    // Honest disclosure (ADR-0015 §7): registrations here are unauth-
    // enticated — whoever can reach this port may claim any alias. The
    // trust-mode switch and the keypair handshake ride the prism gateway
    // (connection plane); this line states the CURRENT posture where it
    // is visible (the log), not only in some config file.
    eprintln!(
        "probe gateway: registrations are unauthenticated — the network is the boundary \
         (node identity: ADR-0015, prism gateway plane)"
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let realm = realm.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(realm, stream, peer).await {
                eprintln!("probe connection error: {e:#}");
            }
        });
    }
}

async fn handle_connection(
    realm: SharedRealm,
    stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
) -> anyhow::Result<()> {
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
    let entry = aura_realm::ProbeConn { sender: tx.clone(), peer };
    {
        let mut r = realm.lock().await;
        if let Some(old) = r.probes.insert(node_alias.clone(), entry.clone()) {
            // Alias replacement (in `open` posture a restarted container
            // must be able to reclaim its name from a stale registration).
            // Allowed — but never silent (ADR-0015 §7 replacement
            // discipline): the event names the alias and BOTH peers, so
            // an operator can always see where their calls actually go.
            eprintln!(
                "probe gateway: alias '{node_alias}' REPLACED — old peer {} displaced by new peer {peer:?}",
                old.peer
            );
            // The old writer channel dies when its sender is dropped by
            // the map overwrite; its reader task exits on send failure.
        }
    }
    // Presence must flip when this connection's task ENDS — by any path,
    // including cancellation (aborted task / connection killed at an
    // await point): code after the read loop never runs in that case, so
    // an unregister written as loop epilogue leaks a dead alias and calls
    // keep routing into a dead writer. A Drop guard fires on every exit
    // path, abort included. Identity-checked (sender equality): a
    // reconnect that replaced this alias leaves the new channel alone.
    let _presence = PresenceGuard {
        realm: Some(realm.clone()),
        alias: node_alias.clone(),
        channel: tx.clone(),
    };

    // Writer task: drain the channel into the socket. Ends when the
    // channel's senders all drop (this function returning + the presence
    // guard releasing its clone).
    tokio::spawn(async move {
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
    // Connection gone (clean EOF): `_presence` drops with the function and
    // unregisters so calls fail fast with "not connected".
    Ok(())
}

/// Unregisters the node alias when the connection task ends — see the
/// presence guard in `handle_connection`. Drop cannot await, so the
/// lock-taking removal runs in a detached task; the identity check inside
/// it keeps a reconnect's registration intact.
struct PresenceGuard {
    realm: Option<SharedRealm>,
    alias: String,
    channel: tokio::sync::mpsc::UnboundedSender<Frame>,
}

impl Drop for PresenceGuard {
    fn drop(&mut self) {
        let Some(realm) = self.realm.take() else { return };
        let alias = std::mem::take(&mut self.alias);
        let channel = self.channel.clone();
        tokio::spawn(async move {
            let mut r = realm.lock().await;
            if r.probes.get(&alias).is_some_and(|conn| conn.sender.same_channel(&channel)) {
                r.probes.remove(&alias);
            }
        });
    }
}
