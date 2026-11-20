//! Effector connection gateway (Phase 3): accept the effectors' OUTBOUND WS
//! connections (they dial us), register them by node alias, and correlate
//! Result frames back to in-flight realm calls.

use aura_realm::SharedRealm;
use effector_protocol::{Frame, HostFrame};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

/// Accept loop: one task per engine; each connection gets a writer task
/// (realm.effectors holds the sender) and a reader task (correlates results).
pub async fn serve_effectors(realm: SharedRealm, addr: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("effector gateway listening on {addr}");
    serve_effectors_listener(realm, listener).await
}

/// Variant taking a pre-bound listener (tests bind :0 to learn the port).
pub async fn serve_effectors_listener(
    realm: SharedRealm,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    // Honest disclosure (ADR-0015 §7): registrations here are unauth-
    // enticated — whoever can reach this port may claim any alias. The
    // trust-mode switch and the keypair handshake ride the prism gateway
    // (connection plane); this line states the CURRENT posture where it
    // is visible (the log), not only in some config file.
    eprintln!(
        "effector gateway: registrations are unauthenticated — the network is the boundary \
         (node identity: ADR-0015, prism gateway plane)"
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        let realm = realm.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(realm, stream, peer).await {
                eprintln!("effector connection error: {e:#}");
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

    // The node speaks first, and it declares shape only: alias + carriers. Identity rides the
    // dial as a token, and this gateway is the UNPLANED one — it has no registry to check that
    // token against (the registry is prism's, ADR-0015 §3), which is why the disclosure above
    // says the network is the boundary here. No challenge, no signature: a proof carried inside
    // a frame is a credential inside every log that carries frames (effector ADR-0003).
    let (node_alias, carriers, class, machine) = match stream.next().await {
        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Frame>(&text)? {
            Frame::Register {
                node_alias,
                carriers,
                // The class is a deployment fact this unplanned gateway does not act on: it has no
                // routing table to refuse with. Recorded in the pool all the same, because
                // selection (ADR-0053) is a question about the node, not about this gateway.
                class,
                // Which box this is (effector ADR-0006): self-reported, and it authorizes
                // nothing — the alias names the connection, the machine names the hardware.
                machine,
            } => (node_alias, carriers, class, machine),
            other => anyhow::bail!("expected Register, got {other:?}"),
        },
        other => anyhow::bail!("bad register frame: {other:?}"),
    };
    sink.send(Message::Text(serde_json::to_string(&Frame::Registered)?))
        .await?;

    // Writer channel: realm calls push frames; this task owns the sink.
    // A clone stays with the reader so it can answer host calls.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    let entry = aura_realm::ActuatorConn {
        sender: tx.clone(),
        peer,
    };
    {
        // The pool, not the realm: a node is reachable from every realm the venue
        // creates, and a job's realm is created after this line (ADR-0053). The
        // handle is read out from under the lock and used after it.
        let pool = {
            let r = realm.lock().await;
            r.pool.clone()
        };
        let record = aura_realm::pool::NodeRecord {
            alias: node_alias.clone(),
            conn: entry.clone(),
            carriers: carriers.clone(),
            class,
            machine: machine.clone(),
            // Nowhere. Labels are the ADMITTING party's word, never the node's own
            // (effector ADR-0005 §2b): a node that could name its labels could name
            // someone else's. This gateway checks no grant — it has no registry to read
            // — so it is in no position to place a node, and every node it admits is
            // unplaced: reachable by jobs that ask for no place, by none that does.
            // Prism, which mints labels from a grant at admission, fills this in.
            labels: vec![],
        };
        let admitted = pool.insert(record);
        if let Some(old) = &admitted.displaced {
            // Alias replacement (in `open` posture a restarted container
            // must be able to reclaim its name from a stale registration).
            // Allowed — but never silent (ADR-0015 §7 replacement
            // discipline): the event names the alias and BOTH peers, so
            // an operator can always see where their calls actually go.
            eprintln!(
                "effector gateway: alias '{node_alias}' REPLACED — old peer {} displaced by new peer {peer:?}",
                old.conn.peer
            );
            // The old writer channel dies when its sender is dropped by
            // the map overwrite; its reader task exits on send failure.
        }
        // One box, two names. Nothing here decides that the two connections are one node:
        // it says what it sees, and an operator reading it knows which machine restarted or
        // which serial two nodes are both claiming (effector ADR-0006).
        if !admitted.same_machine.is_empty() {
            eprintln!(
                "effector gateway: machine '{}' is also connected as {} — the same box under \
                 another alias, or a serial two nodes both claim",
                machine,
                admitted.same_machine.join(", ")
            );
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
            if sink
                .send(Message::Text(serde_json::to_string(&frame)?))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    // Reader loop: correlate Result frames to pending remote calls.
    while let Some(msg) = stream.next().await {
        let Message::Text(text) = msg? else { continue };
        match serde_json::from_str::<Frame>(&text)? {
            // THE NODE IS STILL MOVING (effector `Frame::Progress`). Nothing is answered and
            // nothing is resolved: the only consumer is the deadline, which reads silence, and this
            // frame is the one thing that turns silence into evidence. An unknown `call_id` is a
            // call the caller already gave up on — the report is dropped, like a late answer.
            Frame::Progress { call_id } => {
                let realm = realm.lock().await;
                if let Some(pending) = realm.pending_remote.get(&call_id) {
                    pending
                        .activity
                        .store(aura_realm::mq::now_ms(), std::sync::atomic::Ordering::Relaxed);
                }
            }
            Frame::Result(result) => {
                let mut r = realm.lock().await;
                if let Some(pending) = r.pending_remote.remove(&result.call_id) {
                    let _ = pending.reply.send(result.outcome);
                }
                // Unknown call_id: the caller timed out and was removed —
                // drop the late result (the pending_calls scan owns
                // timeout semantics; a late answer is not re-delivered).
            }
            // THE NODE'S ANSWER TO A STOP (ADR-0047 stage two). The realm does not wait
            // for it — its terminal value is decided by the time this frame is sent — so
            // this is where the honest half lands instead of being dropped: a node that
            // could NOT reach the work (steel and python are in-process VMs pinned to
            // their thread; a call that never armed a handle) is said out loud, so
            // "it stopped" is never the reading of silence.
            Frame::Interrupted { call_id, landed } => {
                if !landed {
                    eprintln!(
                        "aura: node '{node_alias}' could not reach call {call_id} — no \
                         carrier on it can act on a stop, so that call kept running"
                    );
                }
            }
            Frame::Host(HostFrame::Call(call)) => {
                // Ctx bridge over the wire: resolve against the instance
                // the enclosing remote call was routed to (looked up from
                // pending_remote by the call_id the effector carries), then
                // reply on this connection's writer. SPAWNED, never awaited
                // inline: the resolution parks on a hot realm call whose
                // Result frame can only be read by THIS loop, so a ctx
                // target on this same node would wait for a message only
                // this loop can deliver (self-deadlock to the deadline).
                let realm = realm.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let instance = {
                        let r = realm.lock().await;
                        r.pending_remote
                            .get(&call.call_id)
                            .map(|p| p.instance.clone())
                    };
                    let outcome = match instance {
                        Some(inst) => {
                            crate::host_wire::resolve_host_call(&realm, &inst, &call.op).await
                        }
                        None => Err(
                            "unknown call_id: the enclosing remote call is not in flight".into(),
                        ),
                    };
                    tx.send(Frame::Host(HostFrame::Result(
                        effector_protocol::HostResult {
                            host_call_id: call.host_call_id,
                            outcome,
                        },
                    )))
                    .ok();
                });
            }
            other => anyhow::bail!("unexpected frame from effector: {other:?}"),
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
        let Some(realm) = self.realm.take() else {
            return;
        };
        let alias = std::mem::take(&mut self.alias);
        let channel = self.channel.clone();
        tokio::spawn(async move {
            // Identity-checked inside the pool: a reconnect that took the alias over
            // keeps its name, and this guard's channel no longer matches it.
            let pool = {
                let r = realm.lock().await;
                r.pool.clone()
            };
            pool.remove(&alias, &channel);
        });
    }
}
