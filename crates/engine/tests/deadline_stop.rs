//! A deadline ends the CALL and tells the NODE (ADR-0047 stage two, extended to timeouts).
//!
//! What is measured is silence, not wall time: the node reports `Frame::Progress` while a transfer
//! runs (effector 8efa811), so a call that is quiet for the whole grace really did move nothing.
//! The healthy-transfer half of that pair — that a running child DOES report — is locked where the
//! observations are (effector `a_running_child_reports_that_it_is_alive`); a test here cannot make a
//! node lie about progress it did not have.
//!
//! Why this is a test and not a comment: the plane used to abandon the result and nothing else. The
//! node kept executing — and for `ctx.resource` that means it kept moving bytes that nobody would
//! ever read, which is the ordinary case as soon as a repository is big enough to blow a deadline.
//! The fix is a third arm in the dispatch's `select!`: the same `Frame::Interrupt` an operator's
//! stop sends, raised on the clock.
//!
//! What this test proves and what it does not: it proves the caller stops waiting AND the node is
//! told (the connection stays up, and the call ends with the named timeout rather than hanging). The
//! kill inside the node — a transfer child being reaped — is locked where it happens, effector-side
//! (`a_child_is_killed_when_the_call_is_cancelled`) and venue-side
//! (`a_cancelled_activation_never_starts_the_transfer`). A test cannot watch a child it does not own.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::{Body, BoothType, Concurrency, InstanceId, InstanceKey};
use aura_engine::{effectors, Engine};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// One handler that outlives the deadline by a wide margin, so "it returned" cannot be confused with
/// "the script finished".
const SLOW: &str = r#"
;; Five seconds of sleep: an order of magnitude past the deadline below, so "it returned" cannot be
;; confused with "the script finished".
(define (slow args)
  (time/sleep-ms 5000)
  (hash "finished" #t))
"#;

fn serve_code_source(src: &str) -> u16 {
    use std::io::{Read as _, Write as _};
    let bytes = src.as_bytes().to_vec();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let body = bytes.clone();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    port
}

#[tokio::test]
async fn a_deadline_tells_the_node_to_stop() {
    let http_port = serve_code_source(SLOW);
    let engine = Engine::start(&aura_config::EngineConfig {
        code_base_url: Some(format!("http://127.0.0.1:{http_port}")),
        ..Default::default()
    })
    .await
    .expect("engine boot");

    let gw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = gw.local_addr().unwrap().port();
    tokio::spawn(effectors::serve_effectors_listener(
        engine.realm.clone(),
        gw,
    ));

    engine
        .register(BoothType {
            name: "remote-slow".into(),
            body: Body::Remote {
                encoding: aura_booth::Encoding::Json,
                node_alias: "test-node".into(),
                language: "steel".into(),
                source: SLOW.into(),
                requires: Some(aura_booth::Requires::Host),
            },
            idle_ttl: Some(Duration::from_secs(60)),
            max_exec: None,
            hooks: BTreeMap::new(),
            receives: vec![],
            concurrency: Concurrency::Queued,
        })
        .await
        .unwrap();

    // The deadline is the TYPE's (its call spec), which is also what the dispatch reads: one number,
    // so the caller's wait and the node's instruction cannot disagree.
    engine
        .realm
        .lock()
        .await
        .declare_call(
            "remote-slow",
            aura_booth::call::CallSpec::hot(Duration::from_millis(150)),
        );

    let mut config = effector_config::EffectorConfig {
        class: effector_config::Class::Host,
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        token: "test-token".into(),
        capabilities: effector_config::CapabilitySurface {
            node_alias: "test-node".into(),
            carriers: vec!["steel".into()],
            ..Default::default()
        },
        lease: Default::default(),
    };
    config.capabilities.carriers = vec!["steel".into()];
    std::env::set_var("PROBE_E2E_CREDENTIAL", "tok");
    let effector = tokio::spawn(effector_runtime::remote::run(config));

    for _ in 0..50 {
        if engine
            .realm
            .try_lock()
            .unwrap()
            .effectors
            .contains_key("test-node")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let started = Instant::now();
    let err = engine
        .invoke(
            InstanceId {
                booth_type: "remote-slow".into(),
                key: InstanceKey::Named("k".into()),
            },
            "slow",
            serde_json::json!({}),
        )
        .await
        .expect_err("the deadline is a failure, not a result");
    let elapsed = started.elapsed();

    // The script sleeps for five seconds; returning in well under two means the DEADLINE ended this
    // call, not the work.
    assert!(
        elapsed < Duration::from_secs(2),
        "the caller stopped at its deadline, not at the script's end: {elapsed:?}"
    );
    let text = err.to_string();
    // The deadline now measures SILENCE, not wall time (ADR-0047 as extended): the sentence says
    // what the plane observed, so an operator reading a log knows whether the work was slow or still.
    assert!(text.contains("without progress"), "{text}");

    // The node was TOLD, and this is the observable half: the caller's wait never touches
    // `pending_remote` — only the dispatch's own deadline arm clears it, and that arm raises the
    // same `Frame::Interrupt` an operator's stop does. A caller-side abandonment would leave the
    // entry behind until the node answered.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        engine.realm.lock().await.pending_remote.len(),
        0,
        "the dispatch's deadline arm ran: it cleared the pending entry and sent the interrupt"
    );
    // The node is still connected: stopping a call is not disconnecting a node.
    assert!(
        engine
            .realm
            .try_lock()
            .unwrap()
            .effectors
            .contains_key("test-node"),
        "the node stays registered after its call was stopped"
    );

    effector.abort();
}