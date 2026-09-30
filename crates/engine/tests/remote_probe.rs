//! Phase 3 end-to-end: a real probe dials the engine's probe gateway; the
//! realm routes an invoke through the wire; the probe's resident session
//! executes; the result round-trips.

use aura_booth::{BoothType, InstanceId, Body};
use aura_engine::{Engine, probes};
use std::time::Duration;

#[tokio::test]
async fn remote_probe_roundtrip() {
    // ADR-0027: the frame carries a content reference — boot with a code
    // source serving the handler bytes under their hash (the fetch +
    // verify + mismatch + cache matrix is locked probe-side in
    // remote.rs::code_ref_fetch_verify_cache_and_mismatch_rejection).
    const SRC: &str = r#"
(define (double args)
  (let* ((echoed (ctx_invoke (hash "type" "echo" "key" "e1" "handler" "execute" "args" (hash "x" 1)))))
    (hash "doubled" (* 2 (hash-ref args "n"))
          "echo" (hash-ref echoed "x"))))
"#;
    let http_port = serve_code_source(SRC);
    let engine = Engine::start(&aura_config::EngineConfig {
        code_base_url: Some(format!("http://127.0.0.1:{http_port}")),
        ..Default::default()
    })
    .await
    .expect("engine boot");

    // Gateway on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // serve_probes takes the addr; hand it the bound listener's port by
    // spawning with the addr it already bound — reuse accept via a thin
    // wrapper: pass "127.0.0.1:0" would rebind; instead serve on our
    // pre-bound listener via the exported helper (accept over the listener).
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), listener));

    // Register a remote probe booth type pointing at the node the probe
    // will claim.
    engine.register(BoothType {
        name: "remote-counter".into(),
        body: Body::RemoteProbe {
            encoding: aura_booth::ChannelEncoding::Json,
            node_alias: "test-node".into(),
            language: "steel".into(),
            source: SRC.into(),
        },
        idle_ttl: Some(Duration::from_secs(60)),
        max_exec: None,
        on_sleep: None,
        on_wake: None,
        receives: vec![],
    })
    .await.unwrap();

    // Local target for ctx_invoke from the probe script.
    engine.register(
        BoothType::script(
            "echo",
            "steel",
            r#"(define (execute args) args)"#,
        )
    )
    .await.unwrap();

    // Probe side: dial in (runs until the test ends).
    let config = probe_config_shim(port);
    std::env::set_var("PROBE_E2E_CREDENTIAL", "tok");
    let probe = tokio::spawn(probe_runtime::remote::run(config));

    // Wait for registration, then invoke through the wire.
    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key("test-node") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let out = engine
        .invoke(
            InstanceId { booth_type: "remote-counter".into(), key: "k".into() },
            "double",
            serde_json::json!({"n": 4}),
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        serde_json::json!({"doubled": 8, "echo": 1}),
        "ctx invoke resolved over the wire"
    );

    // A remote execution node holds NO state. The ctx bridge over the
    // wire carries ctx_invoke (plus the ADR-0034 iterate verbs and —
    // Phase 4.14 gate 1 — ctx_store_emit, which executes against the
    // type's resolved storage plan; remote types register no schema, so
    // that arm answers the named no-plan error here — the store round
    // trip over a live plan is locked in exec_booth.rs's bgi test). The
    // retired instance-document assertions are gone with the model
    // (ADR-0026 §3).

    // Phase 2.6 acceptance (c/d): connection drop flips registry presence,
    // and the call path fails fast with the normal error-value semantics
    // (residency declared lost, never silently kept). abort → reader sees
    // EOF → the gateway removes the alias.
    probe.abort();
    for _ in 0..50 {
        if !engine.realm.try_lock().unwrap().probes.contains_key("test-node") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let gone = !engine.realm.try_lock().unwrap().probes.contains_key("test-node");
    assert!(gone, "dropped connection unregisters the node alias");
    let err = engine
        .invoke(
            InstanceId { booth_type: "remote-counter".into(), key: "k".into() },
            "double",
            serde_json::json!({"n": 1}),
        )
        .await
        .expect_err("calls to a departed node fail as error values");
    assert!(
        err.to_string().contains("not connected"),
        "fast failure names the departed node: {err}"
    );
}

/// Serve `src` bytes at any path over plain HTTP for as many requests as
/// the test makes (the probe fetches by hash; a per-test source is the
/// simplest stand-in for the future prism /code export).
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

fn probe_config_shim(port: u16) -> probe_config::ProbeConfig {
    probe_config::ProbeConfig {
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        // The deployed remote-actuator form is the wrapped one; this test
        // exercises the call path, not the sandbox (steel runs in-process).
        sandbox: true,
        credential_env: "PROBE_E2E_CREDENTIAL".into(),
        capabilities: probe_config::CapabilitySurface {
            node_alias: "test-node".into(),
            carriers: vec!["steel".into()],
            ..Default::default()
        },
    }
}

// ADR-0027: remote code travels by content reference. The blob lives in
// the meta store under its sha256 (written at register); the frame
// carries url + hash; the probe fetches from the configured base,
// verifies against the frame's hash, and executes. Mismatch or missing
// source = error value (locked probe-side in remote.rs tests).
#[tokio::test]
async fn remote_code_travels_as_reference() {
    // Static code source: serve the registered booth's bytes.
    let code_src = r#"
(define (double args) (hash "doubled" (* 2 (hash-ref args "n"))))
"#;
    let sha = aura_realm::meta::code_hash(code_src);
    let hex = aura_realm::meta::code_hex(&sha);
    let http_port = serve_code_source(code_src);

    let engine = Engine::start(&aura_config::EngineConfig {
        code_base_url: Some(format!("http://127.0.0.1:{http_port}")),
        ..Default::default()
    })
    .await
    .expect("engine boot");

    let gw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = gw.local_addr().unwrap().port();
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), gw));
    let probe = tokio::spawn(probe_runtime::remote::run(probe_config_shim(port)));
    std::env::set_var("PROBE_E2E_CREDENTIAL", "tok");

    // Register through the REAL path (register_inner persists the blob —
    // the reference the dispatch arm builds must resolve in the source).
    engine
        .register(aura_booth::BoothType {
            name: "ref-counter".into(),
            body: Body::RemoteProbe {
            encoding: aura_booth::ChannelEncoding::Json,
                node_alias: "test-node".into(),
                language: "steel".into(),
                source: code_src.into(),
            },
            idle_ttl: None,
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: vec![],
        })
        .await
        .unwrap();
    // The blob landed under the content address.
    {
        let r = engine.realm.try_lock().unwrap();
        let stored = aura_realm::meta::get_blob(&r.mq, &sha).expect("blob stored at register");
        assert_eq!(stored, code_src.as_bytes());
    }

    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key("test-node") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let out = engine
        .invoke(
            InstanceId { booth_type: "ref-counter".into(), key: "k".into() },
            "double",
            serde_json::json!({"n": 21}),
        )
        .await
        .expect("reference delivery");
    assert_eq!(out["doubled"], 42, "probe fetched + verified via {hex}");
    probe.abort();
}

// ---- ADR-0034: the iterate verbs cross the remote probe wire ----

/// Producer lives on the PROBE (remote python booth, generator handler);
/// the local consumer pulls through Realm::iterate — each pull is one
/// ToolCall frame (kind=iterate_next) and the envelope rides home as the
/// ToolResult. This is the remote producer leg.
#[cfg(feature = "python")]
#[tokio::test]
async fn remote_python_booth_produces_stream_locally_consumed() {
    const SRC: &str = r#"
def tokens(args):
    for t in args["list"]:
        yield t
"#;
    let http_port = serve_code_source(SRC);
    let engine = Engine::start(&aura_config::EngineConfig {
        code_base_url: Some(format!("http://127.0.0.1:{http_port}")),
        ..Default::default()
    })
    .await
    .expect("engine boot");

    let gw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = gw.local_addr().unwrap().port();
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), gw));
    let mut config = probe_config_shim(port);
    config.capabilities.carriers = vec!["python".into()];
    std::env::set_var("PROBE_E2E_CREDENTIAL", "tok");
    let probe = tokio::spawn(probe_runtime::remote::run(config));

    engine
        .register(BoothType {
            name: "remote-prod".into(),
            body: Body::RemoteProbe {
            encoding: aura_booth::ChannelEncoding::Json,
                node_alias: "test-node".into(),
                language: "python".into(),
                source: SRC.into(),
            },
            idle_ttl: Some(Duration::from_secs(60)),
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: vec![],
        })
        .await
        .unwrap();

    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key("test-node") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let target = InstanceId { booth_type: "remote-prod".into(), key: "p1".into() };
    let mut got = Vec::new();
    let mut sid: Option<String> = None;
    loop {
        let op = match &sid {
            None => aura_booth::IterateOp::Start {
                target: target.clone(),
                handler: "tokens".into(),
                args: serde_json::json!({ "list": ["x", "y", "z"] }),
            },
            Some(s) => aura_booth::IterateOp::Next { stream_id: s.clone() },
        };
        let slot = aura_realm::Realm::iterate(&engine.realm, op).await.unwrap();
        let aura_booth::call::Waited::Done(result) = slot.wait().await.unwrap() else {
            panic!("iterate is hot-only");
        };
        let env = result.expect("remote pull must succeed");
        if env.get("stream_id").and_then(|s| s.as_str()).is_some() {
            sid = Some(env["stream_id"].as_str().unwrap().to_string());
        }
        let e = aura_booth::Envelope::from_value(&env).unwrap();
        if e.done {
            break;
        }
        got.push(e.item.unwrap());
        if got.len() > 5 {
            panic!("remote stream must terminate: {got:?}");
        }
    }
    assert_eq!(got, vec!["x", "y", "z"], "generator across the wire, item by item");
    assert_eq!(engine.realm.lock().await.streams.len(), 0, "done drains the registry");

    probe.abort();
}

/// Consumer lives on the PROBE (remote python booth running the loaded
/// ctx_iterate wrapper); the producer is a LOCAL booth. The host fns
/// ctx_iter_start/next/dispose cross back over Frame::Host — the remote
/// consumer leg.
#[cfg(feature = "python")]
#[tokio::test]
async fn remote_python_booth_consumes_local_stream() {
    const PROD: &str = r#"
def tokens(args):
    for t in args["list"]:
        yield t
"#;
    const CONS: &str = r#"
def consume(args):
    got = []
    for tok in ctx_iterate("local-prod", "p1", "tokens", {"list": args["list"]}):
        got.append(tok)
        if len(got) == 2:
            break
    return {"got": got}
"#;
    // Serve the CONS bytes (the registered remote source) — the local
    // producer needs no fetch; the probe's CodeRef addresses CONS alone.
    let http_port = serve_code_source(CONS);
    let engine = Engine::start(&aura_config::EngineConfig {
        code_base_url: Some(format!("http://127.0.0.1:{http_port}")),
        ..Default::default()
    })
    .await
    .expect("engine boot");

    let gw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = gw.local_addr().unwrap().port();
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), gw));
    let mut config = probe_config_shim(port);
    config.capabilities.carriers = vec!["python".into()];
    std::env::set_var("PROBE_E2E_CREDENTIAL", "tok");
    let probe = tokio::spawn(probe_runtime::remote::run(config));

    engine.register(BoothType::script("local-prod", "python", PROD)).await.unwrap();
    engine
        .register(BoothType {
            name: "remote-cons".into(),
            body: Body::RemoteProbe {
            encoding: aura_booth::ChannelEncoding::Json,
                node_alias: "test-node".into(),
                language: "python".into(),
                source: CONS.into(),
            },
            idle_ttl: Some(Duration::from_secs(60)),
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: vec![],
        })
        .await
        .unwrap();

    for _ in 0..50 {
        if engine.realm.try_lock().unwrap().probes.contains_key("test-node") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let out = engine
        .invoke(
            InstanceId { booth_type: "remote-cons".into(), key: "c1".into() },
            "consume",
            serde_json::json!({ "list": ["a", "b", "c", "d"] }),
        )
        .await
        .expect("remote consumer");
    assert_eq!(out, serde_json::json!({ "got": ["a", "b"] }), "break after two of four");

    // The remote consumer's wrapper auto-disposed on GeneratorExit; the
    // local producer's stream must be gone from the registry.
    assert_eq!(
        engine.realm.lock().await.streams.len(),
        0,
        "dispose crossed Frame::Host and drained the local registry"
    );
    probe.abort();
}
