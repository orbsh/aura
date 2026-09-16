//! Phase 3 end-to-end: a real probe dials the engine's probe gateway; the
//! realm routes an invoke through the wire; the probe's resident session
//! executes; the result round-trips.

use aura_actor::{ActorType, InstanceId, Body};
use aura_engine::{Engine, probes};
use std::time::Duration;

#[tokio::test]
async fn remote_probe_roundtrip() {
    let engine = Engine::start(&Default::default()).await.expect("engine boot");

    // Gateway on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // serve_probes takes the addr; hand it the bound listener's port by
    // spawning with the addr it already bound — reuse accept via a thin
    // wrapper: pass "127.0.0.1:0" would rebind; instead serve on our
    // pre-bound listener via the exported helper (accept over the listener).
    tokio::spawn(probes::serve_probes_listener(engine.realm.clone(), listener));

    // Register a remote probe actor type pointing at the node the probe
    // will claim.
    engine.register(ActorType {
        name: "remote-counter".into(),
        body: Body::RemoteProbe {
            node_alias: "test-node".into(),
            language: "steel".into(),
            source: r#"
(define (double args)
  (hash "doubled" (* 2 (hash-ref args "n"))))
"#
            .into(),
        },
        idle_ttl: Some(Duration::from_secs(60)),
        on_sleep: None,
        on_wake: None,
        receives: vec![],
    })
    .await;

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
            InstanceId { actor_type: "remote-counter".into(), key: "k".into() },
            "double",
            serde_json::json!({"n": 4}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"doubled": 8}));

    probe.abort();
}

fn probe_config_shim(port: u16) -> probe_config::ProbeConfig {
    probe_config::ProbeConfig {
        control_plane_url: format!("ws://127.0.0.1:{port}"),
        credential_env: "PROBE_E2E_CREDENTIAL".into(),
        capabilities: probe_config::CapabilitySurface {
            node_alias: "test-node".into(),
            carriers: vec!["steel".into()],
            ..Default::default()
        },
    }
}
