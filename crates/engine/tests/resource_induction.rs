//! `ctx.resource` across a PROCESS boundary (ADR-0048/0049 + effector ADR-0003 §1).
//!
//! The in-process bridge (`resource_ctx.rs`) answers a script without a wire because the venue is
//! its own node. An `induction` carrier is the opposite case: the code runs in a child process, so
//! the ask leaves as a typed frame (`{"type": "resource", "op": …}`), the host fn does the mint and
//! the transfer, and only the ANSWER crosses back. This test walks that path with a real
//! repository — the failure mode a fake would hide is a host-fn table that never had the arm
//! registered, which shows up as "host fn 'resource' not bridged" only when a script asks.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::{BoothType, InstanceId, InstanceKey};
use aura_config::EngineConfig;
use aura_engine::Engine;
use std::path::Path;

/// The fixture speaks the line protocol by hand (effector `examples/induction_loop.rs`); the spec is
/// its path, resolved by the carrier's spawn.
fn bin(name: &str) -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../effector/target/debug/examples/"
    );
    let full = format!("{path}{name}");
    assert!(
        std::path::Path::new(&full).exists(),
        "{name} missing — build it in ~/world/effector: cargo build -p effector-runtime --examples"
    );
    full
}

fn git(dir: &Path, args: &[&str]) {
    let out = std::process::Command::new("git")
        .args(["-c", "user.email=a@b", "-c", "user.name=t"])
        .args(args)
        .current_dir(dir)
        .env("NO_PROXY", "*")
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn an_induction_child_asks_the_venue_and_gets_a_path() {
    let dir = std::env::temp_dir().join(format!("aura-res-ind-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let base = dir.join("base");
    let bare = base.join("alice").join("git").join("mine");
    let work = dir.join("work");
    std::fs::create_dir_all(&bare).expect("a dir");
    git(&bare, &["init", "--bare", "-q", "."]);
    std::fs::create_dir_all(&work).expect("a dir");
    git(&work, &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.join("readme.txt"), "moved across a process boundary\n").expect("a file");
    git(&work, &["add", "."]);
    git(&work, &["commit", "-q", "-m", "one"]);
    git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
    git(&work, &["push", "-q", "origin", "main"]);

    std::env::set_var("EFFECTOR_RESOURCES_ROOT", dir.join("root"));

    let engine = Engine::start(&EngineConfig::default())
        .await
        .expect("engine boot");
    // A grant is a row written through the plane's management API, not a line in a config file: it
    // can change while the venue runs, and a script has no arm that could write it.
    engine
        .declare_grant(
            "git",
            aura_realm::resources::Grant {
                desc: "the test's repositories".into(),
                base: base.display().to_string(),
                mode: aura_realm::resources::Mode::ReadOnly,
                secret_name: String::new(),
            },
            None,
        )
        .await
        .expect("the grant is declared");
    engine
        .register(BoothType::script(
            "bin-resource",
            "induction",
            bin("induction_loop"),
        ))
        .await
        .expect("the booth registers");

    // The child forwards its `op` verbatim; the host fn parses it (the schema-blind rule means the
    // child never learns the shape, so the shape lives in one place — `ResourceOp`).
    let out = engine
        .invoke(
            InstanceId {
                booth_type: "bin-resource".into(),
                key: InstanceKey::Named("k1".into()),
            },
            "resource_round_trip",
            serde_json::json!({
                "sender": { "user": "alice" },
                "op": { "op": "get", "kind": "git", "remote": "mine" },
            }),
        )
        .await
        .expect("the venue answers a child too");

    let path = out.as_str().expect("a path, in words");
    assert_eq!(
        std::fs::read_to_string(Path::new(path).join("readme.txt"))
            .expect("the child's answer names a readable clone"),
        "moved across a process boundary\n"
    );
    let _ = std::fs::remove_dir_all(&dir);
}