//! `ctx.resource` for code the VENUE contains (ADR-0048/0049 + effector ADR-0003 §1).
//!
//! Over the wire two parties answer a resource ask: the plane mints the grant, the node moves the
//! bytes. In-process there is nobody to ask and no wire to wait on — the venue is its own node — so
//! the whole chain has to close inside one host call: registry row → material → transfer → a path on
//! this machine. This test walks that chain with a real repository, because the failure mode a fake
//! would hide is exactly the interesting one (a path the script is told and cannot open).
//!
//! The account is the ENVELOPE's (`sender.user`), recorded once at the entrance: a script never names
//! its tenant, and the test proves it by never passing one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::BoothType;
use aura_config::EngineConfig;
use aura_engine::Engine;
use std::path::Path;

fn script() -> &'static str {
    r#"
;; The script asks by KIND and by its own name under the base. It never sees a credential and never
;; names an account — both are the host's business.
(define (fetch args)
  (ctx.resource (hash "op" "get" "kind" "git" "remote" "mine")))
"#
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
async fn an_in_process_booth_gets_a_path_from_the_venue_itself() {
    let dir = std::env::temp_dir().join(format!("aura-ctx-resource-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let base = dir.join("base");
    // `<base>/<tenant>/<kind>/<remote>`: the prefix is the venue's to compose, the last segment is the
    // name the script asked for.
    let bare = base.join("alice").join("git").join("mine");
    let work = dir.join("work");
    std::fs::create_dir_all(&bare).expect("a dir");
    git(&bare, &["init", "--bare", "-q", "."]);
    std::fs::create_dir_all(&work).expect("a dir");
    git(&work, &["init", "-q", "-b", "main", "."]);
    std::fs::write(work.join("readme.txt"), "the venue moved these bytes\n").expect("a file");
    git(&work, &["add", "."]);
    git(&work, &["commit", "-q", "-m", "one"]);
    git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
    git(&work, &["push", "-q", "origin", "main"]);

    // Where the venue puts what it fetches. The node reads the same variable: it is one fact —
    // "the directory this process materialises into".
    std::env::set_var("EFFECTOR_RESOURCES_ROOT", dir.join("root"));

    let config = EngineConfig {
        resources: [(
            "git".to_string(),
            aura_config::ResourceConfig {
                base: base.display().to_string(),
                mode: "ro".into(),
                secret_env: String::new(),
                desc: "the test's repositories".into(),
            },
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    let engine = Engine::start(&config).await.expect("engine boot");
    engine
        .register(BoothType::script("fetcher", "steel", script()))
        .await
        .expect("the booth registers");

    // The account rides the envelope, not the script's ask.
    let out = engine
        .invoke(
            aura_booth::InstanceId {
                booth_type: "fetcher".into(),
                key: aura_booth::InstanceKey::Singleton,
            },
            "fetch",
            serde_json::json!({ "sender": { "user": "alice" } }),
        )
        .await
        .expect("the venue answers");

    let path = out.as_str().expect("a path, in words");
    assert!(
        path.starts_with(dir.join("root").to_str().unwrap()),
        "the data landed beside the venue, under its own root: {path}"
    );
    assert_eq!(
        std::fs::read_to_string(Path::new(path).join("readme.txt")).expect("the clone is readable"),
        "the venue moved these bytes\n"
    );
    let _ = std::fs::remove_dir_all(&dir);
}