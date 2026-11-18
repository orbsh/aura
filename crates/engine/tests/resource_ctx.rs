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

/// A grant is a ROW the plane's management API writes and withdraws — that is what makes it
/// changeable while the venue runs. Two facts this test pins: the listing never carries the
/// credential (a grant names its key), and a withdrawn kind disappears from it, so "declared" and
/// "listed" cannot disagree.
#[tokio::test]
async fn a_grant_is_a_row_that_can_be_withdrawn() {
    let engine = Engine::start(&EngineConfig::default())
        .await
        .expect("engine boot");
    assert!(engine.grants().await.expect("listing").is_empty());

    engine
        .declare_grant(
            "object",
            aura_realm::resources::Grant {
                desc: "the org's bucket".into(),
                base: "s3://bucket".into(),
                mode: aura_realm::resources::Mode::ReadWrite,
                secret_name: "S3_KEY".into(),
            },
            Some("the-secret-value"),
        )
        .await
        .expect("declared");

    let rows = engine.grants().await.expect("listing");
    assert_eq!(rows.len(), 1, "one row per kind");
    assert_eq!(rows[0].0, "object");
    assert_eq!(rows[0].1["base"], "s3://bucket");
    assert_eq!(rows[0].1["mode"], 1, "rw");
    assert_eq!(rows[0].1["secret_name"], "S3_KEY");
    assert!(
        rows[0].1.get("value").is_none(),
        "reading a grant must never be reading the key: {}",
        rows[0].1
    );

    engine.revoke_grant("object").await.expect("withdrawn");
    assert!(
        engine.grants().await.expect("listing").is_empty(),
        "a withdrawn grant that still listed would be a grant nobody can see"
    );
}
