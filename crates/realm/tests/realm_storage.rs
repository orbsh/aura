#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0050 step 1a at the STORAGE layer: what a realm's keys actually look
//! like. Read back through the ENGINE, not through the Collection API — a
//! Collection hides the prefix, and the prefix is the whole point.

use aura_realm::mq::MqStore;
use aura_realm::realm_id::RealmId;
use aura_realm::realm_set::{RealmSet, REALMS_NS, MAIN_REALM};
use aura_realm::timer_entry;
use okm_core::storage::VirtualStorage;

#[tokio::test]
async fn a_key_in_main_is_prefixed_with_realm_id_one() {
    let root = MqStore::mem();
    let set = RealmSet::with_mq(root.clone());
    let main = set.open(MAIN_REALM).await.unwrap();
    // A fresh store issues 1 to the first realm it is asked for, and that is
    // `main` (ADR-0050 §4/§No migration: the layout starts at `1 → main`).
    assert_eq!(main.id, RealmId::Persisted(1));

    // A key written through the realm's own handle …
    main.store()
        .put(b"probe".to_vec(), b"value".to_vec())
        .unwrap();

    // … read back through the engine, where the four id bytes are visible.
    let keys: Vec<Vec<u8>> = root
        .root()
        .scan_range(&[0, 0, 0, 1], Some(&[0, 0, 0, 2]))
        .map(|item| item.map(|(k, _)| k))
        .collect::<okm_core::Result<Vec<_>>>()
        .unwrap();
    let probe = keys
        .iter()
        .find(|k| k.ends_with(b"probe"))
        .unwrap_or_else(|| panic!("the written key is not under [0,0,0,1]: {keys:?}"));
    assert_eq!(
        &probe[..4],
        &[0, 0, 0, 1],
        "a key in `main` starts with the realm's id: {probe:02x?}"
    );

    // And there is no third place a key can live: every key in the engine is
    // either inside a realm's four-byte prefix or a root-band row (the
    // `realms` table). "Unprefixed" is not a shape any more.
    let everything: Vec<Vec<u8>> = root
        .root()
        .scan_range(&[], None)
        .map(|item| item.map(|(k, _)| k))
        .collect::<okm_core::Result<Vec<_>>>()
        .unwrap();
    assert!(!everything.is_empty());
    let ns = REALMS_NS.to_be_bytes();
    for key in &everything {
        let in_realm = key.len() >= 4 && key[..3] == [0, 0, 0];
        let at_root = key.starts_with(&ns);
        assert!(
            in_realm || at_root,
            "a key belongs to a realm or to the root band, never to nothing: {key:02x?}"
        );
    }
}

/// The same restart, but across a real process boundary. fjall holds an exclusive lock on the open
/// store and okm's series log names the holder, so a second open of the same directory cannot happen
/// inside one process — and that is exactly the claim: `main` survives a reboot as id 1 and is not
/// re-allocated as a second realm, because the id is a row rather than a memory.
#[test]
fn the_row_that_names_main_outlives_the_process_that_wrote_it() {
    let dir = std::env::temp_dir().join(format!("aura-realm-row-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // The parent writes the rows and exits its runtime.
    {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let set = RealmSet::with_mq(MqStore::fjall(
                okm_core::FjallStore::open(&dir, "aura").unwrap(),
            ));
            assert_eq!(
                set.open(MAIN_REALM).await.unwrap().id,
                RealmId::Persisted(1)
            );
            assert_eq!(set.open("scratch").await.unwrap().id, RealmId::Persisted(2));
        });
    }

    // The child is a fresh process: it shares the directory and nothing else.
    let exe = std::env::current_exe().expect("the test binary's path");
    let out = std::process::Command::new(exe)
        .args([
            "--exact",
            "the_child_reads_what_the_parent_wrote",
            "--ignored",
            "--nocapture",
        ])
        .env("AURA_REALM_CHILD_DIR", &dir)
        .output()
        .expect("the child test binary runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("CHILD_OK"),
        "the child did not find what the parent wrote\nstdout: {stdout}\nstderr: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The child half: a separate process opening the parent's directory. Ignored by default because it
/// needs `AURA_REALM_CHILD_DIR`; the test above runs it deliberately.
#[test]
#[ignore = "run by the_row_that_names_main_outlives_the_process_that_wrote_it, in a child process"]
fn the_child_reads_what_the_parent_wrote() {
    let dir = std::env::var("AURA_REALM_CHILD_DIR").expect("the child's directory");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let set = RealmSet::with_mq(MqStore::fjall(
            okm_core::FjallStore::open(std::path::Path::new(&dir), "aura").unwrap(),
        ));
        assert_eq!(
            set.open(MAIN_REALM).await.unwrap().id,
            RealmId::Persisted(1),
            "main keeps its id across a restart"
        );
        assert_eq!(
            set.live().await.unwrap(),
            vec!["main".to_string(), "scratch".to_string()],
            "one row per realm: reopening a name must not append a second"
        );
        assert_eq!(
            set.open("third").await.unwrap().id,
            RealmId::Persisted(3),
            "the watermark did not fall back to zero"
        );
    });
    println!("CHILD_OK");
}

#[tokio::test]
async fn a_second_boot_finds_main_by_name_and_keeps_its_id() {
    let root = MqStore::mem();
    let first = RealmSet::with_mq(root.clone());
    let main = first.open(MAIN_REALM).await.unwrap();
    // Some state that must survive the "restart".
    let table = timer_entry::table(main.store());
    timer_entry::put(
        &table,
        timer_entry::TimerEntryKey {
            due_ms: 10,
            id: 1,
        },
        "turn",
        "device-a",
        "later",
    )
    .unwrap();

    // A new set over the SAME engine is what a restarted process sees: no
    // memory, one table.
    let second = RealmSet::with_mq(root);
    let reopened = second.open(MAIN_REALM).await.unwrap();
    assert_eq!(
        reopened.id, main.id,
        "the row is the identity: a restart re-opens the same realm"
    );
    assert_eq!(
        timer_entry::all(&timer_entry::table(reopened.store()))
            .unwrap()
            .len(),
        1,
        "the same prefix means the same state"
    );
}