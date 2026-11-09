#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0016 (amended 2026-11-09): a durable timer is a ROW, the wheel is the
//! fast path, and a restart resumes instead of forgetting.

use aura_realm::mq::MqStore;
use aura_realm::timer_entry::{self, TimerEntryKey};
use aura_booth::{InstanceId, InstanceKey};
use aura_realm::Realm;
use std::time::Duration;

fn target() -> InstanceId {
    InstanceId {
        booth_type: "turn".into(),
        key: InstanceKey::Named("device-a".into()),
    }
}

fn dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("aura-timer-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_registration_is_a_row_and_a_cancel_takes_it_back() {
    let dir = dir("register");
    let store = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura").unwrap());
    let realm = Realm::with_mq(store.clone()).shared_async().await;
    let handle = realm.lock().await.timers.clone();

    let id = handle.register_durable(target(), "wake-for-compression".into(), Duration::from_secs(600));
    // The row is written by the driver (one command hop), not by the caller.
    let table = timer_entry::table(store);
    let mut rows = Vec::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        rows = timer_entry::all(&table).unwrap();
        if !rows.is_empty() {
            break;
        }
    }
    assert_eq!(rows.len(), 1, "a durable registration is a row: {rows:?}");
    assert_eq!(rows[0].0.id, id.0, "the row carries the timer's own id");
    assert_eq!(
        rows[0].1.as_ref().map(|e| e.tag.as_str()),
        Some("wake-for-compression")
    );

    handle.cancel(id);
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if timer_entry::all(&table).unwrap().is_empty() {
            break;
        }
    }
    assert!(
        timer_entry::all(&table).unwrap().is_empty(),
        "a cancelled durable timer owes nothing to the next boot"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boot_arms_what_the_table_owes_and_a_due_row_fires_once() {
    let dir = dir("restore");
    let store = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura").unwrap());

    // A row written by a process that then went away: due 120 ms from now.
    let due = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 120;
    let table = timer_entry::table(store.clone());
    timer_entry::put(&table, TimerEntryKey { due_ms: due, id: 9 }, "turn", "device-a", "resume")
        .unwrap();

    // Boot: the wheel is built FROM the table, not from memory.
    let realm = Realm::with_mq(store.clone()).shared_async().await;
    let table = timer_entry::table(store);

    let mut fired = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if timer_entry::all(&table).unwrap().is_empty() {
            fired = true;
            break;
        }
    }
    assert!(fired, "the row that came due was fired, so the row is gone");
    drop(realm);

    let _ = std::fs::remove_dir_all(&dir);
}