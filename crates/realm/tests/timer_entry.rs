#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0016 (amended 2026-11-09): the durable timer tier is a TABLE, and the
//! wheel is only the fast path. These tests hold the three behaviours the wheel
//! cannot: the order is the due order, a due bound answers without decoding the
//! rest, and the rows survive the process that wrote them.

use aura_realm::mq::MqStore;
use aura_realm::timer_entry::{self, TimerEntryKey};

#[test]
fn the_table_is_ordered_by_due_and_a_bound_reads_only_what_is_owed() {
    let store = MqStore::mem();
    let t = timer_entry::table(store);

    timer_entry::put(
        &t,
        TimerEntryKey { due_ms: 200, id: 2 },
        "turn",
        "device-a",
        "later",
    )
    .unwrap();
    timer_entry::put(
        &t,
        TimerEntryKey { due_ms: 100, id: 1 },
        "turn",
        "device-a",
        "sooner",
    )
    .unwrap();

    // The table's own order IS the due order — the reads never sort.
    let all = timer_entry::all(&t).unwrap();
    assert_eq!(
        all.iter().map(|(k, _)| k.due_ms).collect::<Vec<_>>(),
        vec![100, 200],
        "oldest first"
    );
    assert_eq!(
        all[0].1.as_ref().map(|e| e.tag.as_str()),
        Some("sooner"),
        "the payload rides the row: {all:?}"
    );

    // A due bound answers with the entries owed at or before it — the range is
    // the index, not a filter over a full decode.
    let due = timer_entry::due_by(&t, 150).unwrap();
    assert_eq!(due.len(), 1, "only what is owed by 150: {due:?}");
    assert_eq!(due[0].0, TimerEntryKey { due_ms: 100, id: 1 });

    // Firing and cancelling are the same act: the row goes.
    assert!(timer_entry::purge(
        &t,
        &TimerEntryKey { due_ms: 100, id: 1 }
    )
    .unwrap());
    assert_eq!(timer_entry::all(&t).unwrap().len(), 1, "one left");
    assert!(
        !timer_entry::purge(&t, &TimerEntryKey { due_ms: 100, id: 1 }).unwrap(),
        "purging what is gone answers false, not an error"
    );
}

#[test]
fn the_schedule_outlives_the_process_that_wrote_it() {
    let dir = std::env::temp_dir().join(format!("aura-timer-entry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    {
        let store = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura").unwrap());
        let t = timer_entry::table(store);
        timer_entry::put(
            &t,
            TimerEntryKey {
                due_ms: 5_000,
                id: 7,
            },
            "turn",
            "device-b",
            "wake-for-compression",
        )
        .unwrap();
    }

    // A second process (here: a second open of the same directory) rebuilds the
    // schedule by reading it — nothing was kept in memory to lose.
    let store = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura").unwrap());
    let t = timer_entry::table(store);
    let all = timer_entry::all(&t).unwrap();
    assert_eq!(all.len(), 1, "the row survived: {all:?}");
    let (key, entry) = &all[0];
    assert_eq!(*key, TimerEntryKey { due_ms: 5_000, id: 7 });
    let entry = entry.as_ref().expect("the payload decodes");
    assert_eq!(entry.tag, "wake-for-compression");
    assert_eq!(entry.booth_type, "turn");
    assert_eq!(entry.instance_key, "device-b", "the instance renders into the row");

    let _ = std::fs::remove_dir_all(&dir);
}