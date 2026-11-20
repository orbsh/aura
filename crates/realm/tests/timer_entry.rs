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
//!
//! The last test is the ADR-0050 one: a realm's keys are `[u32 BE id]`-prefixed,
//! so ONE store holds every realm's every table, and a table's scan window sits
//! in the middle of that keyspace instead of at one of its ends.

use aura_realm::mq::MqStore;
use aura_realm::timer_entry::{self, TimerEntryKey};
use okm_core::storage::VirtualStorage;
use okm_core::KeyEncode;

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

/// ADR-0050 step 1a: a realm IS a `[u32 BE id]` prefix, so one store holds the
/// root band's `realms` table AND every one of that realm's tables — the timer's
/// `by_due` window is a few bytes into the keyspace, not at its edge.
///
/// This is the boot read that crashed: `timer_entry::all` walked an OPEN range
/// (`end = None`) from the `by_due` prefix to the end of the whole keyspace, so
/// every higher-ns table of the same realm came back in the answer — and the
/// first one whose key was as long as an entry key (the mq registry's `by_name`
/// entry for a 12-byte name, written below on purpose) was decoded as a `due_ms`
/// column, slicing 8 bytes out of a segment that did not exist. The read now
/// asks for no column it does not use and skips a hit with no row behind it, so
/// a foreign key is dropped instead of decoded.
#[tokio::test]
async fn a_realms_row_and_a_neighbour_table_stay_out_of_the_by_due_scan() {
    let root = MqStore::mem();
    let set = aura_realm::realm_set::RealmSet::with_mq(root.clone());
    // The root band's `realms` table: its row, its name index and the id
    // watermark that issued the id. `main` is realm 1.
    let realm = set.open("main").await.unwrap();
    let store = realm.store();
    assert_eq!(
        &store.prefix()[..4],
        &1u32.to_be_bytes(),
        "the realm's keys are `[u32 BE id]`-prefixed"
    );

    // A neighbour table in the SAME realm and ABOVE the event plane: the meta
    // plane's booth dictionary (ns 30) writes a `by_name` entry
    // `[ns 30][slot by_name][name][id]`, so a 12-byte name makes it 4 + 12 + 4 =
    // 20 bytes — exactly the entry-key width, which is the shape that used to
    // reach the decoder's `u64` (this is the key prism's own boot writes).
    let booth = aura_realm::mq::booth_id_of(&store, "gravity_turn").unwrap();
    assert!(booth > 0, "the dictionary issued an id");

    let t = timer_entry::table(store.clone());
    timer_entry::put(
        &t,
        TimerEntryKey { due_ms: 7, id: 9 },
        "turn",
        "device-a",
        "later",
    )
    .unwrap();

    // The boot read: exactly the one entry, no panic on the realms row, the
    // watermark, the neighbour's index entry or its payload rows.
    let all = timer_entry::all(&t).unwrap();
    assert_eq!(all.len(), 1, "exactly the timer entry: {all:?}");
    assert_eq!(all[0].0, TimerEntryKey { due_ms: 7, id: 9 });
    assert_eq!(
        all[0].1.as_ref().map(|e| e.tag.as_str()),
        Some("later"),
        "the payload rides the row: {all:?}"
    );
    // The bounded read agrees — its end already lands inside the table, so it is
    // the same window minus the tail.
    assert_eq!(timer_entry::due_by(&t, 7).unwrap().len(), 1);

    // THE REVERSE DIRECTION. The realms table's own scan answers its one row and
    // nothing the realm wrote into its slice: `live()` walks that table's rows,
    // so a leak would surface here — as a bogus name, or as a decode failure.
    assert_eq!(
        set.live().await.unwrap(),
        vec!["main".to_string()],
        "the realms table holds one row, whatever the realm's slice holds"
    );

    // …and byte for byte: the entry's row and its `by_due` entry both live under
    // the realm's `[00 00 00 01]` prefix, while no key of the root band (ns
    // `REALMS_NS` = `[0x01, 0x00]`) ends where they do.
    let mut wanted = Vec::new();
    TimerEntryKey { due_ms: 7, id: 9 }.encode_into(&mut wanted);
    // The realm's whole slice: `[u32 BE id]` .. its successor, FULL keys (a
    // suffix scan would strip the very bytes this assertion is about).
    let slice: Vec<Vec<u8>> = root
        .root()
        .scan_range(&1u32.to_be_bytes(), Some(&2u32.to_be_bytes()))
        .map(|item| item.map(|(key, _)| key))
        .collect::<okm_core::Result<_>>()
        .unwrap();
    let mine: Vec<&Vec<u8>> = slice.iter().filter(|k| k.ends_with(&wanted)).collect();
    assert_eq!(
        mine.len(),
        2,
        "the primary row and its by_due entry, both in the realm's slice: {:02x?}",
        slice
    );
    // `[realm 4][ns 2][slot 2][due 8][key 16]` = 32: the index entry, whose
    // field segment is the 8 bytes the old code tried to decode out of nothing
    // (the primary row is the same key under a ONE-byte slot, 23 bytes).
    let index_len = 4 + 2 + 2 + 8 + TimerEntryKey::KEY_LEN;
    let index_entry = mine
        .iter()
        .find(|k| k.len() == index_len)
        .expect("the by_due entry carries its 8-byte field segment");
    assert!(index_entry.starts_with(&1u32.to_be_bytes()));

    let realm_band: Vec<Vec<u8>> = root
        .root()
        .scan_range(
            &aura_realm::realm_set::REALMS_NS.to_be_bytes(),
            Some(&(aura_realm::realm_set::REALMS_NS + 1).to_be_bytes()),
        )
        .map(|item| item.map(|(key, _)| key))
        .collect::<okm_core::Result<_>>()
        .unwrap();
    assert!(
        !realm_band.is_empty(),
        "the realms table wrote its row and its watermark"
    );
    assert!(
        realm_band.iter().all(|k| !k.ends_with(&wanted)),
        "a realms-table scan cannot reach the timer's key: {:02x?}",
        realm_band
    );
}