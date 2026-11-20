//! What a temporary realm costs, measured rather than argued: the engine is a fresh `EphemeralStore`
//! (an empty `BTreeMap` behind an `Arc<Mutex<..>>`), so creation is supposed to be near-free and
//! destroy is supposed to return every byte. This exercises both claims and prints numbers.
//!
//! `cargo run --release --example ephemeral_cost -p aura-realm`

// An example's job is to fail loudly on a broken fixture: if the store or the
// process numbers cannot be read, the measurement is wrong and should not print.
#![allow(clippy::expect_used)]

use std::time::Instant;

use aura_realm::mq::MqStore;
use aura_realm::realm_set::RealmSet;
use okm_core::engine::ephemeral::EphemeralStore;
use okm_core::engine::storage::VirtualStorage;

fn rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s.split_whitespace().nth(1).unwrap_or("0").parse().unwrap_or(0);
    pages * 4 // the page size is 4 KiB on this machine
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    // 1. The engine object alone: how much does "a new memory" cost when it holds nothing?
    let n = 200_000;
    let t = Instant::now();
    for _ in 0..n {
        let store = EphemeralStore::new();
        std::hint::black_box(&store);
    }
    println!(
        "EphemeralStore::new + drop           {:>8.1} ns/op   ({n} ops)",
        t.elapsed().as_nanos() as f64 / n as f64
    );

    // 2. A realm's worth of data: 200 keys of ~64 B key, ~128 B value, then everything is dropped.
    let realms = 2_000;
    let keys = 200;
    let t = Instant::now();
    for r in 0..realms {
        let store = EphemeralStore::new();
        for k in 0..keys {
            let key = format!("{r:08}-{k:08}-{}", "k".repeat(40)).into_bytes();
            let value = vec![b'v'; 128];
            store.put(key, value).expect("put");
        }
        drop(store);
    }
    let total = t.elapsed();
    println!(
        "2k realms x 200 keys, put+drop {:>8.1} µs/realm ({} keys total, {:.1}M keys/s)",
        total.as_micros() as f64 / realms as f64,
        realms * keys,
        (realms * keys) as f64 / total.as_secs_f64() / 1e6
    );

    // 3. The whole path prism would take: mint an id, build the Realm, forget it.
    rt.block_on(async {
        let set = RealmSet::with_mq(MqStore::mem());
        let n = 20_000;
        let before = rss_kb();
        let t = Instant::now();
        for _ in 0..n {
            let realm = set.create_ephemeral().await.expect("mint");
            set.destroy(realm.id).await.ok();
        }
        let elapsed = t.elapsed();
        let after = rss_kb();
        println!(
            "create_ephemeral + destroy     {:>8.1} µs/realm ({n} realms, RSS {before} -> {after} KiB)",
            elapsed.as_micros() as f64 / n as f64
        );
        println!("live realms after the loop: {}", set.live().await.expect("live").len());
    });
}