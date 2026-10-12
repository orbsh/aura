//! The plane-version gate (ADR-0040 Update, PLAN A3): a store written by an
//! older layout must fail the boot LOUDLY — the renumbering placed the new
//! meta band (30-32) on the old event plane's numbers, so an old store's rows
//! decode as the new tables (the silent-decode failure mode the number-reuse
//! discipline exists to prevent). Locks: fresh store seeds the marker; a
//! matching marker boots; a stale marker is a boot error naming the version
//! and the wipe.


#[tokio::test]
#[cfg(feature = "fjall")]
async fn fresh_store_seeds_the_marker_and_boots() {
    let dir = tempfile::tempdir().unwrap();
    let config = aura_config::EngineConfig {
        engine: aura_config::Engine::Fjall,
        data_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };
    // First boot: seeds the marker, completes.
    aura_engine::Engine::start(&config).await.unwrap();
    // Second boot: marker matches, boots again (the durable path).
    aura_engine::Engine::start(&config).await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fjall")]
async fn stale_marker_is_a_boot_error_with_the_wipe_instruction() {
    let dir = tempfile::tempdir().unwrap();
    let config = aura_config::EngineConfig {
        engine: aura_config::Engine::Fjall,
        data_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };
    aura_engine::Engine::start(&config).await.unwrap();
    // Simulate an older-layout store: rewind the marker.
    {
        let db = fjall::Database::create_or_recover(fjall::Config::new(dir.path()))
            .unwrap();
        let store = okm_core::FjallStore::from_db(db, "aura_mq").unwrap();
        use okm_core::storage::VirtualStorage;
        store.put(
            aura_realm::meta::PLANE_VERSION_KEY.to_vec(),
            2026_10_02u32.to_be_bytes().to_vec(),
        );
    }
    let msg = match aura_engine::Engine::start(&config).await {
        Err(e) => format!("{e:#}"),
        Ok(_) => panic!("stale marker must fail the boot"),
    };
    assert!(msg.contains("layout mismatch"), "{msg}");
    assert!(msg.contains("plane version 20261002"), "{msg}");
    assert!(msg.contains("WIPE"), "{msg}");
}
