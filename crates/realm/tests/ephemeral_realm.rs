//! ADR-0050 step 1b at the REALM layer: a temporary realm is created, used and
//! destroyed without leaving a byte behind in the engine's keyspace.
//!
//! The key test here is the last one, and it is a dump rather than a promise:
//! a real fjall keyspace is swept before and after a full lifecycle (mint,
//! register with a storage declaration, write through the plan, call the booth,
//! destroy) and the two key/value sets must be identical. Everything the
//! temporary realm did went into its own in-memory engine, and the sweep is
//! what proves that rather than asserting it by construction.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::call::Waited;
use aura_booth::{Body, BoothType, InstanceId, InstanceKey, StoreOp, StoreOpKind};
use aura_realm::mq::MqStore;
use aura_realm::realm_set::{NamedRealm, RealmSet, MAIN_REALM};
use aura_realm::{store_exec, Realm, RealmId};
use okm_core::storage::VirtualStorage;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A Rust body booth: the call path with no carrier in the way, answering with
/// what it was handed (the pattern the realm tests use).
fn echo(name: &str) -> BoothType {
    BoothType {
        name: name.into(),
        body: Body::Rust(Arc::new(|_ctx, args| Box::pin(async move { Ok(args) }))),
        idle_ttl: None,
        max_exec: None,
        hooks: BTreeMap::new(),
        receives: Vec::new(),
        concurrency: aura_booth::Concurrency::Queued,
    }
}

/// The storage declaration a type upload carries (one collection, key `id`,
/// one hot field `count`): the shape `StorePlan::from_schema` parses into a
/// plan, which is what makes a type own an ns at all.
fn storage_schema() -> serde_json::Value {
    serde_json::json!({
        "storage": { "collections": { "counters": { "schema": {
            "key_len": 8,
            "key_fields": [
                { "name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "layout_version": 1,
            "hot_width": 8,
            "payload_header_len": 3,
            "hot_fields": [
                { "name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "cold_fields": [],
            "slots": { "primary": 0, "dynamic": 1, "field_vocab_id": 2,
                       "field_vocab_name": 3, "declared_index_base": 16385,
                       "declared_reduce_base": 20481, "junction_base": 12288 }
        } } } }
    })
}

fn at(type_name: &str) -> InstanceId {
    InstanceId {
        booth_type: type_name.into(),
        key: InstanceKey::Named("k".into()),
    }
}

/// One hot call, unwrapped to its value (the realm tests' helper shape).
async fn call(
    realm: &NamedRealm,
    target: InstanceId,
    args: serde_json::Value,
) -> serde_json::Value {
    match Realm::call(&realm.realm(), None, target, "execute", args)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap()
    {
        Waited::Done(v) => v.unwrap(),
        Waited::Pending(_) => panic!("a Rust body answers on the first turn"),
    }
}

/// Every key/value pair the engine holds, ordered — a dump of the whole
/// keyspace, root band and realm slices alike.
fn dump(store: &MqStore) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = store
        .root()
        .scan_range(&[], None)
        .map(|item| item.unwrap_or_else(|e| panic!("scan: {e}")))
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn a_booth_registered_in_a_temporary_realm_answers() {
    // The parent's store is held by the test too, so "the parent never sees
    // these rows" is a comparison rather than a claim.
    let parent = MqStore::mem();
    let set = RealmSet::with_mq(parent.clone());
    let eph = set.create_ephemeral().await.unwrap();
    assert!(matches!(eph.id, RealmId::Ephemeral(_)));

    eph.realm()
        .lock()
        .await
        .register_type_with(echo("echo"), Some(storage_schema()));

    // The ns follows the SAME rule as a persisted realm (BOOTH_NS_BASE + id): the
    // id comes from the realm's own dictionary row, which lives in its own store,
    // so the first type registered here is 1 and the ns is 101.
    let ns = eph.realm().lock().await.plan_of("echo").unwrap().ns;
    assert_eq!(
        ns,
        u16::from(aura_realm::meta::BOOTH_NS_BASE as u16) + 1,
        "a temporary realm allocates like a persisted one, from its own dictionary"
    );

    // … and the booth answers, through the same call path every realm uses.
    assert_eq!(
        call(&eph, at("echo"), serde_json::json!({ "n": 7 })).await,
        serde_json::json!({ "n": 7 })
    );

    // Registration wrote the SAME rows a persisted realm writes — the type
    // dictionary (ns 30) and the definition with its schema copy (ns 31) — but
    // into the temporary realm's own engine. The parent store never sees them,
    // which is the only difference that survives: the engine, not the rule.
    assert!(
        !eph.store().root().scan_suffix(&[0, 30u8]).unwrap().is_empty(),
        "the realm's own store is missing the type dictionary row it just assigned"
    );
    // A definition row (ns 31) is the ENGINE's write (`register_into`), not this
    // door's; `engine/tests/ephemeral.rs` asserts that half. What matters here is
    // that nothing of either kind reaches the parent store.
    for ns in [30u8, 31u8] {
        assert!(
            parent.root().scan_suffix(&[0, ns]).unwrap().is_empty(),
            "a temporary realm's rows reached the parent store (ns {ns})"
        );
    }
}

#[tokio::test]
async fn two_temporary_realms_count_their_namespaces_independently() {
    let set = RealmSet::with_mq(MqStore::mem());
    let a = set.create_ephemeral().await.unwrap();
    let b = set.create_ephemeral().await.unwrap();
    assert_ne!(a.id, b.id);
    for realm in [&a, &b] {
        realm
            .realm()
            .lock()
            .await
            .register_type_with(echo("same"), Some(storage_schema()));
    }
    // The same type NAME and the same NUMBER in two realms is not a collision:
    // each realm's dictionary starts at id 1 and each realm's keys live in its own
    // engine, so the two never meet. (The isolation itself — neither realm can
    // reach the other's type or instance — is asserted through the engine's
    // registration door in `engine/tests/ephemeral.rs`.)
    assert_eq!(
        a.realm().lock().await.plan_of("same").unwrap().ns,
        b.realm().lock().await.plan_of("same").unwrap().ns
    );
    // A second type in ONE realm does move that realm's counter.
    a.realm()
        .lock()
        .await
        .register_type_with(echo("second"), Some(storage_schema()));
    let plan = a.realm().lock().await.plan_of("second").unwrap().ns;
    assert_eq!(plan, u16::from(aura_realm::meta::BOOTH_NS_BASE as u16) + 2);
    assert!(
        b.realm().lock().await.plan_of("second").is_none(),
        "a type registered in a is not a type in b"
    );
}

#[tokio::test]
async fn destroy_forgets_it_and_the_recorded_name_never_opens_it() {
    let set = RealmSet::with_mq(MqStore::mem());
    let main = set.open(MAIN_REALM).await.unwrap();
    // A stored shape so `live()` will answer a new realm with an id it is
    // reused against — a persisted realm was registered first and owns 1 and 2,
    // so nothing the ephemeral one does can be confused for them.
    assert_eq!(main.id, RealmId::Persisted(1));

    let eph = set.create_ephemeral().await.unwrap();
    let name = eph.name.clone();
    assert!(
        set.live().await.unwrap().contains(&name),
        "a live temporary realm is listed while it lives"
    );
    // The recorded name is a label, never an address: nothing resolves it back
    // to the temporary realm, not even while that realm is alive.
    assert_eq!(
        set.id_of(&name).await.unwrap(),
        None,
        "a temporary realm's name must not resolve to it"
    );

    set.destroy(eph.id).await.unwrap();
    assert!(
        !set.live().await.unwrap().contains(&name),
        "destroy stops listing it"
    );
    assert_eq!(set.name_of(eph.id).await.unwrap(), None);

    // … and opening the recorded name after the fact mints a PERSISTED realm
    // with a fresh id: the temporary one never had a row, so it never took the
    // name away from anybody.
    let opened = set.open(&name).await.unwrap();
    assert!(
        matches!(opened.id, RealmId::Persisted(_)),
        "opening a temporary realm's name must give a persisted realm"
    );
    assert_ne!(opened.id, eph.id);
}

#[test]
fn an_ephemeral_id_can_never_be_mistaken_for_a_persisted_one() {
    // The TAG, not the width: `Persisted(1)` and `Ephemeral(1)` are the same
    // NUMBER and two different realms — the realm set is keyed by the enum, the
    // `realms` table key is a `u32`, and only the persisted kind has a prefix,
    // so an ephemeral id cannot even form the row key the table answers from. A
    // width-inferred scheme would conflate the two at the table.
    assert_ne!(RealmId::Persisted(1), RealmId::Ephemeral(1));
    assert_eq!(RealmId::Persisted(1).storage_prefix(), Some([0, 0, 0, 1]));
    assert_eq!(RealmId::Ephemeral(1).storage_prefix(), None);
}

/// THE PROOF, not the construction argument: a real fjall keyspace, swept
/// before and after a full temporary-realm lifecycle, is byte-identical.
///
/// Synchronous on purpose — fjall holds the directory and the lifecycle only
/// needs a runtime for the realm's async surfaces, so the store's lifetime is
/// the test's and the sweep brackets it cleanly.
#[test]
fn a_full_temporary_lifecycle_leaves_the_keyspace_byte_identical() {
    let dir = std::env::temp_dir().join(format!(
        "aura-ephemeral-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let root = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura_mq").unwrap());
        let set = RealmSet::with_mq(root.clone());
        let main = set.open(MAIN_REALM).await.unwrap();
        // A persistent tenant with a real row and a real key: "before" is not
        // the empty keyspace, so an equality of two dumps means something.
        main.store()
            .put(b"probe".to_vec(), b"value".to_vec())
            .unwrap();
        let before = dump(&root);
        assert!(
            !before.is_empty(),
            "the store must hold the persisted realm"
        );

        // ---- THE LIFECYCLE ------------------------------------------------
        let eph = set.create_ephemeral().await.unwrap();
        assert!(
            eph.store().is_memory() && eph.store().prefix().is_empty(),
            "a temporary realm's store is memory with no prefix"
        );
        // A write inside it, through its own handle.
        eph.store()
            .put(b"inside".to_vec(), b"the realm".to_vec())
            .unwrap();
        // A type with storage: the ns comes from the realm's counter and the
        // schema from the caller's copy (nothing is read back from a row).
        eph.realm()
            .lock()
            .await
            .register_type_with(echo("writer"), Some(storage_schema()));
        // Write through the resolved plan: ns-prefixed keys, in the realm's
        // own engine, at the top of the band.
        let plan = eph
            .realm()
            .lock()
            .await
            .plan_of("writer")
            .cloned()
            .expect("the declared collections became a plan");
        assert_eq!(plan.ns, u16::from(aura_realm::meta::BOOTH_NS_BASE as u16) + 1);
        store_exec::execute(
            &eph.store(),
            &plan,
            &StoreOp {
                collection: "counters".into(),
                op: StoreOpKind::PutDocument {
                    key: serde_json::json!({ "id": 1 }),
                    doc: serde_json::json!({ "count": 42 }),
                },
            },
        )
        .unwrap();
        let read = store_exec::execute(
            &eph.store(),
            &plan,
            &StoreOp {
                collection: "counters".into(),
                op: StoreOpKind::GetDocument {
                    key: serde_json::json!({ "id": 1 }),
                },
            },
        )
        .unwrap();
        assert_eq!(read["count"], 42, "the temporary realm's storage works");
        // The booth answers, on the same call path as any other realm.
        assert_eq!(
            call(&eph, at("writer"), serde_json::json!({ "ok": true })).await,
            serde_json::json!({ "ok": true })
        );
        // It really did write something — into memory. Otherwise "unchanged"
        // would be a statement about a realm that did nothing.
        assert!(
            !dump(&eph.store()).is_empty(),
            "the lifecycle must have written somewhere for the sweep to mean anything"
        );
        set.destroy(eph.id).await.unwrap();
        // ---- END ----------------------------------------------------------

        let after = dump(&root);
        assert_eq!(
            before, after,
            "a temporary realm's lifecycle changed the persistent keyspace"
        );
        // Named, so a failure says which key leaked if one ever does.
        let before_keys: Vec<&Vec<u8>> = before.iter().map(|(k, _)| k).collect();
        for (key, _) in &after {
            assert!(
                before_keys.contains(&key),
                "a key the temporary realm's lifecycle added: {key:02x?}"
            );
        }
    });

    let _ = std::fs::remove_dir_all(&dir);
}
