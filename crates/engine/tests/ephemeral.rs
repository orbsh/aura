//! ADR-0050 step 1b through the ENGINE's registration door: a temporary realm
//! takes the SAME code path every other realm takes (`Engine::register_into`),
//! persists nothing, and is isolated from its siblings.
//!
//! The byte-level proof that a full lifecycle leaves a persistent keyspace
//! untouched lives in `aura-realm/tests/ephemeral_realm.rs` (the crate that
//! owns `MqStore`); this file asserts the engine-side halves that the store
//! cannot say: that the skip happens on the real registration path, and that
//! two temporary realms cannot see each other's types.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::{Body, BoothType, InstanceId, InstanceKey};
use aura_engine::Engine;
use aura_realm::realm_set::NamedRealm;
use aura_realm::{meta, Realm};
use std::collections::BTreeMap;
use std::sync::Arc;

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

/// A script booth: the kind that HAS a definition row to write, so "it wrote
/// nothing" is a statement about the skip rather than about a type that had
/// nothing to persist (a Rust body has no definition). The carrier does not
/// exist in this build, so introspection answers nothing — which is exactly the
/// shape a failed introspection leaves, and the persistence step must still be
/// skipped for it.
fn declared(name: &str) -> BoothType {
    BoothType::script(
        name,
        "no-such-carrier",
        "(define (interface_schema a) (hash \"storage\" (hash \"collections\" (hash))))",
    )
}

fn at(type_name: &str) -> InstanceId {
    InstanceId {
        booth_type: type_name.into(),
        key: InstanceKey::Named("k".into()),
    }
}

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
        aura_booth::call::Waited::Done(v) => v.unwrap(),
        aura_booth::call::Waited::Pending(_) => panic!("a Rust body answers on the first turn"),
    }
}

#[tokio::test]
async fn a_booth_registered_in_a_temporary_realm_answers_and_stays_inside_it() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let main = engine
        .realm_set
        .open(aura_realm::realm_set::MAIN_REALM)
        .await
        .unwrap();
    let eph = engine.realm_set.create_ephemeral().await.unwrap();

    // Both kinds registered, both through the engine's ONE door.
    engine
        .register_into(&eph.realm(), echo("echo"))
        .await
        .unwrap();
    engine
        .register_into(&eph.realm(), declared("declared"))
        .await
        .unwrap();

    // The booth answers.
    assert_eq!(
        call(&eph, at("echo"), serde_json::json!({ "n": 7 })).await,
        serde_json::json!({ "n": 7 })
    );

    // The registration wrote the SAME rows a persisted realm's does — a type
    // dictionary row (ns 30), a definition with its interface_schema (ns 31) —
    // but into the temporary realm's OWN store, which is dropped with it. The
    // rule is one rule; the engine is what differs (ADR-0050 §6).
    assert_eq!(
        meta::load_all(&eph.store()).unwrap().len(),
        1,
        "the script type has a definition row in the realm's own store; the Rust \
         body has none because `persist::from_type` answers None for a compile-time handler"
    );
    assert_eq!(
        meta::booth_name_of(&eph.store(), 1).unwrap().as_deref(),
        Some("echo"),
        "the first type registered here owns id 1, exactly as it would in fjall"
    );
    assert_eq!(
        meta::booth_name_of(&eph.store(), 2).unwrap().as_deref(),
        Some("declared")
    );

    // … and the PERSISTENT realm never heard of either type: nothing crossed
    // the realm boundary in either direction.
    assert!(
        meta::load_all(&main.store()).unwrap().is_empty(),
        "a temporary realm's registration reached the parent store"
    );
    assert!(meta::ns_of(&main.store(), "echo").is_err());
    assert!(meta::ns_of(&main.store(), "declared").is_err());
    assert_eq!(engine.realm_set.id_of("echo").await.unwrap(), None);
}

#[tokio::test]
async fn two_temporary_realms_register_the_same_type_and_do_not_see_each_other() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let a = engine.realm_set.create_ephemeral().await.unwrap();
    let b = engine.realm_set.create_ephemeral().await.unwrap();

    // The SAME type name in both — the case a job's declarations create when a
    // graph node is named like a platform type (ADR-0051 §Consequences).
    engine
        .register_into(&a.realm(), echo("shared"))
        .await
        .unwrap();
    engine
        .register_into(&b.realm(), echo("shared"))
        .await
        .unwrap();

    // a runs an instance …
    assert_eq!(
        call(&a, at("shared"), serde_json::json!({ "who": "a" })).await,
        serde_json::json!({ "who": "a" })
    );
    assert!(a.realm().lock().await.is_resident(&at("shared")));
    // … which is ABSENT in b: the instance map, the type table and the store are
    // all per realm, so "same name" is two types and no shared instance.
    assert!(
        !b.realm().lock().await.is_resident(&at("shared")),
        "the instance in one temporary realm exists in the other"
    );

    // b answers with its own instance; a is unchanged by it.
    assert_eq!(
        call(&b, at("shared"), serde_json::json!({ "who": "b" })).await,
        serde_json::json!({ "who": "b" })
    );
    assert!(b.realm().lock().await.is_resident(&at("shared")));

    // A third realm that registered nothing cannot call the type at all — the
    // boundary is a missing type, not a routed request.
    let c = engine.realm_set.create_ephemeral().await.unwrap();
    let err = Realm::call(
        &c.realm(),
        None,
        at("shared"),
        "execute",
        serde_json::json!(null),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("unknown booth type"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn destroy_omits_it_from_live_and_its_name_does_not_open_it() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let eph = engine.realm_set.create_ephemeral().await.unwrap();
    engine
        .register_into(&eph.realm(), echo("echo"))
        .await
        .unwrap();
    let name = eph.name.clone();

    assert!(
        engine.realm_set.live().await.unwrap().contains(&name),
        "a live temporary realm is listed"
    );
    assert_eq!(
        engine.realm_set.id_of(&name).await.unwrap(),
        None,
        "a temporary realm's recorded name is not an address"
    );

    engine.realm_set.destroy(eph.id).await.unwrap();
    assert!(
        !engine.realm_set.live().await.unwrap().contains(&name),
        "destroy must stop listing it"
    );
    assert_eq!(engine.realm_set.name_of(eph.id).await.unwrap(), None);

    // Opening the recorded name after the destroy yields a PERSISTED realm with
    // its own id — the temporary realm never had a row, so it never held the
    // name.
    let reopened = engine.realm_set.open(&name).await.unwrap();
    assert!(matches!(reopened.id, aura_realm::RealmId::Persisted(_)));
    assert_ne!(reopened.id, eph.id);
    // And it is a DIFFERENT realm: the type registered in the temporary one is
    // not a type here.
    assert!(meta::ns_of(&reopened.store(), "echo").is_err());
}
