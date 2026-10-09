use aura_realm::mq::{self, InstanceKey};

/// A named slice (the shape a keyed route produces).
fn named(name: &str) -> InstanceKey {
    InstanceKey::Named(name.to_string())
}

#[test]
fn mq_roundtrip() {
    // ADR-0018 step 1: the mq tables bind to a BYTE engine (bytes in,
    // bytes out) — no JSON state store, no base64 bridge. The in-memory
    // byte stand-in keeps the test honest without an engine feature.
    let vs = mq::MqStore::mem();
    let seq1 = mq::append(&vs, "add_to_cart", &named("alice"), &serde_json::json!({"item": "book"})).unwrap();
    let seq2 = mq::append(&vs, "add_to_cart", &named("alice"), &serde_json::json!({"item": "pen", "meta": {"source": "web", "tags": [1, 2]}})).unwrap();
    // The sort key is the per-slice SEQUENCE (MqData's HighWater(seq) issues it): a
    // counter, monotonic, never reset — deliberately not a clock.
    assert!(seq1 > 0 && seq2 > seq1, "the sequence counter is monotonic: {seq1} -> {seq2}");
    // The cursor's subject is the BOOTH (ADR-0038 §2): no participant
    // name, no dictionary row for `"type/key"`.
    let cur = mq::cursor(&vs, "add_to_cart", &named("alice"), "cart").unwrap();
    assert_eq!(cur, 0);
    let bl = mq::backlog(&vs, "add_to_cart", &named("alice"), 0).unwrap();
    assert_eq!(bl.len(), 2);
    mq::advance(&vs, "add_to_cart", &named("alice"), "cart", seq1).unwrap();
    let bl = mq::backlog(&vs, "add_to_cart", &named("alice"), seq1).unwrap();
    assert_eq!(bl.len(), 1);
    // Nested object + array survive the dynamic segment losslessly.
    let (_, v2) = &bl[0];
    assert_eq!(v2["meta"]["source"], "web");
    assert_eq!(v2["meta"]["tags"][1], 2);
    // A non-object payload (unreachable via the emit chain) maps to an
    // empty object — no synthetic "_root" wrapping (okm set_object
    // contract: the input IS a map, callers own the shape).
    let seq3 = mq::append(&vs, "add_to_cart", &named("alice"), &serde_json::json!([7, 8])).unwrap();
    let bl = mq::backlog(&vs, "add_to_cart", &named("alice"), seq2).unwrap();
    assert_eq!(bl[0].1, serde_json::json!({}));
    let _ = seq3;
}

#[test]
fn partition_dictionary_is_a_proxied_vocabulary() {
    // ADR-0039 §1: the partition is a dictionary-issued id, not a hash and
    // not an inline string (an okm primary key is fixed-width by
    // construction). The singleton is a STRUCTURAL marker, so 0 is an id
    // the issuer cannot produce and the mapping reads back (which the hash
    // could not).
    let vs = mq::MqStore::mem();
    assert_eq!(mq::SINGLETON_KEY_ID, 0);
    assert_eq!(mq::instance_key_id(&vs, &InstanceKey::Singleton).unwrap(), mq::SINGLETON_KEY_ID);

    let a = mq::instance_key_id(&vs, &named("alice")).unwrap();
    assert_eq!(a, 1, "the first issued id is 1 — 0 is reserved for the singleton");
    assert_eq!(mq::instance_key_id(&vs, &named("alice")).unwrap(), a, "idempotent: the id is issued, not computed");
    let b = mq::instance_key_id(&vs, &named("bob")).unwrap();
    assert_ne!(a, b, "distinct names get distinct ids");

    // Reverse direction (the ops surface the hash could not offer).
    assert_eq!(mq::instance_key_of(&vs, a).unwrap().as_deref(), Some("alice"));
    assert_eq!(mq::instance_key_of(&vs, mq::SINGLETON_KEY_ID).unwrap(), None, "the singleton is no dictionary row");

    // Peek never allocates.
    assert_eq!(mq::instance_key_id_of(&vs, "carol").unwrap(), None);
    assert_eq!(mq::instance_key_id_of(&vs, "alice").unwrap(), Some(a));

    // The retired sentinel is just another name (ADR-0042): a key whose
    // literal text equals the old "__singleton__" marker is a NAMED
    // instance — no aliasing into the singleton queue (the old sentinel
    // mapped that string to the reserved value, which let a keyed
    // instance share the singleton's queue).
    let odd = mq::instance_key_id(&vs, &named("__singleton__")).unwrap();
    assert_ne!(odd, mq::SINGLETON_KEY_ID);
    assert_eq!(mq::instance_key_of(&vs, odd).unwrap().as_deref(), Some("__singleton__"));
}

#[test]
fn cursor_ttl_predicate_spares_unmarked_rows() {
    // ADR-0039 §2: a cursor row leaves the retention denominator once it
    // has not advanced for `cursor_ttl`. `last_active_ms == 0` is the
    // UNMARKED sentinel (a row written before the field existed) — it never
    // expires; without that rule the first run of the new code would void
    // every existing backlog.
    use std::time::Duration;
    let ttl = Duration::from_secs(3600);
    assert!(mq::cursor_expired(1, 1 + 3_600_000, ttl), "an hour of silence expires");
    assert!(!mq::cursor_expired(1, 1 + 3_599_999, ttl), "just under the horizon does not");
    assert!(!mq::cursor_expired(0, u64::MAX, ttl), "0 = unmarked: never expires");
}

#[test]
fn event_route_registry_persists_and_scans_by_booth() {
    let vs = mq::MqStore::mem();

    // Register two subscribers on one event, one on another; one wildcard.
    mq::route_put(&vs, "order.created", "cart", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();
    mq::route_put(&vs, "order.created", "stats", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();
    mq::route_put(&vs, "order.*", "audit", &aura_booth::RouteResolution::Singleton, true).unwrap();
    // Idempotent re-register (hot-swap re-declaration) overwrites, not duplicates.
    mq::route_put(&vs, "order.created", "cart", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();

    // Forward lookup: every subscriber of one event.
    let subs = mq::routes_of_event(&vs, "order.created").unwrap();
    assert_eq!(subs.len(), 2, "two subscribers on the exact event: {subs:?}");
    assert!(subs.iter().all(|(_, k, w)| matches!(k, aura_booth::RouteResolution::Field(f) if f == "user_id") && !*w));

    // Reverse lookup (by_booth index): one booth's full subscription set.
    let audit = mq::routes_of_booth(&vs, "audit").unwrap();
    assert_eq!(audit.len(), 1, "audit's wildcard route: {audit:?}");
    assert!(audit[0].2, "wildcard flag survives the round trip");

    // The retention denominator's test: a wildcard subscriber's cursor must
    // COUNT for the concrete event its pattern matches (the registry stores
    // the pattern, so an exact-id lookup alone would drop it out). The
    // pattern is `order.*`, whose prefix is `order.` — so the concrete names
    // it matches live in that namespace; `order.created` does, and a name in
    // another namespace does not. A concrete name enters the event dictionary
    // by being emitted or subscribed to (here: the exact route below), which
    // is what `booth_subscribes` resolves; an unknown name has no queue and no
    // cursor, so it answers false before any wildcard is consulted.
    mq::route_put(&vs, "order.cancelled", "cart", &aura_booth::RouteResolution::Field("user_id".into()), false).unwrap();
    let audit_id = mq::booth_id_of(&vs, "audit").unwrap();
    assert!(mq::booth_subscribes(&vs, audit_id, "order.created").unwrap());
    assert!(mq::booth_subscribes(&vs, audit_id, "order.cancelled").unwrap());
    assert!(!mq::booth_subscribes(&vs, audit_id, "user.created").unwrap());
    assert!(!mq::booth_subscribes(&vs, audit_id, "never.registered").unwrap());

    // Deregistration drops the booth's rows; the other subscriber stays.
    mq::routes_drop_booth(&vs, "cart").unwrap();
    let subs = mq::routes_of_event(&vs, "order.created").unwrap();
    assert_eq!(subs.len(), 1, "cart deregistered: {subs:?}");
}

#[test]
fn routes_drop_booth_targets_only_its_own_rows() {
    // The bug this locks: routes_drop_booth once scanned the PRIMARY slot
    // with the booth_id where the event_id lives, so it deleted any row
    // whose event_id happened to equal the booth_id — cross-booth damage,
    // invisible when the ids coincide (as in the test above). Register
    // several events for one booth AND the same events for others, so the
    // booth's id collides with a DIFFERENT event's id in another row.
    let vs = mq::MqStore::mem();
    // event ids allocate in first-seen order: e1=1, e2=2, e3=3.
    // type ids: a=1, b=2, c=3.
    mq::route_put(&vs, "e1", "a", &aura_booth::RouteResolution::Field("k".into()), false).unwrap(); // (event 1, type 1)
    mq::route_put(&vs, "e2", "a", &aura_booth::RouteResolution::Field("k".into()), false).unwrap(); // (event 2, type 1)
    mq::route_put(&vs, "e3", "a", &aura_booth::RouteResolution::Field("k".into()), false).unwrap(); // (event 3, type 1)
    mq::route_put(&vs, "e1", "b", &aura_booth::RouteResolution::Field("k".into()), false).unwrap(); // (event 1, type 2)
    mq::route_put(&vs, "e2", "c", &aura_booth::RouteResolution::Field("k".into()), false).unwrap(); // (event 2, type 3)

    // Drop booth "a" (id 1). The old scan would also hit rows whose
    // EVENT id == 1 (the (e1,b) row), wrongly deleting booth b's route.
    mq::routes_drop_booth(&vs, "a").unwrap();

    assert!(mq::routes_of_booth(&vs, "a").unwrap().is_empty(), "a's rows all gone");
    // b subscribed only e1; that row must SURVIVE a's drop.
    let b = mq::routes_of_booth(&vs, "b").unwrap();
    assert_eq!(b.len(), 1, "b's e1 route survives: {b:?}");
    let c = mq::routes_of_booth(&vs, "c").unwrap();
    assert_eq!(c.len(), 1, "c's e2 route survives: {c:?}");

    // Forward lookups agree: e1 still has exactly b, e2 exactly c, e3 none.
    let e1 = mq::routes_of_event(&vs, "e1").unwrap();
    assert_eq!(e1.len(), 1, "e1 keeps only b: {e1:?}");
    let e3 = mq::routes_of_event(&vs, "e3").unwrap();
    assert!(e3.is_empty(), "e3 was a's alone: {e3:?}");
}

#[test]
fn depth_counts_live_and_skip_to_head_skips_the_backlog() {
    // The zero-scan operational surface (realm.md retention ruling):
    // append folds +1, watermark compaction's delete unfolds -1, depth()
    // is one point read — never a prefix scan.
    let vs = mq::MqStore::mem();
    assert_eq!(mq::depth(&vs, "tick", &named("u1")).unwrap(), 0, "unseen event: depth 0");

    let s1 = mq::append(&vs, "tick", &named("u1"), &serde_json::json!({"n": 1})).unwrap();
    let s2 = mq::append(&vs, "tick", &named("u1"), &serde_json::json!({"n": 2})).unwrap();
    let s3 = mq::append(&vs, "tick", &named("u1"), &serde_json::json!({"n": 3})).unwrap();
    assert_eq!(mq::depth(&vs, "tick", &named("u1")).unwrap(), 3, "three appends fold +1 each");
    // Depth is per (event, partition): another partition keeps its own count.
    mq::append(&vs, "tick", &named("u2"), &serde_json::json!({"n": 9})).unwrap();
    assert_eq!(mq::depth(&vs, "tick", &named("u2")).unwrap(), 1, "partition-scoped count");

    // Watermark compaction unfolds: delete below s2 removes {s1}, count drops.
    let eid = mq::event_id_of(&vs, "tick").unwrap().unwrap();
    let part = mq::instance_key_id_of(&vs, "u1").unwrap().unwrap();
    let removed = mq::delete_before(&vs, eid, part, s2).unwrap();
    assert_eq!(removed, 1, "the pre-watermark row is gone");
    assert_eq!(mq::depth(&vs, "tick", &named("u1")).unwrap(), 2, "unfold -1 on compaction delete");

    // skip-to-head: the cursor jumps to the head; the stale backlog never
    // re-surfaces (advance is monotonic — a later lower seq is a no-op).
    mq::skip_to_head(&vs, "tick", &named("u1"), "cart").unwrap();
    let bl = mq::backlog(&vs, "tick", &named("u1"), mq::cursor(&vs, "tick", &named("u1"), "cart").unwrap()).unwrap();
    assert!(bl.is_empty(), "skipped: nothing ahead of the cursor");
    mq::advance(&vs, "tick", &named("u1"), "cart", s1).unwrap();
    assert_eq!(mq::cursor(&vs, "tick", &named("u1"), "cart").unwrap(), s3,
        "advance never rewinds — the skip survives a stale lower seq");
}