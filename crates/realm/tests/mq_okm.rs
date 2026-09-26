use aura_realm::mq;

#[test]
fn mq_roundtrip() {
    // ADR-0018 step 1: the mq tables bind to a BYTE engine (bytes in,
    // bytes out) — no JSON state store, no base64 bridge. The in-memory
    // byte stand-in keeps the test honest without an engine feature.
    let vs = mq::MqStore::mem();
    let seq1 = mq::append(&vs, "add_to_cart", "alice", &serde_json::json!({"item": "book"})).unwrap();
    let seq2 = mq::append(&vs, "add_to_cart", "alice", &serde_json::json!({"item": "pen", "meta": {"source": "web", "tags": [1, 2]}})).unwrap();
    // The sort key is LOGICAL time (ms via MqHead): monotonic, never
    // reset — not a compact 1,2,... sequence.
    assert!(seq1 > 0 && seq2 > seq1, "logical time monotonic: {seq1} -> {seq2}");
    let cur = mq::cursor(&vs, "add_to_cart", "alice", "cart/alice").unwrap();
    assert_eq!(cur, 0);
    let bl = mq::backlog(&vs, "add_to_cart", "alice", 0).unwrap();
    assert_eq!(bl.len(), 2);
    mq::advance(&vs, "add_to_cart", "alice", "cart/alice", seq1).unwrap();
    let bl = mq::backlog(&vs, "add_to_cart", "alice", seq1).unwrap();
    assert_eq!(bl.len(), 1);
    // Nested object + array survive the dynamic segment losslessly.
    let (_, v2) = &bl[0];
    assert_eq!(v2["meta"]["source"], "web");
    assert_eq!(v2["meta"]["tags"][1], 2);
    // A non-object payload (unreachable via the emit chain) maps to an
    // empty object — no synthetic "_root" wrapping (okm set_object
    // contract: the input IS a map, callers own the shape).
    let seq3 = mq::append(&vs, "add_to_cart", "alice", &serde_json::json!([7, 8])).unwrap();
    let bl = mq::backlog(&vs, "add_to_cart", "alice", seq2).unwrap();
    assert_eq!(bl[0].1, serde_json::json!({}));
    let _ = seq3;
}

#[test]
fn event_route_registry_persists_and_scans_by_booth() {
    let vs = mq::MqStore::mem();

    // Register two subscribers on one event, one on another; one wildcard.
    mq::route_put(&vs, "order_created", "cart", "user_id", false).unwrap();
    mq::route_put(&vs, "order_created", "stats", "user_id", false).unwrap();
    mq::route_put(&vs, "order.*", "audit", "", true).unwrap();
    // Idempotent re-register (hot-swap re-declaration) overwrites, not duplicates.
    mq::route_put(&vs, "order_created", "cart", "user_id", false).unwrap();

    // Forward lookup: every subscriber of one event.
    let subs = mq::routes_of_event(&vs, "order_created").unwrap();
    assert_eq!(subs.len(), 2, "two subscribers on the exact event: {subs:?}");
    assert!(subs.iter().all(|(_, k, w)| k == "user_id" && !*w));

    // Reverse lookup (by_booth index): one booth's full subscription set.
    let audit = mq::routes_of_booth(&vs, "audit").unwrap();
    assert_eq!(audit.len(), 1, "audit's wildcard route: {audit:?}");
    assert!(audit[0].2, "wildcard flag survives the round trip");

    // Deregistration drops the booth's rows; the other subscriber stays.
    mq::routes_drop_booth(&vs, "cart").unwrap();
    let subs = mq::routes_of_event(&vs, "order_created").unwrap();
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
    // booth ids: a=1, b=2, c=3.
    mq::route_put(&vs, "e1", "a", "k", false).unwrap(); // (event 1, booth 1)
    mq::route_put(&vs, "e2", "a", "k", false).unwrap(); // (event 2, booth 1)
    mq::route_put(&vs, "e3", "a", "k", false).unwrap(); // (event 3, booth 1)
    mq::route_put(&vs, "e1", "b", "k", false).unwrap(); // (event 1, booth 2)
    mq::route_put(&vs, "e2", "c", "k", false).unwrap(); // (event 2, booth 3)

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
