use aura_realm::mq;

#[test]
fn mq_roundtrip() {
    // ADR-0018 step 1: the mq tables bind to a BYTE engine (bytes in,
    // bytes out) — no JSON state store, no base64 bridge. The in-memory
    // byte stand-in keeps the test honest without an engine feature.
    let mut vs = mq::MqStore::mem();
    let seq1 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!({"item": "book"})).unwrap();
    let seq2 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!({"item": "pen", "meta": {"source": "web", "tags": [1, 2]}})).unwrap();
    // The sort key is LOGICAL time (ms via MqHead): monotonic, never
    // reset — not a compact 1,2,... sequence.
    assert!(seq1 > 0 && seq2 > seq1, "logical time monotonic: {seq1} -> {seq2}");
    let cur = mq::cursor(&mut vs, "add_to_cart", "alice", "cart/alice").unwrap();
    assert_eq!(cur, 0);
    let bl = mq::backlog(&mut vs, "add_to_cart", "alice", 0).unwrap();
    assert_eq!(bl.len(), 2);
    mq::advance(&mut vs, "add_to_cart", "alice", "cart/alice", seq1).unwrap();
    let bl = mq::backlog(&mut vs, "add_to_cart", "alice", seq1).unwrap();
    assert_eq!(bl.len(), 1);
    // Nested object + array survive the dynamic segment losslessly.
    let (_, v2) = &bl[0];
    assert_eq!(v2["meta"]["source"], "web");
    assert_eq!(v2["meta"]["tags"][1], 2);
    // A non-object payload (unreachable via the emit chain) maps to an
    // empty object — no synthetic "_root" wrapping (okm set_object
    // contract: the input IS a map, callers own the shape).
    let seq3 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!([7, 8])).unwrap();
    let bl = mq::backlog(&mut vs, "add_to_cart", "alice", seq2).unwrap();
    assert_eq!(bl[0].1, serde_json::json!({}));
    let _ = seq3;
}

#[test]
fn event_route_registry_persists_and_scans_by_actor() {
    let mut vs = mq::MqStore::mem();

    // Register two subscribers on one event, one on another; one wildcard.
    mq::route_put(&mut vs, "order_created", "cart", "user_id", false).unwrap();
    mq::route_put(&mut vs, "order_created", "stats", "user_id", false).unwrap();
    mq::route_put(&mut vs, "order.*", "audit", "", true).unwrap();
    // Idempotent re-register (hot-swap re-declaration) overwrites, not duplicates.
    mq::route_put(&mut vs, "order_created", "cart", "user_id", false).unwrap();

    // Forward lookup: every subscriber of one event.
    let subs = mq::routes_of_event(&mut vs, "order_created").unwrap();
    assert_eq!(subs.len(), 2, "two subscribers on the exact event: {subs:?}");
    assert!(subs.iter().all(|(_, k, w)| k == "user_id" && !*w));

    // Reverse lookup (by_actor index): one actor's full subscription set.
    let audit = mq::routes_of_actor(&mut vs, "audit").unwrap();
    assert_eq!(audit.len(), 1, "audit's wildcard route: {audit:?}");
    assert!(audit[0].2, "wildcard flag survives the round trip");

    // Deregistration drops the actor's rows; the other subscriber stays.
    mq::routes_drop_actor(&mut vs, "cart").unwrap();
    let subs = mq::routes_of_event(&mut vs, "order_created").unwrap();
    assert_eq!(subs.len(), 1, "cart deregistered: {subs:?}");
}
