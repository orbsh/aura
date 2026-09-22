use aura_realm::mq;

#[test]
fn mq_roundtrip() {
    // ADR-0018 step 1: the mq tables bind to a BYTE engine (bytes in,
    // bytes out) — no JSON state store, no base64 bridge. The in-memory
    // byte stand-in keeps the test honest without an engine feature.
    let mut vs = mq::MqStore::mem();
    let seq1 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!({"item": "book"})).unwrap();
    let seq2 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!({"item": "pen", "meta": {"source": "web", "tags": [1, 2]}})).unwrap();
    assert_eq!((seq1, seq2), (1, 2));
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
