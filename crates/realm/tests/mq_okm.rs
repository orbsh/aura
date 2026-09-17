use aura_realm::mq;
use aura_storage::InMemoryStore;

#[test]
fn mq_roundtrip() {
    let store: aura_actor::SharedStore = std::sync::Arc::new(InMemoryStore::default());
    let mut vs = mq::StoreAsVirtual(store.clone());
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
    // Non-object top: single _root field roundtrip.
    let seq3 = mq::append(&mut vs, "add_to_cart", "alice", &serde_json::json!([7, 8])).unwrap();
    let bl = mq::backlog(&mut vs, "add_to_cart", "alice", seq2).unwrap();
    assert_eq!(bl[0].1, serde_json::json!([7, 8]));
    let _ = seq3;
}
