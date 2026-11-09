#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0047, stage one: the stop is a bit in the realm's memory, set through the
//! framework's door and read by the activation at a boundary it already has.
//! Nothing here is stored — these tests are the whole lifetime of the fact.

use aura_booth::{InstanceId, InstanceKey};
use aura_realm::{mq::MqStore, stop::Stops, Realm};

fn at(booth: &str, key: &str) -> InstanceId {
    InstanceId {
        booth_type: booth.into(),
        key: InstanceKey::Named(key.into()),
    }
}

#[test]
fn a_stop_is_a_bit_per_instance_and_it_does_not_outlive_its_clear() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    let b = at("turn", "device-b");

    assert!(!stops.is_set(&a), "nobody asked yet");
    stops.set(&a);
    assert!(stops.is_set(&a), "the activation would see it at its boundary");
    assert!(!stops.is_set(&b), "one instance's stop is not another's");

    stops.set(&a);
    assert!(stops.is_set(&a), "setting twice is setting once");
    stops.clear(&a);
    assert!(
        !stops.is_set(&a),
        "a cleared bit cannot stop the NEXT activation of the same instance"
    );

    stops.set(&a);
    stops.forget_all();
    assert!(!stops.is_set(&a), "eviction drops what nothing is left to ask");
}

#[tokio::test]
async fn the_realm_door_sets_exactly_the_activation_it_names() {
    let realm = Realm::with_mq(MqStore::mem()).shared_async().await;
    let a = at("turn", "device-a");
    let b = at("turn", "device-b");

    {
        let locked = realm.lock().await;
        locked.stop_set(&a);
        assert!(locked.stops.is_set(&a));
        assert!(!locked.stops.is_set(&b));
        locked.stop_clear(&a);
        assert!(!locked.stops.is_set(&a), "the door closes as cheaply as it opens");
    }
}