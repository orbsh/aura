#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0047, stage two (the LIFT slot): a running call is not a bit anybody reads
//! later — it is a waiter, and the door notifies it. What these tests lock is the
//! lifetime rule (a slot belongs to ONE call) and the answer the door gives when
//! there is nothing to lift (a cancel arriving after its turn ended).

use aura_booth::{InstanceId, InstanceKey};
use aura_realm::stop::Stops;
use std::time::Duration;

fn at(booth: &str, key: &str) -> InstanceId {
    InstanceId {
        booth_type: booth.into(),
        key: InstanceKey::Named(key.into()),
    }
}

#[tokio::test]
async fn a_lift_wakes_the_call_that_armed_the_slot() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    let armed = stops.begin_lift(&a);

    // Armed and not yet lifted: the future must still be pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), armed.notified())
            .await
            .is_err(),
        "nothing asked yet"
    );

    assert!(stops.lift(&a), "the slot is live, so the door has something to lift");
    tokio::time::timeout(Duration::from_millis(200), armed.notified())
        .await
        .expect("the lift reaches the call that armed the slot");
}

#[tokio::test]
async fn a_lift_with_no_call_running_says_so_instead_of_arming_one() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    // A page's cancel can arrive after its turn ended. That is not an error, and
    // it must not leave a slot behind that lifts the NEXT turn.
    assert!(!stops.lift(&a));

    let armed = stops.begin_lift(&a);
    drop(armed);
    assert!(
        !stops.lift(&a),
        "the slot died with its call: a lift now has nothing to notify"
    );
}

#[tokio::test]
async fn one_slot_per_instance_and_the_newest_call_owns_it() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    let first = stops.begin_lift(&a);
    let second = stops.begin_lift(&a);

    // Dropping the superseded call's guard must NOT disarm the live one — the
    // pointer check in `Drop` is what keeps a stale call from cancelling a lift.
    drop(first);
    assert!(stops.lift(&a), "the second call still owns the slot");
    tokio::time::timeout(Duration::from_millis(200), second.notified())
        .await
        .expect("the live call is the one that hears it");
}