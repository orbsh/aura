#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]
//! ADR-0047, stage two (the INTERRUPT slot): a running call is not a bit anybody reads
//! later — it is a waiter, and the door notifies it. What these tests lock is the
//! lifetime rule (a slot belongs to ONE call) and the answer the door gives when
//! there is nothing to interrupt (a cancel arriving after its turn ended).

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
async fn an_interrupt_wakes_the_call_that_armed_the_slot() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    let armed = stops.begin_interrupt(&a);

    // Armed and not yet raised: the future must still be pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), armed.notified())
            .await
            .is_err(),
        "nothing asked yet"
    );

    assert!(stops.interrupt(&a), "the slot is live, so the door has something to interrupt");
    tokio::time::timeout(Duration::from_millis(200), armed.notified())
        .await
        .expect("the interrupt reaches the call that armed the slot");
}

#[tokio::test]
async fn an_interrupt_with_no_call_running_says_so_instead_of_arming_one() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    // A page's cancel can arrive after its turn ended. That is not an error, and
    // it must not leave a slot behind that interrupts the NEXT turn.
    assert!(!stops.interrupt(&a));

    let armed = stops.begin_interrupt(&a);
    drop(armed);
    assert!(
        !stops.interrupt(&a),
        "the slot died with its call: an interrupt now has nothing to notify"
    );
}

#[tokio::test]
async fn one_slot_per_instance_and_the_newest_call_owns_it() {
    let stops = Stops::default();
    let a = at("turn", "device-a");
    let first = stops.begin_interrupt(&a);
    let second = stops.begin_interrupt(&a);

    // Dropping the superseded call's guard must NOT disarm the live one — the
    // pointer check in `Drop` is what keeps a stale call from cancelling an interrupt.
    drop(first);
    assert!(stops.interrupt(&a), "the second call still owns the slot");
    tokio::time::timeout(Duration::from_millis(200), second.notified())
        .await
        .expect("the live call is the one that hears it");
}