//! The stop bit (ADR-0047): one in-memory flag per activation, and no storage.
//!
//! A stop is resource control, the watchdog's family — not application state.
//! Nothing is owed to an activation the process no longer runs, and the record
//! of what a stopped turn did lives in the store (rows left OPEN, which the
//! projection skips). So this is a set of instance identities and nothing else:
//! no table, no ns, no restore path.
//!
//! Why a bit and not an event: a delivery addressed to an instance that is
//! running work queues behind that work (measured 2026-11-09 on gravity's turn
//! booth), so the door has to be a call the framework can make while the
//! activation runs. That door is `Realm::stop_set` / `Realm::stop_clear`; a
//! guest sees the consequence at its next boundary through the `stop.poll` arm.
//!
//! LIFT (stage two, ADR-0047 §4) lives here too, and it is a different kind of
//! thing: not a flag anyone reads later but a SLOT the running call is already
//! waiting on. The dispatch arms one before it runs a body; `Realm::stop_lift`
//! notifies it, and the caller gets the terminal error value instead of an
//! answer. One per instance at a time (ADR-0047 §3: a stop names an activation).

use crate::InstanceId;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

/// The terminal value of an interrupted call (ADR-0012): an error, not a result,
/// and never retried. Callers tell it apart by downcasting — "the caller changed
/// its mind" is not "the call failed", and a retry policy must not treat them alike.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted")
    }
}

impl std::error::Error for Interrupted {}

/// One running call's lift slot. Armed by the dispatch, notified by the door, and
/// removed on drop (a slot that outlived its call would fire into the next one).
pub struct Lift {
    key: String,
    slot: Arc<tokio::sync::Notify>,
    lifts: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
}

impl Lift {
    /// The future a dispatch races its body against.
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.slot.notified()
    }
}

impl Drop for Lift {
    fn drop(&mut self) {
        let mut map = self
            .lifts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only if it is still OURS: a later call for the same instance may have
        // replaced the slot already, and removing that one would disarm it.
        if map
            .get(&self.key)
            .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
        {
            map.remove(&self.key);
        }
    }
}

/// The set of stopped activations, shared with every ctx bridge that can be
/// asked to poll it. One per realm.
#[derive(Clone, Default)]
pub struct Stops {
    inner: Arc<RwLock<HashSet<String>>>,
    /// The stage-two slots, by instance identity. Empty in every process that is
    /// not running a call — which is the normal state of a stop bit's world.
    lifts: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
}

/// The identity a flag is keyed by: booth type + rendered instance key. Rendered
/// because that is what crosses a process boundary elsewhere (a row, a wire),
/// and one spelling of an identity is worth more than none.
fn identity(instance: &InstanceId) -> String {
    format!("{}/{}", instance.booth_type, instance.key.render())
}

impl Stops {
    /// Ask this activation to stop at its next boundary. Idempotent; asking
    /// twice is the same as asking once.
    pub fn set(&self, instance: &InstanceId) {
        if let Ok(mut set) = self.inner.write() {
            set.insert(identity(instance));
        }
    }

    /// The activation answered the request (or a new one started): forget it. A
    /// stale bit that outlived its activation would stop the NEXT activation of
    /// the same instance, which is the one failure this shape can have.
    pub fn clear(&self, instance: &InstanceId) {
        if let Ok(mut set) = self.inner.write() {
            set.remove(&identity(instance));
        }
    }

    /// What a guest's boundary reads. `true` = someone asked this activation to
    /// stop; the guest decides what "stop" means (ADR-0008: close no turn).
    pub fn is_set(&self, instance: &InstanceId) -> bool {
        self.inner
            .read()
            .map(|set| set.contains(&identity(instance)))
            .unwrap_or(false)
    }

    /// Eviction drops the whole instance's flag with it — a dead activation has
    /// nothing left to ask.
    pub fn forget_all(&self) {
        if let Ok(mut set) = self.inner.write() {
            set.clear();
        }
    }

    /// Arm this call's lift slot. `Some` for every dispatch that runs a body: a
    /// call nobody can lift is the one case this shape is for.
    pub fn begin_lift(&self, instance: &InstanceId) -> Lift {
        let key = identity(instance);
        let slot = Arc::new(tokio::sync::Notify::new());
        self.lifts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone(), Arc::clone(&slot));
        Lift {
            key,
            slot,
            lifts: Arc::clone(&self.lifts),
        }
    }

    /// Raise the lift. `false` = no call is running for this instance, so there is
    /// nothing to lift — which is not an error ("stop my turn" from a page whose
    /// turn already ended) and must not be reported as one.
    pub fn lift(&self, instance: &InstanceId) -> bool {
        let slot = self
            .lifts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&identity(instance))
            .cloned();
        match slot {
            Some(slot) => {
                // `notify_one` stores a permit if the waiter has not registered yet,
                // so an early lift is not lost between the insert and the select.
                slot.notify_one();
                true
            }
            None => false,
        }
    }
}