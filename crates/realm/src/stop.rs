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
//! LIFT (stage two, ADR-0047 §4) is the other half and is NOT here yet: it needs
//! the execution layer's one primitive, and until that exists this file is the
//! cooperative stage only.

use crate::InstanceId;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// The set of stopped activations, shared with every ctx bridge that can be
/// asked to poll it. One per realm.
#[derive(Clone, Default)]
pub struct Stops {
    inner: Arc<RwLock<HashSet<String>>>,
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
}