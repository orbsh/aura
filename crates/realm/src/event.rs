//! Event routing (Phase 3): emit routing table, dead-event ring.
//!
//! Design (wiki §5): the event name IS the reference. Booths never address
//! each other directly — `emit(name, data)` reaches whoever registered
//! `on(name)`; the instance key is extracted from event data, not from
//! the emitter's identity. Two routing layers:
//!
//! - exact: event name → routes (booth_type + instance_key_field)
//! - wildcard: prefix `foo.` → singleton instance routes
//!
//! `emits` is a whitelist: an booth may only emit names it declared.
//! Undeclared emits are rejected at the Realm boundary (audit point).
//! Unmatched events land in the dead-letter ring (observable, bounded).

use serde_json::Value;

/// One registration: which booth type handles this event, and which field
/// of the event payload carries the instance key.
#[derive(Clone, Debug)]
pub struct Route {
    pub booth_type: String,
    /// Field name in the event payload; empty = singleton instance.
    pub instance_key_field: String,
    /// The event name as the handler sees it (kept so multi-event booths
    /// can dispatch; script booths receive a map keyed by event name once
    /// Phase 2.5 lands).
    pub event: String,
}

/// Route table: exact map + wildcard prefixes (linear scan — wildcard
/// count is small by construction; a Trie is over-engineering here).
#[derive(Default, Debug)]
pub struct EventRouter {
    exact: HashMap<String, Vec<Route>>,
    wildcard: Vec<(String, Route)>, // prefix (without trailing `*`)
    // ADR-0012: no emits whitelist — the receiver set is a runtime fact;
    // the dead-event ring is the audit surface.
}

use std::collections::HashMap;

impl EventRouter {
    /// Register a precise subscription: `on("order_created", key="user_id")`.
    pub fn on(&mut self, event: impl Into<String>, booth_type: &str, instance_key_field: &str) {
        let name = event.into();
        self.exact.entry(name.clone()).or_default().push(Route {
            booth_type: booth_type.into(),
            instance_key_field: instance_key_field.into(),
            event: name,
        });
    }

    /// Register a wildcard subscription: `on("order.*")` → prefix "order.".
    /// Routes to the booth type's singleton instance.
    pub fn on_wildcard(&mut self, pattern: &str, booth_type: &str) {
        let prefix = pattern.trim_end_matches('*').to_string();
        self.wildcard.push((
            prefix,
            Route {
                booth_type: booth_type.into(),
                instance_key_field: String::new(),
                event: pattern.to_string(),
            },
        ));
    }

    /// Drop every route bound to one booth type (hot-swap re-registration:
    /// the type's NEW `receives` assembles after this, so the version that
    /// wins is the latest declaration — no duplicates, no stale events).
    pub fn drop_booth(&mut self, booth_type: &str) {
        for routes in self.exact.values_mut() {
            routes.retain(|r| r.booth_type != booth_type);
        }
        self.wildcard.retain(|(_, r)| r.booth_type != booth_type);
    }

    /// All routes matching an event name: exact first, then wildcards.
    /// A single event may hit both — each match delivers independently.
    /// All routes bound by one booth type (its @on declarations) — the
    /// subscription set an activated instance binds its queue Receivers
    /// against (Phase 4.5c step 2).
    pub fn routes_of(&self, booth_type: &str) -> Vec<Route> {
        self.exact
            .values()
            .flatten()
            .filter(|r| r.booth_type == booth_type)
            .cloned()
            .chain(self.wildcard.iter().filter(|(_, r)| r.booth_type == booth_type).map(|(_, r)| r.clone()))
            .collect()
    }

    pub fn matches(&self, event: &str) -> Vec<Route> {
        let mut out: Vec<Route> = self
            .exact
            .get(event).cloned()
            .unwrap_or_default();
        for (prefix, route) in &self.wildcard {
            if event.starts_with(prefix.as_str()) {
                out.push(route.clone());
            }
        }
        out
    }
}

/// Dead-letter ring: unmatched events, bounded, observable. Not a queue —
/// dead events are diagnostic output, not retryable work.
pub struct DeadEvents {
    ring: std::collections::VecDeque<(String, Value, std::time::Instant)>,
    capacity: usize,
    dropped: u64,
}

impl DeadEvents {
    pub fn new(capacity: usize) -> Self {
        Self { ring: Default::default(), capacity, dropped: 0 }
    }

    pub fn push(&mut self, event: &str, data: Value) {
        if self.ring.len() == self.capacity {
            self.ring.pop_front();
            self.dropped += 1;
        }
        self.ring
            .push_back((event.into(), data, std::time::Instant::now()));
    }

    /// Snapshot for observation (oldest first).
    pub fn snapshot(&self) -> Vec<(String, Value)> {
        self.ring
            .iter()
            .map(|(e, d, _)| (e.clone(), d.clone()))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

impl Default for DeadEvents {
    fn default() -> Self {
        Self::new(256)
    }
}
