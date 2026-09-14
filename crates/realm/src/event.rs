//! Event namespace (Phase 3): emit routing, emits whitelist, dead events.
//!
//! Design (wiki §5): the event name IS the reference. Actors never address
//! each other directly — `emit(name, data)` reaches whoever registered
//! `on(name)`; the partition key is extracted from event data, not from
//! the emitter's identity. Two routing layers:
//!
//! - exact: event name → routes (actor_type + partition_key_field)
//! - wildcard: prefix `foo.` → singleton instance routes
//!
//! `emits` is a whitelist: an actor may only emit names it declared.
//! Undeclared emits are rejected at the Realm boundary (audit point).
//! Unmatched events land in the dead-letter ring (observable, bounded).

use serde_json::Value;

/// One registration: which actor type handles this event, and which field
/// of the event payload carries the partition key.
#[derive(Clone, Debug)]
pub struct Route {
    pub actor_type: String,
    /// Field name in the event payload; empty = singleton instance.
    pub partition_key_field: String,
    /// The event name as the handler sees it (kept so multi-event actors
    /// can dispatch; script actors receive a map keyed by event name once
    /// Phase 2.5 lands).
    pub event: String,
}

/// Route table: exact map + wildcard prefixes (linear scan — wildcard
/// count is small by construction; a Trie is over-engineering here).
#[derive(Default, Debug)]
pub struct EventRouter {
    exact: HashMap<String, Vec<Route>>,
    wildcard: Vec<(String, Route)>, // prefix (without trailing `*`)
    /// Whitelist per actor type: which event names it may emit.
    emits: HashMap<String, Vec<String>>,
}

use std::collections::HashMap;

impl EventRouter {
    /// Register a precise subscription: `on("order_created", key="user_id")`.
    pub fn on(&mut self, event: impl Into<String>, actor_type: &str, partition_key_field: &str) {
        let name = event.into();
        self.exact.entry(name.clone()).or_default().push(Route {
            actor_type: actor_type.into(),
            partition_key_field: partition_key_field.into(),
            event: name,
        });
    }

    /// Register a wildcard subscription: `on("order.*")` → prefix "order.".
    /// Routes to the actor type's singleton instance.
    pub fn on_wildcard(&mut self, pattern: &str, actor_type: &str) {
        let prefix = pattern.trim_end_matches('*').to_string();
        self.wildcard.push((
            prefix,
            Route {
                actor_type: actor_type.into(),
                partition_key_field: String::new(),
                event: pattern.to_string(),
            },
        ));
    }

    /// Declare the emits whitelist for an actor type.
    pub fn declare_emits(&mut self, actor_type: &str, emits: Vec<String>) {
        self.emits.insert(actor_type.into(), emits);
    }

    /// Whitelist check (ADR-0011 audit point): an actor with no `emits`
    /// declaration may not emit at all.
    pub fn may_emit(&self, actor_type: &str, event: &str) -> bool {
        self.emits
            .get(actor_type)
            .map(|list| list.iter().any(|e| e == event))
            .unwrap_or(false)
    }

    /// All routes matching an event name: exact first, then wildcards.
    /// A single event may hit both — each match delivers independently.
    pub fn matches(&self, event: &str) -> Vec<Route> {
        let mut out: Vec<Route> = self
            .exact
            .get(event)
            .map(|v| v.clone())
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
