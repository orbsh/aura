//! Event + MQ plane: emit, queue compaction. Split out of lib.rs per
//! ADR-0029.
//!
//! Two rulings shape this file. A queue's consumer set must be CLOSED
//! (ADR-0038 §1: a key-less subscription delivers to the singleton
//! instance, so every partition has exactly one consumer) — the slice is a
//! structural `mq::InstanceKey`, never a magic string. And no emit may be
//! dropped silently (ADR-0038 §4: a matched route producing no real target
//! is as observable as an event with no route at all — the `__default__`
//! fallback instance is retired).
//!
//! Retention (ADR-0039 §2) lives here too, because compaction runs on the
//! write path: the watermark's denominator is the route registry filtered
//! by the `cursor_ttl` predicate.

use super::{Realm, SharedRealm};
use crate::event::DeadReason;
use crate::mq::{self, InstanceKey};
use aura_booth::InstanceId;

/// The instance a queue slice is delivered to: the slice's own value for a
/// named partition (instance key = partition value), the type's singleton
/// instance for a key-less one.
fn instance_of(part: &InstanceKey) -> String {
    match part {
        InstanceKey::Singleton => mq::SINGLETON.to_string(),
        InstanceKey::Named(key) => key.clone(),
    }
}

impl Realm {
    pub async fn emit(
        self_arc: &SharedRealm,
        _emitter: Option<&str>,
        event: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let routes = {
            let mut realm = self_arc.lock().await;
            // ADR-0012: no emits whitelist — the receiver set is a runtime
            // fact; an emit with no subscribers lands in the dead ring.
            // `emitter` stays in the signature for audit/recording.
            let matched = realm.router.matches(event);
            if matched.is_empty() {
                realm.dead_events.push(event, data, DeadReason::NoRoute);
                return Ok(());
            }
            matched
        };
        // Dedupe by queue identity: several routes may bind the same queue
        // (two subscriber types on one event) — the queue fans out to all
        // of them, a second send would double-deliver. Activation of every
        // matched route's target happens in the SAME pass, before any send,
        // so every subscriber's cursor exists before the message lands.
        let mut targets: Vec<InstanceKey> = Vec::new();
        for route in routes {
            // Queue identity: @on-declared key → per-(event, partition); no
            // key → per-event singleton queue. The key comes from the event
            // data (wiki §5.4), not the emitter, and the slice is a
            // STRUCTURAL marker (ADR-0039 §1) — the singleton never enters
            // the name dictionary, so no payload key can alias into it.
            let part = if route.instance_key_field.is_empty() {
                InstanceKey::Singleton
            } else {
                match data.get(&route.instance_key_field).and_then(|v| v.as_str()) {
                    Some(key) => InstanceKey::Named(key.to_string()),
                    // ADR-0038 §4: a matched route whose declared key field
                    // is absent (or is not a string) is a MALFORMED event —
                    // not a delivery to some fallback instance. It reaches
                    // nobody, so it is recorded: the same class as a
                    // zero-target scan.
                    None => {
                        let mut realm = self_arc.lock().await;
                        realm.dead_events.push(event, data.clone(), DeadReason::MissingKeyField);
                        continue;
                    }
                }
            };
            // Virtual-booth activation: emitting to an instance that has
            // never run activates it first, so its @on subscriptions bind
            // before the event lands in the queue.
            let target = InstanceId {
                booth_type: route.booth_type.clone(),
                key: instance_of(&part),
            };
            {
                let realm = self_arc.lock().await;
                if !realm.instances.contains_key(&(route.booth_type.clone(), target.key.clone())) {
                    drop(realm);
                    let mut r = self_arc.lock().await;
                    r.instance(self_arc.clone(), &target).await?;
                }
            }
            // Queue identity is the CONCRETE event name — for a wildcard
            // route that is the emitted name (route.event is the pattern);
            // one row per concrete event per partition, N cursors fan out.
            if !targets.contains(&part) {
                targets.push(part);
            }
        }
        for part in targets {
            let mut realm = self_arc.lock().await;
            // Persistent queues (step 2b): events are passively persisted on
            // emit — an evicted/not-yet-active subscriber's backlog is
            // delivered on re-activation. The dead ring only sees events
            // with NO matching route or no real target (checked above): a
            // matched route with no live instance is a backlog write, not a
            // loss.
            let event_name = event.to_string();
            let store = realm.mq.clone();
            if let Err(e) = mq::append(&store, &event_name, &part, &data) {
                eprintln!("mq append failed for {event_name}/{part:?}: {e}");
                realm.dead_events.push(event, data.clone(), DeadReason::AppendFailed);
                continue;
            }
            // Retention (step 2b follow-up + ADR-0039 §2): min-watermark
            // over REGISTERED subscribers — the route registry matching this
            // concrete event is the denominator (evicted instances still
            // count: their backlog replays; a type whose @on for this event
            // is gone does not), and a cursor past `cursor_ttl` leaves the
            // denominator (its backlog is forfeit). Compaction deletes
            // mq-data below the watermark. Runs on the emit path (write-path
            // compaction per the ruling); the scan cost is bounded by the
            // subscriber count.
            if let Err(e) = Self::compact_queue_locked(&mut realm, &event_name, &part, &store).await {
                eprintln!("mq compaction failed for {event_name}/{part:?}: {e}");
            }
        }
        Ok(())
    }

    async fn compact_queue_locked(
        realm: &mut Realm,
        event: &str,
        part: &InstanceKey,
        store: &mq::MqStore,
    ) -> anyhow::Result<()> {
        let Some(event_id) = mq::event_id_of(store, event)? else {
            return Ok(());
        };
        let instance_key_id = mq::instance_key_id(store, part)?;
        let rows = mq::cursor_rows(store, event_id, instance_key_id)?;
        if rows.is_empty() {
            return Ok(());
        }
        // Registered subscriber check: the row's TYPE must currently
        // register a route that MATCHES this concrete event (exact or
        // wildcard). The EventRoute registry is the watermark's denominator
        // — the durable subscription set, never the raw cursor keys.
        // Eviction (instance scale-to-zero) does NOT deregister, so an
        // evicted instance keeps pinning; a type hot swap drops the route
        // and the stale cursor row falls out.
        let ttl = realm.cursor_ttl;
        let now = mq::now_ms();
        let mut min_seq: Option<u64> = None;
        for (booth_id, cursor, last_active) in &rows {
            if !mq::booth_subscribes(store, *booth_id, event)? {
                continue;
            }
            // ADR-0039 §2: a row that has not advanced within `cursor_ttl`
            // LEAVES the denominator — its backlog is forfeit (compaction
            // may pass it). The row itself stays: deleting it would read the
            // cursor back as 0 and re-deliver whatever survives above the
            // watermark. `last_active_ms == 0` = unmarked, never expires.
            if mq::cursor_expired(*last_active, now, ttl) {
                continue;
            }
            min_seq = Some(match min_seq {
                Some(m) => m.min(*cursor),
                None => *cursor,
            });
        }
        if let Some(min_seq) = min_seq {
            if min_seq > 0 {
                let _ = mq::delete_before(store, event_id, instance_key_id, min_seq)?;
            }
            // Inert rows: a cursor at or below the watermark can never
            // replay anything (the rows it would have read are gone), so the
            // row itself is reclaimable. This is what keeps the cursor table
            // from accumulating one row per partition ever seen.
            for (booth_id, cursor, _) in &rows {
                if *cursor < min_seq {
                    mq::drop_cursor(store, event_id, instance_key_id, *booth_id)?;
                }
            }
        }
        Ok(())
    }

    pub async fn compact_queue_for_test(
        self_arc: &SharedRealm,
        event: &str,
        part: &InstanceKey,
    ) -> anyhow::Result<()> {
        let mut realm = self_arc.lock().await;
        let store = realm.mq.clone();
        Self::compact_queue_locked(&mut realm, event, part, &store).await
    }
}