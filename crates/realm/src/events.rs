//! Event + MQ plane: emit, queue compaction. Split out of lib.rs per
//! ADR-0029.

use super::{Realm, SharedRealm};
use crate::mq;
use aura_actor::{ActorType, InstanceId, Job};
use crate::event;
use std::collections::HashMap;

impl Realm {
    pub async fn emit(
        self_arc: &SharedRealm,
        emitter: Option<&str>,
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
                realm.dead_events.push(event, data);
                return Ok(());
            }
            matched
        };
        // Dedupe by queue id: several routes may bind the same queue
        // (two subscriber types on one event) — the queue fans out to all
        // of them; a second send would double-deliver. Activation of every
        // matched route's target happens in the SAME pass, before any
        // send, so every subscriber binds its Receiver before the message
        // lands.
        let mut queued: std::collections::HashSet<(String, String)> = Default::default();
        let mut targets: Vec<(event::Route, String)> = Vec::new();
        for route in routes {
            // Queue identity: @on-declared key → per-(event, partition);
            // no key → per-event singleton queue. The key comes from the
            // event data (wiki §5.4), not the emitter.
            let partition = if route.instance_key_field.is_empty() {
                mq::SINGLETON.to_string()
            } else {
                data.get(&route.instance_key_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("__default__")
                    .to_string()
            };
            // Virtual-actor activation: emitting to an instance that has
            // never run activates it first, so its @on subscriptions bind
            // before the event lands in the queue.
            let target = InstanceId {
                actor_type: route.actor_type.clone(),
                key: partition.clone(),
            };
            {
                let mut realm = self_arc.lock().await;
                if !realm.instances.contains_key(&(route.actor_type.clone(), partition.clone())) {
                    drop(realm);
                    let mut r = self_arc.lock().await;
                    r.instance(self_arc.clone(), &target).await?;
                }
            }
            // Queue identity is the CONCRETE event name — for a wildcard
            // route that is the emitted name (route.event is the pattern);
            // one row per concrete event per partition, N cursors fan out.
            if queued.insert((event.to_string(), partition.clone())) {
                targets.push((route, partition));
            }
        }
        for (route, partition) in targets {
            let mut realm = self_arc.lock().await;
            // Persistent queues (step 2b): events are passively persisted
            // on emit — an evicted/not-yet-active subscriber's backlog is
            // delivered on re-activation. The dead ring only sees events
            // with NO matching route (checked above): a matched route with
            // no live instance is a backlog write, not a loss.
            let event_name = event.to_string();
            let store = realm.mq.clone();
            if let Err(e) = mq::append(&store, &event_name, &partition, &data) {
                eprintln!("mq append failed for {event_name}/{partition}: {e}");
                realm.dead_events.push(&event, data.clone());
                continue;
            }
            // Retention (step 2b follow-up): min-watermark over REGISTERED
            // subscribers — the route registry is the denominator (evicted
            // instances still count: their backlog replays; a type whose
            // @on for this event is gone does not). Cursor rows whose actor
            // name has no matching registered (type, key) instance fall
            // out; compaction deletes mq-data below the watermark. Runs on
            // the emit path (write-path compaction per the ruling); the
            // scan cost is bounded by the subscriber count.
            if let Err(e) = Self::compact_queue_locked(&mut realm, &event_name, &partition, &store).await {
                eprintln!("mq compaction failed for {event_name}/{partition}: {e}");
            }
        }
        Ok(())
    }

    async fn compact_queue_locked(
        realm: &mut Realm,
        event: &str,
        partition: &str,
        store: &mq::MqStore,
    ) -> anyhow::Result<()> {
        let event_id = match mq::event_id_of(store, event)? {
            Some(id) => id,
            None => return Ok(()),
        };
        let part_id = mq::part_hash_of(partition);
        let rows = mq::cursor_rows(store, event_id, part_id)?;
        if rows.is_empty() {
            return Ok(());
        }
        // Registered subscriber check: the cursor name ("type/key")
        // must belong to a type whose PERSISTED routes include this
        // event (the EventRoute registry — the watermark denominator
        // is the durable subscription set, not the in-memory router
        // and not the raw cursor keys).
        let mut min_seq: Option<u64> = None;
        for (actor_id, cursor) in &rows {
            let Some(name) = mq::actor_name_of(store, *actor_id)? else {
                continue;
            };
            let Some((type_name, _key)) = name.split_once('/') else {
                continue;
            };
            let registered = match mq::actor_id_of(store, type_name) {
                Ok(aid) => mq::routes_of_event(store, event)?
                    .iter()
                    .any(|(r_aid, _, _)| *r_aid == aid),
                Err(_) => false,
            };
            if registered {
                min_seq = Some(match min_seq {
                    Some(m) => m.min(*cursor),
                    None => *cursor,
                });
            }
        }
        if let Some(min_seq) = min_seq {
            if min_seq > 0 {
                let _ = mq::delete_before(store, event_id, part_id, min_seq)?;
            }
        }
        Ok(())
    }

    pub async fn compact_queue_for_test(
        self_arc: &SharedRealm,
        event: &str,
        partition: &str,
    ) -> anyhow::Result<()> {
        let mut realm = self_arc.lock().await;
        let store = realm.mq.clone();
        Self::compact_queue_locked(&mut realm, event, partition, &store).await
    }
}
