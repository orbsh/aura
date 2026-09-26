//! Registry plane: actor types, storage plans, persisted schemas.
//! Split out of lib.rs per ADR-0029.

use super::Realm;
use crate::{mq, store_exec};
use aura_actor::ActorType;
use aura_actor::call::CallSpec;
use std::time::Duration;

impl Realm {
    pub fn register_type(&mut self, actor: ActorType) {
        self.call_specs
            .entry(actor.name.clone())
            .or_insert_with(|| CallSpec::hot(Duration::from_secs(30)));
        // Storage plan resolve (ADR-0026): ns from the type registry, the
        // declared collections from the persisted interface_schema copy.
        // Failure = no plan = no ctx.store surface (surface absence is
        // the correct form for "declared no storage" — never a panic).
        let (ns, schema) = match crate::meta::ns_and_schema_of(&self.mq, &actor.name) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("storage plan resolve failed for `{}`: {e}", actor.name);
                (0, None)
            }
        };
        self.persisted_schemas.insert(actor.name.clone(), schema.clone());
        self.store_plans.remove(&actor.name);
        if let Some(schema) = schema {
            match store_exec::StorePlan::from_schema(ns as u16, &schema) {
                Ok(plan) => {
                    self.store_plans.insert(actor.name.clone(), plan);
                }
                Err(e) => eprintln!("storage plan parse failed for `{}`: {e}", actor.name),
            }
        }
        // One declaration surface per type: routes assemble from the
        // type's own `receives` as a side effect of registration. Re-
        // registration is a REPLACEMENT (hot-swap): drop the type's old
        // routes in both stores first, so the latest declaration is the
        // only one — a re-register that pushed would double-deliver every
        // matched event. Two stores: the in-memory router (matching hot
        // path — rebuilt from the persisted registry is unnecessary; the
        // registry IS rebuilt on restart through this same code from boot
        // reload) and the PERSISTED EventRoute table (mq): subscription
        // facts survive restart and are readable by ops without
        // introspecting scripts.
        self.router.drop_actor(&actor.name);
        if let Err(e) = mq::routes_drop_actor(&self.mq, &actor.name) {
            eprintln!("route drop failed for {}: {e}", actor.name);
        }
        // Hot replacement reaches execution too: resident sessions hold
        // the PREVIOUS source, so instances of this type are reclaimed —
        // the next message cold-starts on the new code. Nothing is lost:
        // the instance map is a discardable hot cache (state lives in the
        // type's collections, backlog in the mq queues, replayed by
        // cursor on re-activation).
        let stale: Vec<_> = self
            .instances
            .keys()
            .filter(|(t, _)| t == &actor.name)
            .cloned()
            .collect();
        for (t, k) in stale {
            self.instances.remove(&(t.clone(), k.clone()));
            self.sessions.evict(&format!("{t}/{k}"));
        }
        for decl in &actor.receives {
            if decl.wildcard {
                self.router.on_wildcard(&decl.event, &actor.name);
            } else {
                self.router.on(decl.event.clone(), &actor.name, &decl.key_field);
            }
            if let Err(e) = mq::route_put(&self.mq, &decl.event, &actor.name, &decl.key_field, decl.wildcard) {
                eprintln!("route persist failed for {}/{}: {e}", decl.event, actor.name);
            }
        }
        self.types.insert(actor.name.clone(), actor);
    }

    pub fn plan_of(&self, type_name: &str) -> Option<&store_exec::StorePlan> {
        self.store_plans.get(type_name)
    }

    pub fn schema_of(&self, type_name: &str) -> Option<&Option<serde_json::Value>> {
        self.persisted_schemas.get(type_name)
    }

    pub fn actor_type(&self, name: &str) -> Option<&ActorType> {
        self.types.get(name)
    }
}
