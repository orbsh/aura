//! Registry plane: booth types, storage plans, persisted schemas.
//! Split out of lib.rs per ADR-0029.

use super::Realm;
use crate::{mq, store_exec};
use aura_booth::call::CallSpec;
use aura_booth::BoothType;
use std::time::Duration;

impl Realm {
    pub fn register_type(&mut self, booth: BoothType) {
        self.register_type_with(booth, None)
    }

    /// Registration with the upload's introspected schema IN HAND.
    ///
    /// This is where a storage plan resolves (ADR-0026), and the two realm
    /// kinds answer that differently — which is the whole seam (ADR-0050 §6):
    ///
    /// - a PERSISTED realm asks its own rows (`meta::ns_and_schema_of`): the
    ///   `BoothName` row carries the ns, the `BoothDef` row carries the
    ///   interface_schema copy, and both outlive the process. `schema` is
    ///   ignored, because the row is the copy that matters.
    /// - a TEMPORARY realm asks ITSELF: the ns comes from the in-memory counter
    ///   (`type_ns::TypeNs`), the schema from the caller's copy, and neither is
    ///   written anywhere. The realm dies and the allocation dies with it.
    ///
    /// Nothing else in this function distinguishes them: the type table, the
    /// router and the instance/executor reclamation are the same code, and the
    /// isolation that follows is per-realm by construction.
    pub fn register_type_with(&mut self, booth: BoothType, schema: Option<serde_json::Value>) {
        self.call_specs
            .entry(booth.name.clone())
            .or_insert_with(|| CallSpec::hot(Duration::from_secs(30)));
        // Storage plan resolve (ADR-0026): ns from the registry that owns this
        // realm's namespaces, the declared collections from the schema copy
        // that registry keeps. Failure = no plan = no ctx.store surface
        // (surface absence is the correct form for "declared no storage" —
        // never a panic).
        let name = booth.name.clone();
        let resolved = match self.ephemeral_ns.as_mut() {
            Some(ns_registry) => ns_registry
                .resolve(&name)
                .map(|ns| (u32::from(ns), schema.clone())),
            None => crate::meta::ns_and_schema_of(&self.mq, &name),
        };
        let (ns, schema) = match resolved {
            Ok(v) => v,
            Err(e) => {
                eprintln!("storage plan resolve failed for `{name}`: {e}");
                (0, None)
            }
        };
        self.persisted_schemas.insert(name.clone(), schema.clone());
        self.store_plans.remove(&name);
        if let Some(schema) = schema {
            match store_exec::StorePlan::from_schema(ns as u16, &schema) {
                Ok(plan) => {
                    self.store_plans.insert(name.clone(), plan);
                }
                Err(e) => eprintln!("storage plan parse failed for `{name}`: {e}"),
            }
        }
        // One declaration surface per type: routes assemble from the
        // type's own `receives` as a side effect of registration. Re-
        // registration is a REPLACEMENT (hot-swap): drop the type's old
        // routes first, so the latest declaration is the only one — a
        // re-register that pushed would double-deliver every matched event.
        // Two stores: the in-memory router (matching hot path — rebuilt from
        // the persisted registry is unnecessary; the registry IS rebuilt on
        // restart through this same code from boot reload) and, for a PERSISTED
        // realm, the EventRoute table (mq): subscription facts survive restart
        // and are readable by ops without introspecting scripts.
        //
        // A temporary realm keeps the router half only (ADR-0050 §6): its rows
        // would die with it, so writing them would be state that exists only to
        // be discarded — and "registration inside one writes nothing" is a
        // property this branch keeps by name.
        let persistent = self.ephemeral_ns.is_none();
        self.router.drop_booth(&booth.name);
        if persistent {
            if let Err(e) = mq::routes_drop_booth(&self.mq, &booth.name) {
                eprintln!("route drop failed for {}: {e}", booth.name);
            }
        }
        // Hot replacement reaches execution too: executors hold
        // the PREVIOUS source, so instances of this type are reclaimed —
        // the next message cold-starts on the new code. Nothing is lost:
        // the instance map is a discardable hot cache (state lives in the
        // type's collections, backlog in the mq queues, replayed by
        // cursor on re-activation).
        let stale: Vec<_> = self
            .instances
            .keys()
            .filter(|(t, _)| t == &booth.name)
            .cloned()
            .collect();
        for (t, k) in stale {
            self.instances.remove(&(t.clone(), k.clone()));
            self.executors.evict(&format!("{t}/{k}"));
        }
        for decl in &booth.receives {
            match &decl.resolution {
                aura_booth::RouteResolution::Scan {
                    collection,
                    index,
                    probe_field,
                } if !decl.wildcard => {
                    self.router.on_resolve(
                        decl.event.clone(),
                        &booth.name,
                        collection.clone(),
                        index.clone(),
                        probe_field.clone(),
                    );
                }
                _ if decl.wildcard => self.router.on_wildcard(&decl.event, &booth.name),
                // A declaration may bind an ENTRY separately from the event (`entry`):
                // subscribe to `ui:add-to-cart`, be called as `add_to_cart`.
                aura_booth::RouteResolution::Singleton => match &decl.entry {
                    Some(h) => self
                        .router
                        .on_as(decl.event.clone(), &booth.name, "", h.clone()),
                    None => self.router.on(decl.event.clone(), &booth.name, ""),
                },
                aura_booth::RouteResolution::Field(f) => match &decl.entry {
                    Some(h) => {
                        self.router
                            .on_as(decl.event.clone(), &booth.name, f.clone(), h.clone())
                    }
                    None => self.router.on(decl.event.clone(), &booth.name, f.clone()),
                },
                // A wildcard scan declaration is a category error at the
                // declaration layer; treated as the wildcard's singleton
                // delivery by the `_ if wildcard` arm above.
                _ => {}
            }
            // The declaration's auth flag becomes the route's: one name, one place, and the
            // venue's gate reads the table instead of re-parsing a schema.
            if decl.auth {
                self.router.gated(&decl.event, &booth.name);
            }
            if persistent {
                if let Err(e) = mq::route_put(
                    &self.mq,
                    &decl.event,
                    &booth.name,
                    &decl.resolution,
                    decl.wildcard,
                ) {
                    eprintln!(
                        "route persist failed for {}/{}: {e}",
                        decl.event, booth.name
                    );
                }
            }
        }
        self.types.insert(booth.name.clone(), booth);
    }

    pub fn plan_of(&self, type_name: &str) -> Option<&store_exec::StorePlan> {
        self.store_plans.get(type_name)
    }

    pub fn schema_of(&self, type_name: &str) -> Option<&Option<serde_json::Value>> {
        self.persisted_schemas.get(type_name)
    }

    pub fn booth_type(&self, name: &str) -> Option<&BoothType> {
        self.types.get(name)
    }
}
