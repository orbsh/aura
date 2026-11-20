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
    /// This is where a storage plan resolves (ADR-0026), and BOTH realm kinds
    /// answer it the same way (ADR-0050 §6): ask this realm's own rows —
    /// `meta::ns_and_schema_of` — where a `BoothName` row carries the ns and a
    /// `BoothDef` row carries the interface_schema copy. A persisted realm's
    /// rows live in fjall and outlive the process; a temporary realm's rows live
    /// in its own memory engine and die with it. That is the only difference, and
    /// it is a difference of engine, not of rule.
    pub fn register_type_with(&mut self, booth: BoothType, caller_schema: Option<serde_json::Value>) {
        self.call_specs
            .entry(booth.name.clone())
            .or_insert_with(|| CallSpec::hot(Duration::from_secs(30)));
        // Storage plan resolve (ADR-0026): ns from the registry that owns this
        // realm's namespaces, the declared collections from the schema copy
        // that registry keeps. Failure = no plan = no ctx.store surface
        // (surface absence is the correct form for "declared no storage" —
        // never a panic).
        let name = booth.name.clone();
        // ONE rule for both kinds of realm (ADR-0050 §6): the ns comes from the
        // dictionary row that lives in THIS realm's own store — fjall for a
        // persisted realm, its own memory for a temporary one — so a temporary
        // realm allocates exactly like a persisted one (BOOTH_NS_BASE + id,
        // monotonic, never reclaimed) and forgets the whole dictionary when it
        // dies. The engine is the only difference between them.
        let (ns, schema) = match crate::meta::ns_and_schema_of(&self.mq, &name) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("storage plan resolve failed for `{name}`: {e}");
                (0, None)
            }
        };
        // A caller holding a schema can seed the plan when the realm's own row
        // has no copy (a Rust body registered without an upload), but the row
        // wins when it exists: it is the copy that survived the last write.
        let schema = schema.or(caller_schema);
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
        // Both halves, for both kinds of realm: the rows go to this realm's own
        // store, so a temporary realm's routes die with it exactly as its type
        // dictionary does. Writing them is not "state that exists only to be
        // discarded" — it is the same code path, against a memory engine.
        self.router.drop_booth(&booth.name);
        if let Err(e) = mq::routes_drop_booth(&self.mq, &booth.name) {
            eprintln!("route drop failed for {}: {e}", booth.name);
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
            // Written for both kinds of realm, into this realm's own store — a
            // temporary realm's route rows die with it, exactly as its type
            // dictionary does (ADR-0050 §6: one rule, two engines).
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
        self.types.insert(booth.name.clone(), booth);
    }

    /// The DECLARATION half of registration: the definition row, the code blob
    /// and the type registration, in that order, against THIS realm's own store.
    ///
    /// It is the same body every registration ends at (`Engine::register_into`
    /// calls it after introspection), exposed here because a host is not the only
    /// thing that can hold a declaration: a job brings its declarations as data
    /// (ADR-0051 §1), and the layer that owns the realm — this crate — must be
    /// able to write them without naming an engine. One implementation, two
    /// callers; there is no second rule.
    ///
    /// `schema` is the caller's storage/interface declaration, handed to
    /// `register_type_with` as the caller copy: a row already in this realm (a
    /// re-registration) still takes precedence there, and an absent declaration
    /// simply leaves the type without a `ctx.store` surface (ADR-0026 — surface
    /// absence, never a panic).
    pub fn register_definition(
        &mut self,
        booth: BoothType,
        schema: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        // A Rust body has no definition to persist (`from_type` answers None):
        // it is a compile-time construct, in either kind of realm.
        if let Some(mut def) = aura_booth::persist::PersistedBooth::from_type(&booth) {
            def.schema = schema.clone();
            crate::meta::persist(&self.mq, &def)?;
        }
        // Content addressing covers remote types too (ADR-0027): the dispatch arm
        // references `sha256(source)`, and the serving source reads CodeBlob — so
        // the bytes must enter the blob store at upload. Same for both realm
        // kinds: a temporary realm's blob goes into its own store and dies with it
        // (ADR-0051 §4).
        if let aura_booth::Body::Remote {
            ref source, ..
        } = booth.body
        {
            crate::meta::put_blob(&self.mq, crate::meta::code_hash(source), source.as_bytes())?;
        }
        self.register_type_with(booth, schema);
        Ok(())
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
