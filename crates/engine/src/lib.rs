//! Engine assembly: config + realm + runtime. Phase 1 — store-backed ctx
//! state, idle-TTL eviction (scale-to-zero), submit-based call path.

use aura_booth::BoothType;
use aura_realm::{Realm, SharedRealm};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct Engine {
    /// The `main` realm (ADR-0050 §4): the realm the engine's own surfaces
    /// work against — CLI echo, single-node tests, the boot reload. It is an
    /// ordinary member of `realm_set` (id 1 on a fresh store), not an
    /// unprefixed default; multi-realm surfaces resolve others by name.
    pub realm: SharedRealm,
    /// The realm set (Phase 3.6 mechanism, ADR-0028 naming; binding
    /// dimension demoted to an application decision per PLAN 4.10):
    /// structural isolation — a NamedRealm handle cannot reach another
    /// realm.
    pub realm_set: Arc<aura_realm::realm_set::RealmSet>,
}

impl Engine {
    /// Start the engine: resolve the state store from config, spawn the
    /// idle evictor. Boot errors on engine/feature mismatches (PLAN Phase 4
    /// matrix: an engine chosen without its feature compiled in fails at
    /// boot, never silently falls back).
    pub async fn start(config: &aura_config::EngineConfig) -> anyhow::Result<Self> {
        let mq = Self::open_planes(&config.engine, config.data_dir.clone(), &config.node_id)?;
        let cursor_ttl = Duration::from_secs(config.cursor_ttl_secs);
        // Realms share the SAME okm engine (one fjall keyspace); each
        // realm derives a prefix-bound handle at construction —
        // state documents AND mq tables ride it (ADR-0018 steps 1+2).
        let realm_set = Arc::new(
            aura_realm::realm_set::RealmSet::with_mq_and_code_base(
                mq,
                config.code_base_url.clone(),
            )
            .with_cursor_ttl(cursor_ttl),
        );
        // ADR-0050 §4: there is no unprefixed realm. `main` is an ordinary
        // persisted realm in the set — opened by name, prefixed by id like
        // every other one — and IT is the handle the engine's own surfaces
        // (register/emit/call, the boot reload) work against. Nothing here
        // writes a key that belongs to no realm.
        let main = realm_set.open(aura_realm::realm_set::MAIN_REALM).await?;
        let realm: SharedRealm = main.realm();
        let main_store = main.store();
        Realm::spawn_evictor(&realm);
        // Boot reload (Phase 4.5b): persisted script booths re-register from
        // the meta store — definitions outlive the process.
        let engine = Self { realm, realm_set };
        // Boot reload (ADR-0025 Plan A): definitions live in the DATA
        // plane's okm instance (booth_defs beside mq/state) — which is
        // `main`'s own slice of the keyspace (ADR-0050): the plan's rows are
        // read from the realm prefix they were written under.
        for def in aura_realm::meta::load_all(&main_store)? {
            engine.register(def.to_type()).await?;
        }
        Ok(engine)
    }

    /// The data plane as ONE okm engine (ADR-0018 steps 1+2): the
    /// "aura_mq" okm keyspace carries BOTH the mq tables and the booth
    /// state documents — one engine, one keyspace, ns-isolated tables.
    /// Fjall opens its own database (single directory). The meta plane
    /// keeps the separate JSON SharedStore (PersistedBooth records).
    fn open_planes(
        engine: &aura_config::Engine,
        dir: Option<std::path::PathBuf>,
        node_id: &str,
    ) -> anyhow::Result<aura_realm::mq::MqStore> {
        match engine {
            aura_config::Engine::Memory => Ok(aura_realm::mq::MqStore::mem()),
            aura_config::Engine::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    let path =
                        dir.unwrap_or_else(|| std::env::temp_dir().join(format!("aura-{node_id}")));
                    let db = fjall::Database::create_or_recover(fjall::Config::new(&path))
                        .map_err(|e| anyhow::anyhow!("fjall open {path:?}: {e}"))?;
                    let mq_store = okm_core::FjallStore::from_db(db, "aura_mq")
                        .map_err(|e| anyhow::anyhow!("fjall mq keyspace {path:?}: {e}"))?;
                    let mq_store = aura_realm::mq::MqStore::fjall(mq_store);
                    // The plane-version gate (ADR-0040 Update): a store
                    // written by an older layout decodes its old rows as
                    // the NEW low-block tables (the renumbering placed the
                    // new meta band on the old event plane's numbers) —
                    // the silent-decode failure mode the number-reuse
                    // discipline exists to prevent. Refuse the boot with
                    // the wipe instruction instead.
                    match aura_realm::meta::plane_version_of(&mq_store)? {
                        None => aura_realm::meta::write_plane_version(&mq_store)?,
                        Some(v) if v == aura_realm::meta::PLANE_VERSION => {}
                        Some(v) => anyhow::bail!(
                            "data-plane layout mismatch: store at {path:?} was written by plane version {v}, \
                             this binary expects {} — the low-block renumbering (ADR-0040/0041) retired the old \
                             bands, and the persisted booth definitions + code blobs are invalid with them. \
                             WIPE the store (delete the data directory) and re-register the booth types; \
                             durable state rides the type's collections, which the wipe also resets.",
                            aura_realm::meta::PLANE_VERSION
                        ),
                    }
                    Ok(mq_store)
                }
                #[cfg(not(feature = "fjall"))]
                {
                    let _ = (engine, dir, node_id);
                    anyhow::bail!("engine=fjall requires building with the `fjall` feature")
                }
            }
        }
    }
}
impl Engine {
    /// Override the idle TTL (realm-wide default; per-type TTL overrides
    /// this — see `BoothType::with_idle_ttl`).
    pub fn with_idle_ttl(self, ttl: Duration) -> Self {
        if let Ok(mut r) = self.realm.try_lock() {
            r.idle_ttl = ttl;
        }
        self
    }

    /// Declare (or replace) what this venue offers for one kind (ADR-0048/0049).
    ///
    /// A grant is a ROW in this venue's own okm table (ns 36), not a line in a config file: it can be
    /// changed while the venue runs, and the surface that changes it is the plane's management API
    /// rather than a deploy. That is the whole distinction the model rests on — a SCRIPT cannot
    /// create one (there is no ctx arm for it), an operator can, and the difference between those two
    /// parties is who holds the plane's admin face.
    ///
    /// The credential is handed in as a VALUE once and lives in the registry's separate table (ns
    /// 38) afterwards; reading the grant never reads the key, which is why they are two namespaces.
    pub async fn declare_grant(
        &self,
        kind: &str,
        grant: aura_realm::resources::Grant,
        secret: Option<&str>,
    ) -> anyhow::Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let r = self.realm.lock().await;
        r.resources.declare(kind, &grant, now_ms)?;
        if let Some(value) = secret {
            r.resources.secret(kind, value)?;
        }
        Ok(())
    }

    /// Every declared grant, in the kind vocabulary's order — what a management surface lists. The
    /// credential is not part of an answer: a grant names its key, it does not carry it.
    pub async fn grants(&self) -> anyhow::Result<Vec<(String, serde_json::Value)>> {
        let r = self.realm.lock().await;
        r.resources.declared()
    }

    /// Withdraw a kind: the grant row AND its credential. `ctx.resource` then answers that kind the
    /// way it answers one that was never declared — by name — because a withdrawn grant that still
    /// resolved would be a grant nobody can see in the listing.
    pub async fn revoke_grant(&self, kind: &str) -> anyhow::Result<()> {
        let r = self.realm.lock().await;
        r.resources.revoke(kind)
    }

    /// Register an booth type with this engine's realm.
    ///
    /// For script types (python/steel/wasm), the registration runs the
    /// script's `interface_schema()` introspection ONCE and adopts
    /// declared metadata into the type definition — `lifecycle.idle_ttl`
    /// seeds `BoothType.idle_ttl` when the host did not set one
    /// explicitly. The host-side builder always wins over the script
    /// declaration (explicit > introspected). The script never touches
    /// the engine: introspection is a pure function the host calls,
    /// direction is host ← script.
    pub async fn register(&self, booth: BoothType) -> anyhow::Result<()> {
        self.register_into(&self.realm, booth).await
    }

    /// Shared registration body: introspection + definition persistence +
    /// type registration, against the caller's realm handle (the engine's
    /// own realm, a named one, or a temporary one). `register` / `register_in`
    /// ride the same path so every kind of realm gets the same ctx.store plan
    /// (ADR-0026 §3).
    ///
    /// A TEMPORARY realm takes this path with its persistence steps SKIPPED
    /// (ADR-0050 §6, ADR-0051 §4): the definition row, the interface_schema
    /// copy it carries, the type dictionary entry and the code blob are all
    /// writes that would die with the realm. Skipping them by name here is what
    /// keeps "nothing in a temporary realm is persisted" true for the ONE code
    /// path every registration goes through, rather than for each write it
    /// happens to issue.
    pub async fn register_into(
        &self,
        realm: &aura_realm::SharedRealm,
        booth: BoothType,
    ) -> anyhow::Result<()> {
        self.register_into_with(realm, booth, None).await
    }

    /// `register_into` with the CALLER'S OWN declaration in hand — the shape a
    /// job's `BoothDecl` arrives in (ADR-0051 §1).
    ///
    /// `declared` is the `interface_schema` the caller brought (its storage
    /// collections, its receives). Introspection still runs, because it seeds
    /// lifecycle facts and the routes a SCRIPT declares; where both speak, the
    /// caller's declaration wins — it is data written for this job, while a
    /// script's answer is a courtesy read from it.
    pub async fn register_into_with(
        &self,
        realm: &aura_realm::SharedRealm,
        mut booth: BoothType,
        declared: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        // Phase 4.5c: derive delivery routes from the introspected schema —
        // `receives` (event → key field) seeds the router per @on
        // declaration; empty key = singleton (per-event queue consumer).
        // A caller that already holds the schema (a dispatched declaration carries its
        // `interface_schema`) does not pay for a second introspection — and, more to the point,
        // must not: a declaration's schema is the one its upload produced, and re-deriving it
        // through a carrier would ask the body for facts it may not be able to answer yet.
        // Everything below (ttl, concurrency, receives, hooks) reads whichever schema won.
        let mut introspected: Option<serde_json::Value> = None;
        let resolved = match declared {
            Some(schema) => Some(schema),
            None => aura_realm::introspect_schema(&booth).await,
        };
        if let Some(schema) = resolved {
            if booth.idle_ttl.is_none() {
                if let Some(ttl) = schema
                    .get("lifecycle")
                    .and_then(|l| l.get("idle_ttl"))
                    .and_then(parse_ttl)
                {
                    booth.idle_ttl = Some(ttl);
                }
            }
            // The instance's concurrency is the BOOTH's declaration (ADR-0046): a
            // body that can be re-entered says how many calls it may run at once,
            // and the host's builder wins over the script's own word (explicit >
            // introspected, the rule every lifecycle fact here follows).
            if booth.concurrency == aura_booth::Concurrency::Queued {
                match schema.get("lifecycle").and_then(|l| l.get("concurrency")) {
                    // A number is a pool size: 1 queues, n > 1 overlaps with n
                    // executors.
                    Some(serde_json::Value::Number(n)) => {
                        if let Some(slots) = n.as_u64().map(|s| usize::try_from(s).unwrap_or(1)) {
                            booth.concurrency = if slots <= 1 {
                                aura_booth::Concurrency::Queued
                            } else {
                                aura_booth::Concurrency::Pool { slots }
                            };
                        }
                    }
                    // `"unbounded"` is the word for "one executor, no queue".
                    Some(serde_json::Value::String(s)) if s == "unbounded" => {
                        booth.concurrency = aura_booth::Concurrency::Unbounded;
                    }
                    _ => {}
                }
            }
            // Introspected declarations land ON THE TYPE (one declaration
            // surface); register_type assembles routes as a side effect.
            // Phase 4.13: each event's resolution is declared per event —
            // `resolve` (scan reference), `key` (payload field), or NEITHER
            // (= singleton delivery; no type-wide default, ADR-0038 §3).
            if let Some(receives) = schema.get("receives").and_then(|r| r.as_object()) {
                for (event, spec) in receives {
                    if let Some(resolve) = spec.get("resolve") {
                        let get = |f: &str| {
                            resolve.get(f).and_then(|v| v.as_str()).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "receives['{event}'].resolve: missing string field `{f}`"
                                )
                            })
                        };
                        booth = booth.on_resolve(
                            event.clone(),
                            get("collection")?,
                            get("index")?,
                            get("probe_field")?,
                        );
                    } else {
                        let key_field = spec.get("key").and_then(|k| k.as_str()).unwrap_or("");
                        // A declaration may bind an ENTRY separately from the event it
                        // subscribes to: the marked-stream case — subscribe to
                        // `ui:add-to-cart`, be called as `add_to_cart`.
                        match spec.get("entry").and_then(|h| h.as_str()) {
                            Some(entry) => booth = booth.on_as(event.clone(), key_field, entry),
                            None => booth = booth.on(event.clone(), key_field),
                        }
                    }
                    // THE SCRIPT'S OWN `auth` RIDES ITS EVENT DECLARATION (`@on(..., auth=True)`,
                    // `(on event (hash "auth" #t) handler)`): the carrier derives it per receive,
                    // and it lands on the same declaration the event name is written on.
                    if spec.get("auth").and_then(serde_json::Value::as_bool) == Some(true) {
                        booth = booth.gated();
                    }
                }
            }
            if let Some(wildcards) = schema.get("wildcard_receives").and_then(|w| w.as_array()) {
                for pattern in wildcards.iter().filter_map(|p| p.as_str()) {
                    booth = booth.on_wildcard(pattern);
                }
            }
            // Reserved lifecycle hooks (ADR-0043 §3): the introspected
            // `hooks` array seeds Declared entries — the handler lives in
            // the resident VM (bound under the reserved name at load); the
            // realm's eviction/reactivation dispatch calls it through the
            // executor. Script `ctx.on("__sleep", h)` is the whole flow.
            if let Some(hooks) = schema.get("hooks").and_then(|h| h.as_array()) {
                for name in hooks.iter().filter_map(|n| n.as_str()) {
                    booth = booth.hook_declared(name);
                }
            }
            // Keep the uploaded copy: it persists with the definition AND
            // seeds the ctx store plan + `ctx.interface_schema` read.
            introspected = Some(schema);
        }
        // Phase 4.5b + ADR-0025 Plan A: persist the definition as a data-plane row
        // (booth_defs beside mq/state) — definitions outlive the process, one okm
        // instance for everything.
        //
        // Both realm kinds take this path (ADR-0050 §6). What decides whether a
        // row outlives the process is the STORE, not a branch here: a persisted
        // realm's store is fjall, a temporary realm's is its own memory engine,
        // and the row is written the same way into either. One rule, two engines.
        //
        // The writes themselves are `Realm::register_definition` (aura-realm): the
        // definition row, the code blob and the type registration are one body, so
        // the crate that OWNS the realm can write a DECLARATION without naming an
        // engine (ADR-0051 §1 — a job brings its declarations as data). This door
        // adds the introspection above and nothing else; there is no second rule.
        realm
            .lock()
            .await
            .register_definition(booth, introspected)?;
        Ok(())
    }

    /// Invoke a registered booth instance (hot path convenience: wait for
    /// the value). The general entry is `call`, returning a CallSlot.
    pub async fn invoke(
        &self,
        target: aura_booth::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        match self.call(target, handler, args).await? {
            aura_booth::call::Waited::Done(result) => result,
            aura_booth::call::Waited::Pending(id) => {
                anyhow::bail!(
                    "cold call returned a Pending slot to a hot caller: {}",
                    id.0
                )
            }
        }
    }

    /// The unified call (Phase 3.5): every surface — CLI, HTTP, remote
    /// Effector, booth ctx.invoke — converges here. Hot targets return
    /// Done on wait; cold targets return Pending(call_id) and the
    /// result arrives via resolve_call.
    pub async fn call(
        &self,
        target: aura_booth::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_booth::call::Waited> {
        // Slot construction errors (unknown type/full queue) are Err;
        // wait results — including Done(Err(timeout/handler failure)) —
        // travel inside the Waited so callers see failure as a value.
        Realm::call(&self.realm, None, target, handler, args)
            .await?
            .wait()
            .await
    }

    /// Register an booth type into a named realm (Phase 3.6 mechanism,
    /// demoted binding per PLAN 4.10; renamed namespace → realm by
    /// ADR-0028). The realm is an EXPLICIT application decision passed at
    /// the call site — construction-time prefix isolation is the
    /// mechanism; what dimension the realm binds (user, project, nothing)
    /// is the application's choice. No credential derivation exists
    /// anywhere on this path.
    pub async fn register_in(&self, realm: &str, booth: BoothType) -> anyhow::Result<()> {
        let ns = self.realm_set.open(realm).await?;
        // Same introspection + persistence path as register() (ADR-0026 §3:
        // the ctx.store plan resolves from the persisted interface_schema —
        // a named-realm type without it has no ctx.store surface).
        self.register_into(&ns.realm(), booth).await
    }

    /// Realm-scoped call (explicit realm name — an application decision,
    /// not a credential derivation): target resolution = realm + node
    /// alias + operation. A realm handle never sees another realm's types
    /// or events — cross-realm delivery is not expressible.
    pub async fn call_in(
        &self,
        realm: &str,
        target: aura_booth::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_booth::call::Waited> {
        let ns = self.realm_set.open(realm).await?;
        Realm::call(&ns.realm(), None, target, handler, args)
            .await?
            .wait()
            .await
    }

    /// Realm-scoped emit (explicit realm — application decision): events
    /// route only within the realm.
    pub async fn emit_in(
        &self,
        realm: &str,
        emitter: Option<&str>,
        event: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let ns = self.realm_set.open(realm).await?;
        Realm::emit(&ns.realm(), emitter, event, data).await
    }

    /// Resolve a pending call (cold re-entry): framework delivers the
    /// result; unknown call_id is a no-op (completed never replay).
    pub async fn resolve_call(
        &self,
        call_id: &aura_booth::call::CallId,
        result: anyhow::Result<serde_json::Value>,
    ) -> bool {
        Realm::resolve_call(&self.realm, call_id, result).await
    }

    /// THE JOB DOOR (ADR-0051 step 2a): run a job in a temporary realm of its
    /// own, built from the declarations that travelled with it.
    ///
    /// The realm is the dispatch's, not the caller's: it is minted here, filled
    /// with the declarations, and destroyed before this returns — whatever the
    /// outcome. The caller gets the per-op answer and never a handle, because a
    /// handle would be a way to touch a realm the job no longer owns (ADR-0051 §2:
    /// a booth asks for work; the realm decides that work needs a space).
    ///
    /// The engine is passed as the `Registrar` — the job's declarations go
    /// through `register_into_with`, the same door a registered booth takes, so a
    /// temporary realm's types are written by the ONE path into the realm's own
    /// store (ADR-0050 §6).
    /// Dispatch a job that ARRIVED: its declarations come out of the envelope's attachment
    /// (ADR-0051 §Attachments), so this is the one door — a remote submission and an in-process
    /// one take the same path, and there is no second way to hand a job a set of definitions.
    ///
    /// A caller that already holds declarations in hand (composition, a test) enters one level
    /// down at `aura_realm::dispatch::Dispatch::run`, which is this call's implementation.
    pub async fn dispatch(
        &self,
        job: &effector_protocol::Job,
    ) -> anyhow::Result<aura_realm::dispatch::JobOutcome> {
        aura_realm::dispatch::Dispatch::new(self.realm_set.as_ref(), self)
            .run_attached(job)
            .await
    }
}

/// Dispatch's registration capability, held by the ENGINE (ADR-0051 §1/§2).
///
/// One impl, no new work: `register_into_with` IS the registration path, and the
/// declaration's own schema rides it. The boxing is the trait's (`dyn Registrar`),
/// not a second code path.
impl aura_realm::dispatch::Registrar for Engine {
    fn register<'a>(
        &'a self,
        realm: &'a SharedRealm,
        decl: &'a aura_realm::dispatch::BoothDecl,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.register_into_with(realm, decl.booth_type(), decl.storage.clone())
                .await
        })
    }
}

/// Parse lifecycle.idle_ttl from a schema value: number (seconds) or a
/// string with a mandatory unit suffix ("300s" / "5m" / "2h").
fn parse_ttl(v: &serde_json::Value) -> Option<std::time::Duration> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(std::time::Duration::from_secs),
        serde_json::Value::String(s) => {
            match (s.chars().last()?, s[..s.len() - 1].parse::<u64>().ok()?) {
                ('s', n) => Some(std::time::Duration::from_secs(n)),
                ('m', n) => Some(std::time::Duration::from_secs(n * 60)),
                ('h', n) => Some(std::time::Duration::from_secs(n * 3600)),
                _ => None,
            }
        }
        _ => None,
    }
}

pub mod effectors;
pub mod host_wire;
