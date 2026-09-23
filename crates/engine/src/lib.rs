//! Engine assembly: config + realm + runtime. Phase 1 — store-backed ctx
//! state, idle-TTL eviction (scale-to-zero), submit-based call path.

use aura_actor::ActorType;
use aura_realm::{Realm, SharedRealm};
use std::sync::Arc;
use std::time::Duration;

pub struct Engine {
    /// The system/default namespace realm (back-compat: single-node tests,
    /// CLI echo). User-facing surfaces use `namespaces` instead.
    pub realm: SharedRealm,
    /// Per-user namespace map (Phase 3.6): structural isolation — a
    /// NamespacedRealm handle cannot reach another namespace.
    pub namespaces: Arc<aura_realm::namespace::Namespaces>,
}

impl Engine {
    /// Start the engine: resolve the state store from config, spawn the
    /// idle evictor. Boot errors on engine/feature mismatches (PLAN Phase 4
    /// matrix: an engine chosen without its feature compiled in fails at
    /// boot, never silently falls back).
    pub async fn start(config: &aura_config::EngineConfig) -> anyhow::Result<Self> {
        let mq = Self::open_planes(&config.engine, config.data_dir.clone(), &config.node_id)?;
        let realm: SharedRealm = Realm::with_mq(mq.clone()).shared_async().await;
        Realm::spawn_evictor(&realm);
        // Namespaces share the SAME okm engine (one fjall keyspace); each
        // namespace derives a prefix-bound handle at realm construction —
        // state documents AND mq tables ride it (ADR-0018 steps 1+2).
        let namespaces = Arc::new(aura_realm::namespace::Namespaces::with_mq(
            mq.clone(),
        ));
        // Boot reload (Phase 4.5b): persisted script actors re-register from
        // the meta store — definitions outlive the process.
        let engine = Self { realm, namespaces };
        // Boot reload (ADR-0025 Plan A): definitions live in the DATA
        // plane's okm instance (actor_defs beside mq/state).
        for def in aura_realm::meta::load_all(&mq)? {
            engine.register(def.to_type()).await?;
        }
        Ok(engine)
    }

    /// The data plane as ONE okm engine (ADR-0018 steps 1+2): the
    /// "aura_mq" okm keyspace carries BOTH the mq tables and the actor
    /// state documents — one engine, one keyspace, ns-isolated tables.
    /// Fjall opens its own database (single directory). The meta plane
    /// keeps the separate JSON SharedStore (PersistedActor records).
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
                    let path = dir
                        .unwrap_or_else(|| std::env::temp_dir().join(format!("aura-{node_id}")));
                    let db = fjall::Database::create_or_recover(fjall::Config::new(&path))
                        .map_err(|e| anyhow::anyhow!("fjall open {path:?}: {e}"))?;
                    let mq_store = okm_core::FjallStore::from_db(db, "aura_mq")
                        .map_err(|e| anyhow::anyhow!("fjall mq keyspace {path:?}: {e}"))?;
                    Ok(aura_realm::mq::MqStore::fjall(mq_store))
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
    /// this — see `ActorType::with_idle_ttl`).
    pub fn with_idle_ttl(self, ttl: Duration) -> Self {
        if let Ok(mut r) = self.realm.try_lock() {
            r.idle_ttl = ttl;
        }
        self
    }

    /// Register an actor type with this engine's realm.
    ///
    /// For script types (python/steel/wasm), the registration runs the
    /// script's `interface_schema()` introspection ONCE and adopts
    /// declared metadata into the type definition — `lifecycle.idle_ttl`
    /// seeds `ActorType.idle_ttl` when the host did not set one
    /// explicitly. The host-side builder always wins over the script
    /// declaration (explicit > introspected). The script never touches
    /// the engine: introspection is a pure function the host calls,
    /// direction is host ← script.
    pub async fn register(&self, mut actor: ActorType) -> anyhow::Result<()> {
        // Phase 4.5c: derive delivery routes from the introspected schema —
        // `receives` (event → key field) seeds the router per @on
        // declaration; empty key = singleton (per-event queue consumer).
        if let Some(schema) = aura_realm::introspect_schema(&actor).await {
            if actor.idle_ttl.is_none() {
                if let Some(ttl) = schema.get("lifecycle").and_then(|l| l.get("idle_ttl")).and_then(parse_ttl) {
                    actor.idle_ttl = Some(ttl);
                }
            }
            // Introspected declarations land ON THE TYPE (one declaration
            // surface); register_type assembles routes as a side effect.
            if let Some(receives) = schema.get("receives").and_then(|r| r.as_object()) {
                for (event, spec) in receives {
                    let key_field = spec.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    actor = actor.on(event.clone(), key_field);
                }
            }
            if let Some(wildcards) = schema.get("wildcard_receives").and_then(|w| w.as_array()) {
                for pattern in wildcards.iter().filter_map(|p| p.as_str()) {
                    actor = actor.on_wildcard(pattern.clone());
                }
            }
        }
        // Phase 4.5b + ADR-0025 Plan A: persist the definition as a
        // data-plane row (actor_defs beside mq/state) — definitions
        // outlive the process, one okm instance for everything.
        if let Some(def) = aura_actor::persist::PersistedActor::from_type(&actor) {
            let realm = self.realm.lock().await;
            aura_realm::meta::persist(&realm.mq, &def)?;
        }
        self.realm.lock().await.register_type(actor);
        Ok(())
    }

    /// Invoke a registered actor instance (hot path convenience: wait for
    /// the value). The general entry is `call`, returning a CallSlot.
    pub async fn invoke(
        &self,
        target: aura_actor::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        match self.call(target, handler, args).await? {
            aura_actor::call::Waited::Done(result) => result,
            aura_actor::call::Waited::Pending(id) => {
                anyhow::bail!("cold call returned a Pending slot to a hot caller: {}", id.0)
            }
        }
    }

    /// The unified call (Phase 3.5): every surface — CLI, HTTP, remote
    /// Probe, actor ctx.invoke — converges here. Hot targets return
    /// Done on wait; cold targets return Pending(call_id) and the
    /// result arrives via resolve_call.
    pub async fn call(
        &self,
        target: aura_actor::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_actor::call::Waited> {
        // Slot construction errors (unknown type/full queue) are Err;
        // wait results — including Done(Err(timeout/handler failure)) —
        // travel inside the Waited so callers see failure as a value.
        Realm::call(&self.realm, None, target, handler, args)
            .await?
            .wait()
            .await
    }

    /// Register an actor type into a user namespace (Phase 3.6). The
    /// namespace is derived from the user credential at registration.
    pub async fn register_in(&self, namespace: &str, actor: ActorType) {
        let ns = self.namespaces.realm_of(namespace).await;
        ns.realm().lock().await.register_type(actor);
    }

    /// Namespaced call: target resolution = user namespace + node alias +
    /// operation. A namespace handle never sees another namespace's types
    /// or events — cross-namespace delivery is not expressible.
    pub async fn call_in(
        &self,
        namespace: &str,
        target: aura_actor::InstanceId,
        handler: &str,
        args: serde_json::Value,
    ) -> anyhow::Result<aura_actor::call::Waited> {
        let ns = self.namespaces.realm_of(namespace).await;
        Realm::call(&ns.realm(), None, target, handler, args)
            .await?
            .wait()
            .await
    }

    /// Namespaced emit: events route only within the namespace.
    pub async fn emit_in(
        &self,
        namespace: &str,
        emitter: Option<&str>,
        event: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let ns = self.namespaces.realm_of(namespace).await;
        Realm::emit(&ns.realm(), emitter, event, data).await
    }

    /// Resolve a pending call (cold re-entry): framework delivers the
    /// result; unknown call_id is a no-op (completed never replay).
    pub async fn resolve_call(
        &self,
        call_id: &aura_actor::call::CallId,
        result: anyhow::Result<serde_json::Value>,
    ) -> bool {
        Realm::resolve_call(&self.realm, call_id, result).await
    }
}

/// Parse lifecycle.idle_ttl from a schema value: number (seconds) or a
/// string with a mandatory unit suffix ("300s" / "5m" / "2h").
fn parse_ttl(v: &serde_json::Value) -> Option<std::time::Duration> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(std::time::Duration::from_secs),
        serde_json::Value::String(s) => match (s.chars().last()?, s[..s.len() - 1].parse::<u64>().ok()?) {
            ('s', n) => Some(std::time::Duration::from_secs(n)),
            ('m', n) => Some(std::time::Duration::from_secs(n * 60)),
            ('h', n) => Some(std::time::Duration::from_secs(n * 3600)),
            _ => None,
        },
        _ => None,
    }
}

pub mod probes;
pub mod host_wire;
