//! Realm: the event/call fabric. Phase 1 — virtual-actor registry, runtime
//! loop driving queuees, idle-TTL eviction (scale-to-zero: on_sleep →
//! drop, on_wake on reactivation). Event routing (emit/on) arrives in
//! Phase 3; the unified CallSlot model in Phase 3.5.

pub mod event;
pub mod mq;
pub mod timer;
pub mod meta;
pub mod value;
pub mod realm_set;
pub mod store_exec;
pub mod registry;
pub mod instance;
pub mod call_slot;
pub mod events;
pub mod remote;
pub mod ctx;

use aura_actor::call::{CallId, CallSlot, CallSpec, PendingEntry, Tier, Waited};
use aura_actor::{ActorType, Instance, InstanceId, Job};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;

/// Shared realm handle: the dispatcher closes over this.
pub type SharedRealm = Arc<tokio::sync::Mutex<Realm>>;

pub struct Realm {
    /// Registered actor types by name.
    types: HashMap<String, ActorType>,
    /// Live instances by (type, key).
    instances: HashMap<(String, String), Instance>,
    /// Queue capacity per instance.
    queue_capacity: usize,
    /// The mq byte engine (ADR-0018 step 1): the event-queue tables bind
    /// to a byte store — the okm `FjallStore` keyspace on fjall, the
    /// in-memory byte stand-in otherwise. NEVER the JSON state store.
    pub mq: mq::MqStore,
    /// Idle TTL: an instance with no job for this long is evicted
    /// (scale-to-zero). State survives via the store; hooks run around it.
    pub idle_ttl: Duration,
    /// Event routing: table + emit matching (Phase 3).
    pub router: event::EventRouter,
    /// Resident script sessions (Phase 2.6): per-instance VM/PTY, owned by
    /// the realm — sessions die with the realm (test isolation) and hot
    /// type replacement can evict selectively.
    pub sessions: probe_runtime::carrier::session::Sessions,
    /// Live probe outbound connections by node alias (Phase 3). The
    /// writer half routes realm calls; the peer address exists so a
    /// takeover names both ends (ADR-0015 §7 replacement discipline —
    /// silent alias replacement is the behaviour being removed).
    pub probes: HashMap<String, ProbeConn>,
    /// Code reference prefix for remote delivery (ADR-0027): a remote
    /// call carries `CodeRef { url: base + hex(sha256), sha256 }`.
    /// None = remote types are undeliverable in this realm (the dispatch
    /// arm answers with an error value; in-process actors never read it).
    pub code_base_url: Option<String>,
    /// In-flight remote calls awaiting the probe's Result frame. The
    /// instance id scopes the ctx-bridge host calls the probe makes while
    /// executing this call (state fields are the instance's own).
    pub pending_remote:
        HashMap<String, RemotePending>,
    /// Static call declarations per actor type (Phase 3.5). Defaults to
    /// hot + 30s when a type registers without a spec.
    call_specs: HashMap<String, CallSpec>,
    /// Registered calls awaiting results (Phase 3.5). Hot in-flight calls
    /// carry a deadline (expiry → failure value); cold calls carry their
    /// session for re-entry routing.
    pending_calls: HashMap<CallId, PendingEntry>,
    call_seq: u64,
    /// Storage plans per actor type (ADR-0026): ns + parsed collections,
    /// resolved once from the type registry + the persisted interface_schema
    /// (4.5b upload copy). The ctx store executor rebuilds the
    /// DynamicCollection from here per op — schema data, no host objects.
    store_plans: HashMap<String, store_exec::StorePlan>,
    /// Persisted interface_schema copies per actor type (the uploaded
    /// version, engine.register seeds from the introspected schema —
    /// `ctx.interface_schema` reads THIS, never a re-introspection).
    persisted_schemas: HashMap<String, Option<serde_json::Value>>,
    /// Unmatched events (bounded ring, diagnostic output).
    pub dead_events: event::DeadEvents,
    /// Unified scheduling surface (ADR-0016 revised): delivery + reclaim
    /// timers on one DelayQueue; the driver task fires entries on expiry.
    pub timers: timer::TimerHandle,
}

pub(crate) async fn dispatch_call(
    realm: SharedRealm,
    target: InstanceId,
    handler: &str,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    match Realm::call(&realm, None, target, handler, args).await?.wait().await? {
        Waited::Done(result) => result,
        Waited::Pending(id) => Err(anyhow::anyhow!(
            "cold target cannot return a value to a parked caller (call {}); emit instead",
            id.0
        )),
    }
}

impl Default for Realm {
    fn default() -> Self {
        // State + mq both ride the okm TestStore through the document
        // model (ADR-0018): there is no JSON state store left to default
        // to — and none is needed.
        Self::with_mq(crate::mq::MqStore::mem())
    }
}

/// Introspect a script type's `interface_schema()` (no ctx — a pure
/// function the host calls at registration; direction host ← script, the
/// script never touches the engine) and extract `lifecycle.idle_ttl` if
/// declared. Accepts a number (seconds) or a string with a mandatory
/// unit suffix ("300s" / "5m" / "2h"). Introspection failure or missing
/// declaration = `None`, never a registration error — declaration is
/// optional metadata.
pub async fn introspect_schema(actor: &aura_actor::ActorType) -> Option<serde_json::Value> {
    let aura_actor::Body::Script { language, source } = &actor.body else {
        return None; // Rust types declare TTL via the builder
    };
    let raw = tokio::task::spawn_blocking({
        let language = language.clone();
        let source = source.clone();
        move || {
            // Uniform contract: carrier::introspect dispatches per
            // language; every carrier assembles/merges `interface_schema`
            // behind this one call. No language branch in the host.
            probe_runtime::carrier::introspect(&language, &source)
        }
    })
    .await;
    raw.ok()?.ok()
}

/// Extract `lifecycle.idle_ttl` from a script type's introspected schema.
pub async fn introspect_idle_ttl(actor: &aura_actor::ActorType) -> Option<Duration> {
    let result = introspect_schema(actor).await?;
    let ttl = result.get("lifecycle")?.get("idle_ttl")?;
    match ttl {
        serde_json::Value::Number(n) => n.as_u64().map(Duration::from_secs),
        serde_json::Value::String(s) => parse_duration_suffix(s),
        _ => None,
    }
}

/// Parse a human duration suffix: "300s" / "5m" / "2h". Bare digits are
/// rejected — units are mandatory so declarations are unambiguous.
fn parse_duration_suffix(s: &str) -> Option<Duration> {
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num.parse().ok()?;
    match unit {
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n * 60)),
        "h" => Some(Duration::from_secs(n * 3600)),
        _ => None,
    }
}

/// One in-flight remote call: the reply path plus the instance whose ctx
/// the probe's host calls resolve against.
pub struct RemotePending {
    pub reply: tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
    pub instance: InstanceId,
}

/// One live probe connection's registry entry: the writer channel plus
/// the peer address. The address is not decorative — ADR-0015 §7's
/// replacement discipline requires a takeover event to name the old and
/// new peers, and a bare channel cannot say where either one came from.
#[derive(Clone)]
pub struct ProbeConn {
    pub sender: tokio::sync::mpsc::UnboundedSender<probe_protocol::Frame>,
    pub peer: std::net::SocketAddr,
}

/// Next remote-call correlation id (realm-owned counter, taken under lock).
pub(crate) async fn next_seq(self_arc: &SharedRealm) -> u64 {
    let mut realm = self_arc.lock().await;
    realm.call_seq += 1;
    realm.call_seq
}

