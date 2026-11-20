//! Realm: the event/call fabric. Phase 1 — virtual-booth registry, runtime
//! loop driving queuees, idle-TTL eviction (scale-to-zero: on_sleep →
//! drop, on_wake on reactivation). Event routing (emit/on) arrives in
//! Phase 3; the unified CallSlot model in Phase 3.5.

// Unit tests may use the unwrap/panic vocabulary the production targets are
// denied (a panicking test is a failing test). `cfg(test)` is on only for this
// crate's own test build; `tests/` crates set the same line at their own root.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::panic_in_result_fn
    )
)]
pub mod call_slot;
pub mod ctx;
pub mod dispatch;
pub mod event;
pub mod events;
pub mod frame_path;
pub mod instance;
pub mod meta;
pub mod mq;
pub mod pool;
pub mod realm_id;
pub mod realm_set;
pub mod registry;
pub mod remote;
pub mod stop;
pub mod resource_bridge;
pub mod resources;
pub mod store_exec;
pub mod timer;
pub mod timer_entry;
pub mod value;

use aura_booth::call::{CallId, CallSpec, PendingEntry, Waited};
use aura_booth::{BoothType, Instance, InstanceId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub use realm_id::RealmId;

/// Shared realm handle: the dispatcher closes over this.
pub type SharedRealm = Arc<tokio::sync::Mutex<Realm>>;

/// The default cursor retention promise (ADR-0039 §2): 30 days. Finite by
/// design — "never" as a default would make unbounded queue growth the
/// default, while the invariant "an instance may die" needs a bounded
/// promise.
pub const DEFAULT_CURSOR_TTL: Duration = Duration::from_secs(30 * 24 * 3600);

pub struct Realm {
    /// This realm's identity (ADR-0050 §1): what its keys are prefixed with
    /// and what a log line or a destroy names it by. Bound at construction,
    /// like the store prefix it must agree with.
    pub id: RealmId,
    /// Registered booth types by name.
    types: HashMap<String, BoothType>,
    /// Live instances by (type, key) — the key is the call-model variant
    /// (ADR-0042), never a string.
    instances: HashMap<(String, aura_booth::InstanceKey), Instance>,
    /// Queue capacity per instance.
    queue_capacity: usize,
    /// The mq byte engine (ADR-0018 step 1): the event-queue tables bind
    /// to a byte store — the okm `FjallStore` keyspace on fjall, the
    /// in-memory byte stand-in otherwise. NEVER the JSON state store.
    pub mq: mq::MqStore,
    /// Idle TTL: an instance with no job for this long is evicted
    /// (scale-to-zero). State survives via the store; hooks run around it.
    pub idle_ttl: Duration,
    /// The cursor retention promise (ADR-0039 §2): a cursor row that has
    /// not advanced for this long leaves the watermark denominator — its
    /// backlog is forfeit. Deliberately NOT bound to `idle_ttl`: that is a
    /// seconds-scale eviction knob, this is a days-scale data promise. A
    /// global value (one EngineConfig field), never per-type: the
    /// denominator is a cross-type `min`.
    pub cursor_ttl: Duration,
    /// Event routing: table + emit matching (Phase 3).
    pub router: event::EventRouter,
    /// Resident script executors (Phase 2.6): per-instance VM/child, owned by
    /// the realm — executors die with the realm (test isolation) and hot
    /// type replacement can evict selectively.
    pub executors: effector_runtime::carrier::executor::Executors,
    /// The venue's connected nodes (ADR-0053). ONE pool per engine, shared by every
    /// realm it creates — a realm minted for a job inherits it the way it inherits the
    /// code base URL, because a per-realm map would be a realm where `Body::Remote`
    /// cannot find the node that owns the executor it was dispatched to.
    pub pool: Arc<pool::NodePool>,
    /// Which of those nodes THIS realm may reach: the venue's own realms see all of
    /// them, a job's realm sees the nodes its `Placement` names.
    pub reach: pool::Reach,
    /// Code reference prefix for remote delivery (ADR-0027): a remote
    /// call carries `CodeRef { url: base + hex(sha256), sha256 }`.
    /// None = remote types are undeliverable in this realm (the dispatch
    /// arm answers with an error value; in-process booths never read it).
    pub code_base_url: Option<String>,
    /// Live iterate streams (ADR-0034 §2): stream_id -> the producer
    /// binding (target instance + handler + start args). A frame
    /// identity, no new machinery — Next/Dispose jobs resolve their
    /// routing through this map the way `pending_remote` correlates
    /// invoke replies. Streams die with the realm (eviction of the
    /// producer executor = failed pulls, per the ADR).
    pub streams: HashMap<String, StreamEntry>,
    /// In-flight remote calls awaiting the effector's Result frame. The
    /// instance id scopes the ctx-bridge host calls the effector makes while
    /// executing this call (state fields are the instance's own).
    pub pending_remote: HashMap<String, RemotePending>,
    /// Static call declarations per booth type (Phase 3.5). Defaults to
    /// hot + 30s when a type registers without a spec.
    call_specs: HashMap<String, CallSpec>,
    /// Registered calls awaiting results (Phase 3.5). Hot in-flight calls
    /// carry a deadline (expiry → failure value); cold calls carry their
    /// executor for re-entry routing.
    pending_calls: HashMap<CallId, PendingEntry>,
    call_seq: u64,
    /// Storage plans per booth type (ADR-0026): ns + parsed collections,
    /// resolved once from the type registry + the persisted interface_schema
    /// (4.5b upload copy). The ctx store executor rebuilds the
    /// DynamicCollection from here per op — schema data, no host objects.
    store_plans: HashMap<String, store_exec::StorePlan>,
    /// Persisted interface_schema copies per booth type (the uploaded
    /// version, engine.register seeds from the introspected schema —
    /// `ctx.interface_schema` reads THIS, never a re-introspection).
    persisted_schemas: HashMap<String, Option<serde_json::Value>>,
    /// The type-ns registry of a TEMPORARY realm (ADR-0050 §6): the memory
    /// counterpart of what `meta::ns_of` reads for a persisted one. `None` for
    /// the persisted kind, whose ns is a column that outlives the process.
    /// `Some` is also this realm's kind test — see `is_ephemeral`.
    /// Unmatched events (bounded ring, diagnostic output).
    pub dead_events: event::DeadEvents,
    /// Unified scheduling surface (ADR-0016 revised): delivery + reclaim
    /// timers on one DelayQueue; the driver task fires entries on expiry.
    pub timers: timer::TimerHandle,
    /// The stop bits (ADR-0047): one in-memory flag per activation, no storage.
    pub stops: stop::Stops,
    /// The resource registry (ADR-0048): rows, versions and keys, in the venue's own store. Built
    /// here rather than by a booth, because a booth that owned it would make every other booth
    /// depend on that one — a build pipeline would have to ask a chat venue for a bucket.
    pub resources: crate::resources::Registry,
    /// The one thing that crosses a temporary realm's boundary (ADR-0051 §4):
    /// frames, one way, out, addressed to the booth that dispatched the job.
    /// `None` everywhere else — a persistent realm has no dispatcher to report
    /// to, and a realm handed an outbound channel by anyone but dispatch would
    /// be able to speak to a booth it does not belong to.
    pub frames_out: Option<crate::frame_path::FramesOut>,
}

pub(crate) async fn dispatch_call(
    realm: SharedRealm,
    from: Option<InstanceId>,
    target: InstanceId,
    handler: &str,
    args: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    // THE ENVIRONMENT FOLLOWS THE CALL (ADR-0048). A booth that invokes another is acting for the
    // same turn, so what the caller was told — the resources it may touch, and whose account it
    // runs for — is the callee's context too. Without this, a mount invoked by a turn carries
    // nothing, and a node is asked to work on data nobody declared.
    //
    // A callee's OWN declaration still wins: `run_job` reads the arriving job's args after this,
    // so a booth that declares resources itself overrides what it inherited.
    if let Some(from) = &from {
        let mut r = realm.lock().await;
        let inherited = r
            .instances
            .get(&(from.booth_type.clone(), from.key.clone()))
            .and_then(|i| i.owner.clone());
        if let Some(owner) = inherited {
            let asked = (target.booth_type.clone(), target.key.clone());
            // Creating it here is the same creation the call would do a moment later.
            let _ = r.instance(realm.clone(), &target).await?;
            if let Some(i) = r.instances.get_mut(&asked) {
                if i.owner.is_none() {
                    i.owner = Some(owner);
                }
            }
        }
    }
    // ADR-0036: a call IS a stream Start — `Realm::call`'s hot arm
    // terminal-unwraps into the parked caller (Start+unwrap sugar at
    // the realm; the reply channel carries envelopes, the surface does
    // not). Cold targets answer Pending: no parked value exists.
    match Realm::call(&realm, None, target, handler, args)
        .await?
        .wait()
        .await?
    {
        Waited::Done(result) => result,
        Waited::Pending(id) => Err(anyhow::anyhow!(
            "cold target cannot return a value to a parked caller (call {}); emit instead",
            id.0
        )),
    }
}

/// Introspect a script type's `interface_schema()` (no ctx — a pure
/// function the host calls at registration; direction host ← script, the
/// script never touches the engine) and extract `lifecycle.idle_ttl` if
/// declared. Accepts a number (seconds) or a string with a mandatory
/// unit suffix ("300s" / "5m" / "2h"). Introspection failure or missing
/// declaration = `None`, never a registration error — declaration is
/// optional metadata.
pub async fn introspect_schema(booth: &aura_booth::BoothType) -> Option<serde_json::Value> {
    let aura_booth::Body::Script {
        language,
        source,
        encoding,
    } = &booth.body
    else {
        return None; // Rust types declare TTL via the builder
    };
    let raw = tokio::task::spawn_blocking({
        let language = language.clone();
        let source = source.clone();
        // Uniform contract: carrier::introspect dispatches per language;
        // every carrier assembles/merges `interface_schema` behind this
        // one call. No language branch in the host. ADR-0037 §2: the
        // throwaway child spawns in the DECLARED codec (a CBOR-declared
        // induction booth's schema frame round trip rides CBOR).
        let encoding = *encoding;
        move || effector_runtime::carrier::introspect_encoded(&language, &source, encoding)
    })
    .await;
    raw.ok()?.ok()
}

/// Extract `lifecycle.idle_ttl` from a script type's introspected schema.
pub async fn introspect_idle_ttl(booth: &aura_booth::BoothType) -> Option<Duration> {
    let result = introspect_schema(booth).await?;
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
/// the effector's host calls resolve against.
pub struct RemotePending {
    pub reply: tokio::sync::oneshot::Sender<Result<serde_json::Value, String>>,
    pub instance: InstanceId,
    /// When the node last said the call was still moving (epoch ms). The dispatch's deadline reads
    /// this instead of the clock: a healthy transfer is silent in every way EXCEPT this frame, so
    /// wall time would call it dead (effector \`Frame::Progress\`).
    pub activity: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The account the dispatching instance runs for, recorded when the call went out. The plane
    /// answers "whose resources may this node touch" from here — one field, stamped once.
    pub owner: Option<String>,
}

/// One live iterate stream (ADR-0034 §2): the producer binding a
/// Next/Dispose job needs to re-address the same handler under the same
/// stream id. Start args ride along because envelope-mode handlers are
/// stateless per call and need their full args every turn (the guard
/// state is theirs, in module globals); generator mode ignores them.
pub struct StreamEntry {
    pub target: InstanceId,
    pub handler: String,
    pub args: serde_json::Value,
}

/// One live effector connection's registry entry: the writer channel plus
/// the peer address. The address is not decorative — ADR-0015 §7's
/// replacement discipline requires a takeover event to name the old and
/// new peers, and a bare channel cannot say where either one came from.
#[derive(Clone)]
pub struct ActuatorConn {
    pub sender: tokio::sync::mpsc::UnboundedSender<effector_protocol::Frame>,
    pub peer: std::net::SocketAddr,
}

/// Next remote-call correlation id (realm-owned counter, taken under lock).
pub(crate) async fn next_seq(self_arc: &SharedRealm) -> u64 {
    let mut realm = self_arc.lock().await;
    realm.call_seq += 1;
    realm.call_seq
}

/// ADR-0048: the account follows the call. A turn is who the envelope says; when it invokes a
/// mount, the mount is what dispatches to the node, so the account has to arrive there too — the
/// plane answers a node's material ask from it. These tests sit on that seam. In-crate because the
/// door they exercise is crate-private.
#[cfg(test)]
mod follows_call_tests {
    use super::*;
    use aura_booth::{Body, BoothType, InstanceId, InstanceKey};
    use crate::mq::MqStore;

fn at(booth: &str, key: &str) -> InstanceId {
    InstanceId {
        booth_type: booth.into(),
        key: InstanceKey::Named(key.into()),
    }
}

fn remote_type(name: &str, node_alias: &str) -> BoothType {
    BoothType {
        name: name.into(),
        body: Body::Remote {
            node_alias: node_alias.into(),
            language: "exec".into(),
            source: "runner".into(),
            encoding: aura_booth::Encoding::Json,
            requires: None,
        },
        idle_ttl: None,
        max_exec: None,
        hooks: Default::default(),
        receives: Vec::new(),
        concurrency: aura_booth::Concurrency::Queued,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_inherits_the_account_of_the_turn_it_acts_for() {
    // A realm is built over a store bound to its id (ADR-0050): the test
    // binds a fresh in-memory engine the way `RealmSet::open` does.
    let root = MqStore::mem();
    let store = MqStore::for_realm(&root, crate::RealmId::Persisted(1)).unwrap();
    let realm = Realm::with_mq(crate::RealmId::Persisted(1), store)
        .unwrap()
        .shared_async()
        .await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    {
        let mut r = realm.lock().await;
        r.code_base_url = Some("http://plane.invalid/code".into());
        r.register_type(remote_type("mount", "node-1"));
        r.pool.insert(crate::pool::NodeRecord {
            alias: "node-1".into(),
            conn: crate::ActuatorConn {
                sender: tx,
                peer: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            },
            carriers: vec!["steel".into()],
            class: effector_protocol::NodeClass::Host,
            machine: "box-1".into(),
            labels: vec![],
        });
    }

    // The turn: its key is the device, its account came from the envelope at the entrance.
    let turn = at("turn", "device-a");
    {
        let mut r = realm.lock().await;
        let _ = r.instance(realm.clone(), &turn).await.unwrap();
        let i = r
            .instances
            .get_mut(&(turn.booth_type.clone(), turn.key.clone()))
            .expect("the turn instance");
        i.owner = Some("alice".into());
    }

    // The invocation: the turn acts, the mount carries the consequence.
    let dispatch = {
        let realm = realm.clone();
        let mount = at("mount", "run-7");
        tokio::spawn(async move {
            crate::dispatch_call(
                realm,
                Some(at("turn", "device-a")),
                mount,
                "execute",
                serde_json::json!({ "code": "print(1)" }),
            )
            .await
        })
    };

    let frame = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
        .await
        .expect("a frame within ten seconds")
        .expect("the node channel stays open");
    let effector_protocol::Frame::Call(call) = frame else {
        panic!("the dispatch reaches the node as a Call, got {frame:?}");
    };

    // The account travels with the call, so the plane can answer a material ask without parsing
    // anybody's key.
    let owner = {
        let r = realm.lock().await;
        r.pending_remote
            .get(&call.call_id)
            .and_then(|p| p.owner.clone())
    };
    assert_eq!(owner.as_deref(), Some("alice"));

    dispatch.abort();
}
}
