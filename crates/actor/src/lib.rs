//! Actor model: definition, context, queue.
//!
//! ctx surface is bounded by ADR-0011: state / metadata / invoke only.
//! emit/on, contracts, and hooks stay off ctx (realm-level or static
//! contract concerns).

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// An Actor type definition. The handler is a Rust async function for now;
/// embedded languages (Phase 2) wrap the same definition with a script body.
///
/// Lifecycle hooks (`on_sleep` / `on_wake`) are Host → Actor calls (ADR-0011,
/// off ctx): optional, declared on the type, invoked by the runtime around
/// eviction and reactivation.

/// Actor body: a Rust closure, or a script executed by a probe carrier.
///
/// The script form imports the probe runtime instead of reimplementing
/// language execution: one set of carriers (steel/python/wasmtime/nushell)
/// serves both the remote actuator and embedded actors. Script actors are
/// pure functions in this phase (args in, value out); the ctx bridge
/// (state/invoke from inside scripts via host functions) is the remaining
/// Phase 2 work.
#[derive(Clone)]
pub enum Body {
    Rust(Arc<Handler>),
    Script {
        language: String,
        source: String,
    },
    /// Remote probe actor (Phase 3): the body lives on a probe node that
    /// dialed into THIS control plane. `node_alias` addresses the probe's
    /// outbound connection; `language` + `source` are delivered per call
    /// (inline payload). The probe executes in its resident sessions.
    RemoteProbe {
        node_alias: String,
        language: String,
        source: String,
    },
}

#[derive(Clone)]
pub struct ActorType {
    /// Registered type name, e.g. "echo".
    pub name: String,
    pub body: Body,
    /// Idle TTL for this type's instances: how long after the last job
    /// before the evictor reclaims the residency (scale-to-zero). `None`
    /// = fall back to the realm-wide default. Per-type because residency
    /// value differs by role — a turn-executor dwells through its
    /// retention window while an entity actor can be reclaimed quickly.
    pub idle_ttl: Option<Duration>,
    /// Execution budget (ADR-0016 revised §4): a reclaim entry fires if a
    /// single job runs longer than this — maximum-duration control, not
    /// idleness. Declared as `lifecycle.max_exec` in interface_schema.
    pub max_exec: Option<Duration>,
    /// Optional on_sleep: called by the Host before the instance is
    /// evicted (scale-to-zero). Return value is ignored; state flushing is
    /// the store's job, not the hook's.
    #[allow(clippy::type_complexity)]
    pub on_sleep: Option<Arc<SleepHook>>,
    /// Optional on_wake: called after reactivation with a fresh ctx, before
    /// the first job of the new residency is delivered.
    #[allow(clippy::type_complexity)]
    pub on_wake: Option<Arc<Handler>>,
    /// Event subscriptions declared on the type (Phase 4.5: one declaration
    /// surface per type). Filled three ways: `.on()` / `.on_wildcard()`
    /// builders (Rust-side declaration), introspection at register (script
    /// types), or both merged. `register_type` assembles routes from this.
    pub receives: Vec<ReceiveDecl>,
}

/// One event subscription: an event name (or wildcard pattern) + the
/// instance key field it binds (empty = singleton consumer).
#[derive(Clone, Debug)]
pub struct ReceiveDecl {
    pub event: String,
    pub key_field: String,
    pub wildcard: bool,
}

pub type Handler = dyn Fn(Ctx, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
    + Send
    + Sync;

pub type SleepHook =
    dyn Fn(Ctx) -> futures_boxed::BoxFuture<'static, anyhow::Result<()>> + Send + Sync;

impl ActorType {
    /// Define a script type executed by a probe carrier.
    pub fn script(
        name: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            body: Body::Script { language: language.into(), source: source.into() },
            idle_ttl: None,
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: Vec::new(),
        }
    }

    /// Declare an event subscription on this type (exact event + instance
    /// key field; empty key = singleton consumer).
    pub fn on(mut self, event: impl Into<String>, key_field: impl Into<String>) -> Self {
        self.receives.push(ReceiveDecl {
            event: event.into(),
            key_field: key_field.into(),
            wildcard: false,
        });
        self
    }

    /// Declare a wildcard (prefix) subscription — singleton consumer.
    pub fn on_wildcard(mut self, pattern: impl Into<String>) -> Self {
        self.receives.push(ReceiveDecl {
            event: pattern.into(),
            key_field: String::new(),
            wildcard: true,
        });
        self
    }

    /// Declare a per-type idle TTL (residency policy). See the field doc.
    /// Execution budget per job (ADR-0016 revised §4): a watchdog reclaim
    /// entry fires when one job exceeds it — evict + fail the reply.
    pub fn with_max_exec(mut self, budget: Duration) -> Self {
        self.max_exec = Some(budget);
        self
    }

    pub fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = Some(ttl);
        self
    }

    pub fn with_on_sleep(mut self, hook: Arc<SleepHook>) -> Self {
        self.on_sleep = Some(hook);
        self
    }

    pub fn with_on_wake(mut self, hook: Arc<Handler>) -> Self {
        self.on_wake = Some(hook);
        self
    }
}

/// Narrow alias so the public API stays readable without a futures dep.
pub mod call;
pub mod persist;
pub mod store_emit;
pub use persist::PersistedActor;
pub use store_emit::{StoreOp, StoreOpKind};

pub mod futures_boxed {
    pub type BoxFuture<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Per-instance context. ADR-0011: exactly state / metadata / invoke.
/// Metadata lands with Openraft (Phase 5); the surface reserves the name.
pub struct Ctx {
    /// This instance's identity: (actor type, instance key).
    pub self_id: InstanceId,
    /// Instance state, backed by the runtime's StateStore. Reads hit the
    /// store; writes are per-field durable units.
    pub state: State,
    /// Call surface — the single controlled path (ADR-0011).
    invoke: Invoke,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InstanceId {
    pub actor_type: String,
    /// Partition key: instance identity within the type (session_id,
    /// node_id, ...).
    pub key: String,
}

/// Per-instance field storage: the actor-visible state contract. The store
/// owns namespacing (`state:{actor_id}:{field}`); handlers never see keys.
/// Writes are per-field durable units (wiki §状态落盘的原子化).
pub trait StateStore: Send + Sync {
    /// The store's own key encoding for (instance, field). Opaque to
    /// callers; nesting wrappers prepend their prefix to THESE bytes
    /// (okm nesting rule: the wrapper knows only its prefix, the engine
    /// encoding stays opaque).
    fn key_for(&self, id: &InstanceId, field: &str) -> Vec<u8>;
    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>>;
    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()>;
    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()>;
    /// Full inner keys sharing a byte prefix — the primitive the nesting
    /// wrapper uses for `fields` (scan own prefix, strip, delegate).
    fn scan_keys(&self, key_prefix: &[u8]) -> anyhow::Result<Vec<Vec<u8>>>;
    /// Raw-key operations: the nesting wrapper's entire surface. The
    /// wrapper composes `prefix + inner.key_for(...)` and calls these —
    /// the inner engine never learns about namespaces.
    fn get_raw(&self, key: &[u8]) -> anyhow::Result<Option<Value>>;
    fn set_raw(&self, key: Vec<u8>, value: Value) -> anyhow::Result<()>;
    fn del_raw(&self, key: &[u8]) -> anyhow::Result<()>;
}

/// Shared handle to the runtime's store.
pub type SharedStore = std::sync::Arc<dyn StateStore>;

/// Instance state view over the shared StateStore. Field-scoped: handlers
/// touch named fields, the store owns namespacing.
pub struct State {
    id: InstanceId,
    store: SharedStore,
}

impl State {
    pub fn new(id: InstanceId, store: SharedStore) -> Self {
        Self { id, store }
    }

    pub fn get(&self, field: &str) -> anyhow::Result<Option<Value>> {
        self.store.get(&self.id, field)
    }
    pub fn set(&self, field: &str, value: Value) -> anyhow::Result<()> {
        self.store.set(&self.id, field, value)
    }
    pub fn delete(&self, field: &str) -> anyhow::Result<()> {
        self.store.delete(&self.id, field)
    }
}

/// Invoke capability: held privately, exposed via `Ctx::invoke`. Target
/// resolution is registry-declared (ADR-0011); the realm fills this in.
#[derive(Clone)]
pub struct Invoke {
    dispatch: dispatch_handle::DispatchHandle,
}

pub mod dispatch_handle {
    use super::*;
    pub type DispatchHandle = Arc<
        dyn Fn(InstanceId, &str, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
            + Send
            + Sync,
    >;
}

impl Invoke {
    /// Call the dispatch target and wait for its result. Used by the script
    /// ctx bridge (which blocks inside spawn_blocking).
    pub async fn call(&self, target: InstanceId, handler: &str, args: Value) -> anyhow::Result<Value> {
        (self.dispatch.clone())(target, handler, args).await
    }
}

impl Ctx {
    pub fn new(self_id: InstanceId, store: SharedStore, dispatch: dispatch_handle::DispatchHandle) -> Self {
        Self {
            state: State::new(self_id.clone(), store),
            self_id,
            invoke: Invoke { dispatch },
        }
    }

    /// The single controlled call surface (ADR-0011).
    pub async fn invoke(&self, target: InstanceId, handler: &str, args: Value) -> anyhow::Result<Value> {
        (self.invoke.dispatch.clone())(target, handler, args).await
    }

    /// The instance's state store handle. Used by the script ctx bridge to
    /// build sync host functions over this instance's own state.
    pub fn state_store(&self) -> SharedStore {
        self.state.store.clone()
    }

    /// The invoke dispatch handle, for sync wrappers around `invoke`.
    pub fn invoke_handle(&self) -> dispatch_handle::DispatchHandle {
        self.invoke.dispatch.clone()
    }
}

/// One instance's queue: MPSC pipeline (Aura §1 topology). The realm owns
/// the senders; the runtime drains and runs handlers.
pub struct Queue {
    pub id: InstanceId,
    pub tx: mpsc::Sender<Job>,
    pub rx: mpsc::Receiver<Job>,
}

/// A unit of work: call args + the reply channel (oneshot, `reply_to`
/// semantics; the general call model lands at Phase 3.5).
pub struct Job {
    /// Handler name the delivery addresses: the event name for event
    /// delivery, the caller-declared function for direct invocation.
    pub handler: String,
    pub args: Value,
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<Value>>,
}

impl Queue {
    pub fn new(id: InstanceId, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self { id, tx, rx }
    }
}

/// A job traveling an event queue: handler name + args. No reply channel —
/// event delivery is fire-and-forget (an actor that must return values is
/// invoked, not emitted to). Clone: broadcast queues fan it out to every
/// subscriber.
#[derive(Clone)]
pub struct QueuedJob {
    pub handler: String,
    pub args: Value,
}

/// Live instance: queue + last-activity instant, the unit the runtime
/// loop schedules and the idle-TTL evicts.
pub struct Instance {
    pub id: InstanceId,
    pub queue: Queue,
    /// Event-queue subscriptions (Phase 4.5c step 2): one private Receiver
    /// per (event, partition) queue this instance's @on declarations bind —
    /// the per-subscription cursor that keeps consumption serial here.
    pub subscriptions:
        Vec<((String, String), tokio::sync::broadcast::Receiver<QueuedJob>)>,
    /// Last job arrival; the evictor compares against idle_ttl.
    pub last_activity: std::time::Instant,
}

impl Instance {
    pub fn new(id: InstanceId, capacity: usize) -> Self {
        let queue = Queue::new(id.clone(), capacity);
        Self { id, queue, subscriptions: Vec::new(), last_activity: std::time::Instant::now() }
    }
}

/// Field map kept for handlers that want a scratch space independent of the
/// durable store (never persisted).
pub type Scratch = HashMap<String, Value>;
