//! Actor model: definition, context, mailbox.
//!
//! ctx surface is bounded by ADR-0011: state / metadata / invoke only.
//! emit/on, contracts, and hooks stay off ctx (realm-level or static
//! contract concerns).

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
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
        entry: Option<String>,
    },
}

#[derive(Clone)]
pub struct ActorType {
    /// Registered type name, e.g. "echo".
    pub name: String,
    pub body: Body,
    /// Optional on_sleep: called by the Host before the instance is
    /// evicted (scale-to-zero). Return value is ignored; state flushing is
    /// the store's job, not the hook's.
    #[allow(clippy::type_complexity)]
    pub on_sleep: Option<Arc<SleepHook>>,
    /// Optional on_wake: called after reactivation with a fresh ctx, before
    /// the first job of the new residency is delivered.
    #[allow(clippy::type_complexity)]
    pub on_wake: Option<Arc<Handler>>,
}

pub type Handler = dyn Fn(Ctx, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
    + Send
    + Sync;

pub type SleepHook =
    dyn Fn(Ctx) -> futures_boxed::BoxFuture<'static, anyhow::Result<()>> + Send + Sync;

impl ActorType {
    /// Define a Rust-closure type with no lifecycle hooks.
    pub fn simple(name: impl Into<String>, handler: Arc<Handler>) -> Self {
        Self { name: name.into(), body: Body::Rust(handler), on_sleep: None, on_wake: None }
    }

    /// Define a script type executed by a probe carrier.
    pub fn script(
        name: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
        entry: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            body: Body::Script { language: language.into(), source: source.into(), entry },
            on_sleep: None,
            on_wake: None,
        }
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
pub mod futures_boxed {
    pub type BoxFuture<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Per-instance context. ADR-0011: exactly state / metadata / invoke.
/// Metadata lands with Openraft (Phase 5); the surface reserves the name.
pub struct Ctx {
    /// This instance's identity: (actor type, partition key).
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
    fn get(&self, id: &InstanceId, field: &str) -> anyhow::Result<Option<Value>>;
    fn set(&self, id: &InstanceId, field: &str, value: Value) -> anyhow::Result<()>;
    fn delete(&self, id: &InstanceId, field: &str) -> anyhow::Result<()>;
    fn fields(&self, id: &InstanceId) -> anyhow::Result<Vec<String>>;
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
        dyn Fn(InstanceId, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
            + Send
            + Sync,
    >;
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
    pub async fn invoke(&self, target: InstanceId, args: Value) -> anyhow::Result<Value> {
        (self.invoke.dispatch.clone())(target, args).await
    }
}

/// One instance's mailbox: MPSC pipeline (Aura §1 topology). The realm owns
/// the senders; the runtime drains and runs handlers.
pub struct Mailbox {
    pub id: InstanceId,
    pub tx: mpsc::Sender<Job>,
    pub rx: mpsc::Receiver<Job>,
}

/// A unit of work: call args + the reply channel (oneshot, `reply_to`
/// semantics; the general call model lands at Phase 3.5).
pub struct Job {
    pub args: Value,
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<Value>>,
}

impl Mailbox {
    pub fn new(id: InstanceId, capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self { id, tx, rx }
    }
}

/// Live instance: mailbox + last-activity instant, the unit the runtime
/// loop schedules and the idle-TTL evicts.
pub struct Instance {
    pub id: InstanceId,
    pub mailbox: Mailbox,
    /// Last job arrival; the evictor compares against idle_ttl.
    pub last_activity: std::time::Instant,
}

impl Instance {
    pub fn new(id: InstanceId, capacity: usize) -> Self {
        let mailbox = Mailbox::new(id.clone(), capacity);
        Self { id, mailbox, last_activity: std::time::Instant::now() }
    }
}

/// Field map kept for handlers that want a scratch space independent of the
/// durable store (never persisted).
pub type Scratch = HashMap<String, Value>;
