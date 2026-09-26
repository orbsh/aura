//! Booth model: definition, context, queue.
//!
//! ctx surface is bounded by ADR-0011: state / metadata / invoke only.
//! emit/on, contracts, and hooks stay off ctx (realm-level or static
//! contract concerns).

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Booth body: a Rust closure, or a script executed by a probe carrier.
///
/// The script form imports the probe runtime instead of reimplementing
/// language execution: one set of carriers (steel/python/wasmtime/nushell)
/// serves both the remote actuator and embedded booths. Script booths are
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
    /// Remote probe booth (Phase 3): the body lives on a probe node that
    /// dialed into THIS control plane. `node_alias` addresses the probe's
    /// outbound connection; `language` + `source` are delivered per call
    /// (inline payload). The probe executes in its resident sessions.
    RemoteProbe {
        node_alias: String,
        language: String,
        source: String,
    },
}

/// An Booth type definition. The handler is a Rust async function for now;
/// embedded languages (Phase 2) wrap the same definition with a script body.
///
/// Lifecycle hooks (`on_sleep` / `on_wake`) are Host → Booth calls (ADR-0011,
/// off ctx): optional, declared on the type, invoked by the runtime around
/// eviction and reactivation.
#[derive(Clone)]
pub struct BoothType {
    /// Registered type name, e.g. "echo".
    pub name: String,
    pub body: Body,
    /// Idle TTL for this type's instances: how long after the last job
    /// before the evictor reclaims the residency (scale-to-zero). `None`
    /// = fall back to the realm-wide default. Per-type because residency
    /// value differs by role — a turn-executor dwells through its
    /// retention window while an entity booth can be reclaimed quickly.
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

impl BoothType {
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
pub use persist::PersistedBooth;
pub use store_emit::{StoreOp, StoreOpKind};

pub mod futures_boxed {
    pub type BoxFuture<'a, T> =
        std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// Per-instance context. ADR-0011: exactly state / metadata / invoke.
/// Metadata lands with Openraft (Phase 5); the surface reserves the name.
pub struct Ctx {
    /// This instance's identity: (booth type, instance key). Answers who
    /// serially processes this message (ADR-0026) — storage addressing
    /// lives in the type's declared collections (the store_emit handle),
    /// never in this struct.
    pub self_id: InstanceId,
    /// Call surface — the single controlled path (ADR-0011).
    invoke: Invoke,
    /// Type-scoped storage executor (ADR-0026 §3): one entry carrying okm
    /// Collection ops as data (`ctx.store.emit(op)`). The runtime injects
    /// the handle bound to the OWNING TYPE's ns — cross-type access is not
    /// expressible through it. `None` when the runtime has no storage plan
    /// for the type (no declared collections → no storage surface).
    store_emit: Option<store_emit_handle::StoreEmitHandle>,
    /// The persisted interface_schema copy (uploaded version, 4.5b
    /// lifecycle). `None` = the type declared none.
    interface_schema: Option<Value>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InstanceId {
    pub booth_type: String,
    /// Partition key: instance identity within the type (session_id,
    /// node_id, ...).
    pub key: String,
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

pub mod store_emit_handle {
    use super::*;
    use crate::StoreOp;
    /// One storage instruction, executed against the owning type's ns
    /// (the runtime resolves the plan; the handle carries no addressing).
    /// Sync: script host fns run inside spawn_blocking.
    pub type StoreEmitHandle =
        Arc<dyn Fn(StoreOp) -> Result<Value, String> + Send + Sync>;
}

impl Invoke {
    /// Call the dispatch target and wait for its result. Used by the script
    /// ctx bridge (which blocks inside spawn_blocking).
    pub async fn call(&self, target: InstanceId, handler: &str, args: Value) -> anyhow::Result<Value> {
        (self.dispatch.clone())(target, handler, args).await
    }
}

impl Ctx {
    pub fn new(self_id: InstanceId, dispatch: dispatch_handle::DispatchHandle) -> Self {
        Self {
            self_id,
            invoke: Invoke { dispatch },
            store_emit: None,
            interface_schema: None,
        }
    }

    /// Inject the type-scoped storage executor (realm-side assembly;
    /// booths never construct a Ctx themselves).
    pub fn with_store_emit(mut self, handle: store_emit_handle::StoreEmitHandle) -> Self {
        self.store_emit = Some(handle);
        self
    }

    /// Inject the persisted interface_schema copy.
    pub fn with_interface_schema(mut self, schema: Value) -> Self {
        self.interface_schema = Some(schema);
        self
    }

    /// The type-scoped storage surface: exactly one entry, `emit(op)` —
    /// okm Collection instructions as data (ADR-0026 §3). Fails when the
    /// type declared no storage (no schema → no collections → no surface).
    pub fn store(&self) -> StoreSurface<'_> {
        StoreSurface { emit: self.store_emit.as_ref() }
    }

    /// The persisted interface_schema (uploaded copy; execution never
    /// re-introspects). `None` = not declared.
    pub fn interface_schema(&self) -> Option<&Value> {
        self.interface_schema.as_ref()
    }

    /// The raw store-emit handle (host-bridge assembly clones the Arc).
    pub fn store_emit_handle(&self) -> Option<&store_emit_handle::StoreEmitHandle> {
        self.store_emit.as_ref()
    }

    /// The single controlled call surface (ADR-0011).
    pub async fn invoke(&self, target: InstanceId, handler: &str, args: Value) -> anyhow::Result<Value> {
        (self.invoke.dispatch.clone())(target, handler, args).await
    }

    /// The invoke dispatch handle, for sync wrappers around `invoke`.
    pub fn invoke_handle(&self) -> dispatch_handle::DispatchHandle {
        self.invoke.dispatch.clone()
    }
}

/// The booth-visible storage surface: one method, `emit`. The op is the
/// protocol type (`StoreOp`); the runtime executes it against the type's
/// declared collections (okm Collection semantics — documents, indexes,
/// preset reduces, dynamic-segment fields).
pub struct StoreSurface<'a> {
    emit: Option<&'a store_emit_handle::StoreEmitHandle>,
}

impl StoreSurface<'_> {
    /// Execute one okm Collection instruction against the type's ns.
    pub fn emit(&self, op: StoreOp) -> Result<Value, String> {
        let f = self
            .emit
            .ok_or_else(|| "this type declares no storage (no storage schema) — ctx.store is unavailable".to_string())?;
        f(op)
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
/// event delivery is fire-and-forget (an booth that must return values is
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
