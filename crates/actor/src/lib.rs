//! Actor model: definition, context, mailbox.
//!
//! ctx surface is bounded by ADR-0011: state / metadata / invoke only.
//! emit/on, contracts, and hooks stay off ctx (realm-level or static
//! contract concerns). Phase 0 wires state (in-memory) and invoke
//! (realm-routed); metadata waits for the Openraft phase.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// An Actor type definition: the handler is a Rust async function for now.
/// Embedded languages (Phase 2) wrap the same definition with a script body.
#[derive(Clone)]
pub struct ActorType {
    /// Registered type name, e.g. "echo". Partition key routing resolves
    /// (type, key) to an instance mailbox.
    pub name: String,
    #[allow(clippy::type_complexity)]
    pub handler: Arc<Handler>,
}

pub type Handler = dyn Fn(Ctx, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
    + Send
    + Sync;

/// Narrow alias so the public API stays readable without a futures dep.
pub mod futures_boxed {
    pub type BoxFuture<'a, T> = std::pin::Pin<
        Box<dyn std::future::Future<Output = T> + Send + 'a>,
    >;
}

/// Per-instance context. ADR-0011: exactly state / metadata / invoke.
pub struct Ctx {
    /// This instance's identity: (actor type, partition key).
    pub self_id: InstanceId,
    /// Instance state, in-memory until Phase 1 sinks it to Fjall.
    pub state: State,
    /// Call surface — the single controlled path (ADR-0011). Phase 0
    /// resolves through the realm's dispatcher.
    invoke: Invoke,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InstanceId {
    pub actor_type: String,
    /// Partition key: instance identity within the type (session_id,
    /// node_id, ...).
    pub key: String,
}

pub struct State {
    fields: HashMap<String, Value>,
}

impl State {
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.fields.get(field)
    }
    pub fn set(&mut self, field: &str, value: Value) {
        self.fields.insert(field.to_string(), value);
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
    pub fn new(self_id: InstanceId, dispatch: dispatch_handle::DispatchHandle) -> Self {
        Self {
            self_id,
            state: State { fields: HashMap::new() },
            invoke: Invoke { dispatch },
        }
    }

    /// The single controlled call surface (ADR-0011 §invoke). Phase 0:
    /// fire the target's mailbox and await its oneshot.
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
