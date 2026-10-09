//! Booth model: definition, context, queue.
//!
//! ctx surface is bounded by ADR-0011 (errata through 2026-09-29):
//! store / invoke / iterate(+dispose). emit/on, contracts, and hooks stay
//! off ctx (realm-level or static contract concerns).

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Booth body: a Rust closure, or a script executed by a probe carrier.
///
/// The script form imports the probe runtime instead of reimplementing
/// language execution: one set of carriers (steel/python/wasmtime embedded,
/// bgi/exec out-of-process)
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
        /// Frame codec for the PROCESS carriers (bgi/exec, ADR-0037 §2 —
        /// dual-protocol by declaration). Json = newline-delimited JSON
        /// lines (the stdlib-reachable default every older booth rides);
        /// Cbor = one self-delimited CBOR document per frame. Embedded
        /// carriers (steel/python/wasm) carry no channel — inert there.
        /// The realm maps this onto probe_runtime's codec enum; this
        /// crate stays probe-free (the same wire VALUES: "json"/"cbor").
        encoding: ChannelEncoding,
    },
    /// Remote probe booth (Phase 3): the body lives on a probe node that
    /// dialed into THIS control plane. `node_alias` addresses the probe's
    /// outbound connection; `language` + `source` are delivered per call
    /// (inline payload). The probe executes in its resident sessions.
    RemoteProbe {
        node_alias: String,
        language: String,
        source: String,
        /// The declared frame codec rides the ToolCall to the node's
        /// process carriers (ADR-0037 §2); inert for embedded carriers.
        encoding: ChannelEncoding,
    },
}

/// The declared frame codec of a process-carrier booth (ADR-0037 §2,
/// dual-protocol). Serde values match the probe protocol's enum
/// ("json"/"cbor"); the realm maps this type onto the runtime's form —
/// aura-booth stays probe-free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelEncoding {
    /// Newline-delimited JSON lines (the default, stdlib-reachable).
    #[default]
    Json,
    /// CBOR documents, one self-delimited frame.
    Cbor,
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

/// One event subscription: an event name (or wildcard pattern) + HOW the
/// target instances resolve (Phase 4.13: the three shapes are three
/// mechanisms, discriminated here — never a sentinel mixing, ADR-0038 §3).
#[derive(Clone, Debug)]
pub struct ReceiveDecl {
    pub event: String,
    pub resolution: RouteResolution,
    pub wildcard: bool,
}

/// How an emit's target instances are resolved (event-flow.md §8.1/§8.2 —
/// the terminal ruling, landed with Phase 4.13). The reference carries
/// NAMES, never a cross-plane id: rows store facts, resolution lives with
/// the schema owner.
#[derive(Clone, Debug)]
pub enum RouteResolution {
    /// Key-less delivery: the type's singleton instance.
    Singleton,
    /// The payload field carries the instance key (exactly one value →
    /// one instance; zero scan — the cheap shape for keyed routes).
    Field(String),
    /// An access-method reference: scan the collection's index with the
    /// probe taken from `probe_field` — each hit row's primary key IS a
    /// target instance key (naturally one-to-many). The collection's
    /// primary key must be a SINGLE key field (ruled at landing: the
    /// instance key is one String; a composite row key has no honest
    /// string rendering, so multi-key collections are a registration
    /// error, not a rendering convention).
    Scan { collection: String, index: String, probe_field: String },
}

pub type Handler = dyn Fn(Ctx, Value) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
    + Send
    + Sync;

pub type SleepHook =
    dyn Fn(Ctx) -> futures_boxed::BoxFuture<'static, anyhow::Result<()>> + Send + Sync;

impl BoothType {
    /// Define a script type executed by a probe carrier (JSON-lines
    /// codec — the default).
    pub fn script(
        name: impl Into<String>,
        language: impl Into<String>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            body: Body::Script {
                language: language.into(),
                source: source.into(),
                encoding: ChannelEncoding::default(),
            },
            idle_ttl: None,
            max_exec: None,
            on_sleep: None,
            on_wake: None,
            receives: Vec::new(),
        }
    }

    /// Declare the frame codec of a PROCESS-carrier booth (ADR-0037 §2 —
    /// dual-protocol): `Cbor` switches the bgi/exec channel to
    /// CBOR documents (self-delimited, no line terminator). A declaration
    /// error, not a silent downgrade: the nushell fifo shape rejects a
    /// Cbor spec at spawn (nu has no CBOR codec). Inert for embedded
    /// carriers (they carry no channel).
    pub fn encoded(mut self, encoding: ChannelEncoding) -> Self {
        match &mut self.body {
            Body::Script { encoding: e, .. } | Body::RemoteProbe { encoding: e, .. } => *e = encoding,
            Body::Rust(_) => {}
        }
        self
    }

    /// Declare an event subscription binding the payload field that
    /// carries the instance key (`Field` shape — the zero-scan form).
    pub fn on(mut self, event: impl Into<String>, key_field: impl Into<String>) -> Self {
        self.receives.push(ReceiveDecl {
            event: event.into(),
            resolution: RouteResolution::Field(key_field.into()),
            wildcard: false,
        });
        self
    }

    /// Declare a scan subscription (Phase 4.13): targets resolve by
    /// scanning `index` over `collection` with the probe taken from the
    /// payload's `probe_field` — one hit row per target, fan-out natural.
    pub fn on_resolve(
        mut self,
        event: impl Into<String>,
        collection: impl Into<String>,
        index: impl Into<String>,
        probe_field: impl Into<String>,
    ) -> Self {
        self.receives.push(ReceiveDecl {
            event: event.into(),
            resolution: RouteResolution::Scan {
                collection: collection.into(),
                index: index.into(),
                probe_field: probe_field.into(),
            },
            wildcard: false,
        });
        self
    }

    /// Declare a wildcard (prefix) subscription — singleton consumer.
    pub fn on_wildcard(mut self, pattern: impl Into<String>) -> Self {
        self.receives.push(ReceiveDecl {
            event: pattern.into(),
            resolution: RouteResolution::Singleton,
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

/// Per-instance context. ADR-0011 (errata through 2026-09-29): exactly
/// store / invoke / iterate(+dispose) — the former `ctx.metadata` was
/// withdrawn (ADR-0025), the point-state model replaced by type-scoped
/// collections (ADR-0026 §3).
pub struct Ctx {
    /// This instance's identity: (booth type, instance key). Answers who
    /// serially processes this message (ADR-0026) — storage addressing
    /// lives in the type's declared collections (the store_emit handle),
    /// never in this struct.
    pub self_id: InstanceId,
    /// Call surface — the single controlled path (ADR-0011).
    invoke: Invoke,
    /// Streaming call surface (ADR-0034): iterate/dispose ride the same
    /// dispatch machinery as invoke — a stateful producer pulled by the
    /// consumer. `None` only on the introspection ctx (no realm to
    /// dispatch against); a booth handler always has it.
    iterate: Option<iterate_handle::IterateHandle>,
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
    /// Instance key: identity within the type (session_id, node_id, ...).
    /// A variant, never a string (ADR-0042): the singleton is a variant,
    /// so no payload key can alias into it — `Named` values carry no
    /// reservation at all.
    pub key: InstanceKey,
}

/// The instance key's structured form (ADR-0042). Twin of the MQ plane's
/// `mq::InstanceKey` slice marker — deliberately isomorphic (the slice
/// value IS the instance key), a distinct type because the two live on
/// different planes (MQ / call model).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum InstanceKey {
    /// The type's singleton instance (at most one; needs no name).
    Singleton,
    /// A named instance — the routing value the payload carried.
    Named(String),
}

impl InstanceKey {
    /// The script/ctx-face rendering (ADR-0042): a named instance is its
    /// key text; the singleton renders as the EMPTY string — it has no
    /// name, and an empty string is documented rather than a reserved
    /// literal. The inverse is `InstanceKey::parse`.
    pub fn render(&self) -> &str {
        match self {
            InstanceKey::Singleton => "",
            InstanceKey::Named(k) => k,
        }
    }
    /// The ctx-string inverse: `""` is the singleton, anything else is a
    /// named instance. Round-trips with `render` by construction.
    pub fn parse(s: &str) -> Self {
        if s.is_empty() {
            InstanceKey::Singleton
        } else {
            InstanceKey::Named(s.to_string())
        }
    }
}

impl std::fmt::Display for InstanceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.render())
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

/// One stream verb for the iterate dispatch path (ADR-0034). Only
/// Start names a target — the realm mints the stream id and the
/// registry resolves Next/Dispose back to the producer, so the cursor
/// and the script host fns route by id alone after start (the id is
/// unguessable and globally scoped within the realm — a stream is an
/// addressing fact, never a permission you re-present).
pub enum IterateOp {
    Start { target: InstanceId, handler: String, args: Value },
    Next { stream_id: String },
    Dispose { stream_id: String },
}

pub mod iterate_handle {
    use super::*;
    pub type IterateHandle = Arc<
        dyn Fn(IterateOp) -> futures_boxed::BoxFuture<'static, anyhow::Result<Value>>
            + Send
            + Sync,
    >;
}

/// The per-pull wire envelope (ADR-0034 §1, unified by ADR-0036 §1):
/// a typed `done` field, always present, never a sentinel value; a
/// terminal round may carry `value` (invoke is the stream whose first
/// reply is terminal — `Realm::call` is Start+unwrap sugar), a
/// non-terminal round carries `item`. `item` is legal only with
/// `done: false`, `value` only with `done: true` — enforced, not
/// convention. (failure is a value, ADR-0012 — mid-stream errors
/// surface through the Result the cursor hands back, not a second
/// channel in the envelope).
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope {
    pub done: bool,
    pub item: Option<Value>,
    pub value: Option<Value>,
}

impl Envelope {
    /// Decode the JSON envelope a session returned. Structural errors
    /// (non-object, missing `done`, cross-field violations) are failure
    /// values — the pull itself failed, distinct from a mid-stream
    /// producer error. The generator-mode null reply is gone (ADR-0036
    /// — the pull that starts a stream answers its first envelope).
    pub fn from_value(v: &Value) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("iterate: envelope must be an object, got {v}"))?;
        let done = obj
            .get("done")
            .and_then(|d| d.as_bool())
            .ok_or_else(|| anyhow::anyhow!("iterate: envelope needs a boolean `done` field, got {v}"))?;
        let item = obj.get("item").cloned();
        let value = obj.get("value").cloned();
        if done && item.is_some() {
            anyhow::bail!("iterate: envelope with `done: true` must not carry an `item` (ADR-0036)");
        }
        if !done {
            if item.is_none() {
                anyhow::bail!("iterate: envelope with `done: false` must carry an `item` (ADR-0036)");
            }
            if value.is_some() {
                anyhow::bail!("iterate: `value` is legal only with `done: true` (ADR-0036)");
            }
        }
        Ok(Envelope { done, item, value })
    }

    /// The terminal envelope an invoke fast path produces: the value
    /// wrapped for a reply channel that carries envelopes end to end.
    pub fn terminal(value: Option<Value>) -> Value {
        match value {
            Some(v) => serde_json::json!({ "done": true, "value": v }),
            None => serde_json::json!({ "done": true }),
        }
    }

    /// The consume-one-value unwrap (ADR-0036 §1/§3): a reply channel
    /// carries envelopes end to end, a value consumer terminal-unwraps.
    /// A non-terminal reply is the invoke misuse named as a protocol
    /// error — a caller wanting items iterates, not unwraps.
    pub fn unwrap_terminal(v: Value) -> anyhow::Result<Value> {
        let env = Envelope::from_value(&v)?;
        if !env.done {
            anyhow::bail!("call: first reply is not terminal — a value consumer must iterate the stream (ADR-0036)");
        }
        Ok(env.value.unwrap_or(Value::Null))
    }
}

/// A live stream cursor (ADR-0034, unified envelope ADR-0036): the
/// consumer's handle to a producer session's state. Lazily started — the
/// first `next()` sends Start (the stream id is minted by the realm) and
/// then pulls; every later `next()` pulls. Stream association derives
/// from `done`, never from a positional field-presence heuristic (0036
/// §4): a non-terminal first reply MUST carry the realm-minted
/// `stream_id`; a terminal first reply carries none (the stream never
/// opened — that reply IS an invoke). Exhaustion is terminal: pulls
/// after `done: true` are rejected. Abandoning mid-stream requires
/// `dispose()` in carriers without a destructor hook; the python
/// wrapper sends it automatically on GeneratorExit. A stream never
/// disposed is released by residency eviction, not by magic.
pub struct StreamCursor {
    iterate: iterate_handle::IterateHandle,
    target: InstanceId,
    handler: String,
    args: Value,
    /// Set after a NON-terminal Start reply: the realm-minted stream
    /// id. The id alone routes Next/Dispose (the realm's registry owns
    /// the binding). A terminal first reply leaves it None — there is
    /// no stream to route to.
    stream_id: Option<String>,
    done: bool,
    /// The terminal round's `value`, once seen (ADR-0036 §3: the
    /// explicit-consumer accessor — the native sugars deliberately do
    /// NOT consume it; iterate a stream for items, call (or pull
    /// explicitly) for a value).
    value: Option<Value>,
}

impl StreamCursor {
    pub(crate) fn new(
        iterate: iterate_handle::IterateHandle,
        target: InstanceId,
        handler: String,
        args: Value,
    ) -> Self {
        Self { iterate, target, handler, args, stream_id: None, done: false, value: None }
    }

    /// Pull one envelope. First call starts the stream — the Start
    /// round trip IS the first pull (the realm delivers the first
    /// envelope together with the minted stream id when the stream
    /// stays open).
    pub async fn next(&mut self) -> anyhow::Result<Envelope> {
        if self.done {
            anyhow::bail!("iterate: stream already exhausted");
        }
        let started = self.stream_id.is_some();
        let v = if started {
            let id = self.stream_id.clone().expect("checked above");
            (self.iterate)(IterateOp::Next { stream_id: id }).await?
        } else {
            (self.iterate)(IterateOp::Start {
                target: self.target.clone(),
                handler: self.handler.clone(),
                args: self.args.clone(),
            })
            .await?
        };
        let env = Envelope::from_value(&v)?;
        if env.done {
            self.done = true;
            self.value = env.value.clone();
        } else if !started {
            // Association from `done` (ADR-0036 §4): a non-terminal
            // first reply without the realm's stream id is a protocol
            // error, never a guess.
            let id = v
                .get("stream_id")
                .and_then(|s| s.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "iterate: non-terminal first reply carries no `stream_id` (ADR-0036 §4)"
                    )
                })?
                .to_string();
            self.stream_id = Some(id);
        }
        Ok(env)
    }

    /// The realm-minted stream id, once the stream has started
    /// (script-side wrappers need it to drive the host fns). `None`
    /// before start AND after a terminal first reply (invoke shape —
    /// no stream ever opened).
    pub fn stream_id(&self) -> Option<&str> {
        self.stream_id.as_deref()
    }

    /// The terminal round's value, available after `next()` returns a
    /// `done: true` envelope that carried one (ADR-0036 §1: invoke is
    /// the stream whose first reply is terminal; a generator's
    /// `return x` arrives here). `None` = no value was carried, or the
    /// stream has not terminated. Deliberately NOT consumed by the
    /// native iteration sugars — this is the explicit accessor.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Abandon the stream mid-flight (ADR-0034 §3). Idempotent; a
    /// stream never started (or already terminal) disposes nothing.
    pub async fn dispose(&mut self) -> anyhow::Result<()> {
        if let Some(id) = self.stream_id.take() {
            (self.iterate)(IterateOp::Dispose { stream_id: id }).await?;
        }
        self.done = true;
        Ok(())
    }
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
    pub fn new(
        self_id: InstanceId,
        dispatch: dispatch_handle::DispatchHandle,
        iterate: iterate_handle::IterateHandle,
    ) -> Self {
        Self {
            self_id,
            invoke: Invoke { dispatch },
            iterate: Some(iterate),
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

    /// Begin a streaming call (ADR-0034): returns a cursor into the
    /// target handler's stateful producer. Nothing crosses until the
    /// cursor's first pull (lazy start). `dispose` is the mandatory
    /// dual — mid-stream abandonment without it is the dead-ring
    /// problem wearing a request badge (released only by eviction).
    pub fn iterate(&self, target: InstanceId, handler: &str, args: Value) -> StreamCursor {
        let iterate = self
            .iterate
            .clone()
            .expect("booth ctx always carries the iterate handle (only the introspection ctx lacks it)");
        StreamCursor::new(iterate, target, handler.to_string(), args)
    }

    /// The iterate dispatch handle, for the script ctx bridge (host fns
    /// drive the same machinery the Rust cursor rides).
    pub fn iterate_handle(&self) -> Option<iterate_handle::IterateHandle> {
        self.iterate.clone()
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

/// A unit of work: the stream verb + handler name + args, and the reply
/// channel (oneshot, `reply_to` semantics; the general call model lands at
/// Phase 3.5). ADR-0036 (unifies ADR-0034): every job is a stream op —
/// `Start` runs the handler as a producer, and an invoke is exactly the
/// stream whose first reply is terminal (the carrier wraps a plain
/// return into `{done:true,value}`, ADR-0036 §2). The JobKind verb
/// vocabulary retired with the protocol split.
pub struct Job {
    /// The stream verb this job carries (ADR-0036: every kind is a
    /// stream op — the Invoke kind retired with the protocol split).
    pub kind: JobKind,
    /// Stream identity: minted by the realm at Start (the same counter
    /// shape pending_calls uses), echoed by Next/Dispose.
    pub stream: String,
    /// Handler name the delivery addresses: the event name for event
    /// delivery, the caller-declared function for direct invocation.
    pub handler: String,
    pub args: Value,
    pub reply: tokio::sync::oneshot::Sender<anyhow::Result<Value>>,
}

/// The verb a Job carries (ADR-0036: the call wire rides ONE envelope
/// vocabulary — every dispatch job is a stream op riding the same
/// machinery; `call` is Start+unwrap sugar at the Realm surface, not a
/// frame kind).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobKind {
    Start,
    Next,
    Dispose,
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
