//! Dispatch creates the realm (ADR-0051, step 2a): the REALM side of a job.
//!
//! A job arrives with its own booth declarations (`BoothDecl` — data, never
//! registrations). This module mints a TEMPORARY realm for it (ADR-0050 §5),
//! registers the declarations inside it through the one registration rule, runs
//! the op list honouring each op's `needs` and `on_error`, and destroys the
//! realm — on every path, succeeded or not. Nothing about the job outlives it.
//!
//! What is deliberately NOT here, and why:
//!
//! - **A second declaration vocabulary.** `BoothDecl` holds a `Body`, the
//!   `interface_schema` document a registration already carries (its `storage`
//!   half is what `StorePlan::from_schema` reads, ADR-0026), and the type's
//!   `receives`. There is no second schema language and no second registry: the
//!   declaration is written by `Realm::register_definition`, which is the body
//!   `Engine::register_into` ends at.
//! - **A second ns rule.** Registration allocates `BOOTH_NS_BASE + id` from the
//!   realm's OWN dictionary row, exactly as it does in fjall (ADR-0050 §6).
//!   A temporary realm's rows land in its own memory engine and die with it.
//! - **Node selection.** `Placement` rides the job and is recorded in the
//!   outcome; resolving `Placement::Labels` needs a node pool, which does not
//!   exist yet. Deferred, and said out loud rather than silently ignored
//!   (ADR-0051 §Status).
//!
//! ## The four boundaries, and what enforces each
//!
//! | ADR-0051 | how it is enforced |
//! |---|---|
//! | (a) cannot write the parent's store | STRUCTURAL — `Realm::with_mq` refuses a non-memory store for an `Ephemeral` id and `MqStore::for_realm` refuses to bind a slice of the engine's keyspace to one. A booth holds its realm's store, and that store IS the realm's own memory engine. |
//! | (b) cannot publish MQ to the parent | STRUCTURAL — every queue table, cursor and route row is written through `Realm::mq`, the realm's own engine. What crosses is a FRAME, addressed and directed (below), never the job's own event: the parent's queue for the emitted name stays at depth 0 and its state namespaces stay byte-identical. |
//! | (c) cannot open a child realm | STRUCTURAL + NAMED REFUSAL — no booth holds a `RealmSet`, so nesting is not expressible (ADR-0050 §7); the door that would express it, `Realm::create_child_realm`, answers `BoundaryRefusal::ChildRealm` instead of minting. |
//! | (d) inherits the parent's view | CONFIGURATION — the realm set carries the engine's code base URL and cursor TTL into every realm it creates, and the job's `Placement` is carried through. Type visibility is deliberately NOT inherited (ADR-0051 §Consequences: the realm boundary is the namespace boundary, so a declaration shadows a same-named parent type). Label-driven node selection is deferred with the node pool. |
//!
//! ## The frames that cross, and where they land
//!
//! §4 allows one thing through the boundary. Dispatch installs the channel
//! (`Realm::frames_out`) before registration, so `needs.emit` is granted as a
//! fact: every event a job's ops emit is delivered by the job's own router AND
//! posted to the dispatcher (`Realm::emit` does both, always — a filtered
//! version of that sentence would hide exactly the frames an operator wants).
//!
//! Delivery is directed, not routed: the frame is appended to the dispatching
//! booth's queue (`job.frame`, partition = its key) on the parent realm, so the
//! parent needs no route row and an offline dispatcher gets a backlog rather
//! than a lost frame. Each frame carries its path — `[{job, op}]` — with the op
//! step rewritten before each call, because ops run one at a time; `job.end`
//! carries `[{job}]` and is posted after the destroy.
//!
//! The channel carries the job's whole observable life (`emit`, `op.done`,
//! `op.skipped`, `op.failed`, `op.not_run`, `job.end`), which is why a dispatch
//! is usable from another process: `Engine::job_frames(dispatcher, job, after)`
//! is the pull face, and the rows live in the PARENT, so they outlive the realm
//! that produced them (ADR-0052).
//!
//! ## Where the declarations come from
//!
//! Out of the ENVELOPE: `Job::attachments` carries one opaque payload named
//! `boothset` (effector ADR-0005 §attachments), and this module decodes it.
//! That is why there is a single door — a job that arrived from another process
//! and a job composed in this one take the same path, and neither has a way to
//! hand a realm definitions the other could not.
//!
//! The wire form is `aura_booth::persist::PersistedBooth` — the same record a
//! registration writes — so there is no second declaration format to keep in
//! step, and the one body that cannot travel (`Body::Rust`, a linked function)
//! is excluded by construction. A refusal here happens before a realm exists:
//! absent, duplicated, misnamed, unknown media, over the cap, undecodable.
//!
//! ## What the envelope still cannot say (reported, not worked around)
//!
//! `JobSource::Plan` is refused by name: advancing a schematic run is the run's
//! own authority, not this step's. And `Need` in effector ADR-0005 is the set of
//! ctx SERVICES an op is granted (`store`/`emit`/`resource`/`timer`), not a
//! dependency list; the op list's order IS the dependency ("in order"), and
//! `needs` is enforced as the capability gate ADR-0005 describes — a service the
//! realm cannot grant is refused before the op runs.

use crate::realm_set::{NamedRealm, RealmSet};
use crate::{Realm, RealmId, SharedRealm};
use aura_booth::call::Waited;
use aura_booth::{Body, BoothType, InstanceId, InstanceKey, ReceiveDecl};
use effector_protocol::{CallKind, Job, JobSource, Need, OnError, Op, Placement, Scope};
use serde::Serialize;
use std::future::Future;
use std::pin::Pin;

// ---------------------------------------------------------------------------
// A declaration is data (ADR-0051 §1).
// ---------------------------------------------------------------------------

/// One booth, declared by the job that needs it.
///
/// The shape is ADR-0051 §1's, mapped onto what a registration actually writes:
///
/// - `body` is the same body a registered booth has (`Body::Script { language,
///   source, encoding }`, `Body::Rust`, or `Body::Remote`);
/// - `storage` is the `interface_schema` document an upload carries — its
///   `storage.collections` is what `StorePlan::from_schema` parses into the
///   type's ns-bound plan (ADR-0026). There is no second collection vocabulary:
///   the schema IS the declaration;
/// - `receives` is the type's ONE declaration surface for event routes
///   (ADR-0038 §3 / ADR-0045): a route is assembled from it as a side effect of
///   registration, in this realm, from this realm's own rows.
///
/// A call interface needs no row: the graph's edges address `(type, entry)`, and
/// an entry is a name the body exposes. What registration has to be told is only
/// what it must WRITE, which is the three facts above.
///
/// A declaration that arrived over the wire can only carry a script body
/// (`Body::Script` / `Body::Remote` — code travels as a `CodeRef`, ADR-0027);
/// `Body::Rust` exists here for an in-process caller (a test, a CLI, a host that
/// composes work it already links) and is not persistable in either realm kind.
#[derive(Clone)]
pub struct BoothDecl {
    /// The type name inside the job's realm. The realm boundary is the
    /// namespace boundary (ADR-0051 §Consequences): this name may shadow a
    /// parent type of the same name, and inside the realm the declaration wins.
    pub name: String,
    pub body: Body,
    /// The storage/interface declaration (`interface_schema`), or `None` = the
    /// type keeps nothing and has no `ctx.store` surface.
    pub storage: Option<serde_json::Value>,
    /// Event subscriptions the type declares (empty = it answers calls only).
    pub receives: Vec<ReceiveDecl>,
}

impl BoothDecl {
    pub fn new(name: impl Into<String>, body: Body) -> Self {
        Self {
            name: name.into(),
            body,
            storage: None,
            receives: Vec::new(),
        }
    }

    /// Declare what the type may keep — the same `interface_schema` document an
    /// upload carries.
    pub fn with_storage(mut self, schema: serde_json::Value) -> Self {
        self.storage = Some(schema);
        self
    }

    /// Declare the events this type receives (one declaration surface, ADR-0045).
    pub fn with_receives(mut self, receives: Vec<ReceiveDecl>) -> Self {
        self.receives = receives;
        self
    }

    /// The type this declaration registers, minus the part that is a write
    /// (`receives` and the body are the definition; the schema rides beside it).
    pub fn booth_type(&self) -> BoothType {
        BoothType {
            name: self.name.clone(),
            body: self.body.clone(),
            idle_ttl: None,
            max_exec: None,
            hooks: Default::default(),
            receives: self.receives.clone(),
            concurrency: aura_booth::Concurrency::Queued,
        }
    }
}

// ---------------------------------------------------------------------------
// The registration door, as a capability handed in.
// ---------------------------------------------------------------------------

/// "Register this declaration into this realm" — the ONE registration path, as a
/// capability the caller hands to dispatch.
///
/// The trait exists because `Engine::register_into` lives in `aura-engine`,
/// which depends on this crate; a trait is the direction the dependency already
/// runs, and it keeps introspection where it belongs (with the host that has the
/// carriers) while the persistence + plan + route body stays HERE, in
/// `Realm::register_definition`. Neither shape duplicates the other: dispatch
/// never introspects, and the engine's impl never writes a row itself.
///
/// Boxed rather than `async fn` in a trait: this must be callable through
/// `&dyn Registrar` (dispatch holds one, not a generic type parameter, so the
/// entry point does not become a monomorphization per host).
pub trait Registrar: Send + Sync {
    fn register<'a>(
        &'a self,
        realm: &'a SharedRealm,
        decl: &'a BoothDecl,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

/// The declaration door for a caller that has no engine: a realm test, a CLI, a
/// future in-process host.
///
/// It skips exactly one step — `interface_schema()` introspection — and that
/// skip is honest rather than a shortcut: a declaration, being data, ALREADY
/// carries the schema introspection would go looking for. A host that wants the
/// carrier's own report (or that must load the code) uses its own `Registrar`;
/// the engine's is the production one.
pub struct DeclaredRegistrar;

impl Registrar for DeclaredRegistrar {
    fn register<'a>(
        &'a self,
        realm: &'a SharedRealm,
        decl: &'a BoothDecl,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            realm
                .lock()
                .await
                .register_definition(decl.booth_type(), decl.storage.clone())
        })
    }
}

// ---------------------------------------------------------------------------
// Boundaries (ADR-0051): the ones with a door, named.
// ---------------------------------------------------------------------------

/// Which of ADR-0051's boundaries refused. A refusal that cannot say WHICH
/// boundary it is leaves the caller guessing whether it hit isolation or a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    /// (a) a booth inside cannot write the parent's store.
    ParentStore,
    /// (b) it cannot publish MQ to the parent.
    ParentMq,
    /// (c) it cannot open a child realm.
    ChildRealm,
}

impl Boundary {
    /// The record and clause this refusal answers, as a stable label — what a
    /// log line and a test both name.
    pub fn label(self) -> &'static str {
        match self {
            Boundary::ParentStore => "ADR-0051 boundary (a) parent store",
            Boundary::ParentMq => "ADR-0051 boundary (b) parent mq",
            Boundary::ChildRealm => "ADR-0051 boundary (c) child realm",
        }
    }
}

/// A named refusal from one of ADR-0051's boundaries.
#[derive(Debug, Clone)]
pub struct BoundaryRefusal {
    pub boundary: Boundary,
    /// The realm the call came from — so the refusal says which temporary realm
    /// tried, not only what it tried.
    pub realm: RealmId,
    pub detail: String,
}

impl BoundaryRefusal {
    pub fn child_realm(realm: RealmId) -> Self {
        Self {
            boundary: Boundary::ChildRealm,
            realm,
            detail: "realms do not nest (ADR-0050 §7): a job's nested jobs run in the SAME \
                     realm, and a realm holds no RealmSet, so there is no door to a child"
                .to_string(),
        }
    }
}

impl std::fmt::Display for BoundaryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} — realm {}: {}", self.boundary.label(), self.realm, self.detail)
    }
}

impl std::error::Error for BoundaryRefusal {}

impl Realm {
    /// THE NESTING DOOR, AND WHY IT ANSWERS "NO" (ADR-0051 §6, ADR-0050 §7).
    ///
    /// Realms do not nest, and the structural reason is that this door is on the
    /// SET: nothing inside a realm holds a `RealmSet`, so a booth has no
    /// vocabulary for realms at all (ADR-0051 §2). This method is that absence
    /// given a name — a caller who believes a realm can make a realm gets a
    /// `BoundaryRefusal::ChildRealm` instead of a silent obligation, and the
    /// return type (`Infallible`) says at compile time that no success value
    /// exists to be used.
    pub fn create_child_realm(&self) -> Result<std::convert::Infallible, BoundaryRefusal> {
        Err(BoundaryRefusal::child_realm(self.id))
    }
}

// ---------------------------------------------------------------------------
// What happened, per op and per job.
// ---------------------------------------------------------------------------

/// What one op came to.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum OpResult {
    /// The call answered. `value` is the op's own answer.
    Done { value: serde_json::Value },
    /// It failed and its policy said to carry on (`OnError::Skip`).
    Skipped { error: String },
    /// It failed for the last time: the job stops here.
    Failed { error: String },
    /// It never ran, because an earlier op stopped the job.
    NotRun { reason: String },
}

/// One op's outcome: what it was told to do on failure, how many attempts it
/// took, and what came of it.
#[derive(Debug, Clone, Serialize)]
pub struct OpOutcome {
    /// Position in the job's op list — the identity an op has (`Op` carries no
    /// name of its own; `call.call_id` is the caller's correlation id).
    pub op: usize,
    /// The policy the op declared, echoed so the outcome is readable without the
    /// job in hand.
    pub policy: OnError,
    /// Runs made. `Retry { times }` means at most `times + 1` (effector ADR-0005:
    /// `times` counts ADDITIONAL attempts); `Never` is always 1.
    pub attempts: u32,
    pub result: OpResult,
}

impl OpOutcome {
    /// Did this op produce a value?
    pub fn succeeded(&self) -> bool {
        matches!(self.result, OpResult::Done { .. })
    }
}

/// The job's outcome, as a value: it exists whether the job succeeded or not,
/// because the job ending is not the same event as the realm being destroyed.
#[derive(Debug, Clone, Serialize)]
pub struct JobOutcome {
    /// The job's idempotency key, echoed (effector ADR-0005).
    pub job: String,
    /// The submitting booth the results belong to, echoed.
    pub booth: String,
    /// The temporary realm the job ran in. It is gone by the time this value is
    /// returned — `destroyed` says so rather than leaving the caller to guess.
    pub realm: RealmId,
    /// Placement the job asked for. RECORDED, NOT ACTED ON: node selection needs
    /// a node pool, which does not exist yet (ADR-0051 §Status).
    pub placement: Placement,
    pub ops: Vec<OpOutcome>,
    /// True when the realm was destroyed. Always true on a returned outcome —
    /// a failed destroy is an error, not a field a caller has to check.
    pub destroyed: bool,
}

impl JobOutcome {
    /// Every op that ran and answered.
    pub fn succeeded(&self) -> bool {
        self.ops.iter().all(OpOutcome::succeeded)
    }
}

// ---------------------------------------------------------------------------
// Job-level refusals (before any op runs).
// ---------------------------------------------------------------------------

/// A job this dispatch cannot carry, refused BY NAME before an op runs.
///
/// These are defects in the job or gaps in the dispatch, never one op's bad
/// luck: an op's own failure is an `OpOutcome`, because the job's policy says
/// what to do with it.
#[derive(Debug)]
pub enum DispatchError {
    /// `JobSource::Plan`: advancing a schematic run belongs to the run's own
    /// authority, not to this door.
    PlanSource,
    /// A `Scope::Shared` job's results are the installation's; this door creates
    /// a realm that dies with the job, so carrying it would silently discard
    /// what the job said to keep.
    SharedScope,
    /// Two declarations under one name: inside a realm a name is a type, and two
    /// definitions of it would make registration order decide the meaning.
    DuplicateDecl { name: String },
    /// A declaration with no name is a type nothing could address.
    UnnamedDecl,
    /// An op names a type the job did not declare. Refused before ANY op runs:
    /// a stale op list against a fresh declaration set is a job defect, and
    /// finding it on the last step would be a partial run.
    UndeclaredType {
        op: usize,
        name: String,
        declared: Vec<String>,
    },
    /// An op's `executor` is not `type/key` (`ToolCall.executor`, ADR-0042's
    /// rendering: the singleton's key segment is empty).
    MalformedTarget { op: usize, executor: String },
    /// The op's verb is not a Start. Next/Dispose address a stream that must be
    /// in flight; driving a stream across a job is deferred with the frame path.
    UnsupportedKind { op: usize, kind: &'static str },
    /// A ctx service the realm cannot grant for this op (effector ADR-0005:
    /// "asking for one the node cannot offer is refused before the op runs").
    Need {
        op: usize,
        need: &'static str,
        why: String,
    },
    /// A declaration could not be written. The realm is destroyed before this is
    /// returned.
    Registration {
        name: String,
        source: anyhow::Error,
    },
    /// One of ADR-0051's four boundaries.
    Boundary(BoundaryRefusal),
    /// The job's declarations did not arrive in a form this door reads: absent, duplicated,
    /// under a name it does not know, in a media it does not decode, over the size cap, or
    /// undecodable. One variant carrying the reason, because the caller cannot act differently
    /// on each — and the reason has to say what actually arrived.
    Attachment { why: String },
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlanSource => write!(
                f,
                "a plan job names a definition by hash and a run to advance — that is \
                 schematic's authority, not this door's (ADR-0051 step 2a carries ops only)"
            ),
            Self::SharedScope => write!(
                f,
                "a shared-scope job's results are the installation's, and this door's realm \
                 dies with the job: dispatching it here would discard what the job said to keep"
            ),
            Self::DuplicateDecl { name } => {
                write!(f, "booth type `{name}` is declared twice in one job")
            }
            Self::UnnamedDecl => write!(f, "a booth declaration has no name to address"),
            Self::UndeclaredType { op, name, declared } => write!(
                f,
                "op {op} names booth type `{name}`, which the job does not declare \
                 (declared: {declared:?})"
            ),
            Self::MalformedTarget { op, executor } => write!(
                f,
                "op {op}: executor `{executor}` is not `type/key` — a job's op addresses an \
                 instance of a declared type (ADR-0042)"
            ),
            Self::UnsupportedKind { op, kind } => write!(
                f,
                "op {op}: `{kind}` addresses a stream that must already be in flight; a job \
                 carries Starts (the frame path is deferred)"
            ),
            Self::Need { op, need, why } => {
                write!(f, "op {op} asks for `{need}`, which this realm cannot grant: {why}")
            }
            Self::Registration { name, source } => write!(
                f,
                "registering booth type `{name}` into the job's realm failed: {source:#}"
            ),
            Self::Boundary(refusal) => write!(f, "{refusal}"),
            Self::Attachment { why } => write!(
                f,
                "the job's declarations did not arrive in a usable form: {why}"
            ),
        }
    }
}

impl std::error::Error for DispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registration { source, .. } => Some(source.as_ref()),
            Self::Boundary(refusal) => Some(refusal),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch (ADR-0051): the door, and the declarations that arrive through it
// ---------------------------------------------------------------------------

/// The attachment name a job's declarations ride under. A name is what makes the payload
/// refusable: a job carrying something else is not "a job with extra bytes", it is a job this
/// door cannot read, and saying so beats silently running without the declarations.
pub const BOOTHSET_ATTACHMENT: &str = "boothset";

/// What a job may carry in attachments, in total. A definition that needs more than this is
/// being shipped as bulk data, which is a different mechanism (an S3 resource, ADR-0025) —
/// the envelope is not a transport for megabytes.
pub const ATTACHMENT_CAP: usize = 1 << 20; // 1 MiB

/// Read a job's declarations out of its envelope.
///
/// The wire form is `PersistedBooth` — the same record a registration writes — because that IS
/// "a booth as data": name, language, source, codec, the `interface_schema` document, ttl and
/// concurrency. Using it means there is no second declaration format to keep in step, and the
/// one body that cannot travel (`Body::Rust`, a linked function) is excluded by construction
/// rather than by a rule someone has to remember.
///
/// Every refusal here happens BEFORE a realm exists, so a job whose declarations did not arrive
/// leaves nothing behind and cannot half-run.
pub fn decls_of(job: &Job) -> Result<Vec<BoothDecl>, DispatchError> {
    let total: usize = job.attachments.iter().map(|a| a.bytes.len()).sum();
    if total > ATTACHMENT_CAP {
        return Err(DispatchError::Attachment {
            why: format!(
                "{total} bytes of attachments exceed the {ATTACHMENT_CAP}-byte cap; ship bulk \
                 data as a resource, not as an attachment"
            ),
        });
    }
    if let Some(other) = job.attachments.iter().find(|a| a.name != BOOTHSET_ATTACHMENT) {
        return Err(DispatchError::Attachment {
            why: format!(
                "attachment `{}` is not a name this door reads (it reads `{BOOTHSET_ATTACHMENT}`) \
                 — an unrecognised attachment is a refusal, not something to skip",
                other.name
            ),
        });
    }
    let mut sets = job.attachments.iter().filter(|a| a.name == BOOTHSET_ATTACHMENT);
    let Some(set) = sets.next() else {
        return Err(DispatchError::Attachment {
            why: format!(
                "no `{BOOTHSET_ATTACHMENT}` attachment: the job names types but did not bring \
                 their declarations, and they do not exist anywhere else"
            ),
        });
    };
    if sets.next().is_some() {
        return Err(DispatchError::Attachment {
            why: format!("`{BOOTHSET_ATTACHMENT}` arrived twice; one job has one declaration set"),
        });
    }

    let booths: Vec<aura_booth::persist::PersistedBooth> = match set.media.as_str() {
        "application/cbor" => ciborium::de::from_reader(set.bytes.as_slice()).map_err(|e| {
            DispatchError::Attachment {
                why: format!("`{BOOTHSET_ATTACHMENT}` is not decodable as CBOR: {e}"),
            }
        })?,
        "application/json" => serde_json::from_slice(&set.bytes).map_err(|e| {
            DispatchError::Attachment {
                why: format!("`{BOOTHSET_ATTACHMENT}` is not decodable as JSON: {e}"),
            }
        })?,
        other => {
            return Err(DispatchError::Attachment {
                why: format!(
                    "`{BOOTHSET_ATTACHMENT}` arrived as `{other}`; this door reads \
                     `application/cbor` or `application/json`"
                ),
            })
        }
    };

    Ok(booths
        .into_iter()
        .map(|b| BoothDecl {
            name: b.name,
            body: Body::Script {
                language: b.language,
                source: b.source,
                encoding: b.encoding,
            },
            storage: b.schema,
            // Routes are assembled from the schema's own `receives` at registration — the same
            // document, the same parse. Restating them here would be a second copy that can
            // disagree with the first.
            receives: Vec::new(),
        })
        .collect())
}

/// The realm side of a job (ADR-0051): mint, register, run, destroy.
pub struct Dispatch<'a> {
    set: &'a RealmSet,
    registrar: &'a dyn Registrar,
}

impl<'a> Dispatch<'a> {
    pub fn new(set: &'a RealmSet, registrar: &'a dyn Registrar) -> Self {
        Self { set, registrar }
    }

    /// The door a job that ARRIVED comes through: its declarations are read out of the
    /// envelope's attachment (ADR-0051 §Attachments). `run` remains the seam below it — the
    /// layer that already holds declarations in hand (composition, tests) enters there.
    pub async fn run_attached(&self, job: &Job) -> anyhow::Result<JobOutcome> {
        let decls = decls_of(job)?;
        self.run(job, &decls).await
    }

    /// Run one job in a temporary realm of its own.
    ///
    /// Order of operations, and each step is where it is for a reason:
    ///
    /// 1. PREFLIGHT — the whole job is checked (source, scope, declarations, op
    ///    targets, `needs`) BEFORE a realm exists. A job defect therefore costs
    ///    nothing and cannot leave half a run behind; and "a job naming a type it
    ///    did not declare" fails by name before any op runs, which is the only
    ///    moment at which that answer is still useful.
    /// 2. MINT — `RealmSet::create_ephemeral`: a uuidv7 and an in-memory engine
    ///    of its own (ADR-0050 §5). No row, no prefix, no watermark step.
    /// 3. REGISTER — every declaration, through the caller's `Registrar`, into
    ///    THIS realm. The same registration rule, a different engine (ADR-0050
    ///    §6).
    /// 4. RUN — the ops in list order, each with its `on_error` policy. `needs`
    ///    was already granted or refused in 1.
    /// 5. DESTROY — unconditionally, then the outcome. The destroy is not the
    ///    last op: it is the epilogue of every path, including the ones that
    ///    return an error.
    pub async fn run(&self, job: &Job, decls: &[BoothDecl]) -> anyhow::Result<JobOutcome> {
        let steps = preflight(job, decls)?;
        let eph = self.set.create_ephemeral().await?;

        // The one channel out (ADR-0051 §4), installed between mint and
        // registration: a declaration registered into a realm that cannot report
        // would be a type whose `ctx.emit` goes nowhere by construction, and
        // `needs.emit` is granted precisely because this line ran.
        //
        // The parent is `main`: a job's own realm holds no `RealmSet`, so it can
        // dispatch nothing (boundary (c)), which is why the only realm that can be
        // a dispatcher is the one the venue opened. The address is `Job::booth`,
        // filled by the dispatcher itself — a job cannot rename its audience.
        let parent = self.set.open(crate::realm_set::MAIN_REALM).await?;
        {
            let handle = eph.realm();
            let mut realm = handle.lock().await;
            realm.frames_out = Some(crate::frame_path::FramesOut {
                parent: parent.realm(),
                dispatcher: job.booth.clone(),
                path: crate::frame_path::FramePath::root(job.id.clone(), 0),
            });
        }

        // The channel is captured before the realm goes away: `job.end` is posted
        // after the destroy, because "the job is over" and "nothing of it is left"
        // are the same sentence to a reader, and the realm being gone is the second
        // half of it.
        let sink = {
            let handle = eph.realm();
            let realm = handle.lock().await;
            realm.frames_out.clone()
        };
        let inside = self.run_inside(&eph, decls, &steps).await;
        let destroyed = self.set.destroy(eph.id).await;

        if let Some(out) = &sink {
            let ops = match &inside {
                Ok(ops) => ops.len(),
                // A job that failed before its op list ran still ends, and says so:
                // a reader waiting for `job.end` must not hang on a registration
                // failure it will never be told about otherwise.
                Err(_) => 0,
            };
            let end = serde_json::json!({
                "kind": crate::frame_path::kind::JOB_END,
                "destroyed": destroyed.is_ok(),
                "ops": ops,
            });
            let path = crate::frame_path::FramePath::whole(job.id.clone());
            crate::Realm::deliver_frame(&out.parent, &out.dispatcher, &path, end).await?;
        }

        match (inside, destroyed) {
            (Ok(ops), Ok(())) => Ok(JobOutcome {
                job: job.id.clone(),
                booth: job.booth.clone(),
                realm: eph.id,
                placement: job.placement.clone(),
                ops,
                destroyed: true,
            }),
            (Ok(_), Err(e)) => Err(e.context(format!(
                "job `{}` finished, but its realm {} could not be destroyed — the realm is \
                 still live and its rows are still in memory",
                job.id, eph.id
            ))),
            (Err(e), Ok(())) => Err(e),
            (Err(e), Err(d)) => Err(e.context(format!(
                "and the job's realm {} could not be destroyed either: {d:#}",
                eph.id
            ))),
        }
    }

    /// Steps 3 and 4, against a realm that already exists. Returns the per-op
    /// outcomes; the caller destroys the realm whatever this answers.
    async fn run_inside(
        &self,
        eph: &NamedRealm,
        decls: &[BoothDecl],
        steps: &[Step<'_>],
    ) -> anyhow::Result<Vec<OpOutcome>> {
        for decl in decls {
            self.registrar
                .register(&eph.realm(), decl)
                .await
                .map_err(|source| DispatchError::Registration {
                    name: decl.name.clone(),
                    source,
                })?;
        }
        // `anyhow` keeps the type, so a caller that wants `DispatchError` back
        // downcasts instead of the error growing a second spelling on the way up.
        self.run_ops(eph, steps).await
    }

    /// The op list, in order, with each op's policy applied.
    ///
    /// Order IS the dependency: a job's ops form a line (effector ADR-0005 —
    /// `JobSource::Ops` is "the graph is a line"), so op N+1 runs only if op N
    /// did not stop the job. An op that stopped it leaves its successors
    /// `NotRun`, said per op rather than implied by a shorter list.
    async fn run_ops(
        &self,
        eph: &NamedRealm,
        steps: &[Step<'_>],
    ) -> anyhow::Result<Vec<OpOutcome>> {
        let mut out = Vec::with_capacity(steps.len());
        let mut stopped: Option<String> = None;
        for step in steps {
            Self::address_op(eph, step.index as u32).await;
            if let Some(reason) = &stopped {
                let result = OpResult::NotRun {
                    reason: reason.clone(),
                };
                Self::post(
                    eph,
                    serde_json::json!({
                        "kind": crate::frame_path::kind::OP_NOT_RUN,
                        "reason": reason,
                    }),
                )
                .await?;
                out.push(OpOutcome {
                    op: step.index,
                    policy: step.op.on_error,
                    attempts: 0,
                    result,
                });
                continue;
            }
            let mut attempts = 0u32;
            let result = loop {
                attempts += 1;
                match self.call_once(eph, step).await {
                    Ok(value) => break OpResult::Done { value },
                    Err(e) => {
                        // `Retry { times }` counts ADDITIONAL attempts (effector
                        // ADR-0005): `times: 2` is at most three runs. `Never` is
                        // one run and is never re-entered — not on a retry, not on
                        // a redelivery this door never performs.
                        let again = match step.op.on_error {
                            OnError::Retry { times } => attempts <= times,
                            _ => false,
                        };
                        if again {
                            continue;
                        }
                        let error = format!("{e:#}");
                        break match step.op.on_error {
                            OnError::Skip => OpResult::Skipped { error },
                            _ => OpResult::Failed { error },
                        };
                    }
                }
            };
            if let OpResult::Failed { error } = &result {
                stopped = Some(format!(
                    "op {} failed and its policy is {:?}: {error}",
                    step.index, step.op.on_error
                ));
            }
            // The op's outcome, as a frame (ADR-0052). The synchronous caller also
            // gets it in `JobOutcome`; the frame is there so a dispatcher reading
            // over a wire sees each op land instead of waiting for the end.
            let frame = match &result {
                OpResult::Done { value } => serde_json::json!({
                    "kind": crate::frame_path::kind::OP_DONE,
                    "value": value,
                }),
                OpResult::Skipped { error } => serde_json::json!({
                    "kind": crate::frame_path::kind::OP_SKIPPED,
                    "error": error,
                    "attempts": attempts,
                }),
                OpResult::Failed { error } => serde_json::json!({
                    "kind": crate::frame_path::kind::OP_FAILED,
                    "error": error,
                    "attempts": attempts,
                }),
                OpResult::NotRun { reason } => serde_json::json!({
                    "kind": crate::frame_path::kind::OP_NOT_RUN,
                    "reason": reason,
                }),
            };
            Self::post(eph, frame).await?;
            out.push(OpOutcome {
                op: step.index,
                policy: step.op.on_error,
                attempts,
                result,
            });
        }
        Ok(out)
    }

    /// One call, against the job's own realm. `Realm::call` is the same call
    /// path every surface uses — the realm resolves the type from ITS registry,
    /// so a declaration is reachable the moment it is registered.
    /// Point the outbound path at the op about to be described. Ops run one at a
    /// time, so the last step is rewritten rather than grown.
    async fn address_op(eph: &NamedRealm, op: u32) {
        let handle = eph.realm();
        let mut realm = handle.lock().await;
        if let Some(out) = realm.frames_out.as_mut() {
            out.path = out.path.at_op(op);
        }
    }

    /// Post one frame to the booth that dispatched this job.
    ///
    /// A realm a job runs in always has the channel (dispatch installs it before
    /// registration), so its absence here is a bug in this crate rather than a
    /// condition to absorb — and a job whose frames vanish would look to the
    /// dispatcher exactly like a job that had nothing to say.
    async fn post(eph: &NamedRealm, payload: serde_json::Value) -> anyhow::Result<()> {
        let out = {
            let handle = eph.realm();
            let realm = handle.lock().await;
            realm.frames_out.clone()
        };
        let Some(out) = out else {
            anyhow::bail!(
                "this job's realm has no outbound channel, so `{}` would never reach the \
                 dispatcher (ADR-0051 §4)",
                payload["kind"]
            );
        };
        crate::Realm::deliver_frame(&out.parent, &out.dispatcher, &out.path, payload).await
    }

    async fn call_once(
        &self,
        eph: &NamedRealm,
        step: &Step<'_>,
    ) -> anyhow::Result<serde_json::Value> {
        // Ops run one at a time, so the outbound path's last step is rewritten
        // rather than grown: a frame that leaves during op N says N.
        let slot = Realm::call(
            &eph.realm(),
            None,
            step.target.clone(),
            &step.op.call.entry,
            step.op.call.args.clone(),
        )
        .await?;
        match slot.wait().await? {
            Waited::Done(value) => value,
            Waited::Pending(id) => anyhow::bail!(
                "op {} reached a COLD instance (call {}): a job's op is a call against a \
                 registration the realm just wrote, so there is no caller to park",
                step.index,
                id.0
            ),
        }
    }
}

/// One op, checked and resolved: its index, the op itself, and the instance it
/// addresses.
struct Step<'a> {
    index: usize,
    op: &'a Op,
    target: InstanceId,
}

/// Everything that can be decided before a realm exists.
fn preflight<'a>(job: &'a Job, decls: &'a [BoothDecl]) -> Result<Vec<Step<'a>>, DispatchError> {
    let ops: &'a [Op] = match &job.source {
        JobSource::Ops { ops } => ops,
        JobSource::Plan { .. } => return Err(DispatchError::PlanSource),
    };
    if job.scope == Scope::Shared {
        return Err(DispatchError::SharedScope);
    }
    let mut names: Vec<&str> = Vec::with_capacity(decls.len());
    for decl in decls {
        if decl.name.is_empty() {
            return Err(DispatchError::UnnamedDecl);
        }
        if names.contains(&decl.name.as_str()) {
            return Err(DispatchError::DuplicateDecl {
                name: decl.name.clone(),
            });
        }
        names.push(&decl.name);
    }
    let mut steps = Vec::with_capacity(ops.len());
    for (index, op) in ops.iter().enumerate() {
        if !matches!(op.call.kind, CallKind::IterateStart) {
            return Err(DispatchError::UnsupportedKind {
                op: index,
                kind: match op.call.kind {
                    CallKind::IterateNext => "iterate_next",
                    CallKind::IterateDispose => "iterate_dispose",
                    CallKind::IterateStart => "iterate_start",
                },
            });
        }
        let target = target_of(index, &op.call.executor)?;
        let decl = decls
            .iter()
            .find(|d| d.name == target.booth_type)
            .ok_or_else(|| DispatchError::UndeclaredType {
                op: index,
                name: target.booth_type.clone(),
                declared: names.iter().map(|n| (*n).to_string()).collect(),
            })?;
        check_needs(index, op, decl)?;
        steps.push(Step { index, op, target });
    }
    Ok(steps)
}

/// `type/key` (ADR-0042's rendering: the singleton's key segment is EMPTY) —
/// the inverse of the `format!("{}/{}", booth_type, key)` the dispatch arm builds
/// for the wire (`instance.rs`), so a job's op addresses the same instance a
/// remote call would.
fn target_of(index: usize, executor: &str) -> Result<InstanceId, DispatchError> {
    let (booth_type, key) = executor
        .split_once('/')
        .ok_or_else(|| DispatchError::MalformedTarget {
            op: index,
            executor: executor.to_string(),
        })?;
    if booth_type.is_empty() {
        return Err(DispatchError::MalformedTarget {
            op: index,
            executor: executor.to_string(),
        });
    }
    Ok(InstanceId {
        booth_type: booth_type.to_string(),
        key: InstanceKey::parse(key),
    })
}

/// The capability gate (effector ADR-0005: `needs` is the ctx services an op is
/// given, and "asking for one the node cannot offer is refused before the op
/// runs").
///
/// `store` is granted exactly when the declaration gave the type storage — the
/// plan is resolved from the same schema the registration writes, so "declared
/// no storage" is the whole of the answer (ADR-0026: surface absence, not an
/// empty surface).
///
/// `emit` and `timer` are granted, `resource` is a named refusal:
///
/// - `emit` — frames back to the dispatching booth are the ONE thing ADR-0051 §4
///   lets across the boundary, and they are wired: dispatch installs the outbound
///   channel on the realm it minted, so every event the job's ops emit also lands
///   in the dispatcher's queue under `job.frame`, carrying the path. An op is
///   granted this because the channel exists, not because it might.
/// - `timer` — the realm owns a timer plane and a job's timer entry dies with it.
/// - `resource` — resources are venue state (ADR-0048/0049): a temporary realm's
///   registry starts empty, and inheriting the parent's grants would be the
///   boundary leak the four rules forbid. An op that needs a handle is refused.
fn check_needs(index: usize, op: &Op, decl: &BoothDecl) -> Result<(), DispatchError> {
    let Need {
        store,
        emit,
        resource,
        timer: _,
    } = op.needs;
    if store && decl.storage.is_none() {
        return Err(DispatchError::Need {
            op: index,
            need: "store",
            why: format!(
                "`{}` declared no storage, so it has no ctx.store surface (ADR-0026)",
                decl.name
            ),
        });
    }
    // `emit` needs no check here: the channel out is installed by dispatch before
    // a declaration is registered, and a temporary realm without one refuses the
    // emit itself (`Realm::emit`) rather than dropping the frame. A preflight test
    // for a fact the realm enforces would be the second copy of one rule.
    let _ = emit;
    if resource {
        return Err(DispatchError::Need {
            op: index,
            need: "resource",
            why: "a temporary realm starts with no declared grants, and inheriting the parent's \
                  would be exactly the boundary leak ADR-0051 forbids (ADR-0048/0049)"
                .to_string(),
        });
    }
    Ok(())
}