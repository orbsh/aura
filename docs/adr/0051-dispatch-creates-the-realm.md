# 0051 — Dispatch creates the realm: work that was never registered

> **Languages:** [English](0051-dispatch-creates-the-realm.md) (primary) · [中文](0051-dispatch-creates-the-realm.zh-CN.md)

**Status:** Accepted (2026-11-19). **Step 2a landed (2026-11-20)** — the realm side of dispatch: `crates/realm/src/dispatch.rs`, entered from the engine at `Engine::dispatch`; see *Landed* below for exactly what it covers and *Deferred* for what it does not. It settles how work that has no registration gets a place to run; ADR-0050 settles what a realm is, and this record uses it.
**Date:** 2026-11-19

## Context

Some work cannot be registered in advance. gravity's skill steps come out of k10r as data: a graph of nodes, each node a booth interface, defined after the platform was deployed and possibly defined once. The current answer is to pre-register a fixed set of nodes in the dispatch layer and route the graph through them — which is where gravity's complexity comes from, and which cannot express a graph a user wrote this morning.

The obvious alternative — register the definitions, run the work, delete the definitions — turns one instant fact into three round trips and a durable state that must be cleaned up. Worse, it is durable state that can be left behind: a crash between run and delete leaves a booth type in the registry that nothing owns.

ADR-0050 gives realms an identity, a creation API and a destroy that means something. That is the opening this record uses.

## Decision

**RULING — a dispatch may bring its own definitions. The realm creates a temporary realm for the job, registers those declarations inside it, runs the work there, and destroys it. Nothing about the job outlives the job.**

1. **Definitions travel with the work as declarations, never as registrations.** A declaration is data:

```rust
struct BoothDecl {
    name: String,            // the type name inside this realm
    body: Body,              // Script { language, source, encoding } — the same body a registered booth has
    storage: Vec<CollectionDecl>,  // collections this type may use; in a temporary realm they live in memory
    calls: Vec<CallDecl>,          // the interfaces the graph's edges address
}
```

The declarations arrive with the submission, because they are not expected to exist yet. In this repository they travel BESIDE the job — `Engine::dispatch(&job, &decls)` — rather than inside it: `effector_protocol::JobSource` is the effector-facing envelope and stays aura-agnostic, while a declaration is aura's own type (a body, a storage schema, its `receives`). The pair travels together; the envelope does not learn that aura exists.

2. **The realm, not the booth, creates the realm.** A booth asks for work; the realm decides that the work needs a space of its own and makes one. This keeps realm creation a capability of the layer that can grant capabilities, and it is why a booth needs no vocabulary for realms at all.

3. **A temporary realm inherits its parent's node view and placement policy.** It can address the same nodes under the same labels; it cannot widen that set. Without this, "temporary" would be a way to escape placement rules — a job could ask for a laptop by asking for a realm that sees laptops.

4. **Nothing in a temporary realm is persisted.** Its type registry is not written to `interface_schema`, its storage namespaces come from its own namespace band rather than rows that outlive the process, and its mq store is an engine of its own with no durability behind it (okm ADR-0072). The only thing that crosses the boundary is the frames the job produces, addressed to the booth that dispatched it.

5. **Destroy is the last step of the job, not a separate chore.** Instances, resident executors, streams and in-flight calls are dropped with the realm; there is nothing in storage to delete, because nothing was written. A crash mid-job leaves no definition behind — which is the property the register/run/delete sequence could not promise.

6. **A temporary realm cannot create a realm.** Jobs may nest (effector ADR-0005); realms do not (ADR-0050 §7). A job's nested jobs run in the same temporary realm, which is also what makes the graph's nodes visible to each other.

## Consequences

- **gravity stops needing fixed nodes in the dispatch layer.** Its skill graph becomes: read the nodes out of k10r (a step), declare them, dispatch. The pre-placed nodes and the routing that fed them are deleted, not kept for compatibility.
- **prism never learns temporary realms exist.** It holds `main` and draws frames; a temporary realm is the realm's business.
- **Frame addressing carries a path.** `(job, op)` is the minimum; a nested job's frames carry the chain, or a booth cannot tell its own step's output from a grandchild's.
- **A job that declares a type can collide with a parent's type of the same name.** Inside the temporary realm the declaration wins and the parent's type is not visible — the realm boundary is the namespace boundary. That is isolation working, and it means a graph must not assume it can reach a platform type by name; it reaches it by dispatching.
- **Cost of a temporary realm must stay small.** Creating one is a map insert, an in-memory ns counter and a stand-in store; if a realm ever becomes expensive to build, this design turns every skill step into an allocation storm and will need pooling — deliberately not designed here, because it would be premature until the cheap path is measured.

## Landed — step 2a (2026-11-20)

The realm side of dispatch is implemented in `crates/realm/src/dispatch.rs` and entered from the engine at `Engine::dispatch(&job, &decls)`. A job now does five things, in this order, and the order is the design:

1. **Preflight, before a realm exists.** The whole job is checked: `JobSource::Ops` only (`Plan` is refused by name — advancing a schematic run is the run's own authority), `Scope::Turn` only (`Shared` results belong to the installation, and a realm that dies with the job cannot hold them), no duplicate or unnamed declaration, every op's `executor` parses as `type/key` (ADR-0042), every type an op names is one of the job's declarations, and every op's `needs` can be granted. A job naming a type it did not declare therefore fails **by name, before any op runs**, and leaves nothing behind.
2. **Mint.** `RealmSet::create_ephemeral` — a uuidv7 and an in-memory engine of its own (ADR-0050 §5). No row, no prefix, no watermark step, so a refused or failed job cannot move the parent's id sequence.
3. **Register.** Every declaration through the caller's `Registrar`. The engine's impl is `Engine::register_into_with` → `Realm::register_definition`, which is the body `Engine::register_into` ends at: the `BoothName` row, the `BoothDef` row carrying the declaration's `interface_schema`, the `EventRoute` rows and the code blob, written into the realm's OWN store. One rule, two engines (ADR-0050 §6) — no second registry, no second ns rule, and the declaration's schema wins over introspection because a declaration is data written for this job.
4. **Run.** The op list in order, each op with its `on_error`: `Fail` records the failure and stops the job, and its successors are reported `NotRun` with the reason (never dropped from a shorter list); `Skip` records it and continues; `Retry { times }` makes at most `times` **additional** attempts (effector ADR-0005: `times: 2` is three runs, not two); `Never` is exactly one attempt and is never re-entered. The outcome lists every op with its policy, its attempt count and its result.
5. **Destroy.** `RealmSet::destroy` runs on every returning path — success, an op that stopped the job, a registration failure — and a failed destroy is an error rather than a field a caller might not read.

`needs` is enforced as what effector ADR-0005 says it is: the set of ctx **services** an op is granted. `store` is granted exactly when the declaration gave the type storage (its plan is resolved from the same schema registration writes); `timer` is granted (the realm owns a timer plane and the entry dies with it); `emit` is granted because the channel out exists — dispatch installs it before a declaration is registered, so "granted" is a fact rather than a promise; `resource` is refused by name, because the venue's grants are not part of this step.

### The frames that cross, and where they land

§4 allows one thing through the boundary, and this is it. Dispatch installs an outbound channel on the realm it minted (`Realm::frames_out`) before a declaration is registered; a temporary realm with no channel refuses its own emits rather than dropping them, so "the job emitted nothing" cannot be a silent state.

The rule is **both, always**: an event the job's ops emit is delivered by the job's own router AND posted to the dispatcher. Filtering — "these are internal, those are news" — would need a per-type declaration that does not exist, and getting it wrong hides exactly the frames an operator is looking for. So every frame leaves, and the local plane is unaffected.

Delivery is **directed, not routed**: the frame is appended to the dispatching booth's queue on the parent realm (`job.frame`, partition = the booth's key), because a dispatcher is not a subscriber to an event — it is the recipient of a message about a job it submitted. Two consequences follow: the parent needs no route row to receive frames, and a dispatcher that is not live gets a backlog instead of a lost frame.

Every frame carries its **path** — `[{job, op}]`, the chain for a nested job, with the op step rewritten before each call because ops run one at a time; a frame the JOB itself posts (its end) carries `[{job}]`, no op. The address is `Job::booth`, filled by the dispatcher itself, so a job cannot rename its audience.

The channel carries the job's whole observable life, not only what its ops emit: `emit`, `op.done`, `op.skipped`, `op.failed`, `op.not_run`, `job.end`, discriminated by a `kind` field, and read back through a cursor (`Engine::job_frames`). ADR-0052 is that decision and its reasoning — one queue, no second buffer.

One-way is structural, not a rule: a job's realm receives frames only from its own `@on` routes, and only the realm writes those queues. There is no inbound door because none was built.

### The four boundaries, and what enforces each

- **(a) a booth cannot write the parent's store — STRUCTURAL.** `Realm::with_mq` refuses a non-memory store for an `Ephemeral` id, and `MqStore::for_realm` refuses to bind a slice of the engine's keyspace to one; both refusals name the reason. A production parent is fjall, so the parent's engine cannot be handed to a job's realm at all. (A memory venue's root handle is indistinguishable from a fresh in-memory engine, which is why the only ephemeral constructor is on the set: `create_ephemeral` mints its own store.)
- **(b) it cannot publish MQ to the parent — STRUCTURAL.** Every queue, cursor and route row is written through `Realm::mq`, the realm's own engine; there is no cross-realm emit to express. Proven rather than asserted: a job that writes through a plan AND emits leaves the parent's STATE namespaces byte-identical, leaves the parent's queue for the emitted event at depth 0, and adds exactly one row to the parent's queue plane — the frame, under `job.frame`, addressed to the dispatcher.
- **(c) it cannot open a child realm — STRUCTURAL, WITH A NAMED REFUSAL.** No booth holds a `RealmSet`, so nesting is not expressible (§2, ADR-0050 §7). The door that would express it, `Realm::create_child_realm`, answers `BoundaryRefusal::ChildRealm` instead of minting — a named refusal, and an `Infallible` success type, so no caller can be holding a "successful" child realm.
- **(d) it inherits the parent's view — CONFIGURATION.** The realm set carries the engine's code base URL and cursor TTL into every realm it creates, and the venue's node pool — with the job's `Placement` narrowed to a `Reach` (ADR-0053). Type view is deliberately NOT inherited (§Consequences: the realm boundary is the namespace boundary, so a declaration shadows a same-named parent type).

### Where the envelope could not say it

The declarations ride the envelope: `Job::attachments` carries one opaque payload named `boothset` (effector ADR-0005 §attachments), and `Engine::dispatch(&job)` decodes it. That is the door — one door, so a job that arrived over a wire and a job composed in this process take the same path, and neither can hand a realm definitions the other could not. The wire form is `aura_booth::persist::PersistedBooth` (the record a registration already writes): name, language, source, codec, the `interface_schema` document, ttl, concurrency. There is no second declaration format, and the one body that cannot travel — `Body::Rust`, a linked function — is excluded by construction rather than by a rule someone has to remember. `BoothDecl` remains this crate's in-memory reading of §1 (`body` + `storage` + `receives`); `Dispatch::run(&job, &decls)` is the seam below the door, for a caller that already holds declarations.

## Deferred

Named so this record cannot be read as a promise larger than it is:

- **Selection BETWEEN nodes for one call** — `Placement` is a filter over the pool (ADR-0053), not a chooser: a declaration names its node, so picking the least-loaded or nearest one for a call needs a late-bound declaration, which is a different change.
- **Nested jobs** — §6 says a job's nested jobs run in the same realm. Nothing dispatches one from inside an op yet; the single isolation level is enforced by boundary (c).
- **gravity / k10r integration** — reading a skill graph out of k10r and dispatching it. §Consequences describes that end state; step 2a is the realm half only.
- **Shared-scope jobs** — refused by name rather than mis-carried; they need a door to a realm that outlives the job, which is a different decision.

## Tests

Run by the workspace suite (`cargo test --workspace`); named because each one is a claim this record makes:

- `crates/engine/tests/dispatch_job.rs` — `a_job_runs_its_ops_in_order_and_needs_store_is_granted_when_declared` (order is the dependency, and `needs.store` is really granted), `skip_records_the_failure_and_carries_on`, `fail_stops_the_job_and_its_successors_are_reported_not_run`, `retry_two_runs_the_step_three_times_and_no_more`, `never_runs_exactly_once_even_when_a_later_step_fails`, `a_failing_never_step_is_one_attempt_and_is_not_retried`, `a_job_naming_a_type_it_did_not_declare_fails_by_name_before_any_op_runs`, `needs_store_is_refused_before_any_op_runs_when_the_type_declared_no_storage`, `a_plan_job_and_a_shared_scope_job_are_refused_by_name`. Every one of these runs through the engine's registration door, so the production path is what is exercised.
- `crates/engine/tests/dispatch_job.rs` — `a_frame_a_job_emits_reaches_the_booth_that_dispatched_it` (a frame an op emits lands in the dispatcher's queue with its path, its event name and its payload), `a_job_with_no_channel_out_cannot_emit_at_all` (a temporary realm with no channel refuses the emit by name).
- `crates/realm/tests/dispatch_realm.rs` — `a_job_leaves_the_parent_keyspace_byte_identical` (a real fjall keyspace swept before and after a job that registers a declaration with storage, writes through its plan and emits an event: the parent's STATE namespaces identical in keys and values, its queue plane gaining exactly one row — the frame — the emitted name still at depth 0, the realm gone from `live()`), `a_booths_realm_cannot_be_bound_to_the_parent_store` (both boundary-(a) refusals, by name), `a_realm_cannot_open_a_child_realm` (boundary (c), by name).

## Still open

- ~~Whether a declaration set is a `JobSource` variant or a field beside `resources`.~~ Settled as neither: it is an ATTACHMENT — a generic carrying slot on the envelope (`Job::attachments`, name + media + bytes) that effector never interprets. A `JobSource` variant would have made the envelope learn aura's vocabulary (a body, a storage schema); an attachment makes it learn only "something travels with this job", and the meaning stays on the side that filled it. The rule this replaced — declarations as a sidecar argument — survived one day and is gone.
- ~~Whether a temporary realm's frames should be visible to other jobs in the same dispatch, or only to the booth that dispatched.~~ Settled as the second: frames are addressed, not broadcast, and the address is the dispatcher's own name. A job's frames reaching a sibling job would need a channel between two realms, which is boundary (a) with a different name.

## Related records

aura ADR-0050 (a realm is a row), ADR-0026 (a booth's storage is type-scoped), effector ADR-0005 (a job is an envelope), gravity ADR-0011 (composition is a graph — the graph being declared here).