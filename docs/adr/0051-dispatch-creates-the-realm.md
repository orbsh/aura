# 0051 — Dispatch creates the realm: work that was never registered

> **Languages:** [English](0051-dispatch-creates-the-realm.md) (primary) · [中文](0051-dispatch-creates-the-realm.zh-CN.md)

**Status:** Accepted (2026-11-19). It settles how work that has no registration gets a place to run; ADR-0050 settles what a realm is, and this record uses it.
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

The declarations may ride the job (`JobSource::BoothSet { decls }`) or be reachable from it (a plan whose steps name types that the job also declares); either way they arrive with the submission, because they are not expected to exist.

2. **The realm, not the booth, creates the realm.** A booth asks for work; the realm decides that the work needs a space of its own and makes one. This keeps realm creation a capability of the layer that can grant capabilities, and it is why a booth needs no vocabulary for realms at all.

3. **A temporary realm inherits its parent's node view and placement policy.** It can address the same nodes under the same labels; it cannot widen that set. Without this, "temporary" would be a way to escape placement rules — a job could ask for a laptop by asking for a realm that sees laptops.

4. **Nothing in a temporary realm is persisted.** Its type registry is not written to `interface_schema`, its storage namespaces come from the in-memory allocator rather than the persisted rows, and its mq store is a stand-in. The only thing that crosses the boundary is the frames the job produces, addressed to the booth that dispatched it.

5. **Destroy is the last step of the job, not a separate chore.** Instances, resident executors, streams and in-flight calls are dropped with the realm; there is nothing in storage to delete, because nothing was written. A crash mid-job leaves no definition behind — which is the property the register/run/delete sequence could not promise.

6. **A temporary realm cannot create a realm.** Jobs may nest (effector ADR-0005); realms do not (ADR-0050 §7). A job's nested jobs run in the same temporary realm, which is also what makes the graph's nodes visible to each other.

## Consequences

- **gravity stops needing fixed nodes in the dispatch layer.** Its skill graph becomes: read the nodes out of k10r (a step), declare them, dispatch. The pre-placed nodes and the routing that fed them are deleted, not kept for compatibility.
- **prism never learns temporary realms exist.** It holds `main` and draws frames; a temporary realm is the realm's business.
- **Frame addressing carries a path.** `(job, op)` is the minimum; a nested job's frames carry the chain, or a booth cannot tell its own step's output from a grandchild's.
- **A job that declares a type can collide with a parent's type of the same name.** Inside the temporary realm the declaration wins and the parent's type is not visible — the realm boundary is the namespace boundary. That is isolation working, and it means a graph must not assume it can reach a platform type by name; it reaches it by dispatching.
- **Cost of a temporary realm must stay small.** Creating one is a map insert, an in-memory ns counter and a stand-in store; if a realm ever becomes expensive to build, this design turns every skill step into an allocation storm and will need pooling — deliberately not designed here, because it would be premature until the cheap path is measured.

## Still open

- Whether `BoothSet` is a `JobSource` variant or a field beside `resources` (it is a fact about the job, like resources, rather than about how to run one op).
- Whether a temporary realm's frames should be visible to other jobs in the same dispatch, or only to the booth that dispatched.

## Related records

aura ADR-0050 (a realm is a row), ADR-0026 (a booth's storage is type-scoped), effector ADR-0005 (a job is an envelope), gravity ADR-0011 (composition is a graph — the graph being declared here).