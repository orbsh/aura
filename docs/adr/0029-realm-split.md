# 0029 — Realm split: file-level decomposition of realm/src/lib.rs

> **Languages:** [English](0029-realm-split.md) (primary) · [中文](0029-realm-split.zh-CN.md)

**Status:** Accepted (2026-09-26) — implemented same day (commit 85573a1)

## Context

`crates/realm/src/lib.rs` is 1,257 lines and holds 28 methods over a 17-field `Realm`
struct. The fields do not form one concern; they are six distinct responsibilities
sharing one owner:

- **Registry plane**: `types`, `store_plans`, `persisted_schemas` (+ `register_type`,
  the introspection helpers).
- **Instance lifecycle**: `instances`, `queue_capacity`, the evictor (`evict_idle`,
  `evict_instance`, `spawn_evictor`).
- **Call-slot plane**: `call_specs`, `pending_calls`, `call_seq` (`call`,
  `dispatch_call`, `resolve_call`, deadline sweep).
- **Event + MQ plane**: `mq`, `router`, `dead_events` (`emit`, queue compaction,
  cursor consumption glue).
- **Remote-probe plane**: `probes`, `pending_remote`, `code_base_url`
  (`RemotePending`, `ProbeConn`).
- **Ctx bridge**: `ctx_for`, `host_bridge_for` — the seam that assembles per-call
  host closures for resident sessions.

The function-size distribution says the same thing: `run_job` 145 lines, `instance`
108, `host_bridge_for` 96, `call` 88, `emit` 88, `register_type` 69. A 1,257-line
file where the largest functions each serve a different plane is not one module; it is
six modules stacked in one file.

A secondary observation motivated this review: lib.rs carries 98 `.clone()` calls, the
highest density in the workspace. The honest analysis (recorded here so the goal is
not misread): the clones fall into three classes — closure/spawn captures
(ownership required by `async move`), registry double-writes (`HashMap` key
ownership), and cheap handle distribution (`self.mq.clone()`). **The file split does
not eliminate classes 1 and 2.** What the split changes is ownership *visibility*:
today many clones exist because every plane hangs off the same `self` and no module
owns a value — a helper borrows `self`, then clones what it needs into a closure.
Once each plane lives in its own module with its own state owner, data can move from
owner to owner instead of being re-cloned per borrow. The realistic outcome is a
partial clone reduction (closure captures stay), with structural clarity as the
primary win. A split performed while all state still hangs on one fat struct would
reduce nothing — the module boundaries below are what make the improvement possible.

## Decision

1. **File-level split inside the same crate — no new crates, no API change.** Rust
   allows multiple `impl Realm` blocks across files in one crate, so the methods move
   to modules while `Realm` remains the single composition struct with `pub(crate)`
   fields. Consumers (`aura-engine`, tests) see no signature change.

   Target layout:

   - `registry.rs` — registry plane: `types`, `store_plans`,
     `persisted_schemas`, `register_type`/`register_inner` plumbing, the
     introspection helpers.
   - `instance.rs` — instance lifecycle: `Instance`, the instances map,
     `instance`, `submit`, `run_job`, `run_job_queued`, the evictor trio.
   - `call_slot.rs` — call slots: `CallSpec`, `PendingEntry`, `call_seq`,
     `call`, `dispatch_call`, `resolve_call`, `declare_call`, deadline sweep.
   - `events.rs` — event + MQ plane: `emit`, route helpers, queue compaction,
     dead-event glue. (`mq.rs` itself stays as-is — it is the store, not the realm
     glue.)
   - `remote.rs` — probe plane: `ProbeConn`, `RemotePending`, `probes`,
     `pending_remote`, `code_base_url`, the remote dispatch arm.
   - `ctx.rs` — ctx bridge: `ctx_for`, `host_bridge_for`, the JSON arg helpers
     currently at the top of lib.rs.
   - `lib.rs` remains: the `Realm` struct definition, constructors (`with_mq`,
     `with_code_base_url`), `Default`, re-exports. Target size ≈ 150–250 lines.

2. **Cross-plane access goes through `pub(crate)` field reads, not accessor
   methods.** The planes genuinely interleave (a remote call resolves a call slot
   which delivers into an instance); inventing a trait or message layer between them
   would be structure for its own sake. The split's purpose is file-level ownership
   clarity; `pub(crate)` keeps the interleaving expressible and keeps the option of a
   later, harder boundary open without committing to one now.

3. **`SharedRealm`/`Weak` discipline is unchanged.** Every closure or task that
   outlives a single job keeps holding the realm `Weak` (the standing retain-cycle
   rule). The split must not create a second capture pattern; module extraction moves
   code verbatim first, and any ownership simplification (move-instead-of-clone) is a
   separate, reviewed change per site.

## Honest semantic cost

- The clone count does not drop by 98-to-small. Closure captures and `HashMap` key
  ownership survive any decomposition; only the "borrow self, clone what the closure
  needs" class gets a move path. The ADR's goal is ownership clarity; the clone
  reduction is partial and incidental.
- Six files sharing one struct can drift back toward a monolith if new fields are
  appended to `Realm` without assigning them a plane. Mitigation is review
  discipline, not code: a new field lands in the module that owns its concern, or the
  proposal states why it is cross-cutting.
- Multi-`impl` blocks trade file locality for module locality — reading one plane no
  longer shows the whole struct. This is the intended trade: the struct's 17 fields
  were already too many to hold in one view.

## Consequences

- Implementation is a pure move commit: functions and field comments migrate to the
  modules above, `use` statements re-plumbed, no behavior change, full test suite
  green (`cargo test -p aura-realm --features "fjall,steel,nushell"` per the
  feature-forwarding rule) plus `cargo test -p aura-engine --features
  "fjall,nushell,steel"` since engine tests cover the realm surfaces.
- Follow-up ownership simplifications (move-instead-of-clone per site) are separate
  work items, each justified at its own call site — not bundled into the move.
- No PLAN phase is created; this is internal structure, not a capability. The PLAN
  session log records the landing.
