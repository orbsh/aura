# 0028 — Realm-set terminology: the outer isolation axis renames namespace → realm

> **Languages:** [English](0028-realm-set-terminology.md) (primary) · [中文](0028-realm-set-terminology.zh-CN.md)

**Status:** Accepted (2026-09-25) — naming ruling; code rename is the first work item, docs sweep follows

## Context

Two isolation axes share one word. The okm key segment (`#[kv_ns(N)]`, the 2-byte table/type
number inside one engine) and the Phase 3.6 outer space (a string-named, prefix-bound data
world: `register_in(namespace, …)`, `MqStore::namespaced`, `node { namespace … }`) both read
as "ns"/"namespace" — every partitioning or 4.10 reading trips over which axis a sentence
means. The collision is not cosmetic: the two axes answer different questions (which key
segment belongs to which table vs which worlds are mutually invisible) and sit in different
layers (compile-time declared numbers vs runtime strings).

The word for the outer axis already exists in the codebase: the outer space IS a `Realm` —
`Namespaces` is internally `HashMap<String, SharedRealm>`, and `realm_of(namespace)` returns
exactly the one `Realm` object that namespace owns. The mapping is 1:1, so "namespace" is a
second name for a thing that has a precise one.

## Decision

1. **The outer axis is named realm; the axis of "which realm" is named realm-set.**
   - `Realm::namespace` string → **realm name**; `EngineConfig.namespace` field → `realm`.
   - The collection type `Namespaces` → `RealmSet` (avoids `Realms`, which reads as the
     already-taken plural of the runtime object); `NamespacedRealm` → `NamedRealm` (a realm
     carrying its name).
   - `MqStore::namespaced(inner, ns)` → `MqStore::for_realm(inner, name)`; every `*_in`
     surface parameter renames likewise.
   - KDL: `node { namespace "x" }` → `node { realm "x" }`. No compatibility alias — the
     field is young, deployments are config, and a silent dual spelling is exactly the
     drift this ADR removes.
2. **The okm key segment keeps `ns`** (`#[kv_ns(N)]`, type ns, ns 42, …). After the rename
   "ns" means exactly one thing: the 2-byte key segment. The abbreviation collision is the
   root being cut, not papered over.
3. **"Tenant" stays out of the vocabulary.** The rename does NOT bind the axis to tenancy —
   a realm may hold one tenant's whole application, one application, one project, or the
   single default of a no-isolation deployment. The binding dimension remains an
   application decision (the 4.10 demotion ruling stands, verbatim, about realms now).

## Honest semantic cost

- `Realm` the runtime struct vs `realm` the name: two uses of one word. Accepted — they are
  1:1, so the reference is never ambiguous ("the realm `alice`" = the Realm object named
  alice), and docs spell the distinction in prose ("realm name") where needed.
- The rename touches a public config key (`node { namespace }` → `node { realm }`) with
  zero deployed consumers to migrate — correctness of the model is the only input (the
  0026 §3 ruling), not migration cost.

## Consequences

- Code (first work item, same commit): `crates/realm/src/namespace.rs` → `realm_set.rs`,
  the identifiers of §1, config field + KDL child, engine plumbing, tests.
- Docs sweep: partitioning §3, storage.md federation lines, actor-api, modeling, PLAN live
  passages, ADR-0026 §1 (a dated update — the decision archive is not rewritten), and the
  wiki's stateless-agent passages that describe the mechanism.
- PLAN 3.6's historical entry describes the landing-time shape — kept as landed (log rule);
  the 4.10 demotion passage is live prose and updates to realm wording.
- Cross-repo prose that quotes aura's surfaces (prism ADRs) follows when prism work next
  touches the mount point.
