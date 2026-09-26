# 0013 — Federation over metadata consensus: node autonomy, data stays home

**Status:** Accepted (2026-09-15)

## Context

The pre-federation architecture synced metadata (Booth registry, shard map,
config) across nodes via Openraft consensus while Booth state stayed
node-local (Fjall) or lake-backed (SlateDB+S3). The premise was that
metadata-first routing is enough for multi-node: a user arriving at any node
gets routed to wherever their state lives.

That premise does not survive contact with the failure model. If user U's
state is on node B and U arrives at node A:

- correct metadata only saves a lookup — the session data still has to
  travel from B, or the user waits on B's availability;
- if B is down, metadata says everything is fine (the registry is
  consistent cluster-wide) while the user's data is actually unreachable —
  the consensus layer manufactured the *appearance* of one system while
  the data plane remained partitioned by construction.

Metadata-only sync pays the full complexity price of consensus (quorum
availability, membership management, log compaction, a second storage
engine's worth of failure modes) for a benefit the data plane cannot cash:
cross-node users still lose their data relative to their home node.
Syncing metadata alone is worse than syncing nothing — it hides the
partition.

## Decision

**Nodes are autonomous federation members (Matrix-style), not peers in a
consensus cluster. No consensus layer, no metadata sync.**

- **Metadata is per-node**: the meta okm instance (Booth registry, shard
  map, config) is written by the node's control plane (single logical
  writer) and read from node-local cache. A single logical writer makes
  consensus structurally unnecessary — there is nothing to agree on.
- **User data follows its home node**: data sovereignty binds to the node
  the user belongs to. Logging into another node does not replicate the
  user's history there. Cross-node access is explicit, not transparent.
- **Inter-node identity via well-known protocol authentication** (public
  key / certificate): nodes interact after cryptographic identity
  verification, isolated per user namespace. No shared storage, no shared
  consensus log.
- **Location transparency is deliberately rejected**: an address carrying
  its node domain is a feature (Matrix's `@user:domain`), not a defect.
  Cross-domain interaction is explicit addressing; only intra-domain
  emit/on is transparent.
- If a scenario ever genuinely requires Booth-state strong-consistent
  replication (the TiDB model), it uses an existing mechanism (TiKV /
  FoundationDB) — building a consensus layer into this architecture is
  permanently off the table.

## Consequences

- The Openraft dependency and the `Distribution` trait are dropped before
  they were ever implemented; no code regresses. `openraft` returns only
  if a real second metadata writer appears (none is on the roadmap).
- The meta okm instance keeps its role from Phase 4.5b (script
  definitions, routes, TTL) — but as node-local state, not replicated
  catalog. The two-instance (data/meta) model is unchanged; only the
  "will be synced" narrative is gone.
- Cross-node semantics become an explicit protocol problem (federation
  messages, identity, namespace isolation) rather than an infrastructure
  illusion — this is where future design effort goes.
- Placement planning (consistent hashing across nodes) dissolves: there
  is no global Booth space to place into, only domains.

> Errata (2026-09-25, PLAN 4.10): the "isolated per user namespace"
> phrasing above predates the namespace-binding demotion — the namespace
> mechanism survives (prefix isolation at construction) but its binding
> dimension is an application decision; the framework's isolation units
> are type ns (storage) and instance serialization (routing). Node trust
> rides ADR-0015 node identity. The two-instance meta narrative is also
> superseded by ADR-0025 Plan A (one data-plane okm instance).
