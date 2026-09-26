# Data Partitioning (Internals)

> Overview lives in the wiki: [Aura architecture §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md).
> This document describes the full partitioning mechanism, from key byte
> layout to cluster topology. Bilingual: [中文](partitioning.md).

## 1. Partition unit: the Booth instance; the instance key decides placement

The smallest partitioning unit is neither a table nor a realm — it is
the **Booth instance**. `InstanceId = (booth_type, key)`, where `key` is the
instance key (session_id, channel_id, order_id, ...). Placement rules:

- **Same key, serial**: all messages for one instance key land in the same
  instance's queue, consumed one at a time by a single consumer — state
  consistency comes from queue serialization, not locks
- **Different keys, parallel**: instances with different keys are fully
  independent and never block each other
- **Wildcard-subscription exception**: an Booth registered via `on_wildcard`
  binds to the singleton `__singleton__` and does not participate in
  partitioning (an observer listening to global events has no meaningful
  state shard)

The extraction of the instance key differs between the current state and
the target state: today the route table declares an `instance_key_field`
(taken from the event payload by field name; a missing field lands on the
`__default__` catch-all instance). In the target state (once dynamic schema
lands), the event name maps to an okm ns and the routing resolves ids
through that ns's access methods — a scan is one-to-many by nature, so a
single emit can deliver to multiple instances (sequenced: PLAN Phase 4.13,
precondition = dynamic schema).

## 2. booth_type: type vs instance

```rust
pub struct InstanceId {
    pub booth_type: String,  // type: which kind of Booth
    pub key: String,         // instance key: which instance of that kind
}
```

`booth_type` is the Booth's type name — the identity of one logical role;
the instance key identifies a concrete instance within that type.

```
BoothType "cart"                ← blueprint: state schema + handler + subscriptions
  ├─ Instance ("cart", "alice")   ← concrete instance: own queue, own state
  ├─ Instance ("cart", "bob")
  └─ Instance ("cart", "carol")   ← same type, different keys: independent, parallel
```

- **Registration**: `engine.register(BoothType::simple("echo", handler))` or
  `BoothType::script("py-ctx", "python", source, entry)` — the type name is
  fixed here; the body (Rust handler or script) attaches to the type and is
  shared by every instance
- **Routing**: `router.on("order.created", "cart", "user_id")` delivers to
  `(type, key extracted from the event)`; `ctx_invoke` addresses targets the
  same way with `{type, key}`
- **State layout**: storage isolation lives at the type level (ADR-0026 §3) —
  each booth type occupies one real okm ns and declares its own collections;
  instances are documents inside it. The instance key only answers "who
  serially processes this message"; it no longer decides storage layout
- **Shard identity**: `(booth_type, key)` together form the full partition
  identity; the key alone is not enough ("alice" under `cart` and under
  `session` are two unrelated instances)

Essentially the **class/instance relationship**: booth_type is the unit of
deployment and code distribution (hot reload swaps definitions per type);
the instance is the unit of serialization and state ownership (addressed,
sharded, and recovered by `(type, key)`).

### 2.1 Partition design principles: which identity picks which key

The criterion for choosing an instance key is the **instance's standing
ownership**, not a field the request happens to carry:

- **Identity equals ownership → use the identity as the key.** User-scoped
  data (carts, sessions) partitions by user_id: the instance identity itself
  encodes the user, and the handler reads `ctx.self_id.key` — a
  construction-level guarantee (the instance belongs to its key alone),
  stronger than caller-supplied mounts (nothing to spoof).
- **Ownership exceeds identity → use the ownership as the key; identity
  rides as a parameter.** Group chat partitions by channel_id: one instance
  serves many users, user_id is not the instance's standing attribute.
  Messages **carry their own channel_id** (the client knows which channel it
  is posting to; no engine-side lookup), and the sender's identity rides as
  a request parameter (membership checks, attribution inside the handler).
  Mounting user_id onto ctx here is a logical conflict — ctx is
  per-instance, so a mounted field would claim "this instance's user", and
  a group-chat instance has no such thing. Nor is an intermediary router
  booth needed to look up the channel by user_id first: that adds a hop, a
  state write, and turns the routing table into a second source of truth
  for membership.
- **Cross-partition reverse indexes (user ↔ channels, user ↔ orders) →
  projection booths**: a per-user booth subscribes to the event stream and
  maintains its own index — isomorphic to projection aggregation, off the
  delivery hot path.

In one sentence: **the instance key answers "who serially processes this
message"; the request parameters answer "who initiated this request"** —
two questions, answered independently, never mounted onto each other.

## 3. Key layout within a node: three binary segments

An instance's state on disk is a concatenation of fixed-width binary
segments (the okm key discipline — no textual separators):

```
[ns 2B BE][slot 1B][field encodings…][pkey]
```

- **ns (2 bytes)**: the okm-level table/edge-table number, addressed
  uniformly within the single okm instance (post ADR-0025, booth
  definitions share the data plane's instance: BoothDef ns 41 beside
  mq/state)
- **slot (1 byte)**: access-method discriminator within the table
  (0 = primary entry); all index entries of one table share its ns segment
- **Booth state**: instance state is NOT one flat document per instance —
  the type declares its collections inside its own ns (schema persisted
  with the interface_schema at upload), and handlers read/write through
  `ctx.store.emit(op)` carrying okm Collection instructions
  (put/get_document, fields, scan, reduce); same-type cross-instance
  aggregation is an ordinary scan/reduce inside the type's ns.

Realm isolation (the Phase 3.6 mechanism, renamed namespace → realm by
ADR-0028) is orthogonal to type nss: the realm prefix wraps the
outermost layer (`MqStore::for_realm`), the type nss live inside —
different realms' keyspaces within one physical engine are structurally
separated, and cross-realm access is not expressible at the type level.
WHAT a realm binds (user, project, or nothing) is an application
decision (the PLAN 4.10 demotion) — the framework's isolation units are
exactly two: type ns (storage) and instance serialization (routing);
user is not among them.

## 4. Serialization boundary: load on activation, flush on sleep

An instance in memory is a live object (a Rust handler or a script); **the
lifecycle of partition state is decoupled from instance residency**:

- **Activation (on_wake)**: after the resident is released the data
  remains in the type's collections; the next touch reactivates the
  instance and reads/writes on demand
- **Sleep (on_sleep/evict)**: the resident is released — scale-to-zero
  drops the resident, not the data (locked by acceptance tests:
  script-booth state over a declared collection survives eviction)
- Phase 6.5's resident window optimizes this boundary: within the retention
  window, consecutive same-partition calls flow through in-memory oneshots
  with zero persistence; state is flushed and the resident released only at
  window end

## 5. Cluster layer: shard map and the routing invariant (Phase 5, not yet implemented)

- **The shard map lives in the node's own storage** (post ADR-0025, the data plane's okm instance) under a
  single-writer model: exactly one logical writer (the control plane)
  writes the shard map / booth registry; nodes read through caches — no
  multi-writer consensus. Federation contains no path to consensus: a
  multi-control-plane deployment is a directional retreat (it overturns
  the federation, not an extension point); internal metadata stays
  control-plane-writable-only to keep the writer count at one; and a "globally unique
  config" does not exist under federation semantics — per-node
  independence is the ruling, not a defect. Moving to a logical
  single-cluster architecture wholesale would be a new ruling overturning
  ADR-0013, not an extension point within this one
- **Routing invariant**: the instance key → shard mapping is stable, and
  **requests follow the data** — every turn of a session routes to the
  machine hosting its partition; history never "goes missing", it just is
  not where a misrouted request looks
- **Structural cost, explicitly accepted**: a node's partitions freeze on
  node failure until recovery/migration — zero replication write
  amplification. A shard needing high availability is carried by FDB/TiKV
  (wiki ruling: no self-built strong-consistency replication) — the
  partitioning scheme and the replication scheme are decoupled; the
  default path carries no replication
- Booth-definition hot reload rides the node's own storage: write the new
  definition → nodes re-read on activation

## Appendix: evictor complexity trade-off

Current eviction is a fixed 5s tick + a full-table linear scan
(O(instance count)). A min-heap scheme (`BinaryHeap<(Instant, InstanceId)>`
ordered by **expiry time** — what goes into the heap is the expiry instant,
not the TTL value; a policy change would otherwise invalidate no stale
entries — with lazy deletion: on pop, cross-check against the `instances`
table and discard entries whose instant no longer matches
`last_activity + ttl`) is explicitly deferred: its payoff requires both a
six-figure resident-instance count on one node AND profiling showing the
evict scan is a real cost — before that point, the HashMap's own lock
contention becomes the bottleneck first. Revisit when both conditions hold.

## Design summary

The skeleton of this scheme is **"serialization unit = partition unit =
recovery unit"**: the instance key simultaneously determines message
serialization, keyspace ownership, and the failure blast radius.
Consistency comes from a single writer plus queue serialization, not
from a consensus protocol; the availability gap (partitions frozen on node
failure) is explicitly accepted and backstopped by an external
strongly-consistent KV rather than built-in replicas.
