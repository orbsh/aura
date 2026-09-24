# Data Partitioning (Internals)

> Overview lives in the wiki: [Aura architecture §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md).
> This document describes the full partitioning mechanism, from key byte
> layout to cluster topology. Bilingual: [中文](partitioning.md).

## 1. Partition unit: the Actor instance; the partition key decides placement

The smallest partitioning unit is neither a table nor a namespace — it is
the **Actor instance**. `InstanceId = (actor_type, key)`, where `key` is the
partition key (session_id, user_id, order_id, ...). Placement rules:

- **Same key, serial**: all messages for one partition key land in the same
  instance's queue, consumed one at a time by a single consumer — state
  consistency comes from queue serialization, not locks
- **Different keys, parallel**: instances with different keys are fully
  independent and never block each other
- **Wildcard-subscription exception**: an Actor registered via `on_wildcard`
  binds to the singleton `__singleton__` and does not participate in
  partitioning (an observer listening to global events has no meaningful
  state shard)

The extraction of the partition key differs between the current state and
the target state: today the route table declares a `partition_key_field`
(taken from the event payload by field name; a missing field lands on the
`__default__` catch-all instance). In the target state (once dynamic schema
lands), the event name maps to an okm ns and the routing resolves ids
through that ns's access methods — a scan is one-to-many by nature, so a
single emit can deliver to multiple instances.

## 2. actor_type: type vs instance

```rust
pub struct InstanceId {
    pub actor_type: String,  // type: which kind of Actor
    pub key: String,         // partition key: which instance of that kind
}
```

`actor_type` is the Actor's type name — the identity of one logical role;
the partition key identifies a concrete instance within that type.

```
ActorType "cart"                ← blueprint: state schema + handler + subscriptions
  ├─ Instance ("cart", "alice")   ← concrete instance: own queue, own state
  ├─ Instance ("cart", "bob")
  └─ Instance ("cart", "carol")   ← same type, different keys: independent, parallel
```

- **Registration**: `engine.register(ActorType::simple("echo", handler))` or
  `ActorType::script("py-ctx", "python", source, entry)` — the type name is
  fixed here; the body (Rust handler or script) attaches to the type and is
  shared by every instance
- **Routing**: `router.on("order.created", "cart", "user_id")` delivers to
  `(type, key extracted from the event)`; `ctx_invoke` addresses targets the
  same way with `{type, key}`
- **State layout**: the first segment of an instance's state key is the type
  (`[4B len(type)][type][4B len(key)][key][field]`) — instances of one type
  cluster in the keyspace, so a prefix scan enumerates them by type
- **Shard identity**: `(actor_type, key)` together form the full partition
  identity; the key alone is not enough ("alice" under `cart` and under
  `session` are two unrelated instances)

Essentially the **class/instance relationship**: actor_type is the unit of
deployment and code distribution (hot reload swaps definitions per type);
the instance is the unit of serialization and state ownership (addressed,
sharded, and recovered by `(type, key)`).

### 2.1 Partition design principles: which identity picks which key

The criterion for choosing a partition key is the **instance's standing
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
  actor needed to look up the channel by user_id first: that adds a hop, a
  state write, and turns the routing table into a second source of truth
  for membership.
- **Cross-partition reverse indexes (user ↔ channels, user ↔ orders) →
  projection actors**: a per-user actor subscribes to the event stream and
  maintains its own index — isomorphic to projection aggregation, off the
  delivery hot path.

In one sentence: **the partition key answers "who serially processes this
message"; the request parameters answer "who initiated this request"** —
two questions, answered independently, never mounted onto each other.

## 3. Key layout within a node: three binary segments

An instance's state on disk is a concatenation of fixed-width binary
segments (the okm key discipline — no textual separators):

```
[ns 2B BE][slot 1B][field encodings…][pkey]
```

- **ns (2 bytes)**: the okm-level table/edge-table namespace, addressed
  uniformly within the single okm instance (post ADR-0025, actor
  definitions share the data plane's instance: ActorDef ns 41 beside
  mq/state)
- **slot (1 byte)**: access-method discriminator within the table
  (0 = primary entry); all index entries of one table share its ns segment
- **Actor state**: instance state is NOT one flat document per instance —
  the type declares its collections inside its own ns (schema persisted
  with the interface_schema at upload), and handlers read/write through
  `ctx.store.emit(op)` carrying okm Collection instructions
  (put/get_document, fields, scan, reduce); same-type cross-instance
  aggregation is an ordinary scan/reduce inside the type's ns.

User namespaces (Phase 3.6) are orthogonal to type nss: the namespace
prefix wraps the outermost layer (`MqStore::namespaced`), the type nss
live inside — different users' keyspaces within one physical engine are
structurally separated, and cross-namespace access is not expressible at
the type level.

## 4. Serialization boundary: load on activation, flush on sleep

An instance in memory is a live object (a Rust handler or a script); **the
lifecycle of partition state is decoupled from instance residency**:

- **Activation (on_wake)**: after the resident is released the data
  remains in the type's collections; the next touch reactivates the
  instance and reads/writes on demand
- **Sleep (on_sleep/evict)**: the resident is released — scale-to-zero
  drops the resident, not the data (locked by acceptance tests:
  script-actor state over a declared collection survives eviction)
- Phase 6.5's resident window optimizes this boundary: within the retention
  window, consecutive same-partition calls flow through in-memory oneshots
  with zero persistence; state is flushed and the resident released only at
  window end

## 5. Cluster layer: shard map and the routing invariant (Phase 5, not yet implemented)

- **The shard map lives in the node's own storage** (post ADR-0025, the data plane's okm instance) under a
  single-writer model: exactly one logical writer (the control plane)
  writes the shard map / actor registry; nodes read through caches — no
  multi-writer consensus. Federation contains no path to consensus: a
  multi-control-plane deployment is a directional retreat (it overturns
  the federation, not an extension point); internal metadata stays
  control-plane-writable-only to keep the writer count at one; and a "globally unique
  config" does not exist under federation semantics — per-node
  independence is the ruling, not a defect. Moving to a logical
  single-cluster architecture wholesale would be a new ruling overturning
  ADR-0013, not an extension point within this one
- **Routing invariant**: the partition-key → shard mapping is stable, and
  **requests follow the data** — every turn of a session routes to the
  machine hosting its partition; history never "goes missing", it just is
  not where a misrouted request looks
- **Structural cost, explicitly accepted**: a node's partitions freeze on
  node failure until recovery/migration — zero replication write
  amplification. A shard needing high availability is carried by FDB/TiKV
  (wiki ruling: no self-built strong-consistency replication) — the
  partitioning scheme and the replication scheme are decoupled; the
  default path carries no replication
- Actor-definition hot reload rides the node's own storage: write the new
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
recovery unit"**: the partition key simultaneously determines message
serialization, keyspace ownership, and the failure blast radius.
Consistency comes from a single writer plus queue serialization, not
from a consensus protocol; the availability gap (partitions frozen on node
failure) is explicitly accepted and backstopped by an external
strongly-consistent KV rather than built-in replicas.
