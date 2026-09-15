# Data Partitioning (Internals)

> Overview lives in the wiki: [Aura architecture §5](https://github.com/orbsh/wiki/blob/main/aura-architecture.md).
> This document describes the full partitioning mechanism, from key byte
> layout to cluster topology. Bilingual: [中文](partitioning.md).

## 1. Partition unit: the Actor instance; the partition key decides placement

The smallest partitioning unit is neither a table nor a namespace — it is
the **Actor instance**. `InstanceId = (actor_type, key)`, where `key` is the
partition key (session_id, user_id, order_id, ...). Placement rules:

- **Same key, serial**: all messages for one partition key land in the same
  instance's mailbox, consumed one at a time by a single consumer — state
  consistency comes from mailbox serialization, not locks
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
  ├─ Instance ("cart", "alice")   ← concrete instance: own mailbox, own state
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

## 3. Key layout within a node: three binary segments

An instance's state on disk is a concatenation of fixed-width binary
segments (the okm key discipline — no textual separators):

```
[ns 2B BE][slot 1B][field encodings…][pkey]
```

- **ns (2 bytes)**: the okm-level table/edge-table namespace. The data and
  meta okm instances each allocate independently; they never collide (the
  two-instance model: normal data and metadata are two separate okm
  instances with independently selectable engines, fjall | slate; in
  single-node mode both run on fjall in different directories)
- **slot (1 byte)**: access-method discriminator within the table
  (0 = primary entry); all index entries of one table share its ns segment
- **Instance state fields**: each ctx_state field is one independent KV
  entry, the field name encoded at the tail of the key
  (`ctx_state_get/set/delete` are point reads/writes over this keyspace)

Cross-instance (Actor-instance, not okm-instance) isolation: user
namespaces wrap the shared engine in a `PrefixStore` that prepends
`[2B len][ns]` at the outermost byte — different users' keyspaces within
one physical engine are structurally separated, and cross-namespace access
is not expressible at the type level.

## 4. Serialization boundary: load on activation, flush on sleep

An instance in memory is a live object (a Rust handler or a script); **the
lifecycle of partition state is decoupled from instance residency**:

- **Activation (on_wake)**: all fields for that `(actor_type, key)` are
  bulk-read back from the StateStore, rebuilding the in-memory state
- **Sleep (on_sleep/evict)**: in-memory state is written back to the
  StateStore and the resident released — scale-to-zero drops the resident,
  not the data (locked by acceptance tests: script-actor state survives
  eviction)
- Phase 6.5's resident window optimizes this boundary: within the retention
  window, consecutive same-partition calls flow through in-memory oneshots
  with zero persistence; state is flushed and the resident released only at
  window end

## 5. Cluster layer: shard map and the routing invariant (Phase 5, not yet implemented)

- **The shard map lives in the meta instance** (slatedb) under a
  single-writer model: exactly one logical writer (the control plane)
  writes the shard map / actor registry; nodes read through caches — no
  multi-writer consensus; openraft returns only when a real second
  metadata writer appears
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
- Actor-definition hot reload rides the meta instance: write the new
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
Consistency comes from a single writer plus mailbox serialization, not
from a consensus protocol; the availability gap (partitions frozen on node
failure) is explicitly accepted and backstopped by an external
strongly-consistent KV rather than built-in replicas.
