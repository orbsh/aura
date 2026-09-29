# okm keyspace layout (ns / partition)

> **Languages:** [English](ns-layout-en.md) (primary) · [中文](ns-layout.md)

One page: which positions aura occupies in the shared okm instance's
keyspace. The authoritative definition lives in the code's `#[ok_ns]` /
`#[ok_partition]` annotations (`crates/realm/src/mq.rs`,
`crates/realm/src/meta.rs`); this page was verified against the code
(2026-09-29) — changing an annotation must update both language copies.
Rulings: ADR-0026 §1 (type-level real ns allocation), ADR-0027 (content-
addressed code in the meta plane), ADR-0023/0024 (preset reduces, key-field
groups).

## 1. Shape

- One realm's durable surface = one okm instance (the `MqStore` handle:
  production = a fjall keyspace, tests = the okm TestStore;
  `MqStore::fjall/mem`).
- The keyspace has three bands: the **framework fixed tables** (compile-
  time declared, low block), **booth-type namespaces** (runtime allocated,
  from 100), and the **empty middle** (36–39, 43–99 — reserved for future
  framework planes, currently unowned).
- Realm namespace isolation (the Phase 3.6 mechanism) prefixes the whole
  engine BENEATH the ns numbers — the same ns id under different realm
  prefixes is a different keyspace (ADR-0026 §1's orthogonality ruling).

## 2. The framework low block (compile-time fixed)

| ns | table | key | purpose | source |
|---|---|---|---|---|
| 30 | EventName | `id u32` | event-name dictionary (`by_name` text index; names are runtime data — events never take a real ns, the proxy-id shape for open vocabularies) | mq.rs |
| 31 | MqData | `[event_id u32][part_id u64][time u64]` | event data: one row per emit, N subscribers = N cursors; the sort key is the LOGICAL time (ms, per-partition monotonic via MqHead — not wall truth) | mq.rs |
| 32 | MqCursor | `[event_id u32][part_id u64][booth_id u32]` | subscription cursor: last consumed seq; monotonic advance, never rewinds | mq.rs |
| 33 | BoothName | `id u32` | subscriber-identity dictionary (`by_name` index, same shape as EventName) | mq.rs |
| 34 | MqHead | `[event_id u32][part_id u64]` | per-partition write head: append reads it, assigns `max(now_ms, last+1)`, writes back — O(1) append + cross-emitter monotonicity | mq.rs |
| 35 | EventRoute | `[event_id u32][booth_id u32]` | the PERSISTED subscription registry (`by_booth` index): routes survive restart (no re-introspection); wildcard subscriptions ride the same row shape | mq.rs |
| 40 | TypeName | `id u32` | type-name dictionary + the `HighWater(id)` preset (global group) — the watermark type_id allocation reads | meta.rs |
| 41 | BoothDef | `type_id u32` | booth definition, one row per type (name, language, `code_sha256` content address, idle TTL; the introspected schema rides the DYNAMIC segment as nTLV, not an opaque text blob) | meta.rs |
| 42 | CodeBlob | `sha256 [u8;32]` | content-addressed code bytes (ADR-0027): pure content rows, immutable by construction — "different bytes at the same key" is a hash collision, not a state | meta.rs |

Partition annotations: `MqData` = partition 1 (by event_id), `MqCursor` =
partition 2 (by event_id); `SINGLETON_PART = 0` is the reserved partition
id — key-less routes (wildcards and singleton subscriptions) land there.

## 3. Booth-type namespaces (runtime allocated)

- Base `BOOTH_NS_BASE = 100` (meta.rs). Registering a type takes
  `type_id = HighWater + 1` and allocates the real ns as `100 + id` — the
  ns rides the id monotonically, never reused, never reclaimed within the
  node's lifetime.
- Allocation is a side effect of `register_type` (the type registry
  already hands out ids; ADR-0026 §1).
- Inside a type's ns: the type declares collections through its
  interface_schema; `StorePlan::from_schema` compiles the declaration into
  the routing table — primary/dynamic slots plus declared indexes and
  declared reduces all encode as `ns + slot` (okm's dict_id/dict_name slots
  and the junction bases 4096/8192/12288 live in each collection's own
  schema constants — the same layout repeats per ns; the real ns is only
  the prefix).
- Instances are documents inside the type's ns: cross-instance aggregation
  within one type is an ordinary scan/reduce over the type's own ns (the
  projection booth survives only for cross-type precomputation).
- Cross-type isolation is structural: the ctx storage handle binds the
  owning type's ns at registration — reaching another type is not
  expressible (ADR-0026 §3).

## 4. Invariants

1. **The low block stays compile-time fixed forever**: framework tables
   add rows and fields, never ns numbers; a new framework plane takes its
   number from the empty middle (36–39, 43–99) and must land in this table
   (both language copies).
2. **Booth types never fall into the low block**: runtime allocation lives
   above `BOOTH_NS_BASE`, by construction.
3. **Event and partition names never take a real ns**: open vocabularies
   ride proxy ids + text indexes (the EventName/BoothName shape); only
   closed vocabularies (booth types) earn a real ns.
4. **Ns ids only grow**: deregistering a type does not reclaim its ns —
   reusing a keyspace prefix reads old data as new data; "ids and ns are
   never reused" is one ruling.
5. **Realm prefix and ns are orthogonal**: the prefix binds at `MqStore`
   construction, below the engine; the ns lives above it — the two
   addressing segments never substitute for each other.

## 5. Code anchors

- `crates/realm/src/mq.rs` — tables 30–35, `SINGLETON_PART`, MqStore/MqEngine
- `crates/realm/src/meta.rs` — tables 40–42, `BOOTH_NS_BASE`, `resolve_type_id` (the single source of the id+ns allocation)
- `crates/realm/src/store_exec.rs` — `StorePlan::from_schema` (declaration → routing table, slot encoding)
- ADR-0026 §1/§2 (type-level ns, document visibility), ADR-0027 (the CodeBlob's origin), ADR-0014 (the queue shape)
