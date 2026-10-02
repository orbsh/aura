# 0039 — Partition encoding and the cursor retention promise

> **Languages:** [English](0039-partition-encoding-and-retention.md) (primary) · [中文](0039-partition-encoding-and-retention.zh-CN.md)

**Status:** Accepted (2026-10-02) — LANDED (Phases 4.18/4.19, 2026-10-02; commit
pending). Derived at the user's request by a first-principles pass; the
cursor-expiry shape is the user's ruling (2026-10-02: keep the two lifetimes
decoupled, add a global configuration).

**Landed shape (2026-10-02):** §1 `PartitionName` (ns 21) issues the ids
(`by_name` + `HighWater`, reverse lookup `partition_name_of`); `mq::Partition` is
the structural slice (`Singleton` | `Named`), so the singleton never enters the
value space; every queue call takes a `&Partition`; `part_hash`/FNV-1a/
`part_id_of`/`part_hash_of` are gone and the key widths tightened (MqData 20→16 B,
MqCursor 16→12 B, MqHead 12→8 B). §2 `cursor_ttl` is one `EngineConfig` field
(KDL `mq { cursor_ttl "30d" }`, default 30 days) carried into every realm;
`MqCursor.last_active_ms` (v2 hot tail) is stamped by `advance`; the compaction
predicate skips an expired row from the denominator and the inert rows (cursor at
or below the watermark) are reclaimed by `drop_cursor`.

**One reading beyond the ruling text (recorded):** the denominator's test became
`mq::booth_subscribes` — "does this type register a route MATCHING this concrete
event (exact or wildcard)". The previous exact-id lookup silently dropped
WILDCARD subscribers out of the denominator (their registry row carries the
pattern's event id), so compaction could eat a wildcard consumer's unconsumed
backlog — a no-silent-drops violation (ADR-0038 §4) found while implementing §2.

**Test-support additions:** `age_cursor` (backdate a row's stamp) — `rewind_cursor`
now stamps NOW, so it pins without expiring, and the TTL path is exercised by
combining the two.

## Context

The partition id is a key FIELD hash (FNV-1a → u64) with `0` reserved for the
singleton partition, and `MqCursor` carries only `cursor: u64`. Both were
recorded in `event-flow.md` §7, neither as the subject of a decision.

Two questions were never asked: why the partition has to be a hash at all, and
what the system promises about a cursor's backlog when its instance is gone.

## Decision

**RULING — the partition becomes a proxied vocabulary (a dictionary-issued id —
one layout with the event and type dictionaries); the hash retires. And a cursor
row expires by a global `cursor_ttl` configuration: expiry removes it from the
watermark denominator, never deletes the row.**

### 1. Partition identity: a proxied vocabulary, not a hash

- What forced a hash was POSITION, not the ADR-0002 compliance argument the
  document gave: the partition is a MIDDLE segment of
  `MqData = [event][part][time]` (and of the cursor key), with segments after it.
  But okm primary keys are fixed-width by CONSTRUCTION — `KeyEncode` panics at
  compile time on a `String` field ("the key encoding is fixed-width"),
  `okm-derive/src/key_encode.rs`) — and a variable-length field exists only in an
  INDEX field segment, at most once, immediately before the primary-key prefix,
  and explicitly never length-prefixed (okm MODELING.md: a length prefix would
  sort by length before bytes and destroy dictionary order). The same fixed-width
  rule governs the `Count` reduce's group fields. So an inline partition string
  inside a primary key is not expressible at all: the "self-describing segment"
  shape is not a choice okm leaves open.
- The document's own rule already answers this: an open vocabulary gets a PROXY ID
  plus a text index (§7: "vocabulary tables never take a real ns"). Partition
  values come from payload fields — an open vocabulary. The hash was the outlier.
- **Ruling:** the partition becomes a proxied vocabulary exactly like EventName and
  BoothName — a `PartitionName` dictionary (ns 21, ADR-0040's bands) issuing a
  fixed-width `u32` id, with a `by_name` text index and a `HighWater` watermark,
  and the reverse direction available (id → name) so ops can render queues in
  human terms. `part_hash` and FNV-1a retire. `SINGLETON_PART = 0` stays as the
  reserved id, now structurally unreachable (issuance starts at 1), and the
  sentinel LEAVES the value space: the routing layer marks key-less delivery with
  a structural marker instead of comparing a magic string.
- Why this shape rather than the hash: it turns all three defects into
  construction failures — a collision cannot happen (ids are issued, not
  computed); the reserved value no longer shares a namespace with the value space
  (a keyed instance whose key literally equals the sentinel can no longer alias
  into the singleton queue); and the mapping is invertible.
- Key widths: the id is `u32`, one layout with the other two dictionaries;
  `MqData` becomes 16 B, `MqCursor` 12 B, `MqHead` 8 B of logical key (today
  20/16/12). Those keys are invalidated by the id re-issuance anyway, so the
  shrink is free.
- **Honest cost:** one registry resolve per append and per cursor/backlog op — the
  same cost class as the `resolve_event_id` append already pays, and it gets the
  same treatment routes get: the persisted registry is the truth, an in-memory map
  is the hot face (the §4 pattern). The dictionary grows one row per distinct
  partition value ever seen, with no expiry (the same unbounded class as cursor
  rows; GC is not ruled).
- **Withdrawn:** this ADR's earlier length-prefixed-segment plan was infeasible in
  okm and is replaced by this ruling.

### 2. The retention promise: cursor TTL, decoupled from idle_ttl

- "An instance may die" needs a BOUNDED promise. Today an evicted instance's
  cursor row stays, and the interpretation is "it replays on re-activation" —
  but nothing defines how long that promise holds, while the promise is exactly
  what decides how long that partition's `MqData` may be kept.
- The two lifetimes are not the same lifetime: `idle_ttl` is measured in
  seconds (eviction of a warm instance), a cursor's forfeit horizon in days.
  Binding them would let a second-scale event decide a day-scale data promise.
- **Ruling:** a global `cursor_ttl` configuration (one `EngineConfig` value,
  seeded into a `Realm` field beside `idle_ttl`/`code_base_url`; KDL duration
  string like `"7d"`). Default = FINITE (30d): "never" as a default would make
  unbounded growth the default. No per-type override — the denominator is a
  `min` comparison over rows that may come from different types, so a per-type
  horizon would mix different promises into one comparison.
- **Mechanism (expiry is NOT row deletion):** expiry means the row leaves the
  watermark denominator, which lets compaction pass it — the backlog is
  forfeit, and that IS the promise's implementation. Deleting the row instead
  would read the cursor back as 0, and `backlog(after = 0)` replays surviving
  rows: an instance whose cursor was AHEAD of the watermark would re-consume
  rows it had already consumed — duplicate delivery. Physical deletion is
  reserved for rows whose cursor is already BELOW the current watermark (their
  backlog no longer exists, so nothing can replay), which also gives cursor rows
  a convergent cleanup rule — otherwise every key ever seen leaves a row.
- **Row:** `MqCursor` gains `last_active_ms: u64` (hot-tail append,
  `#[ok_layout(version = 2)]`, the `BoothDef.encoding` precedent), meaning the
  last `advance` (skip-to-now counts as activity). Sentinel rule: `0` never
  participates in the expiry predicate — old rows decode the missing field as 0,
  and without this rule the first run of the new code would expire everything
  instantly and void every backlog.
- **Evaluation point:** a predicate evaluated at COMPACTION time (the emit path
  already enumerates that partition's cursor rows while computing the
  watermark), not a background watchdog — retention decisions are only due when
  new rows arrive. The timer wheel is needed only for optional reclamation of
  inert rows.
- **Contract:** a cold consumer that does not advance within `cursor_ttl`
  forfeits its backlog. On its return it resumes from its own cursor over the
  surviving rows; whatever was compacted away is permanently gone. What is lost
  is backlog — never state: state lives in the type's collections, the queue
  holds no state persistence duty.
- **No config validation** on `cursor_ttl` vs `idle_ttl` (the direction
  "`cursor_ttl` must dominate `idle_ttl`" is documentation): `idle_ttl` has
  per-type overrides, so a global assertion would misfire.

## Consequences

- **`event-flow.md` / `-en.md`**: §1 invariant 3 gains the forfeit clause; §6.3
  gains the expiry predicate; §7's table gains the `PartitionName` row and the
  narrowed key widths, `MqCursor.last_active_ms` joins the cursor row, and the
  reserved `0` is now structurally unreachable.
- **`crates/config` + realm**: the `cursor_ttl` key and field.
- **Phases**: 4.18 (partition encoding), 4.19 (cursor TTL + the denominator
  change + inert-row reclamation).
- **Open at implementation time (not design):** clean-break vs migration for
  existing mq bytes; the exact inert-row reclamation trigger.