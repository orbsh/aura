# 0014 — Persistent event queue: okm partitions, cursor consumption, min-watermark retention

> **Languages:** [English](0014-persistent-event-queue.md) (primary) · [中文](0014-persistent-event-queue.zh-CN.md)

**Status:** Accepted (2026-09-16)

## Context

Phase 4.5c replaced per-actor mailboxes with per-(event, partition) queues — one event family
per queue, multiple subscribers per queue (one-to-many delivery is structural). The interim
implementation used `tokio::sync::broadcast`, which has three properties inconsistent with the
architecture's own rulings:

- **Late subscribers get nothing**: an instance evicted (scale-to-zero) during an event's
  lifetime never sees it. With ctx_state as the durable truth, missing triggers are silent.
- **Slow subscribers get `Lagged`**: falling behind the broadcast ring discards a whole
  segment silently — the "failure explicitness" preference inverted.
- **"No queue component" was being read as "no persistence"**: the MQ-decomposition ruling
  bans *external heavyweight* queue systems, not embedded persistence of event data. Events
  are the passive-save half of the dual-track design (ctx.state = active save) — passively
  persisted on emit, same engine.

## Decision

### 1. Queues are okm persistent partitions

```
[mq-data][event][part_id][time]        ← event payload, written on emit (passive save)
[mq-cursor][event][part_id][actor]{u64 cursor}
```

- Emit = append to `[mq-data]` (durable, same engine as actor state).
- Consumption = range scan from the subscriber's cursor, then cursor advance.
- Subscribe = register a cursor at *now* (a new subscriber does not replay history).
- Serial-per-instance semantics are preserved by the per-subscription cursor, not by owning
  the queue.

### 2. Retention = min-watermark over ACTIVE subscribers

A `[ev][part_id]` queue keeps only the range that every active subscriber's cursor still
needs; data before the minimum cursor is deleted on write-path compaction.

**The watermark's denominator comes from the route registry** (the 4.5b-persisted `@on`
metadata), never from the raw cursor keys. A permanently-departed actor's stale cursor must
not pin the watermark forever. Deregistering an actor removes its cursor row; its backlog
then falls below the watermark and vanishes with ordinary compaction — no separate reaper.

### 3. Backlog depth is an okm reduce count

Live backlog per `[ev][part_id]` = `#[kv_reduce]` count over the mq-data prefix (insert +1,
watermark-compaction -1 unfold). Zero-scan operational surface; the skip-to-now decision
reads it directly.

### 4. Skip-to-now is the relief valve

A subscriber facing excessive backlog may jump its cursor to the newest message (by message
time), discarding the stale range and resuming at now. Backward-compatible: consumes the
reduce count to decide, costs one cursor write.

## Honest semantic cost

The queue is a **buffer, not a store**. At-least-once holds only while every subscriber
stays registered and consuming; a subscriber permanently deregistered with unconsumed backlog
loses it. This is the deliberate layering: **ctx_state is the durable truth; the queue
guarantees "alive = can catch up", nothing more.** Semantics requiring delivery beyond
subscriber lifetime belong on `ctx.invoke()` (CallSlot cold path) or durable job records, not
on the event bus.

## Consequences

- Broadcast channel demoted to interim; the realm event path is rewritten in 4.5c step 2b
  (queue read = scan + cursor advance; subscribe = register cursor at now).
- Scale-to-zero no longer drops triggers: backlog accumulated during eviction is delivered
  on re-activation.
- Slow consumers accumulate visible, metered backlog (reduce count) instead of silent
  `Lagged` loss; skip-to-now makes "falling behind" a choosable policy.
- Multi-subscriber fan-out stores each event once per queue (N actors = N cursors into one
  partition) — the N-copy duplication of per-actor mailboxes disappears structurally.
- "No queue component" clarified: no *external heavyweight* queue system; embedded
  persistent partitions are the natural form of passive event persistence.

### 5. Storage substrate: LSM-tree, not a dedicated append-only store (2026-09-16)

The queue's load profile — write-heavy appends, range scans, prefix deletes ordered by the
watermark — is squarely in LSM territory:

- Appends touch only the memtable; compaction digests them in the background. B-tree
  engines (BoltDB/redb) pay per-write page addressing for exactly this pattern.
- Watermark deletion is **prefix deletion, not random deletion**: the deleted range is
  always the oldest contiguous segment of `[mq-data][ev][part]` (the watermark only moves
  forward). High stale-ratio SSTables drop wholesale during compaction — the friendliest
  possible case, no tombstone storm.
- Sharing the engine with actor state buys crash recovery and a unified ops surface. Note
  the atomic domain of a batch is one partition — event + state co-atomicity is not a design
  goal here (events are triggers, ctx_state is the truth; see the honest-cost section).

**LSM property to design around**: deletion is not immediate. Watermark compaction writes
tombstones; physical space is reclaimed only after compaction runs. The reduce count
(logical) reflects deletion instantly; physical disk usage lags. Not a new problem (state
deletes behave the same), but the ops semantics must say so: `mq-data` physical size leads
the watermark.

**Key-ordering rule**: `[time]` as the key suffix — never a monotonic sequence number.
Time-ordered keys make both skip-to-now and watermark deletion pure prefix semantics; a
sequence suffix would break time-range deletion.

**When a dedicated append-only store WOULD be warranted** (neither signal exists today):
per-partition throughput hitting disk sequential-write limits (~millions of events/s/partition),
or a semantic change to "immutable, replayable, machine-shared log" with consumer offsets
independent of data lifetime. Aura's events are node-private, watermark-dying buffers —
semantically a subscriber-bounded queue, not a log; a bespoke WAL adds a lifecycle system
with zero payoff.

**Partitioning note**: partition membership is declared **per type** — `#[kv_partition(N)]`
on the row generates `PARTITION_ID: Option<u8>`; `Some(N)` prepends a 2-byte **escape
segment `[0xFF][N]`** before the ns header, `None` (the default) adds **no segment at all**
(zero key-encoding cost for the 99% of tables without partition needs). The 0xFF first byte
is a reserved escape: legal ns headers (big-endian u16, first byte constrained to 0x00-0xFE
by the ns dictionary) never start with it, so partitioned and unpartitioned keys are
**structurally disjoint** — no numbering discipline needed between the two dictionaries
(a bare 1-byte `[N]` segment was rejected precisely because a plain table's `ns_hi` could
collide with it). Fjall routes the type's handle to its own partition (lazy, cached, names
hashed from the id) — compaction patterns never pollute each other. The **atomic domain of
`commit_batch` is a single partition**: cross-partition writes carry no atomicity guarantee,
by ruling rather than by engine limitation — partition semantics IS workload isolation, and
data needing atomic co-write belongs in one partition. Engines without partition semantics
ignore the physical split; the key encoding (escape segment included) is identical
everywhere, and the okm ns abstraction stays unaware of partition handles (kv_ns remains a
key-prefix contract; partition is a separate, independent dimension).
