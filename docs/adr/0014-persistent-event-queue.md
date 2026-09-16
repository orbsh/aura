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
