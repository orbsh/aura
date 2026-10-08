# 0040 — Framework keyspace bands: 2x for the event plane, 3x for the meta plane (amends ADR-0026 §1)

> **Languages:** [English](0040-keyspace-bands.md) (primary) · [中文](0040-keyspace-bands.zh-CN.md)

**Status:** Accepted (2026-10-02) — LANDED (the `#[ok_ns]` renumbering rode
Phases 4.18/4.19, 2026-10-02, commit `2d1fafb`). User ruling: the bands are `2x`
and `3x`; the `4x` band is not used (2026-10-02).

**Landed shape (2026-10-02):** `crates/realm/src/mq.rs` declares ns 20–25
(EventName 20, PartitionName 21, MqData 22, MqCursor 23, MqHead 24, EventRoute
25) and `crates/realm/src/meta.rs` ns 30–32 (BoothName 30, BoothDef 31, CodeBlob
32); the event plane's own subscriber dictionary is gone (ADR-0038 §2) and takes
no number. The WIPE this implies is an OPERATIONAL act on an existing deployment
(old bytes under `30–35`/`40–42` must not be reinterpreted) — a fresh store has
nothing to wipe, and this repo's tests build fresh stores per run.

## Context

The framework low block grew historically: the event plane took `30–35` and the
meta plane `40–42`, the numbers recording the order in which tables arrived
(`EventName` first, `CodeBlob` last). Nothing in the numbers expressed which
plane a table belongs to, and the block had drifted into a third decade for no
reason.

The moment to fix it is this one: ADR-0039's partition switch re-issues partition
ids and ADR-0038 §2 re-issues the type id in every cursor row, so every
event-plane row is already invalid. A renumbering that rides the same window
costs no extra breakage.

## Decision

**RULING — the framework low block has two bands: ns `20–29` = the event plane,
ns `30–39` = the meta plane. Within a band, table order is CONCEPTUAL
(vocabulary → data → positions → registry), not historical.**

### The layout

| ns | table | plane |
|---|---|---|
| 20 | EventName — the event vocabulary (`by_name` text index) | event |
| 21 | PartitionName — the partition vocabulary (ADR-0039 §1; `by_name` + `HighWater`) | event |
| 22 | MqData — event data + the live `Count` reduce | event |
| 23 | MqCursor — subscription cursors (third segment = the ns 30 booth id) | event |
| 24 | MqHead — partition write heads (the sequence issuer) | event |
| 25 | EventRoute — the persisted subscription registry (`by_booth` index) | event |
| 30 | BoothName — the unique booth-TYPE dictionary (also allocates data ns `100+id`) | meta |
| 31 | BoothDef — one row per booth type | meta |
| 32 | CodeBlob — content-addressed code bytes | meta |

Reserved: `26–29` (the event plane's empty middle) and `33–99`. `BOOTH_NS_BASE =
100` is unchanged: booth types still allocate above the framework block.

### Consequences of the renumbering that must be stated, not discovered

- **The new meta band lands on numbers the OLD event plane used.** New `BoothName`
  rows at ns 30 would decode old `EventName` rows as type names — precisely the
  "reading old data as new data" failure the never-reuse rule exists to prevent.
  The renumber is therefore only safe under a COMPLETE wipe of the low block.
- **That wipe is bigger than "mq bytes are transient".** Persisted booth
  definitions and code blobs go with it: a deployment must re-register its types
  (the code blobs are re-derivable from source, the definitions are re-uploaded).
  Accepted — the engine is pre-production (Phase 4, single node) and ADR-0018 set
  the clean-break precedent — but it is recorded here because it is the real cost
  of the band change.
- **The alternative, for the record**: put the meta band ABOVE the old event band
  (e.g. `36–38`) so no new number coincides with an old one — then old definition
  rows could be read once and rewritten into the new numbers (migration stays
  possible). Not chosen: band tidiness over a migration path no deployment needs
  yet.
- **The bands are dense — no hole is carried over.** The retired subscriber
  dictionary (ADR-0038 §2, old ns 33) gets no slot: under the wipe its old number
  is dead and its new band position is fresh. The never-reuse rule is satisfied by
  the wipe, and after this layout a number is still never reassigned to a
  different table.
- **Old numbers `30–35` and `40–42` are dead**: they describe a layout that no
  longer exists. Code annotations, the event-flow §7 table and ADR-0026 §1 move to
  the new numbering; documents that recorded the old numbers at their own time
  (older ADRs, PLAN log entries, descriptive design pages) keep their landing-time
  wording.

## Consequences

- **Code**: the `#[ok_ns]` annotations in `crates/realm/src/mq.rs` (30–35 → 20–25)
  and `crates/realm/src/meta.rs` (40–42 → 30–32), plus the two new numbers above.
  Lands with Phase 4.18/4.19, not before — the wipe and the renumber are one
  operation.
- **Docs**: `event-flow.md`/`-en.md` §7 is the authoritative table (both
  languages); ADR-0026 §1 gets a dated update note; ADR-0038/0039 follow the new
  numbers where they name one.
- **`ns-layout.md`/`ns-layout-en.md`**: pointers, no change (they name no
  numbers).