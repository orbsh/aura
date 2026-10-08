# 0041 — The event plane's internal layout: the instance-key vocabulary, the issuer on the data table, one physical partition (amends ADR-0039 §1 and ADR-0040's table)

> **Languages:** [English](0041-event-plane-internal-layout.md) (primary) · [中文](0041-event-plane-internal-layout.zh-CN.md)

**Status:** Accepted (2026-10-08) — implementation pending (docs land with the
ADR; the code rename/retirement batch follows). User ruling (2026-10-08): the
slice segment is named after what its value is (the instance key), names that
carry no information (`part`) are out, the issuer does not need a table of its
own, and the layout prose stops narrating engine annotations.

## Context

The event plane's storage is written and read **by aura alone**. `mq.rs` is the
only entry point; its callers are the emit path (`events.rs`), the consumer loop
(`instance.rs`) and two relief-valve host fns (`ctx.rs`). A booth declares
receives and emits; the only MQ surface it reaches is those two fns, and both
resolve `bound_*` from the caller's OWN instance key — a booth cannot address
another instance's queue at all. The access surface is therefore static, closed,
and single-writer (the realm lock).

That fact was never drawn on, and three defects follow from it:

1. **Two different things were both called "partition".** The queue slice is
   segment 2 of the MQ keys (a dictionary proxy id); okm's physical KV partition
   is `#[ok_partition(N)]` (a `[0xFF][N]` key prefix = a compaction group). The
   derivation narrated the engine knob beside the event-plane vocabulary and
   attached an orthogonal reason to it ("按 event_id 分片" describes the key's
   prefix order, not the physical partition). A name that means two things in
   the same struct is a defect, not a shorthand.
2. **The issuer was justified against the wrong alternative.** `MqHead` was
   argued only as "O(1) instead of scanning the slice for max" — while the same
   page hangs a live `Count` reduce on the data table, and okm's `HighWater`
   preset is literally "read the accumulator, +1".
3. **No stated axis separated `MqCursor`'s physical partition from the default
   keyspace.** The recorded rationale for `#[ok_partition]` (PLAN, 2026-09-16)
   is append/watermark-delete vs point-write compaction profiles, which
   `MqData` alone already satisfies.

## Decision

**RULING — the event plane is documented as a framework CONTRACT with an
internal LAYOUT, and its storage vocabulary is the instance key.**

### 1. The slice segment is the instance key

- The segment's VALUE is the routing-resolved instance key: the subscriber side
  passes its own instance key (`bound_instance_key`), the emit side takes the
  payload field the route declares, and the two agree by construction. The id
  names that value.
- **Renames (one vocabulary, no homonyms):** `part_id` → `instance_key_id`;
  `PartitionName` (ns 21) → `InstanceKeyRegistry`; `mq::Partition { Singleton |
  Named }` → `mq::InstanceKey`; `SINGLETON_PART` → `SINGLETON_KEY_ID` (still
  `0`, still reserved, still unreachable by issuance, because the dictionary
  issues from 1); `part_id` / `partition_id_of` / `partition_name_of` /
  `resolve_partition_id` / `bound_partition` → `instance_key_id` /
  `instance_key_id_of` / `instance_key_of` / `resolve_instance_key_id` /
  `bound_instance_key`.
- **Why `Registry` here and not `…Name`.** The table is a registry of names for a
  key space (id ↔ name, one text index, one watermark); `InstanceKeyName` reads as
  "the name of an instance key", which is not what a row holds. The sibling
  dictionaries keep their names: `EventName`/`BoothName` are informative as they
  stand (the row's payload IS the name), they are cited across many landing-time
  records, and "registry" already denotes the persisted subscription table
  (`EventRoute` — the subscription registry) — pushing three dictionaries under
  that word would make one term mean two things, the defect this ADR exists to
  remove.
- **What the id is NOT.** It is not the composite instance identity — aura's
  `InstanceId { booth_type, key }` is; this segment is the key half. One id
  serves one instance PER subscriber type: the data key carries no booth
  segment (both types share one row) while the cursor key does, and emit
  dedupes by slice before appending — that is the design's fan-out dedup, so
  the segment names a shared slice, never one instance. The reserved `0` covers
  key-less delivery, where no key value exists at all (the singleton INSTANCE
  still has a name of its own, `__singleton__`, which is an instance-namespace
  name and not this id).
- Landed shape: ns 21 `InstanceKeyRegistry` — key `id u32` (4 B, issuance from 1);
  (`Registry` is the noun for this shape: the table IS a registry of names for a
  key space. The sibling dictionaries EventName/BoothName keep their names — see
  the note in §1.)
  value `name String` (the instance key) + `id u32` mirror + `global u32`
  (single group, constant 0) + `by_name` text index (name → id) + `HighWater(id)`
  reduce. The mirror stays until okm's key-field fold retires it (ADR-0024's
  landing is host-dependent — see §3).

### 2. Contract and layout are two layers of the document

- The document narrates the **contract**: emit/`@on`, the queue existing before
  its consumers, the closed consumer set, retention, no silent drops.
- The keyspace is **internal layout**: described once as a layout table, with
  `#[ok_ns]` / `#[ok_partition]` / `#[ok_index]` / `#[ok_reduce]` living there
  as implementation notes. A reader of the event plane should never need to know
  `#[ok_partition(1)]` exists; an implementer should never need to read the
  contract to learn the key widths. The annotations remain authoritative in code;
  the table is their index.

### 3. The issuer is the data table's watermark

- The constraint is unchanged: appending must not scan the slice for max, and
  there is exactly one issuer.
- **`MqHead` (ns 24) retires.** The head is a monotone watermark over `seq`:
  `#[ok_reduce(HighWater(seq) { group(event_id, instance_key_id) })]` on MqData.
  Append reads the accumulator (one point read of the reduce entry), computes
  `+1`, and the data row's own put folds it.
- **Why it is legitimate.** okm's reduce discipline is a ledger identity —
  `acc(group)` equals the fold over the rows the table currently holds — and
  `HighWater`/`LowWater` are its DECLARED exception (unfold is a no-op). An
  issuer's value must outlive its rows, and a view cannot; here the exception is
  the mechanism, not a workaround. Retention compaction deleting rows therefore
  never lowers the head.
- **Why the separate table was withdrawn.** `MqHead` made the issuer an
  explicit statement, defensible in general — but nothing here needs the issuer
  to exist independently of the data table: the access surface is static,
  single-writer and framework-internal, so there is no third-party compatibility
  surface to preserve. Keeping both would be two mechanisms for one value.
- **Not withdrawn:** the counter-vs-timestamp argument (the retirement of
  `max(now_ms, last+1)`). It holds unchanged and stays in the design doc's step
  for the issuer: this value is the sort key and therefore the row's identity, so
  it must be totally ordered and unique, and only read-the-counter-plus-one gives
  that. Uniqueness is still guaranteed by the realm lock (`self_arc.lock()`) —
  the lock is the guarantee, the counter is the bookkeeping.
- **Cost/benefit:** one fewer table and one fewer write per append; the head's
  fold rides the data row's put, so issuer and row land in one physical
  partition (the append's atomic domain). Host caveat, recorded so the choice is
  not silently dependent on it: aura reads the fold through the derive path,
  where okm resolves a key-field aggregate by the two-source rule (KEY WINS);
  okm-dynamic's preset path ignores the decoded key and would not resolve it.
  That asymmetry is okm's own open item (ADR-0024's host parity), not a
  constraint on this ruling.

### 4. Physical partitions: one table carries one

- `#[ok_partition]` is the engine's workload knob (a compaction group), not an
  event-plane concept, and the event plane needs exactly one: **MqData**, the
  only bulk-append + range-delete workload (the `Count` and `HighWater` folds
  ride it too).
- **`MqCursor` loses `#[ok_partition(2)]`.** It is a point table — the same class
  as booth state, which lives in the default keyspace — and the axis that would
  justify a group of its own was never stated. The cursor's relation to MqData is
  unaffected either way (that is fixed by MqData's own annotation); the change
  only decides whether the cursor shares the default tree.
- **ns 24 is vacated** and returns to the event plane's empty stretch: tables at
  20, 21, 22, 23, 25; reserved 24 and 26–29.

## Consequences

- **Code** (`crates/realm/src/mq.rs` and its callers `events.rs` / `instance.rs` /
  `ctx.rs`, plus tests): the renames of §1, the `HighWater` reduce declaration on
  MqData, the retirement of the `MqHead` struct/key/table and its ns, the
  removal of `#[ok_partition(2)]`, and the append/skip-to-head rewrite from
  "read the head row" to "read the reduce entry".
- **Docs**: `event-flow.md` / `-en.md` §1 (vocabulary), §2 (steps 2–5), §6.1 and
  §7 (the layout table and the annotation note) rewrite in the same pass, both
  languages. ADR-0039 §1 and ADR-0040's table keep their landing-time wording;
  they are amended by this ADR's note, not rewritten (ADR-0040's own rule for
  documents that recorded numbers at their own time).
- **Migration**: existing deployments clear the low block anyway (ADR-0040), and
  the cursor rows' bytes change (annotation removal) along with every renamed
  table — all absorbed by that wipe. A fresh store has nothing to migrate. The
  never-reuse rule is untouched: no number is reassigned to a different table.
- **Open (not design)**: the exact shape of the head read helper (`reduce_get`
  through the generated entry-key function) and whether `instance_key_of` keeps
  its ops-facing name.