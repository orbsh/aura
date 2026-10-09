# 0038 — The event plane's consumer set and identity (wildcard narrowing, one type dictionary, declaration semantics)

> **Languages:** [English](0038-event-plane-identity.md) (primary) · [中文](0038-event-plane-identity.zh-CN.md)

**Status:** Accepted (2026-10-02) — LANDED (Phase 4.17, 2026-10-02, commit `2d1fafb`;
pending). Rulings derived at the user's request by a first-principles pass over
`docs/design/event-flow.md` §8 with the previously sketched options (the A/B
forks) set aside; where a ruling lands on the same place as an earlier sketch, it
is because the derivation forces it, not because the sketch did.

**Landed shape (2026-10-02):** §1 the consumer loop binds a key-less route only
on the singleton instance (`instance.rs`), and zero broadcast loops can exist;
§2 mq's own subscriber dictionary is gone — routes and cursors key on the meta
plane's booth id (`mq::booth_id_of` → `meta::resolve_booth_id`), the cursor key's
third segment is `booth_id` (it was named `type_id` at ruling time; the field was
renamed at landing — ADR-0032's booth-naming sweep), and the resolver takes the TYPE name (no more
`split_once('/')`); §4 `DeadReason` rides every dead-ring record
(`NoRoute`/`MissingKeyField`/`AppendFailed`) and the `__default__` fallback is
retired. §3's per-event declaration and reference-by-name remain the row shape
ADR-0038 and Phase 4.13 land TOGETHER (today's `key_field` is already the
per-event shape; the resolve reference replaces its payload when the scan face
arrives) — nothing interim was built.

**Residual (recorded 2026-10-02; CLOSED by ADR-0042, 2026-10-08):** the INSTANCE
key space used a sentinel string (`InstanceId { key: "__singleton__" }`), so a
payload key with that literal text aliased the singleton INSTANCE (a different bug
from the partition aliasing §1/ADR-0039 §1 fixed). ADR-0042 makes instance identity
structural (`InstanceId.key` becomes an enum; the sentinel retires) and lands with
Phase 4.13.

## Context

`event-flow.md` §8 carried four questions: whether a key-less (wildcard)
subscription reaches one instance or every instance of the type; whether the
event plane keeps its own subscriber-identity dictionary (the old ns 33) beside the
type dictionary (ns 30 under ADR-0040's bands); what granularity a declaration has (per event vs
type default) and what a route row stores; and what happens when a matched route
produces no real target (`__default__` fallback cases).

The pass re-asked each question as "what constraint does this shape solve",
rather than comparing the sketched shapes.

## Decision

**RULING — a queue's consumer set must be CLOSED; a key-less subscription
delivers to the type's singleton instance; there is exactly ONE type dictionary
and the event plane borrows it; a declaration names its target resolution per
EVENT and its absence means singleton; and no emit may be dropped silently.**

### 1. A closed consumer set: wildcards deliver to the singleton instance

- A cursor row is a consumer's position, and the retention watermark's
  denominator is exactly the set of those rows — so the denominator IS the
  system's retention promise. A promise needs a finite, knowable, reclaimable
  set: closure is a precondition of the semantics, not an efficiency property.
- A keyed route: partition = the instance key, so a partition has exactly one
  consumer (the collision exception is ADR-0039 §1).
- A key-less route: partition = singleton. Broadcasting to every instance of
  the type would make the denominator "the type's currently live instances",
  which is an OPEN set — every emit for a new key activates a new instance, and a
  new instance's cursor reads 0, so it replays the entire surviving history of
  that queue. The watermark is then pinned by the newest (or slowest) member
  forever, and "subscription" has silently become "state synchronization".
- **Ruling:** a key-less subscription delivers to the type's singleton instance.
  Broadcast is retired.
- **Honest cost:** the singleton becomes a serialization point for that type's
  wildcard traffic. A type that needs per-key handling of pattern events should
  declare a keyed route (or resolve targets by scan, the Phase 4.13 end state) —
  broadcast was the wrong implementation of that need, not a wider form of it.
- **Note:** `partitioning.md` §1's text already says wildcard subscriptions bind
  the singleton; the implementation was wider than the document. The narrowing
  returns to the document, and the wildcard fan-out tests rewrite WITH the
  semantic.

### 2. One type dictionary: the event plane's own dictionary retires, the event plane borrows the type dictionary

- The requirement that produces a `type name → small integer` table is ns
  ALLOCATION (ADR-0026: every type needs a fixed-width integer data ns), not the
  event plane. After ADR-0025 the meta and mq tables live in the same okm
  instance, so one table serves both.
- The event plane's own dictionary's (old ns 33) only irreplaceable job was issuing
  ids for PARTICIPANT names (`"type/key"`, an open vocabulary) — the by-product of
  §1's broadcast. With broadcast retired, it has no remaining reason to exist.
- **Ruling:** the meta plane's unique booth-type dictionary (ns 30 under ADR-0040's
  bands; it was ns 40 when this was written, and the rename landed in dbc5d60
  before the renumbering landed in Phase 4.18) is the
  only dictionary. The event plane's own dictionary is deleted. The mq plane
  resolves type names through ns 30, and the cursor key's third segment becomes
  `booth_id` (fixed-width u32; named `type_id` when this was ruled).
- **Honest cost:** the `split_once('/')` symptom disappears for free, but
  `bound_partition` / `routes_of_*` / the compact path and the test call sites
  that drive the mq surface with `"cart/alice"` strings all change; and mq's
  resolution now depends on meta's (a module dependency inside one crate, not a
  new crate edge).

### 3. Declaration semantics: per event, absence = singleton, three shapes are three mechanisms

- `resolve` answers "which of MY instances does THIS event's payload point at" —
  a function of (type, event), not of type. Different events of one type effector
  different collections/indexes; a type-wide default is defined only when every
  event of the type happens to be isomorphic, which is a coincidence, not a rule.
- A default value must fix its semantics uniquely: no `resolve` and no
  `key_field` means singleton delivery. Adding "absence = inherit" gives one
  field two readings (inheritance vs singleton) — the declaration/execution
  drift class, not a style question.
- The three declaration shapes are three mechanisms, not three syntaxes:
  (1) no target information → singleton (a pure functional consumer);
  (2) the target rides the payload → zero-storage resolution, and this is the
  ONLY rule available to a type that declares no collections at all;
  (3) resolution by index scan → types with storage (Phase 4.13).
  Keeping (2) is not an optimization of (3); it is what makes stateless types
  expressible.
- A route row stores the collection/index/effector **names**; resolution lives with
  the schema owner. Storing slot/ns numbers would make position the identity, and
  any schema edit would silently re-point the row.
- **Consequence:** Phase 4.13's open question ("per-event naming vs type default
  + per-event override") is closed by this ruling.

### 4. Delivery-completeness invariant: no silent drops

- A matched route that yields no real target means the fact reached nobody. That
  must be observable, exactly like an event with no matching route at all.
- **Ruling:** (a) the `__default__` fallback retires — a payload missing the
  declared key field is a malformed event, recorded in the dead ring with a
  reason tag; (b) a scan that yields zero targets is the same class and records
  the same way; (c) the dead ring therefore collects three classes: no matching
  route, `mq::append` failure, and no real target.
- **Honest cost:** the fallback instance's historical behaviour disappears.
  No declaration ever named it, so its user count is zero by definition; what
  was really lost is the silence, which is the point.

## Consequences

- **`docs/design/event-flow.md` / `-en.md`**: §1's invariants gain the
  delivery-completeness clause; §6.1's footnotes record the retired fallback and
  the singleton reading; §7's table marks the event plane's own dictionary as
  deleted and the cursor key as type-keyed; §8's settled entries become this
  ADR's text.
- **`partitioning.md` §1**: the wildcard sentence is the ruling now, not a
  narrower document than the code.
- **Phases**: 4.17 (implementation of §1–§4) — the wildcard semantic + test
  rewrite, the retired dictionary + the resolver chain, the row-shape/declaration work,
  and the dead-ring reason tags.
- **Superseded sketches:** the earlier A/B forks in `event-flow.md` §8.1/§8.3
  are closed — A's destination with a different derivation, B rejected for the
  structural reason (an open consumer set), not for convenience.