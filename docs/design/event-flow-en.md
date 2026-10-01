# Event Flow (the emit / on mechanics)

> **Languages:** [English](event-flow-en.md) (primary) · [中文](event-flow.md)

The single integrated document for the event plane: declaration → registration →
matching → delivery → persistent queues → cursor consumption → retention and
compaction → observation surface, with the keyspace layout folded in (ns-layout
merges into this document; the old `ns-layout.md`/`ns-layout-en.md` become
pointers) and the open-questions list (the Phase 4.13 backlog). Rulings it
records: ADR-0007/0012 (the receiver set is a runtime fact), ADR-0026 (type-level
storage plane), ADR-0002 (events never occupy a real ns),
`docs/design/partitioning.md` §1 (the routing end state).

## 1. Vocabulary and invariants

The event plane has three orthogonal words; mixing them is the historical error
magnet:

- **Event**: a fact that happened — an open vocabulary. Events never occupy a
  real ns; they get a proxy id from the EventName dictionary.
- **Partition**: the queue's slice identity, derived from the instance key the
  route resolves. String → FNV-1a fixed-width u64 (a key FIELD hash, not an ns
  dictionary — ADR-0002's rejection targets ns, not a delivery fan-out key);
  `0` is reserved for the singleton partition (a hash collision maps to 1 — the
  reservation is structural, not a coincidence).
- **Booth**: who consumes. Type names ("cart") and participant names
  ("cart/alice") share one dictionary today — the root of an open question
  (§6, §8.3).

Invariants:

1. **An emit carries no receiver address.** The caller states the fact
   `emit(event, data)`; the receiver set is a runtime fact — unknowable at
   emit time and it should stay that way (ADR-0012: no emits whitelist, the
   dead ring is the observation surface).
2. **Subscribing is self-registration.** The subject of one `on` line IS the
   BoothType the declaration lives on; the authoring surface never writes a
   type name (`ReceiveDecl{event, key_field, wildcard}` has no type field;
   the framework fills the subject in `router.on(event, booth.name, decl)`).
   Declaring a subscription "for cart" from the order booth is inexpressible
   on the declaration surface — registering under a name IS the hot-swap
   semantics (the latest version wins).
3. **The queue outlives the consumer.** MqData keys carry no booth identity
   ("an event belongs to no booth"); instances may die (scale-to-zero),
   queues do not — backlog replay rides the cursor, not a live process.
4. **Subscriptions are type-level; consumption positions are instance-level.**
   The EventRoute row's subject is a type; the MqCursor row's subject is a
   participant (the current shape — the pending narrowing is §8.3).

## 2. The declaration surface (what authors write)

| Surface | Shape | Subject binding |
|---|---|---|
| Rust booth | `BoothType::rust(...).on("order.created", key_field)` builder | the type object the declaration hangs on |
| python | `@on("order.created", key="user_id")` collector | the module the declaration lives in |
| steel | `(on "order.created" "user_id" handler)` collector | same |
| bgi/exec/wasm | hand-written `interface_schema` frame (the `receives` block) | same (no collector — the declaration IS data) |

Script declarations materialize into `interface_schema.receives` at UPLOAD time
via `carrier::introspect`; the host never branches per language. A declaration
answers exactly two questions: **which event** (name / wildcard pattern) and
**how to locate the instance** (today: `key_field` — which payload field is the
instance key; the 4.13 end state: the access-method resolve reference, §8).

Names in subscription syntax are open vocabulary; **the declaration living
inside a type is the identity** — the event name itself carries no subject.

## 3. Registration (once, register_type)

`crates/realm/src/registry.rs`:

1. Hot-swap semantics first: `router.drop_booth(name)` (memory) +
   `mq::routes_drop_booth` (the persisted table, via the `by_booth` index scan)
   — same-name re-registration means the latest version owns, no double delivery.
2. Per `receives` declaration, fill both tables: the in-memory
   `router.on(event, booth.name, key_field)` + the persisted
   `mq::route_put(event, booth.name, key_field, wildcard)` — the row's subject
   (booth_id) is resolved by the FRAMEWORK, never written by the author.
3. Reclaim every resident instance and session of the type (the previous source
   stops answering; the next message cold-starts on the new code — the instance
   map is a discardable hot cache: state lives in the type's collections, the
   backlog in the queues, replay rides the cursor).
4. Side-effect chain of registration: `meta::ns_and_schema_of` resolves the
   type's ns (the unique type dictionary issues the id and allocates
   `BOOTH_NS_BASE + id`) + takes the persisted uploaded schema copy →
   `StorePlan::from_schema` compiles the storage routing table (collections /
   indexes / reduces, slot encodings).

## 4. Matching (every emit, the hot path)

`crates/realm/src/events.rs::emit`:

- Matching runs on the in-memory `EventRouter`:
  `exact: HashMap<name, Vec<Route>>` + `wildcard: Vec<(prefix, Route)>` (linear
  scan — the wildcard count is small by construction; a Trie is
  over-engineering).
- The match is a SET, not a single value: one event name may hit exact routes
  AND wildcard routes; each delivers independently.
- **No match = the dead ring** (`realm.dead_events.push` — bounded, observable;
  the ADR-0012 audit surface). Direction matters: the dead ring sees events
  with NO matching route; a matched route whose instance is not live is not a
  loss — it is a backlog write (§5).
- The persisted source of truth is the EventRoute registry (ns 35): routes
  survive restart without re-introspecting scripts; the in-memory router is its
  hot face, rebuilt on boot reload through the SAME registration code. Registry
  rows are the subscription facts AND the compaction watermark denominator.

## 5. Delivery and partition resolution (the current shape)

Per matched route:

```
partition =  if key_field empty → "__singleton__"
             else payload[key_field].as_str() → unwrap_or("__default__")   // exactly one
target   = InstanceId{booth_type, the partition value IS the instance key}
activation → not in the instance table? instance() first (same pass, before any send)
enqueue    → mq::append(event_id, part_id, payload)   deduped by (event, partition)
```

Three honest footnotes:

1. **`__default__` is a silent fallback**: a missing payload field does not
   error — it lands in the fallback instance. A typo'd field name is invisible
   at the document layer. This is one motivation for the 4.13 switch to
   access-method scans (typing catches mistakes at the boundary).
2. **One route yields exactly one target** (single-key routing). Type-level
   fan-out always existed (several types on one event = several EventRoute
   rows); what cannot be expressed is fanning out WITHIN a type by business
   fact (region.escalation → every western store) — a single field cannot,
   a scan naturally can (§8).
3. **Delivery/consumption partition agreement is by construction**: the
   consumer's `bound_partition` re-derives the route's key_field semantics
   from "this instance's key" — instance name = partition name = key-field
   value is the trinity today (a contract, not a mechanism).

## 6. Persistent queues, cursors, consumption

The six MQ tables live in ONE okm instance (`MqStore`: production = fjall
keyspace, tests = TestStore); the realm prefix sits under ns — the same ns
number means different things under different realm prefixes. Full layout: §7.

- **MqData**: `[event_id][part_id][time]` → payload. The sort key is LOGICAL
  time (ms, monotonic per partition via MqHead — `max(now_ms, last+1)`: O(1)
  append + cross-emitter monotonicity; wall truth rides payload fields and
  never sorts).
- **MqCursor**: `[event_id][part_id][booth_id]` → last consumed seq.
  **Monotonic, never rewinds** (this is what makes skip-to-now durable: a
  skipped backlog must not re-surface next drain; `rewind_cursor` is
  test-support only).
- **The mixed vocabulary in the booth_id segment**: one resolver function
  serves both type names (EventRoute registration, `routes_of_booth` — the
  subject is a TYPE) and participant names `"type/key"` (cursor semantics —
  the subject is an INSTANCE), issuing ids from an open vocabulary. The
  compact path's `split_once('/')` — hand-extracting the type from a
  participant name to verify registration — is the downstream symptom
  (pending: §8.3).
- **The consumption loop** (`instance.rs`): activation binds the subscription
  set (wildcards re-expand against registered event names every pass — new
  concrete names join automatically); each cycle sweeps ALL bound queues:
  backlog batch → run_job serially → advance cursor. Same-instance serial,
  cross-instance parallel lives HERE, not in locks.
- **Wildcard semantics (as implemented)**: a wildcard binds every instance to
  the singleton partition with a per-participant cursor each = a BROADCAST to
  every instance of the type (including the singleton). Note that
  `partitioning.md` §1's text says wildcard subscriptions "bind the singleton
  `__singleton__`" — the implementation is WIDER than the document (broadcast
  to all instances). This divergence is exactly the A/B fork of the cursor-key
  ruling (§8.3): narrow to the document vs enshrine the implementation.

## 7. Keyspace layout (ns-layout merged)

One realm's persistent plane = one okm instance; the keyspace has three bands:
**framework fixed tables** (compile-time, the low block), **booth type nss**
(runtime-allocated, from 100), **the empty middle** (36–39, 43–99 — reserved).

The framework low block:

| ns | table | key | purpose | code |
|---|---|---|---|---|
| 30 | EventName | `id u32` | event-name dictionary (`by_name` text index; open vocabulary takes proxy ids, never a real ns) | mq.rs |
| 31 | MqData | `[event_id u32][part_id u64][time u64]` | event data: one row per emit, N subscribers = N cursors; sort key = logical time | mq.rs |
| 32 | MqCursor | `[event_id u32][part_id u64][booth_id u32]` | subscription cursor: last consumed seq, monotonic | mq.rs |
| 33 | ~~BoothName~~ | `id u32` | **under consolidation**: subscriber identity dictionary (types and participants share it) — deleted after §8.3 rules; the number is never reused | mq.rs |
| 34 | MqHead | `[event_id u32][part_id u64]` | partition write head: O(1) append + cross-emitter monotonic | mq.rs |
| 35 | EventRoute | `[event_id u32][booth_id u32]` | persisted subscription registry (`by_booth` index); wildcards ride the same row shape storing the PATTERN | mq.rs |
| 40 | **BoothName** (was TypeName; rename landed) | `id u32` | the unique booth-TYPE dictionary: `by_name` index + `HighWater(id)` preset + data-ns allocation (`100+id`); resolves BOTH ways (name→id `resolve_booth_id`, id→name `booth_name_of`) | meta.rs |
| 41 | BoothDef | `type_id u32` | booth definition row per type: name/language/encoding/idle_ttl + the `code_sha256` pointer; the introspected schema rides the dynamic segment (structured nTLV, never opaque text) | meta.rs |
| 42 | CodeBlob | `sha256 [u8;32]` | content-addressed code bytes (ADR-0027): pure content rows, immutable by construction — same key different bytes is a hash collision, not a state | meta.rs |

Partition annotations: MqData = partition 1 (by event_id), MqCursor =
partition 2; `SINGLETON_PART = 0`.

Booth type nss (runtime): registering a type issues the id from the unique
dictionary and allocates data ns = `100 + id` — monotonic with the id, never
reused within the node. Inside a type ns: the interface_schema-declared
collections + their access methods (slots encode as `ns + slot`; the
dict/junction bases live in each collection's schema constants — the real ns
is only a prefix). Instances are documents inside the type ns. Type isolation
is structural: ctx storage handles bind the owning type's ns at registration
(ADR-0026 §3).

Invariants: the low block is compile-time fixed — new framework tables take
numbers from the empty middle and MUST join this table (both languages); booth
types never fall into the low block; `#[ok_ns]`/`#[ok_partition]` annotation
changes must keep this page in sync — the authoritative definitions live in
code.

**Vocabulary tables never take a real ns**: open vocabularies (event names,
participant names) ride proxy ids + text indexes; only closed vocabularies
(booth types) earn a real ns — the EventName/BoothName shape follows from this.
Nss only grow: deregistering a type reclaims nothing — reusing a keyspace
prefix reads old data as new data, and "ids and nss are never reused" is one
ruling.

Consolidation status of the two dictionaries: §8.3 (the ns 40 rename + reverse
resolution landed; deleting ns 33 awaits the cursor-key ruling).

## 8. Open questions (the 4.13 backlog)

### 8.1 The routing end state: instance key via access-method scans

The target (partitioning.md §1): replace `key_field` payload extraction
(exactly one value → one instance) with an ACCESS-METHOD SCAN over the type's
ns (naturally one-to-many). The mechanism, three segments (authors write only
the first):

```
declare (in receives):        resolve = {collection, index, probe_field}
register (framework, once):   EventRoute rows carry the reference as NAME strings
                              (addressed by name — never a cross-plane id; rows
                              store facts, resolution lives with the schema owner)
evaluate (framework, per emit): probe = payload[probe_field]
                              → StorePlan.entries →(okm-entry::collection_from_entry)
                                DynamicCollection over the type ns (the 4.16 face,
                                already landed — the scan path is free)
                              → scan the index prefix: each hit's ROW PROXY KEY =
                                a target instance key (fan-out, b-default)
                              → per-target activation + enqueue deduped by
                                (event, partition) (already exists)
```

Ruled this round:

- **Subject binding unchanged**: the resolve reference lives only in the type's
  own `receives`; the framework fills the row subject — the author surface
  still never names a type.
- **Target = the hit row's proxy key (b as the DEFAULT AND ONLY shape)**: no
  `target` field in the declaration — "fan out to the rows themselves" is the
  whole semantics. The payload-field shape (a) is excluded (it adds a second
  misspellable reference and, when the field is indexed, bypasses your own
  index); the index-tail shape (c) would change okm's scan return shape and
  is premature (Windmill gate). Instance identity is never declared — the scan
  produces it; booth code reads its own via `ctx.self_id.key`.
- **The old single-field shape survives as a second row shape** (discriminated
  at the row-structure layer, no sentinel mixing): for keyed cases the two are
  equivalent and key_field is cheaper (zero scan); migration is per type.
- **Multi-target failure semantics**: per-target dead-ring/record, consistent
  with ADR-0012 (appends are already per-partition independent — failure
  granularity comes for free).
- **MQ formats unchanged**: `append(event, partition)` rows and per-(event,
  partition) cursors already fit one-to-many delivery; only the route side
  must produce a SET of partitions.

Still open (next decision): ① declaration granularity — per-event (A,
recommended) vs type-default-with-override (B; rejected for now: a default scan
target has no physical counterpart for multi-collection types, and "empty =
inherit" would collide with "empty = singleton" in one field — the
declaration/execution drift class ADR-0007 records). **B's rejection awaits
the user's final confirmation.**

### 8.2 The EventRoute row shape

The `key_field: String` payload becomes the reference payload. Open: whether
references carry the collection's ns NUMBER — no: collection/index names
resolve inside the type's own plan; a stored ns would recreate the
cross-plane-id trap the double dictionary just exposed.

### 8.3 Double-dictionary consolidation + cursor-key orthogonality (half-landed, pending)

Landed (uncommitted): meta.rs `TypeName`→`BoothName` full-chain rename + the
public `booth_name_of` reverse resolver — one dictionary, both directions.

The blocker: the cursor key's third segment resolves PARTICIPANT names
("type/key", an open vocabulary), not type names. The orthogonal shape
(booth_id = type_id; instance identity already carried by the part segment —
`"type/key"` leaves the dictionary, `split_once('/')` disappears) collides
with an implemented semantic: **wildcard queues fan out per participant**
(each instance holds its own cursor on the singleton partition = broadcast to
every instance). Under the orthogonal key two instances share one cursor row
and one message is consumed by exactly one of them — a semantic regression.

Two directions for the ruling:

- **A (recommended — the document is the truth source)**: narrow wildcards to
  "deliver to the type's singleton instance" (the partitioning.md §1 reading);
  delete ns 33; the wildcard fan-out tests rewrite WITH the semantic.
- **B (enshrine the implementation)**: keep participant-level cursors —
  instance subscription identity does need an id; ns 33 survives renamed/
  namespaced, the `split_once` awkwardness stays.

Consequential work either way: the never-reuse ns-number ops fact; the
`bound_partition`/`routes_of_*`/compact resolver chain; test call sites
(`mq_okm.rs`/`events.rs`/`queue_relief.rs` drive the mq surface with
`"cart/alice"` strings directly); the ns-layout + ADR-0026 §1 table names
(both languages); probe USAGE (both languages) for the 4.16c frame shape.

### 8.4 Misc backlog

- `events_matching` incrementality (wildcard re-expansion is a 50 ms full
  rescan per cycle; cacheable once the vocabulary grows — Windmill gate).
- cold call over the wire (needs a Phase 6 consumer; never build ahead of one).
