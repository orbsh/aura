# Event Flow (the emit / on mechanics)

> **Languages:** [English](event-flow-en.md) (primary) · [中文](event-flow.md)

The single integrated document for the event plane, organized as a DERIVATION:
what constraints force each persistent surface (§2) → what authors declare (§3) →
the one-time registration assembly (§4) → matching per emit (§5) → delivery,
consumption and retention (§6) → the derivation's landing point, the keyspace
layout (§7) → the terminal rulings and their implementation checklist (§8, the
4.13+ backlog). The old `ns-layout.md`/`ns-layout-en.md` become pointers.
Rulings it records: ADR-0007/0012 (the receiver set is a runtime fact),
ADR-0026 (type-level storage plane), ADR-0002 (events never occupy a real ns),
ADR-0038 (the consumer set and identity: wildcard narrowing, one dictionary,
declaration semantics, no silent drops), ADR-0039 (partition encoding and the
cursor retention promise), `docs/design/partitioning.md` §1 (the routing end
state).

## 1. Vocabulary and invariants

The event plane has three orthogonal words; mixing them is the historical error
magnet:

- **Event**: a fact that happened — an open vocabulary. Events never occupy a
  real ns; they get a proxy id from the EventName dictionary.
- **Partition**: the queue's slice identity, derived from the instance key the
  route resolves. Landed (ADR-0039 §1): a PROXIED VOCABULARY — the PartitionName
  dictionary (ns 21) issues a fixed-width `u32` id, one shape with the event and
  type dictionaries. Reason: an okm primary key is fixed-width by construction
  (`KeyEncode` panics on a `String`), so an inline string is not expressible,
  while "an open vocabulary gets a proxy id" is this document's own rule. The
  hash is gone, `0` is an id the issuer cannot produce, and the routing layer
  speaks `mq::Partition` (`Singleton | Named`) instead of a magic string.
- **Booth**: who consumes. The type name takes its id from the ONE type dictionary
  (ns 30); instance names enter no dictionary at all — a cursor's subject is the
  TYPE (ADR-0038 §2, landed; see §8.3).

Four invariants (§2 is their unfolding):

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
   queues do not — backlog replay rides the cursor, not a live process. The
   promise is BOUNDED: idle long enough and the backlog is forfeit (ADR-0039
   §2, `cursor_ttl`).
4. **Subscriptions are type-level; consumption positions are instance-level.**
   The EventRoute row's subject is a type; the MqCursor row's subject is a
   participant (the current shape — ADR-0038 §1 narrows it to the singleton
   instance; see §8.3).
5. **No silent drops.** Every matched route of every emit either produces at
   least one real target, or leaves an observable record (ADR-0038 §4; today's
   entries are §5 and §6.1). The accompanying precondition: a queue's consumer
   set must be CLOSED — it is the premise of both the retention promise (§6.3)
   and the wildcard narrowing (ADR-0038 §1).

## 2. Deriving the persistent surfaces from the constraints

The event plane has no design space to pick from: every step is forced by the
previous one. Walk the chain and §7's keyspace table is simply where it lands
(each step names its landing table and ns).

1. **The caller does not know who is listening** (invariant 1). → Delivery
   cannot address receivers, and no checkable receiver whitelist exists; "nobody
   subscribed" is observable only after the fact (the dead ring).
2. **A fact may have N consumers, and they do not know each other.** → Delivery
   cannot be a function call or a return value; it can only be "the fact lands
   first, each party takes what is theirs". **Landing: MqData (ns 22)** — one
   row per emit, payload riding along; the key carries no booth identity,
   because an event belongs to no booth (invariant 3).
   Key `[event_id u32][part_id u32][seq u64]` (16 B, `#[ok_partition(1)]`, sharded by
   event_id); value = the payload's DYNAMIC segment (a native nTLV map with no declared
   fields — the key fixes identity, and the event's own shape is the author's data) plus
   the live `Count` reduce (group = the key's event_id + part_id, step 11).
3. **Each consumer must know independently how far it has read, and consumers
   can die** (scale-to-zero, eviction, hot swap). → The consumption position
   must be persisted per subscriber and must be monotonic: **Landing: MqCursor
   (ns 23)**, its subject a subscriber, never rewinding (this is what makes
   skip-to-head durable — a skipped backlog must not re-surface on the next
   drain).
   Key `[event_id u32][part_id u32][booth_id u32]` (12 B, `#[ok_partition(2)]`);
   value = `cursor u64` (the last consumed seq; 0 = nothing consumed) +
   `last_active_ms u64` (v2 hot tail: the `cursor_ttl` predicate's input; 0 = unmarked,
   never expired).
4. **Different batches of the same event must queue separately** (cross-instance
   parallel, same-instance serial). → The queue must be sliced: **partition =
   the instance key the route resolves**. A partition value is an open vocabulary,
   so it takes the proxy id this document's §7 already prescribes (ADR-0039 §1,
   landed: the PartitionName dictionary issues it and the hash is gone — a hash only
   bought fixed width, and an okm primary key is fixed-width anyway, so an inline
   string was never expressible).
   The dictionary's landing — **PartitionName (ns 21)**: key `id u32` (4 B, issuance
   starts at 1; 0 is left to the singleton and is structurally unreachable); value =
   `name String` (the raw partition value) + `id u32` mirror (the reduce folds payload
   fields) + `global u32` (single group, constant 0) + the `by_name` text index
   (name → id) + the `HighWater(id)` reduce (a registry-wide watermark — the same shape
   steps 6/8 use).
5. **Appending must not scan the whole partition for max; and the sequence can have
   only one issuer.** → **Landing: MqHead (ns 24)**, the
   per-partition write head: key `[event_id u32][part_id u32]` (8 B), value =
   `last_seq u64` (the largest sequence number issued for that partition); append reads
   it, computes `last + 1`, writes it back — O(1), with no second scan.
   — **"Why a counter and not a timestamp?"** Because this value is the SORT KEY, and
   within a partition the sort key is the row's identity, so it must be totally ordered
   and unique — and only "read the head, add one" gives that: (1) a second source (a raw
   wall reading) produces EQUAL keys for two appends in the same millisecond, and equal
   okm primary keys mean the later write OVERWRITES the earlier row — not a duplicate
   delivery, data loss; (2) the wall clock can step backwards (NTP, multi-node skew), and
   a value that falls below an already-consumed cursor is never read again — a silent
   drop. Mixing a clock reading with "the previous value + 1" in one number (as
   `max(now_ms, last+1)` once did) only makes it look like a time: it is neither *when*
   (it can run ahead of the wall clock) nor plainly *which one* (its magnitude jumps with
   the clock). It is now a pure sequence, and the name matches the semantics. **What
   guarantees uniqueness**: the emit path holds the realm lock across append
   (`self_arc.lock()`), so issuance is single-writer — the lock is the guarantee, the
   counter is just the bookkeeping. **A fact's wall time, if it must be preserved, goes
   in a payload field**: the seq that sorts and the instant that happened are two
   different things — never squeezed into one value.
6. **Keys are fixed-width binary segments with no text delimiter — a row cannot
   store a string; and event names are an open vocabulary that earns no real
   ns.** → **Landing: EventName (ns 20)**, the proxy-id dictionary: key `id u32` (4 B);
   value = `name String` + the `by_name` text index (one variable-length field, ADR-0005:
   at most one, last, no length prefix — so matching needs the row verify: with no
   delimiter, "add" prefix-matches "add_to_cart", and the row comparison is the
   exactness).
7. **A queue alone does not tell anyone whom to deliver to; the subscription
   facts must also survive restart** (without re-introspecting scripts).
   → **Landing: EventRoute (ns 25)**: registration assembles each type's
   `receives` into rows — key `[event_id u32][booth_id u32]` (8 B); value = `booth_id u32`
   mirror (index fields must be payload fields) + `key_field String` (empty = a key-less
   subscription; under a wildcard it stores the PATTERN) + `wildcard u8` (0 exact /
   1 wildcard — okm's field types have no Bool); the
   `by_booth` index serves activation binding and the compaction watermark
   denominator (§6.3). A wildcard PATTERN over an open vocabulary rides the
   same row shape — the pattern is stored in the row, matching is the emit
   path's job.
8. **The route rows likewise cannot store type names.** → Subscriber identity
   needs an issued id → **Landing: the BoothName dictionary (ns 30)** — ONE, the
   meta plane's unique TYPE dictionary: key `id u32` (4 B, issuance starts at 1); value =
   `name String` + `id u32` mirror + `ns u32` (the type's data ns, the result of the
   `100+id` allocation) + `global u32` (single group, constant 0) + the `by_name` index
   (name → id) + the `HighWater(id)` reduce (which also issues the data ns).
   ADR-0038 §2 landed: the event plane's own table
   (the old ns 33) is deleted and its number is not reused (§8.3).
9. **A booth is more than its subscriptions.** → **Landing: BoothDef (ns 31)**
   (one row per type) + **CodeBlob (ns 32)** (ADR-0027 content addressing).
   BoothDef: key `booth_id u32` (4 B); value = `name String` + `language String` +
   `code_sha256 [u8;32]` (a pointer to the code, no longer the code) + `idle_ttl_secs u64`
   (0 = realm default — a zero TTL is meaningless) + `encoding u64` (v2 hot tail: 0 = json,
   1 = cbor, ADR-0037 §2) + the `schema` dynamic segment (the introspected schema,
   structured nTLV, never opaque text).
   CodeBlob: key `sha256 [u8;32]` (32 B); value = `sha256 [u8;32]` mirror + `data Bytes`.
   The sha256 IS the version identity, re-registering identical code dedups, immutable by
   construction.
10. **A type's own state must live in its own ns.** → The registration
    side-effect chain: the unique type dictionary issues the id → data ns =
    `100 + id` (ADR-0026, monotonic, never reused) → take the persisted uploaded
    schema copy → `StorePlan::from_schema` compiles the storage routing table
    (collections / indexes / reduces, slot encodings).
11. **Ops must read queue depth with zero scans** (the skip-to-now decision
    input). → A live `Count` reduce on MqData (group(event_id, part_id)):
    append folds +1, watermark compaction deleting rows folds −1, so `depth()`
    is one point read, never a scan.

The chain collapses into one line: **an event is a fact (needs a queue), a
subscription is a relation (needs a registry), neither may carry names in its
keys (needs two id dictionaries), and a booth's own stuff — definition, code,
state — each gets its table.**

## 3. The declaration surface (what authors write)

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
instance key; the 4.13 end state: the access-method resolve reference, §8.1).

Names in subscription syntax are open vocabulary; **the declaration living
inside a type is the identity** — the event name itself carries no subject.

## 4. Registration (once, register_type)

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
4. Side-effect chain of registration (§2 step 10's landing):
   `meta::ns_and_schema_of` resolves the type's ns (the unique type dictionary
   issues the id and allocates `BOOTH_NS_BASE + id`) + takes the persisted
   uploaded schema copy → `StorePlan::from_schema` compiles the storage routing
   table (collections / indexes / reduces, slot encodings).

## 5. Matching (every emit, the hot path)

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
  loss — it is a backlog write (§6.1). There is a second entry: a matched route
  whose `mq::append` fails (a storage fault) also lands in the dead ring — it
  collects "facts that were never written", sharing one observation surface
  with "nobody subscribed". A third entry (a matched route with no real target)
  landed with ADR-0038 §4 (`MissingKeyField`), see §1 invariant 5.
- The persisted source of truth is the EventRoute registry (ns 25): routes
  survive restart without re-introspecting scripts; the in-memory router is its
  hot face, rebuilt on boot reload through the SAME registration code. Registry
  rows are the subscription facts AND the compaction watermark denominator.

## 6. Delivery, consumption, retention

### 6.1 Delivery and partition resolution (the current shape)

Per matched route:

```
partition =  key_field empty → Partition::Singleton
            else payload[key_field] as str → Partition::Named(that value)   // exactly one
            absent/non-string → a MALFORMED event: dead ring with MissingKeyField (ADR-0038 §4)
target   = InstanceId{booth_type, the singleton instance's key = "__singleton__", else the value}
activation → not in the instance table? instance() first (same pass, before any send)
enqueue    → mq::append(event, &Partition, payload)   deduped by (event, partition)
```

Honest footnotes:

1. **The `__default__` fallback is RETIRED** (ADR-0038 §4): a payload missing its
   declared key field is a malformed event — it lands in the dead ring with a
   reason tag instead of being fed silently to an instance nobody addressed; the
   same class as a zero-target scan.
2. **One route yields exactly one target** (single-key routing). Type-level
   fan-out always existed (several types on one event = several EventRoute
   rows); what cannot be expressed is fanning out WITHIN a type by business
   fact (region.escalation → every western store) — a single field cannot,
   a scan naturally can (§8.1).
3. **Delivery/consumption partition agreement is by construction**: the
   consumer's `bound_partition` re-derives the route's key_field semantics
   from "this instance's key" — instance name = partition name = key-field
   value is the trinity today (a contract, not a mechanism).
4. The dedupe key is `(concrete event name, partition)`: several types on one
   event enqueue once and the queue fans out to all subscribers; two rows for
   one partition from one emit would be double delivery.

### 6.2 The consumption loop

`crates/realm/src/instance.rs`: activation binds the subscription set (the
in-memory `router.routes_of`) and then each instance runs one consumer loop:

- A keyed route: partition = this instance's key; a key-less route (wildcards
  included): partition = the singleton.
- Every cycle sweeps ALL bound queues: a wildcard subscription re-expands via
  `mq::events_matching(prefix)` into concrete event names each pass (new names
  join automatically); other subscriptions use the declared event name itself.
- Per queue: read the cursor → `mq::backlog` (the rows after the cursor) → run
  each row's `run_job` serially → `mq::advance`. `backlog` is a full scan of the
  partition prefix (no batch cap); convergence rides cursor advance and
  compaction.
- A cycle with no progress parks 50 ms. **Same-instance serial, cross-instance
  parallel lives in this loop, not in locks.**

The cursor's subject is the TYPE (ADR-0038 §2): two types on one event hold
independent cursors.

- **The mixed vocabulary in the booth_id segment is history**: one resolver once
  served both type names (EventRoute registration, `routes_of_booth`) and
  participant names `"type/key"` (cursor semantics), and the compact path's
  `split_once('/')` was its downstream symptom. **Landed (ADR-0038 §2)**: one
  dictionary (ns 30, `mq::booth_id_of` → `meta::resolve_booth_id`), the cursor
  key's third segment is `booth_id`, and participant names are no longer issued ids.
- **Wildcard semantics**: a key-less subscription delivers to the type's SINGLETON
  instance (`partitioning.md` §1's text already says this), because a queue's
  consumer set must be closed (§1 invariant 5, §6.3). Landed (ADR-0038 §1): the
  consumer loop binds a key-less route only on the singleton instance and no other
  instance binds it; the wildcard fan-out tests rewrote with the semantic.

### 6.3 Retention and compaction

The watermark runs on the EMIT path (write-path compaction), and its
denominator is the **EventRoute registry**, not the raw cursor keys:

- A cursor row enters the denominator only if its type CURRENTLY registers a
  route that MATCHES this concrete event (`mq::booth_subscribes`: exact rows by
  event_id, wildcard rows by prefix — the registry stores the pattern). Eviction
  neither adds nor removes a row (its backlog still awaits replay); a type hot
  swap or deregistration drops the route, and the stale cursor row falls out of
  the denominator (the downstream reading of §4's drop semantics).
  (Added while landing §6.3: the previous exact-event_id lookup silently dropped
  WILDCARD subscribers out of the denominator, so compaction could eat their
  backlog — a violation of §1 invariant 5.)
- The watermark = the minimum cursor among the denominator;
  `delete_before(event, partition, min_seq)` deletes mq-data rows with
  seq < watermark. An empty denominator (no registered subscriber) does nothing.
- The seq comparison IS the ordering comparison (§2 step 5: a counter, not a time).
- **depth**: one point read of MqData's live `Count` reduce (§2 step 11) — the
  skip-to-head decision input, never a scan.
- **skip-to-head** (`mq::skip_to_head`; the script-facing host fn is the same name,
  `ctx_skip_to_head` — probe's carrier allowlist was updated in step): jump the cursor to the partition write
  head, discarding the stale backlog (the relief valve). The cursor's
  monotonicity (`advance` only moves forward) is what makes it durable;
  `rewind_cursor` is test-support only, never a production path.
- **Cursor expiry (ADR-0039 §2, landed)**: a global `cursor_ttl` (one
  `EngineConfig` field, KDL `mq { cursor_ttl "30d" }`, default 30 days), decoupled
  from `idle_ttl` — days vs seconds; binding them would let a second-scale event
  decide a day-scale data promise; no per-type override, because the denominator
  is a cross-type `min` comparison). Expiry = the row leaves the denominator
  (compaction may pass it = the backlog is forfeit), and is NEVER row deletion: a
  deleted row reads the cursor back as 0, and `backlog(after = 0)` replays
  surviving rows — an instance whose cursor was ahead of the watermark would
  re-consume them. Physical deletion (`drop_cursor`) is reserved for rows whose
  cursor is already below the watermark. `last_active_ms` (v2 hot tail) refreshes
  on `advance`; the `0` sentinel never participates (old rows decode the missing
  field as 0, and without the sentinel the first run of the new code would expire
  everything at once). The predicate is evaluated at compaction time, not by a
  background watchdog.
- The scan cost is bounded by the subscriber count (it runs per emit).

## 7. Keyspace layout (ns-layout merged)

One realm's persistent plane = one okm instance; the framework low block has two
bands (ADR-0040): **the event plane, ns 20–29**, and **the meta plane, ns 30–39**
(inside a band, table order is conceptual — vocabulary → data → positions →
registry); **booth type nss** allocate at runtime from 100; `26–29` (the event
plane's empty middle) and `33–99` stay reserved.

The framework low block (ADR-0040, landed). **Full key/value layouts — field order,
widths, sentinels and the index/reduce shapes — live in §2's landing lines per step;
this table is the index, and the authoritative definition lives in the code
(`#[ok_ns]`/`#[ok_layout]`)**:

| ns | table | key | purpose | code |
|---|---|---|---|---|
| 20 | EventName | `id u32` | event-name dictionary (`by_name` text index; open vocabulary takes proxy ids, never a real ns) | mq.rs |
| 21 | **PartitionName** (new) | `id u32` | the partition dictionary (ADR-0039 §1): `by_name` text index + `HighWater` watermark, reverse resolution (id → name) for ops; the FNV-1a hash is gone | mq.rs |
| 22 | MqData | `[event_id u32][part_id u32][seq u64]` | event data: one row per emit, N subscribers = N cursors; sort key = the per-partition sequence (a counter); carries the live `Count` reduce (same-key group) behind `depth()`'s point read | mq.rs |
| 23 | MqCursor | `[event_id u32][part_id u32][booth_id u32]` | subscription cursor: last consumed seq, monotonic; the third segment is the ns 30 type id; `last_active_ms` joins the row (ADR-0039 §2, landed) | mq.rs |
| 24 | MqHead | `[event_id u32][part_id u32]` | partition write head (the sequence issuer): value `last_seq u64`; O(1) append (`last+1`), single-writer under the realm lock | mq.rs |
| 25 | EventRoute | `[event_id u32][booth_id u32]` | persisted subscription registry (`by_booth` index); wildcards ride the same row shape storing the PATTERN; `booth_id` = the ns 30 type id | mq.rs |
| 30 | **BoothName** (was TypeName; renamed dbc5d60, renumbered Phase 4.18) | `id u32` | the unique booth-TYPE dictionary: `by_name` index + `HighWater(id)` preset + data-ns allocation (`100+id`); resolves BOTH ways (name→id `resolve_booth_id`, id→name `booth_name_of`) | meta.rs |
| 31 | BoothDef | `booth_id u32` | booth definition row per type: name/language/encoding/idle_ttl + the `code_sha256` pointer; the introspected schema rides the dynamic segment (structured nTLV, never opaque text) | meta.rs |
| 32 | CodeBlob | `sha256 [u8;32]` | content-addressed code bytes (ADR-0027): pure content rows, immutable by construction — same key different bytes is a hash collision, not a state | meta.rs |

The event plane's own subscriber-identity dictionary (the old ns 33) is **deleted**
(ADR-0038 §2) and takes no number in the new bands.

Partition annotations: MqData = partition 1 (by event_id), MqCursor =
partition 2; `SINGLETON_PART = 0`.

**Landed (ADR-0039 §1)**: the partition is a PROXIED VOCABULARY —
`PartitionName` (ns 21) issues a fixed-width `u32` id (one layout with the other
two dictionaries), `0` stays reserved for the singleton and issuance starts at 1
(structurally unreachable), `part_hash`/FNV-1a/`part_id_of`/`part_hash_of` are
gone, and the routing layer speaks `mq::Partition` (`Singleton | Named`) rather
than comparing a magic string. The reason: an open vocabulary takes a proxy id,
which is this document's own rule; an okm primary key is fixed-width by
construction (`KeyEncode` panics on a `String`), so an inline string is not
expressible; and the hash brought mis-delivery on collision, a reserved value
sharing a namespace with the value space, and a mapping no ops surface can read
back. Key widths tightened: MqData 20→16 B, MqCursor 16→12 B, MqHead 12→8 B.
`MqCursor.last_active_ms` (§6.3's expiry predicate) landed in the same batch.

Booth type nss (runtime): registering a type issues the id from the unique
dictionary and allocates data ns = `100 + id` — monotonic with the id, never
reused within the node. Inside a type ns: the interface_schema-declared
collections + their access methods (slots encode as `ns + slot`; the
dict/junction bases live in each collection's schema constants; the real ns
is only a prefix). Instances are documents inside the type ns. Type isolation
is structural: ctx storage handles bind the owning type's ns at registration
(ADR-0026 §3).

Invariants: the low block is compile-time fixed — new framework tables take
numbers from the empty middle and MUST join this table (both languages); booth
types never fall into the low block; `#[ok_ns]`/`#[ok_partition]` annotation
changes must keep this page in sync — the authoritative definitions live in
code. **A number is never reused**: once a number belongs to a table it never
belongs to another. This renumbering retires the whole old set
(`30–35`/`40–42`), and the new meta band (30–32) lands exactly on numbers the old
event plane used — so on an EXISTING deployment wiping the low block is an
operational act (without it, a new `BoothName` reads old `EventName` rows as type
names: reading old data as new data). The wipe costs more than "mq bytes are
transient": persisted booth definitions and code blobs go with it, so a deployment
re-registers its types (ADR-0040 records this). A fresh store has nothing to wipe.

**Vocabulary tables never take a real ns**: open vocabularies (event names,
partition names, participant names) ride proxy ids + text indexes; only closed
vocabularies (booth types) earn a real ns — the EventName/PartitionName/BoothName
shape (ns 20/21/30) follows from this. Nss only grow: deregistering a type
reclaims nothing — reusing a keyspace prefix reads old data as new data, and
"ids and nss are never reused" is one ruling.

Consolidation of the two dictionaries is DONE: the only dictionary is ns 30
(ADR-0038 §2 landed; the event plane's own table is deleted).

## 8. Terminal rulings and the implementation checklist (the 4.13+ backlog)

This section records the event plane's **terminal rulings** (the ruling text
lives in ADR-0038/0039/0040; what stays here is the mechanism, the reason in
brief, and the implementation fallout). §8.3 (identity and delivery) and §8.4
(partition identity, bands, the retention promise) are **LANDED**; §8.1 (the
routing end state: access-method scans) still awaits its precondition (dynamic
schema); §8.5 stays open.

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
  whole semantics. The payload-field shape (a) and the index-tail shape (c) both
  stay OUT of the vocabulary — the former creates a second misspellable
  declaration, the latter would change okm's scan return shape and is
  essentially an optimization of (a) (premature, Windmill gate). Instance
  identity is never declared — the scan produces it; booth code reads its own
  via `ctx.self_id.key`.
- **The old single-field shape survives as a second row shape** (discriminated
  at the row-structure layer, no sentinel mixing): for keyed cases the two are
  equivalent and key_field is cheaper (zero scan); migration is per type.
- **Multi-target failure semantics**: per-target dead-ring/record, consistent
  with ADR-0012 (appends are already per-partition independent — failure
  granularity comes for free).
- **MQ formats unchanged**: `append(event, partition)` rows and per-(event,
  partition) cursors already fit one-to-many delivery; only the route side
  must produce a SET of partitions.

**Ruled (ADR-0038 §3)**: ① declaration granularity = per event, with no type
default — `resolve` is a function of (type, event), and a type-wide default is
defined only when every event of the type happens to be isomorphic; moreover a
default value must fix its semantics uniquely (no `resolve` and no `key_field` =
singleton delivery), while "absence = inherit" would give one field two readings
— the declaration/execution drift class. Option B (type default + override) is
therefore rejected. The **multi-target failure semantics** are ruled too:
per-target dead-ring, consistent with ADR-0038 §4. Implementation = Phase 4.17.

### 8.2 The EventRoute row shape

The `key_field: String` payload becomes the reference payload. **Ruled
(ADR-0038 §3)**: a reference carries NO collection ns number — the row stores the
collection/index/probe **names** (resolution lives with the schema owner), while
a stored slot/ns number would make position the identity and any schema edit
would silently re-point the row. The three declaration shapes (singleton /
payload resolution / index scan) are three mechanisms, discriminated at the row
structure layer, never by sentinel mixing. Implementation = Phase 4.17.

### 8.3 Double-dictionary consolidation + cursor-key orthogonality (landed, ADR-0038 §1/§2)

A key-less subscription delivers to the type's SINGLETON instance — broadcast made
the consumer set OPEN: every emit for a new key activates a new instance, and a
new instance's cursor reads 0, so it replays the entire surviving history of that
queue; the watermark is pinned forever and subscription drifts into state
synchronization. A queue's consumer set must be closed (§1 invariant 5).
Consequently: the unique type dictionary is ns 30, the event plane's own table is
deleted (its number never reused), the cursor key's third segment is `booth_id`, and
mq delegates type resolution to `meta::resolve_booth_id`. **Direction B (enshrine
participant-level cursors) is rejected** — not for inconvenience, but because it is
an open set.

On record: the orthogonal scheme's original blocker WAS the wildcard's
participant-level fan-out (two instances sharing the singleton cursor meant one
message consumed by one of them, read at the time as a semantic regression); the
ruling resolves it from the other end — that semantic should have been singleton
delivery all along.

Landed fallout (done): the `bound_partition`/`routes_of_*`/compact resolver chain,
the test call sites, `by_type` → `by_booth`, `split_once('/')` gone. Residual (not
covered): the INSTANCE key space still uses the `"__singleton__"` sentinel string,
so a payload key with that literal text still aliases the singleton INSTANCE —
making instance identity structural would touch `InstanceId` across the call model
and the probe seam (recorded in ADR-0038).

### 8.4 Partition identity, keyspace bands, and the cursor retention promise (landed, ADR-0039 §1/§2, ADR-0040)

The partition becomes a proxy dictionary (`PartitionName`; the hash is gone); the
low block has two bands (the event plane 20–29, the meta plane 30–39, conceptual
order inside a band); `cursor_ttl` is global (default 30 days) and expiry removes a
row from the denominator without deleting it. The mechanism and the reason are in
§6.3 and §7.

Landed fallout (done): the `#[ok_ns]` renumbering; the `PartitionName` table plus
`mq::Partition` as the structural marker; the tightened key widths; `mq {
cursor_ttl }` in `crates/config` + the realm field; `MqCursor.last_active_ms` (v2
hot-tail + the `0` sentinel); the denominator change (`booth_subscribes`) plus
inert-row reclamation (`drop_cursor`).
**Same-day renames (2026-10-02, second batch)**: the third segment and the cursor key
went `type_id` → `booth_id`, and the derived index `by_type` → `by_booth` (ADR-0032's
booth-naming sweep reaching the field layer). MqData's third segment is a SEQUENCE
(`seq`, `MqHead.last_seq`, `last+1`) — not a timestamp: it is both the sort key and the
row's identity, so mixing in a wall reading only buys collisions (same-millisecond
overwrite = data loss) and backwards steps (falling below a consumed cursor = a silent
drop); uniqueness comes from "the emit path holds the realm lock = one writer".
`mq::skip_to_now` → `mq::skip_to_head` (the script-facing host fn was renamed with it:
`ctx_skip_to_now` → `ctx_skip_to_head`, in aura and in probe's steel carrier allowlist). An EXISTING deployment must wipe the low
block (the new meta band lands on numbers the old event plane used; the cost
includes persisted definitions and code blobs — ADR-0040 records it).

### 8.5 Misc backlog (still open)

- `events_matching` incrementality (wildcard re-expansion is a 50 ms full
  rescan per cycle; the singleton narrowing already bounds the cost to one
  consumer — worth caching only at a large vocabulary, Windmill gate).
- cold call over the wire (needs a Phase 6 consumer; never build ahead of one).