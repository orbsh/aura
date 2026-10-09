# Event Flow (the emit / on mechanics)

> **Languages:** [English](event-flow-en.md) (primary) · [中文](event-flow.md)

The single integrated document for the event plane, in two layers: the CONTRACT
(emit/`@on`, queues, the retention promise) and the INTERNAL LAYOUT (the keyspace
and the engine annotations — the latter are implementation notes only, ADR-0041).
The reading order:

1. §2 derives every persistent surface from constraints (the derivation chain);
2. §3–§6 are the runtime mechanisms, walked in the order they happen: what authors
   declare → the one-time registration assembly → matching per emit → delivery,
   consumption and retention (each chapter points back at the derivation step it
   implements);
3. §7 is where the derivation lands — the keyspace table, doubling as the internal
   layout (engine annotations appear there as implementation notes only);
4. §8 holds the terminal rulings and the implementation checklist (the 4.13+ backlog).

The old `ns-layout.md`/`ns-layout-en.md` become pointers.

Rulings it records:

- ADR-0007/0012 — the receiver set is a runtime fact (no emits whitelist; the dead
  ring is the observation surface);
- ADR-0002 — events never occupy a real ns; ADR-0026 — the type-level storage plane;
- ADR-0038 — the consumer set and identity: wildcard narrowing, one dictionary,
  declaration semantics, no silent drops;
- ADR-0039 — partition encoding and the cursor retention promise;
- ADR-0041 — the event plane's internal layout: the instance-key vocabulary, the
  issuer on the data table, one physical partition;
- `docs/design/partitioning.md` §1 — the routing end state;
- the wiki's [Distributed Collaboration Topology](https://github.com/orbsh/wiki/blob/main/distributed-collaboration-topology.md)
  §2 — the ceiling on order: why cross-partition events are never globally ordered.

## 1. Vocabulary and invariants

The event plane has three orthogonal words; mixing them is the historical error
magnet:

- **Event**: a fact that happened — an open vocabulary. Events never occupy a
  real ns; they get a proxy id from the EventName dictionary.
- **Instance key**: the addressing unit of a delivery — on the subscriber side
  the subscriber's own instance key, on the emit side the payload value the
  route's `key_field` points at (the two agree by construction, §6.1). It is at
  the same time the queue's SLICE identity: one event is sliced by instance key,
  one fact per key lands as ONE row, and each subscriber type reads it through
  its own cursor. The key value comes from data (an open vocabulary), so it takes
  a proxy id: the **InstanceKeyRegistry dictionary (ns 21)** issues a fixed-width
  `u32` (one layout with the event and type dictionaries), `0` stays reserved for
  key-less delivery (the singleton slice) and is unreachable by issuance, and the
  routing layer speaks `mq::InstanceKey` (`Singleton | Named`) instead of a magic
  string. Reason: an okm primary key is fixed-width by construction (`KeyEncode`
  panics on a `String`), so an inline string is not expressible; a hash once
  bought the width and was deleted for mis-delivering on collision and being
  unreadable (ADR-0039 §1). **The word belongs to the event plane alone**: okm's
  physical KV partition (`#[ok_partition]`) is the engine's compaction group, a
  different thing, and appears only as an implementation note in §7 (ADR-0041 §2).
- **Booth**: who consumes. The type name takes its id from the ONE type dictionary
  (ns 30); CONSUMER identity enters no dictionary — a
  cursor's subject is the TYPE (ADR-0038 §2, landed; see §8.3); the instance key
  does have a dictionary of its own (ns 21), but it issues ids for the queue
  slice, not for a consumer identity.

Five invariants (§2 is their unfolding):

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
4. **Subscriptions are type-level; consumption positions are type-level.**
   The EventRoute row's subject is a type, and so is the MqCursor row's (ADR-0038
   §2 landed — a participant-level cursor makes the consumer set open and is
   refused; see §8.3).
5. **No silent drops.** Every matched route of every emit either produces at
   least one real target, or leaves an observable record (ADR-0038 §4; today's
   entries are §5 and §6.1). The accompanying precondition: a queue's consumer
   set must be CLOSED — it is the premise of both the retention promise (§6.3)
   and the wildcard narrowing (ADR-0038 §1).

## 2. Deriving the persistent surfaces from the constraints

The event plane has no design space to pick from: every step is forced by the
previous one. Walk the chain and §7's keyspace table is simply where it lands
(each step names its landing table and ns). §7 is also the internal-layout table:
engine annotations (`#[ok_ns]`/`#[ok_partition]`/…) appear there as implementation
notes — they are how a landing is implemented, never why it is the landing
(ADR-0041 §2).

1. **The caller does not know who is listening** (invariant 1). → Delivery
   cannot address receivers, and no checkable receiver whitelist exists; "nobody
   subscribed" is observable only after the fact (the dead ring).
2. **A fact may have N consumers, and they do not know each other.** → Delivery
   cannot be a function call or a return value; it can only be "the fact lands
   first, each party takes what is theirs". **Landing: MqData (ns 22)** — one
   row per emit, payload riding along; the key carries no booth identity,
   because an event belongs to no booth (invariant 3).
   Key `[event_id u32][instance_key_id u32][seq u64]` (16 B); value = the payload's
   DYNAMIC segment (a native nTLV map with no declared fields — the key fixes
   identity, and the event's own shape is the author's data) plus TWO live reduces:
   `Count { group(event_id, instance_key_id) }` (queue depth, step 11) and
   `HighWater(seq) { group(event_id, instance_key_id) }` (**the write head**, i.e.
   the issuer, step 5). (The table carries `#[ok_partition(1)]`: the engine's
   physical KV partition, see §7's implementation note.)
3. **Each consumer must know independently how far it has read, and consumers
   can die** (scale-to-zero, eviction, hot swap). → The consumption position
   must be persisted per subscriber and must be monotonic: **Landing: MqCursor
   (ns 23)**, its subject a subscriber, never rewinding (this is what makes
   skip-to-head durable — a skipped backlog must not re-surface on the next
   drain).
   Key `[event_id u32][instance_key_id u32][booth_id u32]` (12 B); value =
   `cursor u64` (the last consumed seq; 0 = nothing consumed) + `last_active_ms u64`
   (v2 hot tail: the `cursor_ttl` predicate's input; 0 = unmarked, never expired).
   It is a point table, the same class as booth state, and carries no physical
   partition (ADR-0041 §4).
4. **Different batches of the same event must queue separately** (cross-instance
   parallel, same-instance serial). → The queue is sliced by **instance key**:
   **the slice segment's value = the instance key the route resolves** (the
   subscriber side = this instance's key, the emit side = the payload value of the
   route's `key_field`; the two agree by construction). The key value comes from
   data, so it is an open vocabulary and takes the **proxy id** this document's §7
   already prescribes (ADR-0039 §1, landed: the hash is gone — a hash only bought
   fixed width, and an okm primary key is fixed-width anyway, so an inline string
   was never expressible). One id covers ONE INSTANCE PER SUBSCRIBER TYPE: the data
   key carries no booth segment, so one fact per key lands as one row that several
   types read through their own cursors (the fan-out dedup — not "one instance,
   one id", ADR-0041 §1).
   The dictionary's landing — **InstanceKeyRegistry (ns 21)**: key `id u32` (4 B,
   issuance starts at 1; 0 is left to key-less delivery and is structurally
   unreachable); value = `name String` (the instance key verbatim) + `id u32` mirror
   (the reduce folds payload fields) + `global u32` (single group, constant 0) + the
   `by_name` text index (name → id) + the `HighWater(id)` reduce (a registry-wide
   watermark — the same shape steps 6/8 use).
5. **Appending must not scan the whole slice for max; and the sequence can have
   only one issuer.** → **Landing: the `HighWater(seq)` watermark on MqData**
   (already hung in step 2): append reads the watermark (one point read of the
   reduce entry), computes `+1`, and the data row's own put folds it — O(1), with
   no second scan, and issuance and the row land in ONE physical partition (the
   append's atomic domain). **Why a watermark can be the issuer**: okm's reduce
   discipline is "acc = the fold over the rows the table currently holds", and
   `HighWater` is that discipline's DECLARED exception (unfold is a no-op) — an
   issuer's value must outlive its rows, and a view cannot; so retention
   compaction deleting rows never lowers the head. The separate head table
   (MqHead, ns 24) is retired by this: the access surface is static,
   single-writer and framework-internal, so no third-party compatibility surface
   needs the issuer to exist on its own (ADR-0041 §3).
   — **"Why a counter and not a timestamp?"** Because this value is the SORT KEY, and
   within a slice the sort key is the row's identity, so it must be totally ordered
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
    input). → A live `Count` reduce on MqData (group(event_id, instance_key_id)):
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
- **The dead ring** (`realm.dead_events.push`: a bounded ring buffer, process
  memory, empty on restart — it collects "facts that never reached anyone" as
  diagnostic output, not retryable work). Direction matters: a matched route whose
  instance is not live is not a loss — it is a backlog write (§6.1). Three entries
  (`DeadReason`): `NoRoute` (no route matched at all), `MissingKeyField` (a route
  matched but the payload lacks the declared key field, ADR-0038 §4), and
  `AppendFailed` (matched but the `mq::append` storage fault — "facts that were
  never written" share the observation surface with "nobody subscribed").
- The persisted source of truth is the EventRoute registry (ns 25): routes
  survive restart without re-introspecting scripts; the in-memory router is its
  hot face, rebuilt on boot reload through the SAME registration code. Registry
  rows are the subscription facts AND the compaction watermark denominator.

## 6. Delivery, consumption, retention

### 6.1 Delivery and slice resolution (the current shape)

One emit runs in two phases: resolve a slice per matched route (activating targets
in the same pass), then enqueue deduped by slice. **This phase touches no KV at
all**: matching reads the in-memory router (the persisted truth is EventRoute ns 25,
§5), resolution reads the emit's own payload, and targets go into the in-memory
instance table — the first storage write is the enqueue. Per matched route:

```
slice   key_field empty     → InstanceKey::Singleton            (key-less delivery)
        payload[key_field]  → InstanceKey::Named(that value)    (exactly one target)
        absent / non-string → a MALFORMED event: dead ring with
                              MissingKeyField (ADR-0038 §4)
target  InstanceId{ booth_type,
          key = the singleton instance's key "__singleton__" (key-less)
              / the slice value (keyed) }
activate  target not in the instance table → instance() first, BEFORE the write
          (same pass; activation precedes enqueue)
enqueue   mq::append(event, &InstanceKey, payload) → MqData (ns 22),
          deduped globally by (concrete event name, slice)
```

Four footnotes:

1. **The `__default__` fallback is RETIRED** (ADR-0038 §4): a payload missing its
   declared key field is a MALFORMED event — it reaches nobody and lands in the
   dead ring with a reason tag, the same class as a zero-target scan; it is no
   longer fed silently to an instance nobody addressed.
2. **One route yields exactly one target** (single-key routing). Cross-type fan-out
   already exists: several types on one event = several EventRoute rows = each
   type's cursor reading the same row. What cannot be expressed is fanning out
   WITHIN a type by business fact (`region.escalation` → every western store) — a
   single field cannot; an access-method scan is naturally one-to-many (§8.1's
   target shape).
3. **Delivery/consumption slice agreement is by construction**: the emit side reads
   the payload's `key_field`; the consumer side's `bound_instance_key` re-derives
   the SAME route's key_field semantics from its own key — two readings of one
   value, aligned without an agreement (ADR-0041 §1).
4. **The dedupe key is `(concrete event name, slice)`**: two types subscribing to
   one event whose keys resolve to the same value enqueue ONCE, and the queue fans
   out to all subscribers (one data row, N cursors); two rows for one slice from
   one emit would be double delivery.

### 6.2 The consumption loop

`crates/realm/src/instance.rs`: activation binds the subscription set (the
in-memory `router.routes_of`) and then each instance runs one consumer loop:

- A keyed route: slice = this instance's key; a key-less route (wildcards
  included): slice = the singleton.
- Every cycle sweeps ALL bound queues: a wildcard subscription re-expands via
  `mq::events_matching(prefix)` into concrete event names each pass (new names
  join automatically); other subscriptions use the declared event name itself.
- Per queue: read the cursor → `mq::backlog` (the rows after the cursor) → run
  each row's `run_job` serially → `mq::advance`. `backlog` is a full scan of the
  slice prefix (no batch cap); convergence rides cursor advance and
  compaction.
- A cycle with no progress parks 50 ms. **Same-instance serial, cross-instance
  parallel lives in this loop, not in locks.**

The cursor's subject is the TYPE (ADR-0038 §2): two types on one event hold
independent cursors.

- The cursor key's third segment is `booth_id` (the ns 30 unique type dictionary,
  `mq::booth_id_of` → `meta::resolve_booth_id`; participant names are no longer
  issued ids — the mixed-vocabulary era's full story is §8.3).
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
registry); **booth type nss** allocate at runtime from 100; `24` (vacated when
ADR-0041 retired MqHead) and `26–29` (the event plane's empty middle), plus `33–99`,
stay reserved.

The framework low block (ADR-0040, landed). **Full key/value layouts — field order,
widths, sentinels and the index/reduce shapes — live in §2's landing lines per step;
this table is the index, and the authoritative definition lives in the code
(`#[ok_ns]`/`#[ok_layout]`)**:

| ns | table | key | purpose | code |
|---|---|---|---|---|
| 20 | EventName | `id u32` | event-name dictionary (`by_name` text index; open vocabulary takes proxy ids, never a real ns) | mq.rs |
| 21 | **InstanceKeyRegistry** | `id u32` | the instance-key dictionary (ADR-0039 §1 established, ADR-0041 §1 named): `by_name` text index + `HighWater` watermark, reverse resolution (id → instance key) for ops; the FNV-1a hash is gone | mq.rs |
| 22 | MqData | `[event_id u32][instance_key_id u32][seq u64]` | event data: one row per emit, N subscribers = N cursors (types share the row); sort key = the per-slice sequence (a counter); carries TWO live reduces: `Count` (behind `depth()`'s point read) and `HighWater(seq)` (the write head = the issuer, ADR-0041 §3); the table carries `#[ok_partition(1)]` | mq.rs |
| 23 | MqCursor | `[event_id u32][instance_key_id u32][booth_id u32]` | subscription cursor: last consumed seq, monotonic; the third segment is the ns 30 booth id; `last_active_ms` joins the row (ADR-0039 §2); no physical partition (ADR-0041 §4) | mq.rs |
| 24 | — (empty) | | the write head retired with MqHead; MqData's `HighWater(seq)` carries it (ADR-0041 §3) | |
| 25 | EventRoute | `[event_id u32][booth_id u32]` | persisted subscription registry (`by_booth` index); wildcards ride the same row shape storing the PATTERN; `booth_id` = the ns 30 booth id | mq.rs |
| 30 | **BoothName** (was TypeName; renamed dbc5d60, renumbered Phase 4.18) | `id u32` | the unique booth-TYPE dictionary: `by_name` index + `HighWater(id)` preset + data-ns allocation (`100+id`); resolves BOTH ways (name→id `resolve_booth_id`, id→name `booth_name_of`) | meta.rs |
| 31 | BoothDef | `booth_id u32` | booth definition row per type: name/language/encoding/idle_ttl + the `code_sha256` pointer; the introspected schema rides the dynamic segment (structured nTLV, never opaque text) | meta.rs |
| 32 | CodeBlob | `sha256 [u8;32]` | content-addressed code bytes (ADR-0027): pure content rows, immutable by construction — same key different bytes is a hash collision, not a state | meta.rs |

The event plane's own subscriber-identity dictionary (the old ns 33) is **deleted**
(ADR-0038 §2) and takes no number in the new bands; with it the double-dictionary
consolidation is done — the only dictionary is ns 30.

**Two numbering disciplines** (details in ADR-0040):

- **A number is never reused**: once a number belongs to a table it never belongs to
  another. Nss only grow — deregistering a type reclaims nothing, and reusing a
  keyspace prefix reads old data as new data ("ids and nss are never reused" is one
  ruling).
- **The low block is compile-time fixed**: new framework tables take numbers from the
  empty middle and MUST join this table (both languages); booth types never fall into
  the low block; `#[ok_ns]`/`#[ok_partition]` annotation changes must keep this page in
  sync — the authoritative definitions live in code. The 2026-10-02 renumbering retired
  the whole old set (`30–35`/`40–42`), and the new meta band (30–32) lands exactly on
  numbers the old event plane used — so on an EXISTING deployment wiping the low block
  is an operational act (the cost includes persisted booth definitions and code blobs;
  the deployment re-registers its types; ADR-0040 records it). ADR-0041's renames and
  the MqHead retirement are absorbed by the same wipe. A fresh store has nothing to
  wipe.

**Implementation note (physical partitions — not event-plane vocabulary)**:
`#[ok_partition]` is the engine's compaction group, unrelated to the event plane's
"slice". Only MqData carries one (partition 1) — it is the sole bulk-append +
range-delete workload; MqCursor's point writes are the same class as booth state and
live in the default keyspace (ADR-0041 §4).

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
instance keys) ride proxy ids + text indexes (the ns 20/21 shape); only closed
vocabularies (booth types) earn a real ns (ns 30).

## 8. Terminal rulings and the implementation checklist (the 4.13+ backlog)

This section records the event plane's **terminal rulings** (the ruling text
lives in ADR-0038/0039/0040; what stays here is the mechanism, the reason in
brief, and the implementation fallout). §8.3 (identity and delivery) and §8.4
(partition identity, bands, the retention promise) are **LANDED**; §8.1 (the
routing end state: access-method scans) LANDED with Phase 4.13 (commits
`33ef84e`, `e3690ac`, landing ADR-0042's structural instance identity in the
same batch); §8.5 stays open.

### 8.1 The routing end state: instance key via access-method scans (LANDED, Phase 4.13)

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
                                (event, slice) (already exists)
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
  with ADR-0012 (appends are already per-slice independent — failure
  granularity comes for free).
- **MQ formats unchanged**: `append(event, slice)` rows and per-(event,
  slice) cursors already fit one-to-many delivery; only the route side
  must produce a SET of slices.

**Ruled (ADR-0038 §3)**: ① declaration granularity = per event, with no type
default — `resolve` is a function of (type, event), and a type-wide default is
defined only when every event of the type happens to be isomorphic; moreover a
default value must fix its semantics uniquely (no `resolve` and no `key_field` =
singleton delivery), while "absence = inherit" would give one field two readings
— the declaration/execution drift class. Option B (type default + override) is
therefore rejected. The **multi-target failure semantics** are ruled too:
per-target dead-ring, consistent with ADR-0038 §4. **Implemented** (Phase 4.13).

Landed fallout (Phase 4.13, commits `33ef84e`/`e3690ac`): `RouteResolution
{ Singleton, Field, Scan }` discriminated at the row-structure layer; the
EventRoute row (ns 25) carries the reference payload (`resolution` u8 tag +
collection/index/probe_field name columns); `instance_of` is gone — the
slice variant flows to the delivery target with no string round trip;
evaluation rides the 4.16 DynamicCollection face
(`store_exec::resolve_scan_targets`); scan-route collections are ruled to a
SINGLE key field (the instance key is one String; a composite row key has
no honest rendering — multi-key is a registration error, not a convention).
End-to-end lock: `engine/tests/events.rs::scan_route_fans_out_to_the_hit_rows`.

**Residual (an okm-dynamic alignment item, not a design divergence)**:
okm-dynamic's declared index (`AccessMethod`) rejects variable-width fields
outright (`fields_width`), while the COMPILE-TIME mode (okm-core `KvIndex`
+ okm-derive) SUPPORTS them — `IndexFuncResult for String` documents "raw
UTF-8, no length prefix, text-first", and the derive's validation rule is
"at most one variable-width field, and it must be LAST"
(`okm-derive/src/schema.rs`: the primary-key tail is cut from the entry's
END, so a trailing variable-width segment still locates); aura's own
`EventName`/`InstanceKeyRegistry` (a by_name index over a String) is the
compile-time mode's living example. The dynamic mode's primary-key tail is
equally fixed-width (`schema.key_len`), so `okm_core::scan_index`'s
tail-truncation logic applies directly — this is okm-dynamic LAGGING the
existing okm-core shape, an implementation gap needing no new entry shape
and no framing. Once aligned, a scan-route probe matches a String field
(last index field) directly. Until then, the capacity constraint on a
variable-length business key lives explicitly in the schema (a fixed-width
field) — a declaration-time constraint, not a runtime hash alias (any
"digest index" would reopen the aliasing class ADR-0038/0042 just closed;
not done).

### 8.2 The EventRoute row shape (LANDED, Phase 4.13)

The `key_field: String` payload is replaced by the reference payload: a
`resolution: u8` tag (0 singleton / 1 payload field / 2 scan) plus
collection/index/probe_field name columns. **Ruled (ADR-0038 §3)**: a
reference carries NO collection ns number — the row stores the
collection/index/probe **names** (resolution lives with the schema owner),
while a stored slot/ns number would make position the identity and any
schema edit would silently re-point the row. The three declaration shapes
(singleton / payload resolution / index scan) are three mechanisms,
discriminated at the row structure layer, never by sentinel mixing.
**Implemented** (Phase 4.13).

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

Landed fallout (done; the resolver was named `bound_partition` at the time,
`bound_instance_key` since ADR-0041): the resolver chain, the test call sites,
`by_type` → `by_booth`, `split_once('/')` gone. ~~Residual (not
covered): the INSTANCE key space still uses the `"__singleton__"` sentinel
string…~~ **Residual CLOSED (2026-10-09, ADR-0042, landed with Phase 4.13)**:
`InstanceId.key` became the `aura_booth::InstanceKey { Singleton, Named }`
enum; the sentinel string retired from the framework — the singleton is a
variant, not a value, and the literal aliasing is structurally unreachable
(commit `33ef84e`).

### 8.4 Partition identity, keyspace bands, and the cursor retention promise (landed, ADR-0039 §1/§2, ADR-0040)

The instance key becomes a proxy dictionary (named `PartitionName` when ruled; the
hash is gone); the low block has two bands (the event plane 20–29, the meta plane
30–39, conceptual order inside a band); `cursor_ttl` is global (default 30 days) and
expiry removes a row from the denominator without deleting it. The mechanism and the
reason are in §6.3 and §7.

Landed fallout (done): the `#[ok_ns]` renumbering; the `InstanceKeyRegistry` table
(named `PartitionName` when ruled) plus `mq::InstanceKey` as the structural marker; the
tightened key widths; `mq { cursor_ttl }` in `crates/config` + the realm field;
`MqCursor.last_active_ms` (v2 hot-tail + the `0` sentinel); the denominator change
(`booth_subscribes`) plus inert-row reclamation (`drop_cursor`); the cursor-key
identifiers `type_id` → `booth_id` (ADR-0032's booth-naming sweep reaching the field
layer); seq changed from `max(now_ms, last+1)` to a pure sequence (the reason lives in
§2 step 5 and is not repeated here).
**2026-10-08 (ADR-0041)**: the vocabulary moves to the instance key, `MqHead` (ns 24)
is retired in favor of MqData's `HighWater(seq)`, and `#[ok_partition(2)]` leaves
MqCursor — see §2/§7 and ADR-0041; not repeated here.
An EXISTING deployment must wipe the low block (the cost is §7's numbering-discipline
paragraph; ADR-0040 records it).

### 8.5 Misc backlog (still open)

- `events_matching` incrementality (wildcard re-expansion is a 50 ms full
  rescan per cycle; the singleton narrowing already bounds the cost to one
  consumer — worth caching only at a large vocabulary, Windmill gate).
- cold call over the wire (needs a Phase 6 consumer; never build ahead of one).