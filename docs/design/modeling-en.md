# Aura Modeling Guide (Booths Guide)

How to model a domain onto Aura booths: pick the instance key first (the standing-ownership criterion), then define events and handlers, keep state instance-scoped, and collaborate across instances through events and projections. This guide is the application-side norm; for mechanisms see [partitioning.md](partitioning-en.md), [realm.md](realm.md), [booth-api.md](booth-api.md).

> **Languages:** [中文](modeling.md) · English (this file)

## Step 1: pick the instance key for each booth

An booth type = one piece of code + the swarm of instances expanded by its instance key. The criterion for choosing a key is the **instance's standing ownership**, not a field the request happens to carry:

- **Identity equals ownership → use the identity as the key.** User-scoped data (carts, sessions, user profiles) partitions by user_id. The instance identity itself encodes the user: the handler reads `ctx.self_id.key` — a construction-level guarantee (the instance belongs to its key alone), stronger than caller-reported identity, nothing to spoof.
- **Ownership exceeds identity → use the ownership as the key; identity rides as a parameter.** Group chats, rooms, order collaboration partition by channel_id/room_id/order_id: one instance serves many users, and user_id is not the instance's standing attribute. Messages **carry their own ownership key** (the client knows which channel it is posting to; no engine-side lookup), and the sender's identity rides as a request parameter (membership checks, attribution inside the handler). Mounting user_id onto ctx here is a logical conflict — ctx is per-instance, so a mounted field would claim "this instance's user", and a group-chat instance has no such thing.
- **Singleton (key-less) → global observers only.** Wildcard subscriptions (`@on("order.*")`) and key-less handlers land on the singleton instance — they have no state-sharding semantics by nature; never place partitionable data there.

In one sentence: **the instance key answers "who serially processes this message"; the request parameters answer "who initiated this request"** — two questions, answered independently, never mounted onto each other.

```
Partitioned by user_id (cart)        Partitioned by channel_id (group chat)

emit("add_to_cart",                  emit("channel_msg",
  { user_id: "u1", ... })              { channel_id: "c1",
       │                                 caller: { user_id: "u1" }, ... })
       ▼                                      │
("cart", "u1") instance                       ▼
handler reads ctx.self_id.key = "u1"   ("channel", "c1") instance
— identity comes from the instance     handler reads caller.user_id
  — nothing to pass                    — ownership comes from the
                                         instance key, identity from
                                         the request parameter
```

## Step 2: define events and the subscription surface

Events are the only collaboration channel between booths (field pub/sub, see booth-api.md):

- **The event name is the addressing name**: `@on(event, key=...)` declares what each handler listens to; the delivery instance key is taken from the event data's key field (e.g. `key="channel_id"` reads `data.channel_id`), falling to the `__default__` catch-all instance when missing — so emitted events **must carry the field declared as key**, otherwise everything piles into the catch-all.
- **emits are not declared** (ADR-0012): the receiver set is a runtime fact; events without subscribers land in the dead-event ring (an observable audit surface). Modeling requires no "who listens" ledger — and must not keep one.
- **One queue, many subscribers** is structural: several booth types may consume the same event (e.g. `order.created` feeds both `inventory` and `audit`), each with its own cursor, mutually independent.
- **Direct calls (`ctx_invoke`) are the exception path**: for request-response shapes; the payload must declare the target `{type, key, handler, args}`. Prefer events when they can express the collaboration — events leave a delivery record and admit multiple subscribers by nature.

## Step 3: state lives in the type's collections

State is stored at the TYPE level (ADR-0026 §3): each booth type occupies one real okm ns, the type declares its collections (schema persisted with the interface_schema at upload), and instances are documents inside the type's ns; handlers read/write through `ctx.store.emit(op)`. Modeling constraints:

- **Collections hold only data the type standingly owns**: the cart type holds its line-item documents; the channel type holds its member table and recent messages. Copying another type's data in creates a second source of truth.
- **Cross-type reads are inexpressible**: by design, not limitation — storage addressing binds to the type's ns at registration; cross-type collaboration goes through events (the other booth processes and emits the result), or a direct call fetches the answer.
- **Same-type cross-instance reads are an ordinary scan/reduce**: the type's collections are visible to all its instances — that is what type-level isolation is FOR (the type's code is trusted logic).
- **State does not replicate across nodes** (federation ruling ADR-0013): data follows its home node; node-level deployment choices are in partitioning-en.md §5.

## Step 4: cross-instance aggregation goes through projection booths

Cross-partition queries (aggregate every user's cart in a department; which channels a user is in) cannot JOIN and must not full-scan — use a **projection booth**:

- An ordinary event-receiving booth, partitioned along its own dimension (dept_id or user_id), `@on`-subscribed to events the upstream instances emit, continuously aggregating them into its own collections. **Same-type instance aggregation no longer needs a projection** (ADR-0026 §3: an ordinary scan/reduce inside the type's ns); projections remain only for cross-type precomputation.
- Queries read the projection instance's state directly (direct call) — the same principle as stream pre-aggregation: computed when not queried.
- A projection is **rebuildable derived data**: the upstream event stream is the truth; a lost projection state can be rebuilt by replaying events (mq queues retain the range above active subscribers' watermarks).

```
("cart","u1") ──emit cart_updated──► event queue
("cart","u2") ──emit cart_updated──►      │
                                          ▼
                          ("dept_stats", "d7")  ← projection partitioned by dept_id
                          dept_totals collection: { dept_total, ... }
                                          ▲
                        query: ctx_invoke(dept_stats, "d7")
```

## Residency: whether to stay warm, in three ledgers

Sleeping never loses data (state is durable, events stay in the queue); residency saves the **next activation's cost**: re-spawning the VM/probe, reloading the script, rebuilding in-memory state. `idle_ttl` is declared per booth type (`with_idle_ttl` builder, or the lifecycle section of `interface_schema`); not resident is the default — decide by comparing activation cost against the usage pattern:

- **State-mutation flows (add to cart, change password) → no residency**: when the next operation appears is unpredictable; residency is pure waste. State is durable; activation rebuilds it.
- **Stateless high-fanout queries (product listings) → depends on sharing**: when every query returns non-repeating content with nothing to cache, all that remains to save is the probe/VM start cost. If the listing is per-user, one user's refresh rate never beats the start cost → no residency. If the listing is shared by all users (the instance is a singleton or a handful of partitioned instances) and requests keep arriving → short residency (e.g. a 10s idle_ttl) — the effect is exactly a traditional cache, except the warm instance **is** the cache; no second caching facility.
- **Long-lived sessions (gravity conversations, chat channels) → long residency**: real-time streaming applications; an active channel's messages nearly never stop, and within the residency window the activation cost is zero. When LLM call costs dwarf instance residency costs, residency is the obvious choice — exactly the turn-executor shape (Phase 6.5): long per-type TTL, same-session consecutive calls ride the in-memory oneshot, released at turn end or window expiry.

The criterion in one sentence: **stay resident when the money saved (activation cost × expected arrivals within the window) exceeds the money spent (memory × window duration)** — per-type TTL is the mechanism that turns this judgment into a single declaration.

## High-frequency state shape: game rooms (direct fjall writes vs session memory)

A real-time game room is the extreme case of state frequency, and it forces a boundary that was previously implicit — **how far direct collection writes into fjall scale, and when session memory becomes necessary**.

The arithmetic first: 100 players × 20Hz tick = 2,000 field writes/s per room. A direct fjall write (in-process, no wire protocol round trip, no socket, no RTT) costs ~1µs, so a room consumes ~0.2% of one core — **direct fjall writes comfortably cover casual and mid-scale rooms**. Compare an external Redis at the same write rate: ~100µs RTT per write plus every room queued behind a single-threaded event loop. The in-process engine is two orders of magnitude faster; this is the "hierarchy is a physical constraint" principle applied again — when the state's consumer (the handler) and the storage live in the same process, out-of-process storage (Redis/Kafka) is a structural disadvantage, not a tuning problem.

The real costs on the direct-write path are not fjall itself but two secondary ones:

- **JSON ↔ DynamicValue conversion** (the value.rs seam): once per field. Merge each frame into ONE snapshot write instead of 100 tiny per-field writes.
- **Document-granularity semantic mismatch**: okm's per-field document write is designed for low-frequency fields; "one snapshot per frame" must collapse into a single put to keep snapshot semantics.

Beyond that scale (denser ticks, more fields, more rooms), switch to the **session memory** shape:

```
Direct fjall writes (default, casual/mid-scale)    Session memory (heavy: denser ticks / more fields)
  One whole-frame snapshot put per tick into        Hot state lives in the resident session
  the collection = one native-encoded write,          (process memory) = zero serialization,
  ~µs                                                 plain memory-array access
  Crash recovery for free (state is always in        plain memory-array access
  the engine), no dual-authority problem             Crash recovery via event replay (the MQ
                                                      partition replays the input stream) or
                                                      low-frequency checkpoints into the
                                                      collection (lose N seconds)
```

Decision criteria, in priority order:

1. **Default to direct fjall writes** — the structurally cleaner shape: single source of truth (state is always in the engine), free crash recovery, zero ops surface. Confirm the budget first (write rate × field count × room count against the ~0.2%/room measurement); do not jump to memory on instinct.
2. **Session memory only when the direct-write budget is exceeded** — and switching brings its recovery plan with it (event replay first: input events are already persisted in the MQ partition; replay = the existing backlog scan + cursor; a snapshot is merely a replay accelerator) and accepts the dual-authority boundary (session memory is the hot authority, the collection the low-frequency truth).
3. **Either way, frame-merge the input events** (clients sample at 20Hz; the server merges all inputs within one tick into a single event before it lands in the MQ) — standard input sampling for real-time multiplayer; it is what bounds the MQ write path.

One-line criterion: **direct fjall writes are the default; session memory is an upgrade for "frame budget exceeded AND a replay/checkpoint plan exists", never the starting point.**

The three-tier game-server mapping (prism connections / aura logic / probe execution) and the frame-driven vs message-driven gap: see PLAN Phase 8 and partitioning.md §5.

## Outbound delivery: where the field's boundary sits

The field's event delivery covers booth ↔ booth; pushing messages to connections outside the field (a user's WS connection) is **outbound delivery**, borne by the outer connection plane (prism, Phase 8) — aura never knows WS exists:

```
Group-chat channel booth (instance key c1)
  │  after handling one group message, deliver to every online user in the channel
  ▼
emit("outbound_message", { user_id: "u1", payload: ... })   ← one event per target user
emit("outbound_message", { user_id: "u2", payload: ... })
  │
  ▼  subscriber: the connection plane's outbound bridge (singleton, assembled on the prism side)
prism finds each user's WS connection by payload.user_id and pushes downstream
  │  no connection (offline) → the bridge parks it in a per-user offline queue / drops it (application semantics decide)
  ▼
the user's browser receives the push
```

Key points:

- **Booths only emit; they never address connections**: the emit payload carries the target user_id; the WS connection registry lives in the connection plane — the booth layer never learns "who is online, where connections are". The same boundary application of "instance key answers routing, parameters answer identity".
- **The outbound bridge is an ordinary subscriber**: a singleton wildcard consumer of `outbound.*`, no special channel — isomorphic to a projection booth, except its side effect is delivering instead of writing state.
- **The inbound direction is already solved**: client messages enter the field through prism by event name (`emit("channel_msg", ...)`, channel_id riding in the message) — see the group-chat example in Step 1.
- **Offline users**: the emit lands in the persistent queue; per-user session/offline logic is carried by an ordinary booth partitioned by user_id (or the connection plane's offline queue) — an application modeling decision, not an engine mechanism.

## Step 5: business data import/export goes through a dedicated booth

The engine provides no business-data channel; bulk interaction with external storage (S3, files, external databases) is carried by an ordinary booth: `@on("import_users")` receives a batch → the handler writes through the external channel → emits a completion event. Isomorphic to a projection booth — the same event model covers it, no second class of infrastructure.

## Quick reference

| Need | Shape |
|---|---|
| A user's own data | Booth partitioned by user_id; identity read from `ctx.self_id.key` |
| A shared collaboration space | Partition by channel_id/room_id; the ownership key rides in the message, the initiator's identity as a parameter |
| Global observation / audit | Wildcard-subscribed singleton booth |
| Cross-partition stats / reverse lookup | Projection booth (partitioned by the aggregation dimension) |
| Request-response | `ctx_invoke` (declares the handler); prefer events |
| External data import/export | Dedicated booth wrapper |
| Long waits on external results | Timer wheel / event re-entry, no residency held |
| Residency decision | per-type `idle_ttl`: mutation flows don't stay; shared high-frequency queries stay briefly (cache shape); long sessions / LLM calls stay long |
| Pushing to online users | emit `outbound_message` (user_id in the payload) → the connection plane's outbound bridge pushes over WS; offline users via a per-user queue |
| Per-frame high-frequency state (game rooms) | Default: direct fjall writes (whole-frame merged single put); switch to session memory + event-replay recovery only when the budget is exceeded |
