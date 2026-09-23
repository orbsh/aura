# Aura Modeling Guide (Actors Guide)

How to model a domain onto Aura actors: pick the partition key first (the standing-ownership criterion), then define events and handlers, keep state instance-scoped, and collaborate across instances through events and projections. This guide is the application-side norm; for mechanisms see [partitioning.md](partitioning-en.md), [realm.md](realm.md), [actor-api.md](actor-api.md).

> **Languages:** [中文](modeling.md) · English (this file)

## Step 1: pick the partition key for each actor

An actor type = one piece of code + the swarm of instances expanded by its partition key. The criterion for choosing a key is the **instance's standing ownership**, not a field the request happens to carry:

- **Identity equals ownership → use the identity as the key.** User-scoped data (carts, sessions, user profiles) partitions by user_id. The instance identity itself encodes the user: the handler reads `ctx.self_id.key` — a construction-level guarantee (the instance belongs to its key alone), stronger than caller-reported identity, nothing to spoof.
- **Ownership exceeds identity → use the ownership as the key; identity rides as a parameter.** Group chats, rooms, order collaboration partition by channel_id/room_id/order_id: one instance serves many users, and user_id is not the instance's standing attribute. Messages **carry their own ownership key** (the client knows which channel it is posting to; no engine-side lookup), and the sender's identity rides as a request parameter (membership checks, attribution inside the handler). Mounting user_id onto ctx here is a logical conflict — ctx is per-instance, so a mounted field would claim "this instance's user", and a group-chat instance has no such thing.
- **Singleton (key-less) → global observers only.** Wildcard subscriptions (`@on("order.*")`) and key-less handlers land on the singleton instance — they have no state-sharding semantics by nature; never place partitionable data there.

In one sentence: **the partition key answers "who serially processes this message"; the request parameters answer "who initiated this request"** — two questions, answered independently, never mounted onto each other.

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
                                         partition key, identity from
                                         the request parameter
```

## Step 2: define events and the subscription surface

Events are the only collaboration channel between actors (field pub/sub, see actor-api.md):

- **The event name is the addressing name**: `@on(event, key=...)` declares what each handler listens to; the delivery partition key is taken from the event data's key field (e.g. `key="channel_id"` reads `data.channel_id`), falling to the `__default__` catch-all instance when missing — so emitted events **must carry the field declared as key**, otherwise everything piles into the catch-all.
- **emits are not declared** (ADR-0012): the receiver set is a runtime fact; events without subscribers land in the dead-event ring (an observable audit surface). Modeling requires no "who listens" ledger — and must not keep one.
- **One queue, many subscribers** is structural: several actor types may consume the same event (e.g. `order.created` feeds both `inventory` and `audit`), each with its own cursor, mutually independent.
- **Direct calls (`ctx_invoke`) are the exception path**: for request-response shapes; the payload must declare the target `{type, key, handler, args}`. Prefer events when they can express the collaboration — events leave a delivery record and admit multiple subscribers by nature.

## Step 3: keep state instance-scoped

`ctx.state` is the instance's own KV state (`ctx_state_get/set/delete`), durable per field, loaded/written wholesale with wake/sleep. Modeling constraints:

- **Only data the instance standingly owns**: a cart instance holds its line items; a channel instance holds its member table and recent messages. Copying another instance's data in creates a second source of truth.
- **Cross-instance reads are inexpressible**: by design, not limitation — cross-instance collaboration goes through events (the other actor processes and emits the result), or a direct call fetches the answer.
- **Instance state does not replicate across nodes** (federation ruling ADR-0013): data follows its home node; node-level deployment choices are in partitioning-en.md §5.

## Step 4: cross-instance aggregation goes through projection actors

Cross-partition queries (aggregate every user's cart in a department; which channels a user is in) cannot JOIN and must not full-scan — use a **projection actor**:

- An ordinary event-receiving actor, partitioned along its own dimension (dept_id or user_id), `@on`-subscribed to events the upstream instances emit, continuously aggregating them into its own ctx.state.
- Queries read the projection instance's state directly (direct call) — the same principle as stream pre-aggregation: computed when not queried.
- A projection is **rebuildable derived data**: the upstream event stream is the truth; a lost projection state can be rebuilt by replaying events (mq queues retain the range above active subscribers' watermarks).

```
("cart","u1") ──emit cart_updated──► event queue
("cart","u2") ──emit cart_updated──►      │
                                          ▼
                          ("dept_stats", "d7")  ← projection partitioned by dept_id
                          ctx.state: { dept_total, ... }
                                          ▲
                        query: ctx_invoke(dept_stats, "d7")
```

## Residency: whether to stay warm, in three ledgers

Sleeping never loses data (state is durable, events stay in the queue); residency saves the **next activation's cost**: re-spawning the VM/probe, reloading the script, rebuilding in-memory state. `idle_ttl` is declared per actor type (`with_idle_ttl` builder, or the lifecycle section of `interface_schema`); not resident is the default — decide by comparing activation cost against the usage pattern:

- **State-mutation flows (add to cart, change password) → no residency**: when the next operation appears is unpredictable; residency is pure waste. State is durable; activation rebuilds it.
- **Stateless high-fanout queries (product listings) → depends on sharing**: when every query returns non-repeating content with nothing to cache, all that remains to save is the probe/VM start cost. If the listing is per-user, one user's refresh rate never beats the start cost → no residency. If the listing is shared by all users (the instance is a singleton or a handful of partitioned instances) and requests keep arriving → short residency (e.g. a 10s idle_ttl) — the effect is exactly a traditional cache, except the warm instance **is** the cache; no second caching facility.
- **Long-lived sessions (gravity conversations, chat channels) → long residency**: real-time streaming applications; an active channel's messages nearly never stop, and within the residency window the activation cost is zero. When LLM call costs dwarf instance residency costs, residency is the obvious choice — exactly the turn-executor shape (Phase 6.5): long per-type TTL, same-session consecutive calls ride the in-memory oneshot, released at turn end or window expiry.

The criterion in one sentence: **stay resident when the money saved (activation cost × expected arrivals within the window) exceeds the money spent (memory × window duration)** — per-type TTL is the mechanism that turns this judgment into a single declaration.

## Outbound delivery: where the field's boundary sits

The field's event delivery covers actor ↔ actor; pushing messages to connections outside the field (a user's WS connection) is **outbound delivery**, borne by the outer connection plane (prism, Phase 8) — aura never knows WS exists:

```
Group-chat channel actor (partition key c1)
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

- **Actors only emit; they never address connections**: the emit payload carries the target user_id; the WS connection registry lives in the connection plane — the actor layer never learns "who is online, where connections are". The same boundary application of "partition key answers routing, parameters answer identity".
- **The outbound bridge is an ordinary subscriber**: a singleton wildcard consumer of `outbound.*`, no special channel — isomorphic to a projection actor, except its side effect is delivering instead of writing state.
- **The inbound direction is already solved**: client messages enter the field through prism by event name (`emit("channel_msg", ...)`, channel_id riding in the message) — see the group-chat example in Step 1.
- **Offline users**: the emit lands in the persistent queue; per-user session/offline logic is carried by an ordinary actor partitioned by user_id (or the connection plane's offline queue) — an application modeling decision, not an engine mechanism.

## Step 5: business data import/export goes through a dedicated actor

The engine provides no business-data channel; bulk interaction with external storage (S3, files, external databases) is carried by an ordinary actor: `@on("import_users")` receives a batch → the handler writes through the external channel → emits a completion event. Isomorphic to a projection actor — the same event model covers it, no second class of infrastructure.

## Quick reference

| Need | Shape |
|---|---|
| A user's own data | Actor partitioned by user_id; identity read from `ctx.self_id.key` |
| A shared collaboration space | Partition by channel_id/room_id; the ownership key rides in the message, the initiator's identity as a parameter |
| Global observation / audit | Wildcard-subscribed singleton actor |
| Cross-partition stats / reverse lookup | Projection actor (partitioned by the aggregation dimension) |
| Request-response | `ctx_invoke` (declares the handler); prefer events |
| External data import/export | Dedicated actor wrapper |
| Long waits on external results | Timer wheel / event re-entry, no residency held |
| Residency decision | per-type `idle_ttl`: mutation flows don't stay; shared high-frequency queries stay briefly (cache shape); long sessions / LLM calls stay long |
| Pushing to online users | emit `outbound_message` (user_id in the payload) → the connection plane's outbound bridge pushes over WS; offline users via a per-user queue |
