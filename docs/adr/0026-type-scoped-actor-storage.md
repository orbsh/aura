# 0026 — Type-scoped actor storage: one real ns per actor type, capability-level ctx ops

> **Languages:** [English](0026-type-scoped-actor-storage.md) (primary) · [中文](0026-type-scoped-actor-storage.zh-CN.md)

**Status:** Accepted (2026-09-24) — design; implementation pending, see Consequences

## Context

The current storage model gives each actor **instance** one flat state document (`InstanceState`, `realm/src/state.rs`) addressed through `ctx.store.get/set/delete` — field-level point reads and writes scoped to the instance's own document. The instance key `(actor_type, key)` simultaneously answered three questions: message serialization, storage isolation, and recovery granularity.

Three pressures expose this as over-isolation on the storage axis:

1. **The point interface cannot carry real models.** `get/set` on one document's fields has no scan, no index use, no reduce. Complex logic (per-type aggregation, secondary views, composite keys) cannot be expressed inside one actor and is forced outward — split into more actor types, or into projection actors whose only job is compensating for the interface's weakness.
2. **The isolation is stronger than the threat model.** An actor type's code is uploaded, trusted logic — not an arbitrary tenant query surface. Structural isolation between TYPES is warranted (one type must not reach another type's data); structural isolation between INSTANCES of the same type is not — the type's own code legitimately wants to see across its instances (that is what its ns is FOR).
3. **The k10r shape.** A krystallizer-class application is a global singleton whose internal sharding cannot and need not be planned up front. Under the current model it would have to either model everything as one instance's flat fields or pre-partition around `user_id` — both distort the model to fit the interface.

The corrected division of labor: **the instance key answers exactly one question — "who serially processes this message" (routing/serialization/recovery); it stops deciding storage isolation.** Storage isolation moves to the type level.

## Decision

### 1. Each actor TYPE occupies one real okm ns

Actor types are a DECLARED, deployment-scale vocabulary — the application author writes every type by hand, so the count is bounded by design (tens per deployment, not per user). This satisfies the ruling that closed, bounded vocabularies may take real ns allocations while open, unbounded ones may not (events/partitions stay on registry + hash shapes, ADR-0014's mq block).

- The low ns block is reserved for aura itself: mq tables (30–35), meta/state (40–41), future framework planes. Actor types allocate from a fixed base above the reserved block.
- Allocation happens at `register_type` as a side effect of the type registry (the same registry that assigns `type_id` today — the dynamic actor→ns registry this design requires already exists in embryo); ns ids are never reused within a node's lifetime.
- Namespace isolation (the Phase 3.6 mechanism, today the prefix-bound `MqStore::namespaced` handle) keeps its MECHANISM as orthogonal — it prefixes the whole engine, beneath which actor-type nss live. Its binding dimension is an APPLICATION DECISION (demoted, PLAN Phase 4.10 LANDED): gravity may bind user, a no-user application binds nothing; "probe registration credential = user credential → namespace derived" is superseded.

### 2. Instances are documents inside the type's ns

The `InstanceState` model gives each INSTANCE one document as its whole world — the state document IS the isolation unit. That shape is superseded. In okm's current vocabulary (collection/document, replacing table/row), the new unit of visibility is the NS: an actor type's ns plays the role a SQL schema plays in a database — the type declares its own COLLECTIONS (via its schema, §4) inside it, and instances are documents among them. What was "one document per instance" becomes "many collections per type, one document per (collection, instance, key)". A global-singleton type is the degenerate one-instance case; cross-instance aggregation inside one type is an ordinary scan/reduce over the type's own ns — **the projection actor retires for same-type aggregation** (it existed to compensate for the point interface) and remains only for cross-type aggregation (precomputation over several types' data).

Serialization semantics are unchanged: same `(type, key)` serial through the instance queue, cross-key parallel. What changes is only what the storage layout isolates.

### 3. ctx storage ops rise to okm capability level

`ctx.store` gains the okm operation set over the type's declared collections — put / get / scan / reduce (exact op names set at implementation; the surface is what okm's Collection API exposes over dynamic documents). The field-level `get/set/delete` point model is replaced by this surface.

- **Binding is structural**: every ctx storage handle is constructed against the owning type's ns at registration — cross-type access is not expressible, the same construction-time guarantee the mq namespace prefixes give today. Inside the ns, the type's own code is trusted with the full surface.
- **Script-side protocol: one `emit` carrying okm ops** (this fixes the role of the §1 emit naming on the storage path): `ctx.store` exposes exactly ONE interface — `ctx.store.emit(op)`, where op is one okm instruction (collection name + operation + arguments, `DynamicValue` payload), returning DynamicValue. Two shapes by language:
  - **python**: the script implements okm's `VirtualStorage` adapter, translating each engine call into one `ctx.store.emit` — from there the script uses the `Collection` API directly (the typed facade over put/get/scan/…); the bridging cost is paid once, in the adapter.
  - **steel / nushell**: no adapter — direct single-op `emit` calls (op set = the narrowed set from the op-set work item; ops stay at the Collection semantic layer, not raw VirtualStorage primitives).
  - **wasm (Rust source) is the full-power path**: okm itself compiled into the module — the script implements `VirtualStorage` over `aura_host` imports (each engine call = one emit op through the host bridge) and runs the REAL `Collection` API in-module. Static derive macros work at wasm build time; the dynamic op-instruction path exists for the actor, not for wasm. A wasm actor's storage code is then indistinguishable from a native Rust actor's: same derive, same invariants, compile-time checked.
  - Naming lineage: `emit` reuses the event-delivery shape (an instruction is a payload delivered through the host bridge), same root as the wire-level `ev` — one verb, two sites (wire delivery, storage instruction), one semantics: hand the receiver a fact to execute.
- **Nested invoke addressing**: when actor A invokes actor B, B's handlers operate in B's ns — resolution goes through the type registry (type name → ns), never through caller-supplied keys.

### 4. Dynamic schema declares through interface_schema

Tables and their key/index declarations ride the existing Phase 4.5b upload lifecycle:

- **python**: the okm type definitions in the script are wrapped with a decorator that derives the schema; introspection merges it into `interface_schema` (the same implicit+explicit merge as `@on` metadata, ADR-0014's step 1). One declaration surface, derived.
- **steel / nushell**: no AOP — the schema is a hand-written literal inside `interface_schema` (data, not derivation); handlers call the ctx store functions directly. nushell additionally has no host bridge in its PTY carrier, so its position is unchanged: memory-state only until that bridge lands.
- **wasm** (Rust source): okm compiles into the module — schema declaration is the derive macros at build time, no dynamic interface_schema form needed for storage; the delivered `.wasm` artifact carries its schema in code, introspection reads it out at upload (per the 4.5b lifecycle).
- The merged schema persists with the ActorDef (dynamic segment) at registration; **the execution path never regenerates schema** — type→schema cost exists only at upload. `ctx.interface_schema` reads the persisted copy (cheap), for handlers that reflect over their own declared shape; development-time completion is served by the LLM receiving the same interface_schema.

## Naming: one event semantic end to end

Protocol and concept layer carry ONLY `event`. The wire field is `ev` — in both directions; the protocol does not encode direction. `emit`/`on` are per-end implementation details: on aura, an actor's `@on` declaration and `emit` call; on prism's client side, `ws.send` / `ws.on`. Prism is the natural extension of aura's event semantics to the user end — there is no separate "action→server, type→client" vocabulary to translate through. This supersedes the earlier prism-facing phrasing ("client sends actions, server sends typed frames") wherever it appears; the server→client frame field ruling (never reuse `action` across directions) is satisfied a fortiori: the field is `ev`, direction does not exist at the protocol layer.

## Honest semantic cost

- **ns space is spent on actor types.** That is the point of the bounded-vocabulary ruling — but it makes the reservation real: a deployment that registers and re-registers many throwaway type names burns ns ids (they are never reused). Registration-side dedup (same source hash → same type) is the mitigation; the budget is u16-wide, thousands of types.
- **The one-document-per-instance state model is a breaking replacement.** Existing instance-state bytes are discarded, no migration (the ADR-0018 precedent). Tests written against `ctx.store.get/set` field semantics rewrite against the collection surface.
- **Inside-ns trust is real trust.** A buggy (not malicious) handler can now scan and reduce over its whole type's data — the blast radius of a script bug grows from one instance's document to the type's keyspace. Accepted: the code is uploaded and trusted; the boundary that matters (type vs type, user vs user) stays structural. Same for direct okm primitives through emit: the developer holds full control of the storage — bypassing Collection invariants (a raw put skipping index/reduce compensation) is self-sabotage only, no guard is built.

## Consequences

- LANDED (2026-09-24, probe bbefac8 + aura 7828031): the retirement is done — `realm/src/state.rs` (`StateDocumentStore`/`InstanceState`) deleted whole; the `StateStore` trait, `SharedStore`, and `Ctx.state` are gone from aura-actor; the `ctx_state_get/set/delete` host fns and their wire arms (`HostOp::State`) are gone from the engine and probe-protocol. `ctx.store` exposes exactly `ctx.store.emit(op)` + `ctx.interface_schema` over declared collections. **No backward-compat sugar, period**: the reason to retire is the model ruling (point document superseded by the collection surface), not a migration-cost calculation — "users already exist" is as invalid as "no external users yet" as an argument for keeping the old surface; correctness of the model is the only input. `register_in` rides the same introspection+persist path as `register` so namespaced types resolve a ctx.store plan. Remote-probe actors are invoke-only over the wire (execution nodes hold no state; `ctx_store_emit` requires a resolved plan = 4.5b). Remaining Phase 4.9 work: per-language schema declaration, docs/wiki sweep.
- Serialization, instance routing, timer, and mq semantics are untouched — this ruling moves storage isolation only.
- Projection actors remain exactly where they were always justified: cross-type precomputation. Same-type aggregation becomes an ordinary scan inside the type's ns.
- Prism protocol naming (`ev`, direction-free) lands on the prism side with its connection-plane work (Phase 8); aura's side is documentation only.
- Wiki sync (`~/.hermes/wiki/aura-architecture.md`) is due when the implementation lands, together with the storage.md/partitioning.md rewrites — this ADR supersedes the "instance state is one document" passages there.
