# 0030 — MqStore handle semantics: one shared engine, no Mutex

> **Languages:** [English](0030-mqstore-handle-semantics.md) (primary) · [中文](0030-mqstore-handle-semantics.zh-CN.md)

**Status:** Accepted (2026-09-26) — revised same day: step 1 superseded, the okm
trait relaxation (its ADR-0026) landed first and the terminal shape is implemented
directly

## Context

`realm/src/mq.rs` defines the store every mq table and (via `ns_raw`) every wasm
storage plane binds to:

```rust
pub struct MqStore {
    prefix: Vec<u8>,
    inner: Arc<Mutex<MqEngine>>,
}
```

Two findings shaped this ADR.

**Finding 1: the derived handles change the lock boundary silently.**
`MqStore::for_realm` and `MqStore::ns_raw` do not clone the handle — they
decompose and re-wrap it:

```rust
Self { prefix: p, inner: Arc::new(Mutex::new(inner.inner.lock().unwrap().clone())) }
```

Every derived realm/type handle therefore carries its OWN mutex. The base `MqStore`
and its `for_realm` children do not serialize against each other; only the engine
inside (fjall's own internal synchronization) serializes anything. Nothing breaks
today — every `VirtualStorage` op is a single-shot put/get/scan with no cross-handle
batch — but the code reads as if `Arc<Mutex<>>` were one shared coordination point,
and it is not. A future reader adding a multi-op invariant at the mutex would be
wrong on exactly the handles created by the two constructors above.

**Finding 2: the `Arc<Mutex<>>` exists only because okm's trait says `&mut self`.**
okm-core's `VirtualStorage` declares `put(&mut self, …)` and `del(&mut self, …)`
(storage.rs:28–31). Yet every real engine behind the trait is already an
Arc-inner cheap-clone handle that manages its own concurrency:

- `FjallStore` — `#[derive(Clone)]`, documented "Clone IS a shared handle
  (Arc-inner)"; fjall's `Database`/`Keyspace` are internally synchronized.
- `TestStore` — the arms wrap `Arc<SlatedbSync>` / owned `FjallStore` /
  `RedbStore` (itself Arc-wrapped: "clones share the underlying keyspace").
- `SlatedbSync` — fields are `Arc<Runtime>` + `Db` (both shared-handle types).

The `&mut` on `put`/`del` is therefore *over-specification*: it does not buy
exclusivity at the data layer (the engine already has it), but it does force every
consumer to own a `&mut` path — which is why `MqStore` wraps the engine in
`Arc<Mutex<>>` at all, and why aura's call sites read `self.mq.clone()` into a local
to obtain `&mut`. The trait already acknowledges shared-handle semantics one layer
up: `SharedVirtualStorage::shared_handle()` returns "a handle to the same physical
engine. Cheap; shares all state." The two statements of the contract disagree: the
handle is shared, the operations pretend it is not.

## Decision

1. **The okm trait relaxes first (okm ADR-0026): `put`/`del` take `&self`.** The
   reasoning below (§3 of the original draft) is the justification okm's ADR
   adopts. Every shipped engine is already a synchronized shared handle, so the
   `&mut` communicated a false exclusivity requirement and taxed consumers with
   outer mutexes; `SharedVirtualStorage::shared_handle()` already stated the
   true semantics. A counter-argument exists and is REJECTED there: "`&mut` lets
   a stateful engine use cheaper interior code paths (buffer reuse across
   writes)". No shipped engine does this; and the one place `&mut` was genuinely
   used by engines — redb's table-creation-on-first-write — is equally reachable
   through interior mutability, which redb already requires. If a future engine
   needs `&mut`, it wraps itself in a Mutex internally — the exclusivity belongs
   to the engine that needs it, not to the contract imposed on all.

2. **MqStore drops the Mutex entirely (the terminal shape, implemented).**
   With the relaxed trait, `MqStore` reduces to
   `{ prefix: Vec<u8>, inner: MqEngine }` — clone is a pure cheap handle
   (Arc clone inside the engine + a few prefix bytes), the Mutex layer and the
   per-handle lock-boundary divergence disappear together, and `ns_raw`/
   `for_realm` become pure prefix assembly over `inner.inner.clone()`. The
   originally planned step 1 (sharing the one `Arc<Mutex<>>` as an interim fix)
   is SUPERSEDED — it was correct under the old trait, but landing it first
   would have touched the same constructors twice for a shape that dies in the
   same change; per the cross-repo rule the sibling landed first and aura adopts
   the terminal shape in one pass.

## Honest semantic cost

- The lock-boundary divergence is gone by deletion, not by unification: with no
  outer Mutex at all, serialization lives only inside the engine — which is
  where it always truly lived. A long scan on one realm handle now blocks puts
  through another handle only to the engine's internal degree (fjall's own
  keyspace synchronization); no current code relied on stronger behavior (scans
  return owned `Vec`s, and the old per-handle mutexes never shared state
  anyway).
- The relaxation is a cross-repo change with a real migration tail: the okm
  trait change touched every `VirtualStorage` impl in okm-core (7 impls) and
  every downstream consumer impl — aura's two here, prism's echo plane when its
  workspace consumes the new okm. The `KvBatch` trait keeps `&mut` — batch
  accumulation is genuinely stateful.
- Removing a defensive signature widens what a handle permits (writes from
  shared refs). No runtime behavior of the engines changed; their internal
  locking is untouched.

## Consequences

- Implemented in `crates/realm/src/mq.rs`: `MqStore { prefix, inner: MqEngine }`,
  Mutex-free impls, `ns_raw`/`for_realm` as prefix assembly + engine handle
  clone, the shared-handle semantics documented at the constructors. Verified
  against the local okm via the temporary `[patch]` section:
  `cargo test -p aura-realm --features "fjall,steel,nushell"` (11 passed) and
  `cargo test -p aura-engine --features "fjall,nushell,steel,wasmtime"`
  (41 passed).
- okm lands its ADR-0026 (trait relaxation + its 7 impls) FIRST; this repo's
  working tree compiles against the relaxed trait via the path patch and goes
  green on the pinned okm the moment the sibling's commit is pushed and
  `cargo update -p okm-core` picks it up (the temporary `[patch]` section in
  Cargo.toml is removed at that point).
- The `&mut`-shaped plumbing in mq.rs's own helpers (`resolve_event_id(store:
  &mut MqStore, …)` and siblings) narrows to `&MqStore` mechanically; lib.rs
  call sites like `routes_drop_actor(&mut self.mq.clone(), …)` simplify with
  them. That is mechanical follow-through of this ADR, not a separate design
  decision.

