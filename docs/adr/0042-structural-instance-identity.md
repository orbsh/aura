# 0042 — Structural instance identity: the singleton sentinel retires (closes ADR-0038's residual)

> **Languages:** [English](0042-structural-instance-identity.md) (primary) · [中文](0042-structural-instance-identity.zh-CN.md)

**Status:** Accepted (2026-10-08) — **LANDED 2026-10-09 with Phase 4.13**
(commits `33ef84e` + `e3690ac`: `InstanceId.key` is the
`aura_booth::InstanceKey { Singleton, Named }` enum; the sentinel retired;
the route-resolution row shape migrated in the same batch — the two touch
the same EventRoute row shape; separating them would mean two migrations).
User ruling (2026-10-08): make instance identity structural now, riding the 4.13
window.

## Context

ADR-0038's residual note records the bug: the INSTANCE key space still uses the
sentinel string `__singleton__` (`aura_booth::InstanceId { key: ... }`), so a
payload whose key field literally equals that text aliases the singleton
INSTANCE — a different bug from the partition aliasing that ADR-0039 fixed (the
slice id 0 is now structurally unreachable; the instance key is still a string
compared against a magic value).

Where the sentinel actually lives today (all call sites verified):

1. `events.rs` — `instance_of(&InstanceKey)`: `Singleton` → `mq::SINGLETON`
   string, used as the delivery target's `InstanceId.key`.
2. `instance.rs` — the consumer loop binds a key-less route only when
   `id.key == mq::SINGLETON`.
3. `mq.rs::bound_instance_key` — returns `Singleton` only when the caller's
   instance key equals the sentinel.
4. `instance.rs` — the probe session identity is the string
   `"{booth_type}/{key}"`; a singleton instance's key contributes
   `__singleton__` to it.
5. `ctx.rs` — the relief-valve fns pass `self_id.key` (the sentinel, for a
   singleton instance) back into `bound_instance_key`.

Verified NOT affected: the state plane. `store_exec::execute` keys documents by
the script-provided key against the TYPE's ns — the instance key never enters a
state key, so no state migration rides this ruling.

The bug's shape: `InstanceId.key` is an open string space, and the framework
reserves one value of it without declaring a reservation. Any payload key equal
to `__singleton__` becomes undeliverable-as-named: a keyed emit for that value
resolves to a distinct `Named("__singleton__")` slice, but its delivery target
collides with the type's singleton instance — two logical instances share one
`InstanceId`.

## Decision

**RULING — `InstanceId.key` becomes a structured enum; the sentinel string
retires from the framework entirely.**

```rust
pub enum InstanceKey {
    /// The type's singleton instance (there is at most one; it needs no name).
    Singleton,
    /// A named instance — the routing value the payload carried.
    Named(String),
}

pub struct InstanceId {
    pub booth_type: String,
    pub key: InstanceKey,
}
```

- The singleton is a VARIANT, not a string value. Alias-by-literal is
  structurally unreachable — the same treatment ADR-0039 gave the slice id 0,
  applied one plane up.
- `Named(key)` is payload-sourced and unreserved: no string is special, no
  reservation to document, no collision to police. `SINGLETON`/`__singleton__`
  retires from `mq.rs`; a named instance whose key text equals the old sentinel
  is just an instance named that.
- **Session identity** (`instance.rs`): the probe session key formats
  `Singleton` as `{type}/` (empty key segment — the session is per type) and
  `Named(k)` as `{type}/{k}`. An empty key segment is unambiguous because
  `Named` keys are non-empty by the routing rule (a payload key field must
  yield a non-empty string; empty = malformed, `MissingKeyField` class).
- **Ctx surface**: scripts keep seeing a string key (`ctx.self_id.key`
  equivalent). The singleton instance's key renders as the empty string —
  honest: it has no name. A script that branched on the literal sentinel was
  relying on an undocumented reservation; the empty string is the documented
  rendering.
- **`bound_instance_key`** takes `&InstanceKey` instead of `&str`; the
  singleton-vs-keyed comparison becomes a variant match, not a string compare.
- **Events wiring**: `instance_of` disappears — the target's key IS the
  `InstanceKey` variant already resolved for the slice; no string round-trip.

### What this does NOT change

- The MQ keyspace is untouched: the slice segment is already the proxy id
  (ADR-0039 §1); the sentinel never lived in storage keys.
- The route registry (ns 25), the dictionaries, the cursor key — all keyed by
  ids, unaffected.
- `InstanceKey` (the slice enum, `mq::InstanceKey`) keeps its name; the new
  enum is `aura_booth::InstanceKey` (the instance-plane twin). The two are
  intentionally isomorphic — the slice value IS the instance key — but they
  are distinct types: one belongs to the MQ plane, one to the call model.

## Consequences

- **Code**: `aura_booth::InstanceId.key` type change ripples to every
  `InstanceId { key: "..." }` literal in tests and the CLI (mechanical:
  `Named("k".into())`), the four framework call sites above, and
  `dispatch_call`/session bookkeeping. No storage migration — nothing
  persisted carries the sentinel.
- **Phase pairing**: lands with Phase 4.13 — both rewrite the EventRoute row's
  resolution contract, and 4.13's `resolve` end state already produces
  instance keys by scan, which is where the variant flows in naturally.
- **ADR-0038's residual note is closed by this ruling** (kept in place, marked
  superseded-by pointer per the dated-records rule).
- **Docs**: `event-flow.md`/`-en.md` §1 (vocabulary: the instance key gains the
  variant form), §6.1 (the target line), §6.2 (the binding rule), §8.3 (the
  residual paragraph closed).
- **Probe seam**: the session-key format change is observable only in probe's
  session directory naming; probe holds no dependency on the sentinel literal
  (grep verified), so no probe change rides this.
