# 0043 — The ctx object: one ambient execution context (closes ADR-0011's placement regime, breaking)

> **Languages:** [English](0043-ctx-object.md) (primary) · [中文](0043-ctx-object.zh-CN.md)

**Status:** Accepted (2026-10-10) — design; implementation pending.
Supersedes ADR-0011's placement decisions in full while keeping its
direction rule (entry stays native) and its rejection of dynamic-runtime
subscription (re-expressed as a phase gate). User ruling (2026-10-10):
**full rename, no compatibility layer** — the flat `ctx_*` names retire.

## Context

ADR-0011 admitted capabilities to `ctx` by a two-prong criterion:
instance-identity-bound AND host-controlled. In practice it protected two
real invariants — subscription truth is knowable at deployment (the route
registry, `ctx_queue_depth`, backlog governance all depend on it) and
every capability has one named governance point. But the criterion
conflated the invariants with an accident of implementation: it scattered
the booth-facing surface across **four placement mechanisms** — injected
flat `ctx_*` fns, script-level bare functions (the emit/on ruling),
script exports (interface_schema, on_sleep/on_wake), and language-native
syntax sugar (decorators). The evidence it was carrying too much: the
naming collision we hit building the mudra host — `ctx_store_emit` (one
storage instruction) versus the never-landed script-level `emit` (event
publishing) share a verb because placement, not semantics, decided where
each name lived. And the one structural gap in the whole surface (script
`emit` decided in 0011, never injected into any carrier) exists precisely
because it had no ctx home to fall into.

The user's reframe: `ctx` is not "the instance-identity object" — it is
the **ambient execution context the host grants one delivery** (the
gRPC `context.Context` / web-framework Request-object shape). Under that
reading, identity-binding is not an admission test; it is optional
content of specific members. What survives as the real boundary is the
**direction** rule 0011 already used for `return` and hooks.

## Decision

### 1. The boundary: exit plane vs entry contract

**ctx = the booth→host exit capability plane.** Everything the booth
calls outward lives as a method on one object. **Entry stays
language-native** (0011's rule, kept verbatim in force): the handler's
`return` fills the reply envelope (no `ctx.return` ever exists), failure
is the native exception/error surfaced as the outer error value,
`on_sleep`/`on_wake` are Host→Booth exports, `set(lang, script)` is
deployment-plane and never visible to an executing booth.

### 2. The surface (one object, grouped methods)

```
ctx.self                      # InstanceId (struct key: Singleton|Named)
ctx.payload                   # the call's input, read-only (§4)
ctx.invoke(target, handler, args) -> Value          # the one controlled call
ctx.iterate(target, handler, args) -> Cursor        # start/pull/dispose
ctx.store(op) -> Value        # one okm Collection instruction as data (ADR-0026 §3)
ctx.emit(event, data)         # LANDS the 0011 half-bridge: publish into the MQ plane
ctx.timer.register(at_ms, tag) -> id                # ADR-0016 §3b
ctx.timer.cancel(id)          # idempotent
ctx.queue.depth(event)        # backlog point-read
ctx.queue.skip_to_head(event) # relief valve (`to_head` is load-bearing: directional discard, not filler)
ctx.schema                    # the persisted interface_schema copy, as data (reflection)
ctx.on(event, handler)        # phase-gated: activation only (§3)
```

The `store`/`emit` collision disappears with the flat names: storage is
`ctx.store(op)` (a method group), publishing is `ctx.emit(event, data)`
— verbs follow semantics, not placement. Naming rule for the whole
surface: inside the ctx namespace a member takes the short unambiguous
name — the length of `interface_schema` was a flat-namespace tax
(self-identifying among bare globals), paid away as `ctx.schema`.
(Exception checked, kept: `ctx.queue.skip_to_head` — `to_head` carries
the directional-discard semantics (the cursor is monotone; skipped
backlog never revives), it is not filler.)

`ctx.emit`'s emitter field: the host closure captures the **type name**
(never an instance id — publishing is instance-free per 0011's own
observation; the audit line still reads who published).

### 3. `ctx.on`: the declarative schema stays the contract's home

0011 rejected dynamic subscription and it stays rejected — re-expressed
sharper: **`ctx.on` is legal during load/activation only; called inside
a message handler it is an error value** (the phase gate matches the
system's existing "no silent no-op" discipline). This is not a new
registration channel competing with introspection: the
`interface_schema.receives` data remains the ONE persisted contract
source (route registry, depth/skip governance, mudra's hello-schema all
read it). Decorators (`@on`) are syntax sugar **lowering to** `ctx.on`;
static languages (Rust booths, bgi binaries) may write `ctx.on` directly
as the low-level form — the schema is assembled from the same activation
registry either way. Placement of `on` on ctx changes the data flow:
registration happens by executing the load-time entry once (already the
carrier's job for decorator collection) instead of a separate static
scan.

### 4. Payload rides the context; the handler signature collapses to one parameter

`ctx.payload` is the most literal member of an execution context — the
data of this delivery. Consequence: handler signatures unify to
`(ctx)` across every carrier (Rust: `Handler = Fn(Ctx)` — today
`Fn(Ctx, Value)`; python/steel/nu/bgi: one parameter). The typed-input
convenience in statically-typed languages moves from signature position
to body (`let p = ctx.payload().into_value::<T>()?`): compile-time to
run-time, accepted.

### 5. bgi: the host-frame arm set becomes a 1:1 map of ctx methods

The out-of-process carrier's `host: {type: …}` arms mirror the object
(`invoke | iterate | store | emit | timer.register | timer.cancel |
queue.depth | queue.skip_to_head | schema` — `self`/`payload`
are host-pushed at call time, `on` lives in the hello schema, no frame).
`emit` is fire-and-forget: the arm carries **no reply** (same shape as
its script-level twin; a hung extension stalls the session only if it
also stalls reading stdout — the pipe timeout already governs that).
The mudra extension protocol (its ADR-extension-protocol) rides the same
vocabulary with its own profile deltas (no `store` arm there — that
ruling is unchanged).

### 6. Migration (breaking, no aliases)

- `ctx_invoke` → `ctx.invoke`; `ctx_store_emit` → `ctx.store`;
  `ctx_interface_schema` → `ctx.schema`;
  `ctx_timer_register/cancel` → `ctx.timer.register/cancel`;
  `ctx_queue_depth` / `ctx_skip_to_head` → `ctx.queue.depth` /
  `ctx.queue.skip_to_head`; `ctx_iter_start/next/dispose` → the cursor
  form `ctx.iterate` (script carriers that lack a cursor type keep the
  three-call shape **under the grouped names** `ctx.iterate.start` etc.
  — grouping is naming, cursor is the Rust form).
- The flat `ctx_*` injection table retires; every language environment
  receives one object (python: a module-level `ctx` bound per delivery;
  steel: `ctx` global, the `ctx_*` names in today's steel docs retire;
  wasm: host fns keep a flat ABI — the object is the SOURCE-level shape,
  the `aura_alloc`-style ABI is allowed to be flat (the ABI is wire, not
  contract).
- `docs/design/booth-api.md`'s host-fn table and language-difference
  table rewrite against this surface; wiki `aura-architecture.md` §5.3
  follows (it already speaks `ctx.invoke()` — the object reading makes
  that literal).
- ADR-0011 gets a dated Update note (history body untouched, per the
  standing discipline).

## Consequences

- One admission test replaces the two-prong one: **is it booth→host?**
  If yes it is a ctx method; if it is host→booth it is an export or a
  return value; if it is deployment it is off both planes.
- The last structural gap in the event model (`emit` never landing)
  gains a natural home — the fix that was going to add a fifth placement
  mechanism instead adds one method.
- 0011's "On ctx" table entries that retired since (ctx.state,
  ctx.metadata) and its bare-script-function rulings are superseded in
  one move instead of piecewise errata.
- Accepted costs: typed-arg ergonomics shift runtime-ward in Rust; the
  load-time execution of `ctx.on` means registration is runtime work
  (bounded: activation already executes the entry to collect decorator
  bindings); every existing script/test fixture renames (small — the
  surface is young, which is why the user could rule no-compat).
- Deferred, unchanged: timer durability (ADR-0016's durable half), the
  0012 source-level emit collection (the Windmill criterion still gates
  it — ctx.emit does not create that consumer).
