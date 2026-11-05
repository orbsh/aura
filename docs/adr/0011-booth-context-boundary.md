# 0011 — Booth context boundary: what lives on ctx, what stays out

**Status:** Accepted (2026-09-12)

## Context

A Booth sees exactly two things from the host: the ambient `ctx` object and, per delivery,
the event payload as its handler's argument. Across aura-architecture.md the rest of the
surface had grown implicitly: state over a flat document, `ctx.invoke()` (§5.13), while
`emit`/`on` appeared as bare script-level functions and lifecycle hooks (`on_sleep`/`on_wake`)
as script exports. No document had adjudicated the boundary — what belongs on ctx and what
must stay off it. Without an explicit rule, every new capability (Effector targets, cold-call
declarations, timer APIs) risks drifting onto ctx by default, and the entry-function signature
becomes an unprincipled grab-bag.

## Decision

**The rule: ctx is one global object, bound once at load, carrying every name the HOST
provides to the script. A definition the script offers the host — a handler, a lifecycle
handler, the explicit `interface_schema` — lives in the module namespace and reaches ctx only
through the registration primitive. Deployment-plane declarations sit on neither plane, and
per-delivery entry data (the payload, the return) rides the call path.**

ctx is a namespace, not an instance-identity token and not a parameter the handler threads
around.

### On ctx

| Capability | Rationale |
|:--|:--|
| `ctx.store(op)` | One okm Collection instruction as data, against the owning type's declared ns; the handle is built at registration, so cross-type access is not expressible (ADR-0026 §3). Never leaves the node's data plane. |
| `ctx.invoke()` | The single controlled call surface — timeout, audit, rate-limit, observability all terminate here (§5.13). Bypassing it (PyO3/httpx direct) is forbidden. Target resolution (HTTP / realm Booth / remote Effector) is registry-declared; CallSlot two-tier waiting is a runtime-level split invisible to the Booth. |
| `ctx.iterate` / `dispose` | Streaming calls between booths, riding the same dispatch machinery as invoke (ADR-0034). The cursor belongs to the calling instance; the realm mints stream ids and owns the registry. |
| `ctx.emit(event, data)` | Publish into the realm's MQ plane. Realm pub/sub does not depend on instance identity — the delivery partition comes from event data, not the emitter — and the emitter field carries the type name. |
| `ctx.on(event, handler)` | The single registration primitive; the subscription phase gate is the rule that keeps dynamic subscription rejected. Legal at load/activation only — a message-phase call is an error value, and the assembled registry IS the persisted `interface_schema.receives`, the one contract source. |
| `ctx.schema` | The persisted `interface_schema`, as data — reflection over the declaration, not a runtime capability. |
| `ctx.timer.register(at, tag, durable)` / `ctx.timer.cancel(id)` | Delivery scheduling: non-blocking, explicit re-arm, delivery through the instance's own queue as an ordinary job; eviction still applies (ADR-0016). |

### Off ctx

| Capability | Where it lives | Why off |
|:--|:--|:--|
| Entry-function `return` | Language-native | Host intercepts the return value and fills it into the reply. No `ctx.return(...)`; native failure is the outer error value. |
| The event payload | The handler's argument position | Per-delivery entry data. A session-lifetime object carrying a value that changes under every call would either show staleness to a handler that saved the reference or be rebuilt per call. |
| `interface_schema()` | Script export, called once by Host at startup; readable as `ctx.schema` | Definition-time contract, not a runtime capability. |
| `set(lang, script)` | Deployment plane | Defines the Booth type; an executing Booth never sees it. |
| Lifecycle handlers | Reserved event names (`__sleep` / `__wake`) bound through `ctx.on` | Direction is Host → Booth: the host's eviction/reactivation dispatch looks the binding up in the same registry the route table reads. |
| `@cron` / `on_debounce` timers | Declarative trigger modes | Timers deliver events (wake the Booth), they are not callable timer APIs. No `ctx.sleep()` / `ctx.every()` — the blocking and self-scheduling forms stay rejected. |
| Logging, computation, language stdlib | Host-language native | Anything requiring Host control is addressed through `ctx.invoke()` targets; the rest is the embedded language's own facilities. |

## Consequences

- The entry-function signature stays minimal and stable: `(ctx, <event params>)` in, `return`
  out.
- New capabilities face the provider test explicitly: a name the host offers → ctx; a
  definition the script offers → the module namespace, through the registration primitive;
  a deployment declaration → neither plane; per-delivery entry data → the call argument and
  the return. Effector capability targets (`effector:<node_id>:<cap>`) land on `ctx.invoke()`
  under this rule with no signature change.
- Instance-identity binding survives as content, not as an admission test: `ctx.store` and
  `ctx.iterate` are scoped to the calling instance's type ns and stream cursor, while
  `ctx.emit` is deliberately instance-free.
- The wiki (aura-architecture.md §5.3) carries the user-facing statement of this boundary;
  this ADR is the decision record.