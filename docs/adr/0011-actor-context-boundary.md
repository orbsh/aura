# 0011 — Actor context boundary: what lives on ctx, what stays out

**Status:** Accepted (2026-09-12)

## Context

The Actor entry function receives a single `ctx` argument. Across aura-architecture.md the
surface has grown implicitly: `ctx.state` (§2.3, dual API with `ctx.metadata`), `ctx.invoke()`
(§5.13), while `emit`/`on` appear as bare script-level functions and lifecycle hooks
(`on_sleep`/`on_wake`) as script exports. No document had adjudicated the boundary — what
belongs on ctx and what must stay off it. Without an explicit rule, every new capability
(Probe targets, cold-call declarations, future timer APIs) risks drifting onto ctx by default,
and the entry-function signature becomes an unprincipled grab-bag.

## Decision

**The criterion: ctx carries exactly the runtime capabilities that are (a) instance-identity-
bound and (b) subject to Host control or record.** Everything else is expressed elsewhere.

### On ctx

| Capability | Rationale |
|:--|:--|
| `ctx.state` | This instance's state; per-instance Fjall/SlateDB path, WAL-per-field. Not Raft. |
| `ctx.metadata` | Controlled global metadata (registry, sharding, counters); every write goes through Openraft consensus. |
| `ctx.invoke()` | The single controlled call surface — timeout, audit, rate-limit, observability all terminate here (§5.13). Bypassing it (PyO3/httpx direct) is forbidden. Target resolution (HTTP / realm Actor / remote Probe) is registry-declared; CallSlot two-tier waiting is a runtime-level split invisible to the Actor. |

### Off ctx

| Capability | Where it lives | Why off |
|:--|:--|:--|
| `emit(name, data)` / `on(name, fn)` | Script-level bare function / activation-time wiring | Realm pub/sub does not depend on instance identity (delivery partition comes from event data, not the emitter); emit is in-process MPSC fire-and-forget with no lifecycle to govern; `on` is part of the static contract (`interface_schema` receives/wildcard_receives), assembled at activation — putting it on ctx would imply dynamic runtime subscription. |
| `interface_schema()` | Script export, called once by Host at startup | Definition-time contract, not a runtime capability. |
| `set(lang, script)` | Deployment plane | Defines the Actor type; an executing Actor never sees it. |
| `on_sleep` / `on_wake` | Script exports, Host-invoked | Direction is Host → Actor. ctx is what the Actor uses; hooks are what the Host calls. |
| Entry-function `return` | Language-native | Host intercepts the return value and fills it into the reply_to oneshot. No `ctx.return(...)`. |
| `@cron` / `on_debounce` timers | Declarative trigger modes | Timers deliver events (wake the Actor), they are not callable timer APIs. No `ctx.sleep()` / `ctx.every()`. |
| Logging, computation, language stdlib | Host-language native | Anything requiring Host control is addressed through `ctx.invoke()` targets; the rest is the embedded language's own facilities. |

## Consequences

- The entry-function signature stays minimal and stable: `(ctx, <event params>)`.
- New capabilities face the criterion explicitly: instance-bound + Host-controlled → ctx;
  static contract / Host-driven / realm-level → elsewhere. Probe capability targets
  (`probe:<node_id>:<cap>`) land on `ctx.invoke()` under this rule with no signature change.
- The wiki (aura-architecture.md §5.3) carries the user-facing statement of this boundary;
  this ADR is the decision record.
