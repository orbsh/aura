# 0047 — A stop is a control bit in the realm, and its door is a framework primitive

> **Languages:** [English](0047-stop-is-a-control-bit.md) (primary) · [中文](0047-stop-is-a-control-bit.zh-CN.md)

**Status:** Accepted (2026-11-09), and BUILT the same day in two stages. Stage one (SET's cooperative half): `crates/realm/src/stop.rs`, `Realm::stop_set`/`stop_clear`, the `stop.poll`/`stop.clear` arms, cleared on eviction. Stage two (INTERRUPT): the realm arms an interrupt slot per running call and races the body against it, the caller takes `Interrupted` (ADR-0012's terminal value), and a Remote body tells the node with `Frame::Interrupt` — the node's half is effector's, the guest's half is induction ADR-0003. Named limits, not gaps in the decision: a carrier with no way in answers `landed: false` (steel and python are in-process VMs), and one-shot executors are not armed yet.
**Date:** 2026-11-09

## Context

A turn can run for minutes. Something has to be able to stop it, and the two shapes that look obvious both fail here.

**An application event.** An event addressed to an instance that is running work is delivered to that instance's queue and waits behind the work. Measured 2026-11-09 on gravity's turn booth: a cancel keyed by `device` took effect only after the turn ended, while the same entry called on that instance overlapped with it. So a message cannot reach a busy activation — it is the same conclusion gravity's ADR-0008 reached from the other side.

**A stored marker.** A row is reachable by everyone, including while the turn runs — but it makes cancellation application state, which it is not. Cancellation is resource control, the same family as the watchdog (`max_exec`) that already lives in this realm: nothing is owed to an activation the process no longer runs, and the record of what the turn did is the store's (rows left OPEN, which the projection skips). Persisting a stop buys durability for a fact that dies with the work it stops.

## Decision

**RULING — one in-memory control state per activation, one door with two actions, and the door is the framework's.**

1. **The state is memory, not storage.** A map keyed by `(instance, call)` beside the watchdog's own bookkeeping, guarded by the same lock discipline the realm already uses for per-instance control. Eviction clears it with everything else the instance held. No table, no ns, no restore path — there is nothing to restore.
2. **Two actions: SET and INTERRUPT.** SET is cooperative: the activation reads it at a boundary it already has and stops issuing work. INTERRUPT abandons the operation in flight, through the execution layer, for the one case SET cannot reach — an activation parked inside an LLM stream, a mount call or a script.
3. **INTERRUPT comes back as a value, not a signal.** The call fails with an INTERRUPTED error that crosses to the guest the way every other failure does (ADR-0012), and it is TERMINAL: a handler must not retry it. That is the only way a forced stop survives a retry loop.
4. **Who may ask.** The venue's sender envelope says who is asking (a bound user or nobody). The application decides whose work that is: gravity checks the marker's member against the turn's own. The realm does not invent an ownership model it cannot see (ADR-0018 keeps identity on the connection side).
5. **The execution layer offers exactly one primitive.** Per carrier: kill the script child, drop the in-flight request, or — where an embedded language cannot be reached — terminate the node process, the watchdog's coarse family. It does not carry two stages; soft stopping is the application's business.
6. **The door is not a booth event and not a public RPC.** It is a realm call made by whoever already holds authority over the connection (prism, for the browser) or by the realm itself (the watchdog's use of it). A booth cannot ask to stop another instance: a guest that needs that is a guest whose boundary logic is missing.

## Considered and rejected

- **A row in the app's ns as the marker** (the interim gravity shipped): it works and is what makes today's cancel possible, but it stores resource control, and it needs a second instance to write while the first runs. It retires with this record's primitive.
- **A queued cancel event**: measured not to arrive at a busy activation. Rejected as a mechanism, kept as the wire shape a client uses to ASK.
- **Aborting by killing the process as the cooperative path**: the watchdog's silhouette, and it loses every row the turn wrote. It stays the last resort of an interrupt, never the first answer to a click.

## Consequences

- One map and one lock data structure; a test can drive SET from outside an activation and observe the handler stopping at its boundary, and an interrupt against a parked call and observe the INTERRUPTED value.
- The realm gains an authority it did not have: it can now interrupt an activation. The scope is deliberately one instance, one call.
- `max_exec` and an interrupt are two retirement routes to the same wake — the watchdog evicts, an interrupt returns. They must not be folded: one is a budget, the other is an intent.
