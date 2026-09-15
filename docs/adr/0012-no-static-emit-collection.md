# 0012 — No static emit collection: the runtime is the only source of receiver truth

**Status:** Accepted (2026-09-15)

## Context

Phase 4.5c replaces hand-written `emits` declarations with static collection:
emit call sites in the script source are scanned (python decorator-style
collection, steel AST extraction), and the collected set is validated at
registration time against statically known receive targets. The motivation was
that registration-time knowledge of the emitter→receiver graph could feed
three uses: early validation, warm-up pre-activation, and placement planning.

The collection mechanism itself is sound (the same scanner that derives
`receives` from `@on` can scan `emit()` call sites). The question is whether
the collected data earns its cost.

## Arguments for collection, examined

### 1. Registration-time validation of emit targets — does not hold

A declared `@on("order_created")` and an `emit("order.created")` mismatch is
caught only if **all** of these hold: both scripts registered, both use string
literals, and the check runs after the pair exists. None is guaranteed:

- The emitter registers first; the subscribing actor uploads three weeks
  later. The emitter's validation passed at registration against a receiver
  set that has since changed — the check is only a snapshot, not a fact.
- `emit(event_type, data)` with a computed name (common in AI-generated
  scripts) collects nothing usable; the lint silently degrades to nothing.

The failure mode it is supposed to catch — an event landing in the dead-event
ring — is already observable at runtime with strictly better fidelity: the
ring records the actual event name, at the actual time, including dynamically
constructed names. A static lint reports plausible errors; the dead ring
reports real ones.

### 2. Warm-up and placement — worse than nothing when wrong

Pre-activating subscribers or co-locating emitter-dense actors on one node
assumes the static graph is the true graph. But static collection cannot
guarantee that (see above: literals only, snapshot only). Acting on an
incorrect graph wastes the exact resources the optimization was meant to
save — activating instances that will never receive, placing actors by a
topology the real traffic does not follow. Meanwhile the runtime delivery
log already produces an accurate emitter→receiver record as a byproduct of
doing the delivery it is accountable for.

### 3. A second copy of the same fact — the drift argument

`emit()` call sites are the single source of truth for what an actor emits.
Collecting them into metadata duplicates that fact into a second location
that must be kept aligned: every edit to an emit call becomes a potential
silent divergence between the code and the declared graph. This is the same
failure the hand-written whitelist had — machine-copying the fact does not
remove the copy, it only removes the typing. The hand-written emits
declaration was abolished for precisely this reason; the collection mechanism
reintroduces the duplication it was meant to replace.

The Windmill parser analogy (parse imports → auto-install dependencies) is
the real case for source analysis: there the parsed graph *directly drives*
an action that fails immediately and loudly when the parse is wrong, and no
other source of truth exists. Aura has no consumer that needs the graph
today; if placement planning or a dependency-driven mechanism ever needs it,
the parsers (steel s-expressions are directly traversable) are the right tool
at that time — built against a real consumer, not in anticipation of one.

## Decision

**emits are never collected, declared, or validated. The receiver set of an
event is a runtime fact, observable only through delivery itself.**

- No emits field in `interface_schema` or any derived metadata.
- No registration-time validation of emit targets — an emit with no
  subscribers lands in the dead-event ring; that is the observable, and the
  ring is the audit surface.
- Undeclared emit is not a violation; there is nothing to declare against.
- An emitter→receiver dependency graph, when a control-plane need (warm-up,
  placement, impact analysis) actually exists, is derived from **runtime
  delivery observation** — the dead ring and the delivery log — not from
  source analysis. Source parsing is deferred until it serves a real
  consumer (the Windmill criterion: parse when the parse drives an action
  that only the parse can make correct).

Phase 4.5c step 4 (static emit collection + registration validation) is
dropped accordingly; the emits whitelist check in the current emit path
(`may_emit`) is removed with it.

## Consequences

- One fewer consistency burden on script authors: emit call sites are the
  only place emission behavior exists.
- Misnamed events surface as dead-ring entries, not registration errors —
  monitoring must treat dead-ring growth as a signal (it already exists as
  a bounded, observable structure).
- The meta-store record for an actor type carries `receives` /
  `wildcard_receives` / lifecycle only.
