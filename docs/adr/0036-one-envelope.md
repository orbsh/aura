# 0036 — one envelope: invoke is iterate's 1-stream

> **Languages:** [English](0036-one-envelope.md) (primary) · [中文](0036-one-envelope.zh-CN.md)

**Status:** Accepted (2026-09-28); LANDED (Phase 4.15, 2026-09-29 — probe
96c56df / aura d21397c). Supersedes the protocol-level verb separation in
ADR-0034 (the ctx surface keeps two verbs; the wire carries one envelope).
Raised by the user's symmetry: done is always a boolean field, the terminal
done may carry a value — invoke is the done that arrives on the first round.

## Context

ADR-0034 landed the iterate envelope `{item, done}` alongside `ctx.invoke`'s
single value, and its own honest-cost section wrote the admission that
motivates this ADR: *"iterate is invoke-many with a stateful producer."*
The user pressed the symmetry from the other side: **invoke is the 1-stream
whose `done` arrives on the first round, carrying a value.** The two call
shapes are not siblings — they are special cases of one envelope, differing
only in which round `done` arrives and what it carries.

The python language layer already proves the shape: a generator's
`StopIteration.value` is the return value of a stream that ends with a
value (`return x` inside a `def` that also yields). The wire has no reason
to split what the host language already unifies.

And the merge fixes a real looseness in the landed protocol: stream
association is currently **positional** — the Start reply is recognized by
the presence of a `stream_id` field, which is a shape heuristic, not a
rule. The unified envelope derives association from `done` itself.

## Decision

**RULING — one envelope on the call wire for every handler response, in
every transport (in-process seam, remote frame, exec stdio). The ctx
surface keeps `invoke` and `iterate` as two verbs.**

### 1. The envelope

```
{ "done": false, "item": <value>, "stream_id": "<id>" }   first reply of a started stream
{ "done": false, "item": <value> }                         subsequent pulls
{ "done": true, "value": <value> }                         terminal, with value (invoke / generator return)
{ "done": true }                                           terminal, empty
```

- `done` is always present, always boolean — the user's rule, adopted verbatim.
- `item` is legal only with `done: false`; `value` only with `done: true`.
- `stream_id` accompanies a **non-terminal** first reply (the realm mints
  at Start, discards the registration when the first reply is terminal —
  see §4; a terminal first reply carries no id).
- Failure stays on the outer Result channel (ADR-0012): no error field
  inside the envelope.

`invoke` is a stream whose first reply is `done: true`; the realm unwraps
`value` into the parked caller as the single value. That is the whole
merge — no new verb, no second mechanism.

### 2. Carrier shapes

Plain returns (non-generator python, Rust closures, remote one-shot
scripts) wrap to `{done: true, value: <return>}`. Native generators
(python) project `StopIteration.value` into the terminal envelope — the
framework reads `.value`, not just the exception. Envelope-mode handlers
(steel/nushell/wasm, ADR-0034 §1) write `{done: true}` and may write a
`value`; validation rejects `item` on terminal rounds and requires `done`
everywhere. The dispose dual is untouched: `GeneratorExit` → close,
explicit `dispose()` where there is no hook.

### 3. Two verbs on the ctx surface

The names stay separate because the **consumer intents** are different:
take-one-value versus iterate-a-sequence. Merging the names would force
every consumer to branch on `done` — a second decision at every call site
to save one protocol branch in the framework. ADR-0011's minimum-surface
rule counts semantic completeness, not character count: two orthogonal
intents, two names, one wire. ADR-0034's "three verbs, one mechanism"
becomes "two verbs, one envelope."

The terminal `value` is addressed by the **full-pull shape**: one round,
terminal, unwrap. That is exactly `ctx.invoke` — it already is the
value-consuming sugar over the unified mechanism; native iteration
discard trailing values (python `for` over a generator discards
`StopIteration.value`; a Rust cursor loop drops the final `Envelope.value`).
Consumers who need it pull explicitly or invoke.

### 4. Association derived from `done`, not from field presence

The positional heuristic (Start reply recognized by a `stream_id` field)
is replaced: a reply with `done: false` and no `stream_id` is a protocol
error; the realm registers a stream at Start only to unregister it
immediately when the first reply is terminal (invoke fast path: mint and
discard cost nothing measurable, and the code path is one — no
"invoke jobs skip registration" special case). Event delivery (fire-and-
forget) stays: envelope produced, discarded by the drop.

## Honest semantic cost

- **Rework of code landed the same day.** The envelope rule touches the
  probe `ResidentSession` seam (`call` folded into `iterate`), python's
  generator projection (`.value` now read), the shared `envelope_pull`
  validation, aura's Job/`JobKind` (invoke kind disappears), `Realm::call`
  (becomes Start+unwrap sugar), and the six `iterate.rs` acceptance tests
  (shapes change, assertions mostly survive). A real diff; the user's
  symmetry is worth more than one day's stability, and the protocol is
  young enough that no external consumer has pinned the old shapes.
- **Every carrier return passes through a wrapper.** Plain values become
  terminal envelopes — one JSON-object construction per call at the seam.
  The unwrapped value is delivered to handlers unchanged; the cost is
  framework-side construction and the validation surface.
- **Validation grows two cross-field rules** (`item` iff non-terminal,
  `value` iff terminal). The typed `done` field is what makes them
  checkable — this ADR trades the sentinel footgun (which ADR-0034
  removed) for a shape rule that is enforced, not convention.
- **`StreamCursor::value()` after done is new surface** that the native
  sugars deliberately do not consume — documented as the explicit-consumer
  accessor, or it invites the same branching §3 rejected.

## Consequences

- **probe:** `ResidentSession::call` folds into the stream seam; python
  captures `StopIteration.value`; `envelope_pull` gains terminal-value
  validation; nushell/wasm/steel handler docs updated (`{done:true, value}`
  legal at terminal).
- **aura:** `JobKind::Invoke` removed (all jobs are stream ops);
  `Realm::call` becomes sugar over Start+terminal-unwrap; `Envelope`
  gains `value`; `StreamCursor::value()`; ctx invoke/iterate signatures
  unchanged. ADR-0034 errata appended (its §1 table and "three verbs"
  line are superseded in form, not in decision).
- **okm/probe-protocol:** no wire-format change (the envelope is schema
  riding existing frames, exactly as ADR-0034 priced it).
- **gravity/Phase 4.14:** the exec carrier (ADR-0035) is implemented
  against the unified envelope from its first frame — no transitional
  shape; the `store_emit` arm is orthogonal and unaffected.
- **PLAN:** Phase 4.15 — envelope unification; sequenced AFTER 4.14
  lands (exec must not chase a moving protocol; 0036's merge does not
  alter the frame vocabulary either way).

## Alternatives considered

- **Merge the ctx verbs too** (`ctx.call → cursor` for everything):
  rejected for the §3 reason — it moves the done-branch from framework
  to every consumer site. The user's proposal named the protocol merge,
  not a name merge; recorded here because a one-word instruction can
  still overrule it.
- **Keep two envelopes (invoke's bare value vs iterate's pair):** the
  status quo until this ADR; rejected because it keeps the positional
  stream_id heuristic and doubles the validation surface (two shapes
  to learn per carrier wrapper), for zero saved wire bytes — the
  envelope object is the same size either way.
- **`value` on every envelope (always-present, null-able):** rejected —
  `done:false` rounds with a value field invite the reading that value
  is the payload mid-stream; `item` is already the payload there. Cross-
  field rules are kept where they are checkable.

## Related records

ADR-0034 (the envelope, the dispose dual, residency accounting — all
stand; only the verb-separated protocol form is superseded), ADR-0012
(failure is a value), ADR-0011 (ctx boundary — two verbs join as one
mechanism), ADR-0035 §3 (the exec frame carries the unified envelope),
PLAN Phase 4.15.
