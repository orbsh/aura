# 0034 — iterate: streaming calls between booths as generators

> **Languages:** [English](0034-iterate-streaming-primitive.md) (primary) · [中文](0034-iterate-streaming-primitive.zh-CN.md)

**Status:** Accepted (2026-09-28). New ctx primitive alongside emit/on/invoke.
Motivated by the LLM-provider need (gravity → OpenAI token streams) which,
per ADR-0031, is served by a sibling booth's own code — but that exposed the
missing shape: how one booth consumes another's *stream*.

## Context

`ctx.invoke` returns a single value. Some handler work is inherently
sequential-output: an SSE token stream, a paginated fetch, a long-running
scan. The question: what delivery shape serves it?

The tempting shape — emit-per-item (a WS-style message stream) — is
rejected. Three structural defects:

1. **No built-in termination.** A stream must declare an end by convention
   (sentinel event) — collisions with legal data, coordination not
   structure. A generator's exhaustion IS its end.
2. **No consumer binding.** Emits route by event name; nobody stops
   producing when the consumer disappears — dead-ring noise, wasted work.
3. **No backpressure.** The queue absorbs at producer rate. The relief
   valve (skip-to-now, Phase 4.5c) is semantically wrong here: skipping
   tokens drops content.

The generator/iterator model has the properties built in: pull-based
(termination, backpressure, consumer liveness all fall out of who asks for
the next item), and per-language idioms wrap it natively.

## Decision

**RULING — `ctx.iterate(target, handler, args)` is a first-class ctx
primitive, parallel to emit/on/invoke.** It returns a cursor; the cursor is
consumed as a native iterable in every carrier, and as pull-until-done in
carriers without iteration protocol.

### 1. Wire shape: a typed envelope, not a magic value

Every pull round-trip exchanges an envelope:

```
{ "item": <value>, "done": false }        ← next item
{ "done": true }                           ← exhausted (structural end)
{ "error": "..." }                         ← mid-stream failure (ADR-0012)
```

Termination is a typed field, never a sentinel string. Languages WITH
native iteration translate: the wrapper raises `StopIteration` on
`done: true` — the producer handler written as a native generator
(python `yield`, steel closure, rust `Iterator`) never sees the wire
protocol; the framework drives the generator, and generator exhaustion
encodes `done: true` symmetrically.

Languages WITHOUT native iteration (nushell; wasm is consumer-side only,
see §4): the handler is a **repeatedly callable function that returns the
envelope explicitly** — `done: true` is written, not derived. No framework
invention beyond the envelope itself; the guard value is the schema field.

### 2. Stream identity and session binding

The stream lives in the producer's resident session (Phase 2.6 — the
residency is what makes a pull-protocol stateful session possible, not a
coincidence). `stream_id` correlates pulls the same way `pending_remote`
correlates invoke replies: a frame identity, no new machinery. Partition
key choice (e.g. `key = consumer session_id` for a provider booth) is the
booth's business, following the usual partition-key rules.

### 3. dispose: the mandatory dual

Consumers may abandon mid-stream (`break`). Carriers with native
destructor hooks (python/steel wrapper on `GeneratorExit`) send dispose
automatically; carriers without one (nushell, and explicit break in any
consumer) MUST call `cursor.dispose()`. A stream with no further pulls and
no dispose is released by eviction, not by magic. `iterate` without
`dispose` would be the dead-ring problem wearing a request badge.

### 4. Residency accounting: TTL counts from stream stop

Pulls are session activity: each `next` resets the idle timer, so a long
stream keeps its producer resident without special declaration. When the
stream ends — exhaustion or dispose — the timer is (re)armed through the
existing timer API (ADR-0016), and standard `idle_ttl` eviction applies.
Provider booths declare TTL like any other booth; no "TTL ≥ stream length"
upper bound is needed. A stream interrupted mid-flight by eviction is a
failed pull (error envelope), and ADR-0012 governs.

### 5. Tier: hot only

Each pull is a hot call (oneshot + timeout). Cold streaming (a human
as producer, pulls parked indefinitely) is out of scope: no known consumer,
and the cold-tier discipline says don't pave for absent traffic.

### 6. The producer side of LLM serving is script carriers

The provider booth pattern: python (or steel) handler — `httpx.stream`,
parse SSE, `yield` per token event — the carrier's native generator drives
the envelope. wasm remains consumer-side (pull-until-done loop): wasm has
no generator shape to yield from, and its outbound HTTP need — the reason
this primitive exists — is served by consuming a sibling booth, not by
wasi-http host plumbing (the PLAN 2026-09-28 wasi-http entry is superseded
and withdrawn by this ADR; ADR-0031's ruling stands: the booth decides its
access method inside its own code — the sibling booth IS that code, reached
through the field, which keeps the realm's observation surface honest).

## Responsibility cut (what moves out of gravity)

The provider booth is a **transport adapter**: HTTP/SSE mechanics, retry
backoff at the transport level, key custody (env-injected, never in the
module). Gravity keeps **orchestration decisions**: model selection,
fallback, circuit breaking, transcript-aware retry — everything that needs
the turn's context stays where the inference starts. "LLM identity stays
Gravity-side" (gravity PLAN) is untouched; what relocates is the raw
socket-per-turn work, which gravity-as-wasm cannot do natively (§6).

## Honest semantic cost

- **Pull is a round-trip per item batch.** Default pull granularity is one
  item; `pull(n)` batching is available as a knob. LLM token latency is
  dominated by generation, not by the hop — but on fast producers the hop
  is real cost, priced at the consumer's choice.
- **iterate is invoke-many with a stateful producer.** Nothing about MQ
  delivery, cursors, queues changes — and nothing helps you either: a
  stream is NOT durable, NOT replayable. Producer eviction mid-stream =
  failed stream, consumer eviction = dead wrapper. If a use case needs
  at-least-once streaming, it wants events and this ADR is the wrong tool.
- **Three verbs, one mechanism.** iterate/dispose are frame-level calls
  riding the existing call machinery; the envelope is schema, not protocol.
  The "cost" is documentation surface: carriers must all wrap the same
  envelope, and the nushell/guard-value path invites hand-written bugs —
  an envelope shape with a typed `done` field narrows but does not
  eliminate the footgun.

## Consequences

- **aura:** ctx surface + frame plumbing for iterate/dispose; carrier
  wrappers (python/steel native; nushell explicit; wasm consumer-loop);
  ADR-0011's ctx-boundary list gains iterate/dispose as instance-bound
  host-gated capabilities.
- **gravity:** provider booth as python booth type (transport adapter);
  Phase 1 LLM layer consumes via iterate.
- **okm/probe:** frame vocabulary gains the call kinds (probe-protocol —
  remote producers/consumers ride the same envelope).
- **Supersedes:** the wasi-http carrier task (aura PLAN 遗留节, filed
  2026-09-28) — withdrawn; wasm outbound access = consume sibling booths.

## Related records

ADR-0031 (outbound access is booth code — the provider booth is its first
application), ADR-0011 (ctx boundary), ADR-0012 (failure is a value —
mid-stream errors), ADR-0016 (timer API — TTL rearm at stream stop),
Phase 2.6 (residency — the stateful producer), Phase 3.5 (hot tier —
every pull), modeling.md (streaming-consumption pattern section).
