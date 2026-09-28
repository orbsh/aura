# 0031 — Remote booths: why they are not adopted

> **Languages:** [English](0031-remote-booths.md) (primary) · [中文](0031-remote-booths.zh-CN.md)

**Status:** Accepted (2026-09-28). This number has one history: the earlier
draft of this ADR (2026-09-26) proposed remote booths — a realm dialing out to
external participants. On review the proposal contradicted a founding setting
of the project that had been forgotten; this record is the ruling against it.
No implementation had started, so there is nothing to roll back.

## Context — what the proposal was

The draft extended the surface beyond tier-1 remote **execution** (the probe,
`Body::RemoteProbe`, PLAN Phase 3) to tier-2 remote **participation**: a booth
itself — definition, code, storage — living on an external service, with the
realm dialing out to its WS endpoint, registering the endpoint on the
registration frame, holding the connection, pinging 1–5s for liveness,
reconnecting with backoff, and speaking the realm's own frame vocabulary in
CBOR. The mental model: aura becomes an MQ broker and the remote booth a
microservice hanging off it — consuming subscribed events, answering calls,
carrying its own code and database, un-inspectable by the broker. Its framing:
operating a service "is the tax paid for the freedom," and whoever pays it
lifts the language/storage restrictions.

The two observations that carried the draft survive the proposal and are worth
keeping on the record (§"What survives"): they are true statements about the
model, and they support tier-1. What they never did was establish that the
**realm** should own the socket for remote logic — the draft refined that
mechanism across review rounds without ever questioning it.

## The founding setting the proposal forgot

**There are no remote booths.** The logic belongs to the booth; configuration
is not global; the booth decides its own access method. A booth that needs to
talk to an external service does so inside its own code, with a client of its
own choosing — an HTTP library in a script body, a WASI HTTP host for a wasm
booth whose carrier lacks one natively. The realm's job ends at the field
boundary: route domain events, answer invocations, observe deliveries.

The two shapes differ on every axis that matters:

| concern | the remote-booth proposal | the founding setting |
|---|---|---|
| who holds the socket | realm (dial-out connection manager) | the booth's own code |
| endpoint declaration | registration frame carries the URL | nothing crosses the realm's API |
| liveness | realm-side ping 1–5s, backoff, DISCONNECTED state | the booth's problem, like any of its logic |
| wire format | realm-defined CBOR frame vocabulary | whatever the external service speaks |
| failure semantics | failed delivery as a value into the dead ring | a handler error like any other handler error |

## Decision

**RULING — outbound access is a booth-code concern, not realm infrastructure.**

1. **No dial-out facility.** The realm builds no connection manager for
   external services, registers no endpoints, pings nothing. The proposal's
   five-constituent relocation of a booth does not happen: a booth whose
   logic needs an external service keeps the coordination inside the booth —
   handler code opens the connection per call (or holds it per
   resident-session lifetime — Phase 2.6 residency is the booth's own choice
   of client state), parses whatever the other side speaks, and treats
   failure as a value in its own return.
2. **Language carriers already answer this.** steel/python/nushell bodies
   have native HTTP clients; a wasmtime booth reaching HTTP needs a WASI
   HTTP host — a runtime-capability question about the carrier, not a
   realm-config question. Adding `outbound_http` to a node's KDL would make
   the realm the guarantor of a connection it cannot semantically interpret.
3. **The browser case does not need remote booths either.** A fluxen
   user's session is a *session*, not a *booth*: it lives on the
   connection plane (prism, ADR-0017), where the auth block, the
   account/session prefix stamp, and the streaming subscription half
   already govern it. The draft's acceptance test ("the browser is
   a remote booth") conflated the two planes; the browser is prism's
   participant and the realm never sees a booth-shaped thing behind a
   socket it did not dial.
4. **Direction mirror already exists.** For the *outward push* half —
   getting field events to non-realm consumers — the modeling guide
   (`docs/design/modeling.md` §outbound) already has the shape: the booth
   emits an ordinary event, the outer plane's bridge subscribes. Aura
   never knows WS exists. Per-call outbound access is the same stance in
   the other direction: the booth acts, the realm observes what crosses
   the field boundary as events, nothing more.

**Why the tax argument inverts.** The draft said operating a service "is the
tax paid for the freedom." True — and that is the argument *against* the
proposal, not for it: if the owner pays the operating tax anyway, the realm
holding the socket buys nothing. The realm gains no observability from it
(the delivery log and dead ring cover field events only; the socket's
contents are opaque either way), gains no trust (the draft's own §1 ruling:
trust comes from ownership, no credential chain is needed), and takes on
liveness policy for a connection whose semantics it cannot inspect. The
remote booth was a broker wearing its client's clothes.

**What the realm still does offer remote ends.** Tier-1 remote
*execution* — the probe (`Body::RemoteProbe`, ADR-0027) — is untouched
and is the only remote shape: the control plane ships content-addressed
code into an execution node and holds that WS connection (node keypair,
ADR-0015). It belongs because the control plane is the code's author;
that relationship has no analogue for a third-party service the realm
merely "visits."

## Honest semantic cost

- **Outbound calls are invisible to the realm.** A booth's HTTP call, its
  latency, its failure, its side effects on an external system: none
  appear in the observation surface. This is not new opacity — script
  bodies already run arbitrary code on the control plane — but the ruling
  makes it explicit so integrators stop expecting outbound auditing from
  the realm. A booth that needs to *record* its external interactions
  should emit events doing so; the pattern is the booth's, the log is the
  field's.
- **Retries and idempotency are the booth's to build.** The realm's
  "no retry, failure is a value" stance (ADR-0012) transfers: a booth
  calling an external service owns its duplicate-order problem — the draft
  gave that problem to the external service; the difference now is that
  the code lives in the booth, where the author can actually see it.
- **No new vocabulary rule was needed.** The draft's §4 rules (dial-out
  bare names, dial-in forced prefix) dissolve: no booth registers with an
  endpoint, so there is no dial-out name surface; dial-in is prism's
  session plane with its existing stamp rule. The one vocabulary ruling
  that stands is the one already in effect — the EventName registry is a
  realm-wide fact, every booth in the field uses bare names, and the proxy
  stamps what it proxies.

## What survives

Two observations from the draft were correct and remain on the record:

1. **The actor model never demanded determinism.** Realm semantics are
   queue delivery (PLAN 4.5c; ADR-0014), serial per instance key, failures
   as values (ADR-0012). No replay assumption, no recoverable randomness,
   no latency assumption — a human behind a prism session is not a special
   case any more than a Go binary with its own Postgres would be.
2. **The unified call model is target-agnostic** (Phase 3.5:
   `CallSpec{tier, timeout}`, `CallSlot`, `pending_remote`). It serves the
   `RemoteProbe` arm today and would serve any future remote target —
   which is precisely why no remote-booth *machinery* was needed to prove
   it: the machinery that exists serves tier-1, and nothing else asked to
   ride it.

## Consequences

- **prism `docs/PLAN.md` Phase 1.8:** the DESIGN CONSTRAINTS block recorded
  from the draft's discussion is rewritten to this ruling — the parts that
  stand are prism's own (session plane, auth block, prefix stamp, Phase 1
  streaming half); the dial-out/CBOR/remote-booth registry half is removed.
  The probe mount (`/probe/<alias>`) and node registry are tier-1 and
  unaffected.
- **Code:** no change. `code_base_url` stays a node-level concern
  (ADR-0027 serves tier-1); no remote-booth arm existed to delete. Two
  comment/string sites that said "remote booth" while meaning the probe
  were corrected to "remote probe" (`config/src/kdl.rs`,
  `realm/src/instance.rs`).
- **Wasm outbound, when a wasmtime booth needs it:** a carrier-capability
  task (wire a wasi-http host into the wasmtime carrier, per booth type),
  not a realm-config task. Open until someone needs it.

## Related records

ADR-0012 (failure is a value; no retry), ADR-0015 + ADR-0027 (tier-1
remote execution, the surviving remote shape), ADR-0017 (the connection
plane — where browsers actually live), `docs/design/modeling.md` §outbound
(the push direction of the same stance), ADR-0032 (booth terminology; its
"remote booths" references were part of the withdrawn draft's naming sweep).
