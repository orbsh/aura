# 0031 — Remote booths: two tiers, ownership-decided trust, live delivery

> **Languages:** [English](0031-remote-booths.md) (primary) · [中文](0031-remote-booths.zh-CN.md)

**Status:** Proposed (2026-09-26). Revised the same day after user review: the
first draft's rulings — tier-2 approval *stricter* than tier-1, a three-axis
key space, and an event-vocabulary *grant* ceremony — are withdrawn; the
argument below replaces them. No implementation has started.

## Context

The extension surface today has one remote shape: the probe
(`Body::RemoteProbe`, PLAN Phase 3) — **remote execution**. A probe dials in
over WS, registers by node alias, and runs code the control plane wrote: the
frame carries `CodeRef{url, sha256}` (ADR-0027), execution happens in the
probe's resident sessions. Everything the booth *is* — definition, state,
scheduling — stays on the control plane; the remote end contributes only
execution capacity.

This ADR records **remote participation**: an booth itself — definition,
code, storage — lives remotely and collaborates as a realm participant: it
declares an interface, receives its subscribed events, and answers
invocations. The remote end chooses any language and any database.

The motivation is exact. aura/probe restricts language and data sources
because everything is held where it can be inspected — code is
content-addressed, storage is okm. The remote booth lifts that restriction,
and the lift is not free: operating a service — its language, its database,
its uptime — *is* the tax paid for the freedom. **The realm's attitude to
non-native languages and storage: neither encourage nor restrict.** Whoever
pays the tax lifts the restriction.

The mental model the user supplied: aura becomes something like an MQ broker
and the remote booth a service hanging off it — a traditional microservice
that consumes subscribed events and answers calls, with its own code and
database, un-inspectable by the broker. One correction the next section
makes: this broker's subscriptions are **live**, not durable (§3).

Two observations survive review and shape the argument (each traceable to
landed decisions — this is assembly, not new mechanism):

1. **The actor model never demanded determinism.** Realm semantics are
   queue delivery — per-(event, partition) queues + per-subscriber cursors
   since PLAN 4.5c, which retired the per-booth mailbox model (ADR-0014);
   serial per instance key, failures as values, delivery
   log and dead ring as the observation surface (ADR-0012). No replay
   assumption, no recoverable randomness, no latency assumption. A human
   behind the socket is not a protocol special case: the human is the fluxen
   booth's non-deterministic execution, exactly as a Go binary with its own
   Postgres is a deterministic one.

2. **The remote-invocation machinery is already built.** The unified call
   model (Phase 3.5: `CallSpec{tier, timeout}` declared statically at
   registration, `CallSlot` two-tier waiting, `pending_remote` correlation)
   is target-agnostic — it serves the `RemoteProbe` arm today and serves a
   remote booth by deleting the CodeRef construction (§5).

The first draft's third observation — "prism's client is already an
unregistered remote booth, so remote booths must reuse that mount" — is
withdrawn. The two shapes are essentially different (dial-out with no
credentials vs dial-in with session-scoped gating), and forcing them into
one mould buys nothing; a topology mode added to the connection plane is a
separate thing, not half a split of it.

## Decision

Proposed; paragraphs marked **RULING** are the load-bearing pieces.

### 1. Two tiers; the trust question follows ownership, not capability

Decompose a realm booth into its five constituents and relocate each:

| constituent | local booth | remote booth | what moved |
|---|---|---|---|
| type definition | BoothDef persisted at `register`; `interface_schema` introspected from held source | registration carries the self-declared schema; persistence / route-assembly / plan-resolution paths unchanged | source of truth: "I inspected your code" → "you declared your interface" |
| instance identity | EventRouter + MQ queues + cursors | live connection set; an event with no connected endpoint is a failed delivery, not a backlog | delivery guarantee: durable → live (§3) |
| storage | okm planes, `ctx.store` ns addressing (ADR-0026) | the remote end's own database; the control plane sees a stateful black box | audit surface: the realm can no longer see what the booth read or wrote |
| residency | `Sessions` registry, VM per instance (Phase 2.6) | connection up/down; reconnect = re-register | failure model: "crashed" becomes "disconnected", indistinguishable and irrelevant |
| invocation | run_job dispatch | the existing `RemoteProbe` arm minus CodeRef construction | the code, and with it the control plane's knowledge of *what will run* |

**RULING — trust is an organizational fact, not a cryptographic artifact.**
The first draft asked "what credential chain makes a declaration
trustworthy?" The review answer: the question dissolves for own services.
Auditing another service's code is not an architecture question — two
services written by the same team are not adversaries, and expecting the
realm to verify their mutual behavior would be like expecting one microservice
to audit its peer's source. The same owner who adds a cart booth and wires
its emit into an inventory handler *chooses* the coordination; deliberately
shipping an booth that deletes its own users' data is sabotage, and sabotage
is an insider problem — no credential chain catches it, and none is needed.

So the two tiers ask *different* trust questions, in *opposite* directions:

- **Tier 1, remote execution (probe):** approval answers "trust this machine
  to run *my* code." Content is mechanically verifiable — content-addressed
  (ADR-0027), auditable byte-for-byte — and the heavier credentialing
  belongs *here*, because the control plane ships code into the machine.
- **Tier 2, remote participation (external booth):** approval is the
  deployment decision itself. The realm owner adds their own service; there
  is no ceremony because there is nothing external to approve. Approval at
  tier 2 is **lighter**, not stricter — the first draft's ruling reversed.

| tier | who vouches | direction | credentials | vocabulary |
|---|---|---|---|---|
| probe (execution) | control plane (code is its own) | dials in | node keypair, ADR-0015 | n/a — delivers execution results, not domain events |
| own service (logic) | realm owner | **dials out**: the realm connects to a remote WS endpoint | none — config-level identity (URL / allow-list), same as any microservice client | bare names allowed (§4) |
| user session (logic, fluxen) | the human, proxied by prism | **dials in** over prism's WS plane | device/user anchors, ADR-0017 §2 | forced prefix at the proxy boundary (§4) |

Topology follows the trust model: an own service inside a private network is
reached by the realm dialing out — making the endpoint reachable is the
deployer's problem, exactly as in microservice mesh practice. A browser
cannot be dialed, and cannot hold a long-term secret, so it dials in and the
realm owner's risk simply isn't present there; what remains is
*user-scoping*, which prism already builds (Phase 1's streaming half).

### 2. Connection direction is a consequence, not a knob

**RULING — direction = ownership's topology expression.** Own services
dial out; proxied sessions dial in. Consequences that the first draft got
backwards:

- Own services need no credential chain. What they need is an address.
- Prism's per-event `auth` block is kept — its consumer is fluxen-class
  dial-in participants, and it gates what a *user's* connection may emit
  and receive (the gateway enforces the declared block; lying locks the
  liar out — self-punishment, not trust).
- The first draft's three-axis key space (machine / workload / session) is
  withdrawn. Own services have no workload identity at all — the endpoint
  URL *is* the identity. Only two key spaces remain real: the node keypair
  (transport, ADR-0015) and prism's device/user anchors (sessions,
  ADR-0017 §2). The account-plane table proposed for (account, booth_type)
  is not built.

### 3. Delivery is live, synchronous, on a durable connection

**RULING — remote delivery has no queue semantics: no cursor, no backlog,
no replay.** An event routed to a remote booth is pushed on the live
connection; if no endpoint is connected, the delivery **fails as a value**
and lands in the delivery log / dead ring as failure evidence — the same
observation surface ADR-0012 already gives for unroutable events. The
persistent queues (PLAN 4.5c) and per-subscriber cursors serve **realm-
internal** delivery only; the first draft's "scale-to-zero semantics
transfer free" is withdrawn — the remote participant is *not* scale-to-zero,
it must be up, like any microservice.

This is the tax made concrete: the remote booth must expose a WS server
endpoint and keep it reachable. The draft considered "webhook POST, WS as
an efficiency upgrade" — withdrawn: the obligation is the same in both
shapes, and a half-duplex POST is strictly worse than a durable WS session.
The remote wire is the realm's own frame vocabulary in
**CBOR** (self-describing; schema-first formats like protobuf were rejected
because the participant picks its own schema tooling). A dial-in browser
participant is exempt: JS speaks JSON natively, and its surface is the
session-scoped mount, not the registry plane (§6).

Liveness (a connection idle of events must still be known to be alive) is a
heartbeat question — open, §Consequences.

### 4. Vocabulary: two mechanical rules, no grant ceremony

**RULING — the vocabulary rule follows direction.** The first draft's
mechanisms — qualified emit names as the default, bare-name grants as the
approved exception, verdict/conflict tables reused for event names — are
withdrawn. The EventName registry is still a realm-wide fact
(`order.created` is one name, one meaning), but what protects it:

- **Dial-out (own services): bare names allowed.** The service that emits
  `order.created` is the owner's own code; a mismatch between what it emits
  and what the routes expect is an *internal* bug of the owner's business,
  same class as a native booth's typo — not governed by the realm.
  Cross-namespace collisions between two of the owner's own services are
  the owner's routing design problem, exactly as in microservices practice.
- **Dial-in (proxied sessions): forced prefix.** Prism stamps the session's
  account on every event the browser emits (`<account>/<session>.…`) before
  it reaches the realm, so a browser can never forge or touch bare internal
  names. On the receive side the per-event `auth` block already gates which
  names reach the connection. **Both directions must apply to fluxen
  emissions** — a user-side booth is namespaced by the account that owns
  the session, never by its node or its declared type.

Namespace assignment is mechanical string handling at the proxy boundary —
not an approval decision, not a table, not a credential. The prefix is
prism's envelope: stamped on ingress, stripped on egress — the browser's
vocabulary is bare names on both ends, and a name it emitted that routes
back has its OWN session prefix stripped before delivery (foreign prefixes
either never reach the subscription match — the auth block gates them — or
stay intact; stripping someone else's prefix would undo the isolation in
the outbound direction). Provenance does not ride the name: the sender
envelope (ADR-0017 §7) already ships in the delivery payload. Prefix for
isolation, envelope for provenance — the two do not couple. The open
question the first draft left ("who grants bare names?") has no residue:
nobody grants anything; dial-out gets names by ownership, dial-in never
gets them.

### 5. The call shape: invoke rides the connection, failure is a value

No new mechanism. A remote type declares a `CallSpec` at registration
(static, Phase 3.5): **hot** — the caller parks on the oneshot, a WS
round-trip, timeout expiry is a failure value; **cold** — a human behind the
session (fluxen), no deadline, the caller's task ends, reply frame
correlates via `pending_remote` and re-enters the suspended turn. Timeout
and "call dropped (sender gone)" are failure *values* delivered through the
same channel (ADR-0012); **there is no retry and no fallback, and none
should be built** — at-least-once semantics would require the durable
delivery §3 just rejected, and retrying a side-effecting remote booth is
the microservice duplicate-order problem, which the service — not the
broker — owns. The realm's duty is the honest observation surface: delivery
log and dead ring.

### 6. fluxen as the acceptance test

fluxen applications delivered to browsers are the proposal's first real
consumer and its sharpest correctness test: the workload (a cart page's
state machine) is remote; its deterministic execution is replaced by a human
(observation 1); it dials in (direction, §2); its emissions are prefix-bound
(vocabulary, §4); it receives domain events through the auth block (the
prism Phase 1 streaming half — now load-bearing for this ADR, unlike the
withdrawn "one mould" framing).

Acceptance criterion for the abstraction, stated for review: **the own Go
binary and the user's browser are both "declare an interface, receive events,
answer calls" participants with one protocol — and their only differences are
exactly the two the first draft conflated: direction and vocabulary rule.**
If any *other* distinction appears (separate registration path, separate
frame language, separate observation surface), the tier boundary was drawn
wrong.

## Honest semantic cost

- **The control plane gives up knowing what runs.** Code invisible, storage
  invisible: the audit surface collapses to delivery log, dead ring, and a
  self-reported schema. For own services this costs little — trust comes
  from ownership (§1), but *knowledge* does not: a remote booth's typo or
  version drift is found via failed deliveries after the fact, invisible
  before it. The distinction between trust and knowledge survives the trust
  simplification and must be stated to integrators. Some consumers (Gravity
  prompts that assume booth semantics are inspectable) must be re-examined;
  none may assume correctness of a remote participant's claims.
- **Live delivery means downtime costs events.** Realm-internal subscribers
  can vanish for days and resume with their cursor; a remote participant
  that is down loses its window — it observes failures, it does not replay.
  Anyone whose business needs durable external delivery has outgrown this
  architecture's scope.
- **A lie surface remains, and it is small.** Self-declared auth blocks and
  receives sets are promises; enforcement of the auth declaration stays real
  (gating by the declared block), under-declaration is undetectable by
  design. For own services lying to yourself is structurally pointless; for
  dial-in sessions the auth block is the user's own consent surface, not an
  adversarial one.
- **The remote wire is a second wire.** CBOR frames on the registry plane,
  JSON on the browser mount: two encodings, one protocol. Justified because
  the trust models genuinely differ (§4), but the duplication is real.
- **What this does not claim:** not a proposal to make realm *state*
  remote-consistent. A remote booth's storage is its own; the realm offers
  exactly none of the durability, ordering, or transactionality guarantees
  ADR-0026/0027 give native booths. The remote participant opts out of all
  of them knowingly — and pays the tax (§Context) for the freedom to have
  chosen so.

## Consequences

- **Ordering (proposed):** (1) dial-out delivery: registration carries an
  endpoint URL, the realm dials and holds the connection, event push and
  invoke ride it (dispatch arm = `RemoteProbe` minus CodeRef); (2) prism
  Phase 1 streaming — the auth-gated receive half — lands the dial-in
  surface fluxen needs; (3) the vocabulary prefix rule (prism-side string
  handling) before fluxen emissions open. No account-key infrastructure, no
  verdict-table extension, no ADR for bare-name grants — those were the
  first draft's sequencing and are cancelled.
- **Touch surface when implemented:** realm dispatch arm + dial-out
  connection manager (registry-plane, CBOR), registration-frame plumbing,
  EventRoute writes for endpoint-backed types; prism prefix stamp + egress
  strip + subscription streaming. All small; the cost lives in the decisions
  above.
- **Cross-repo:** probe-protocol (dial-out frame shapes; CBOR wire for the
  registry plane), prism (connection plane hosts the session mount; stamp
  rule), aura (realm + ADR chain).
- **Deliberately open:** (a) heartbeat/liveness policy for held dial-out
  connections; (b) fluxen staging — does it dogfood the dial-in mount before
  any real business event flows; (c) whether dial-out endpoints get their
  own posture (`open`/`required`-like) for *who* the realm dials — probably
  just an allow-list, but the shape deserves one sentence somewhere;
  (d) the `auth` block's posture semantics for a tier-2 dial-out service
  (it has none — confirm that's right or fix §5).
- **Follow-up (landed 2026-09-26):** prism `docs/PLAN.md` Phase 1.8 carried the
  *first* draft's recorded constraints (stricter tier-2 approval, three key
  spaces, vocabulary gate as prerequisite). Its DESIGN CONSTRAINTS block has
  been rewritten to this ADR's §1–§4 (ownership-decided trust, two key spaces,
  mechanical vocabulary rules, live delivery, CBOR dial-out wire).

## Related records

- ADR-0012 (emits uncollected; failures are values; dead ring as
  observation surface — §3/§5 are applications of it), ADR-0015 (node
  identity — transport-only, the key space §2 keeps), ADR-0017 (connection
  plane; its Phase 1 streaming half is now the dial-in delivery surface),
  ADR-0026/0027 (what remote participants opt out of), PLAN 4.5c
  (persistent queues — realm-internal only, per §3), Phase 2.6 (resident
  sessions — probe-side, contrast with §3's "remote booths must be up"),
  Phase 3.5 (the unified call model §5 rides).
