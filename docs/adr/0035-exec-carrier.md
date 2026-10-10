# 0035 — exec carrier: out-of-process booths in two modes

> **Languages:** [English](0035-exec-carrier.md) (primary) · [中文](0035-exec-carrier.zh-CN.md)

**Status:** Accepted (2026-09-28). Design; implementation pending, see
Consequences. Motivated by gravity's full-Rust wish (wasm limits felt as
needless) and the nushell PTY carrier's cost (the most fragile machinery
among the four residents). Raised by the user as "effector gains a CGI-like
mode — info travels through the pipes, not environment variables".

## Context

The resident carriers are three embedders plus one wrapper: steel (in-process
VM), python (in-process CPython), wasmtime (in-process compiled), nushell
(PTY-driven REPL). Each embeds where the language allows embedding. Nushell's
entry is the exception that proves the rule: the REPL is coerced into RPC
service by cursor-query answering, prompt-silence draining, and file-based
req/resp polling — machinery maintained to imitate a resident session rather
than to run one.

Meanwhile the wanted capability is simpler than any carrier: run an
executable. A Rust booth binary should not need a wasm sandbox it never
asked for; an AI-generated SKILL (dynamic script, run once, return) should
not need a REPL it never asked for either. And per ADR-0031, a booth's
outbound behaviour belongs to the booth's own code — an out-of-process
binary is that code at its strongest, with no host-side capability surface
to keep honest.

The question this ADR settles: what shape makes direct execution a carrier
without dissolving the booth model (state, iterate, ctx calls, residency
accounting) that the in-process carriers provide.

## Decision

**RULING — two out-of-process shapes named by their lineage. bgi (Booth
Gateway Interface — framed, resident) carries the booth model; exec
(bare one-shot, no protocol) carries invoke and nothing else — stateless
by definition, like the cgi/fpm lineage it descends from.**

### 1. bgi: the resident bridge (the booth mode)

Effector spawns the child once per booth instance and keeps it alive; frames
flow over the child's stdin/stdout. It maps onto the existing
`ResidentSession` seam without new runtime shapes: load = spawn + handshake,
call = request frame in / response frame out, evict = close stdin + SIGKILL
escalation. Supervision reuses the existing sandbox policy (bwrap): the
child's only fds are the two pipes — no inherited environment beyond the
declared credential vars, no network unless the jail grants it.

"CGI-like" from the founding request is a misnomer worth naming: CGI is
spawn-per-call; what makes booths work here is the FastCGI shape — the
process persists, the protocol lives on the pipes.

### 2. exec: bare one-shot (the cgi shape — no protocol at all)

Refined the same day (the user's fcgi-vs-cgi analysis): exec is NOT
"bgi minus the loop" — it does not inherit the framing either. Spawn,
write the whole request as one JSON document on stdin (`{"handler":
"<event>", "args": <value>}`), close (EOF is the script's cue to run),
read stdout to EOF as the result value. A one-shot script implements
NOTHING: no loop, no frame parsing, no exit protocol. The php-fpm
lineage is exact and on purpose — nothing survives between calls —
which is precisely what a SKILL wants (run once, return) and what
nushell's runtime can actually do (its `open` delivers at writer-EOF).

The vocabulary follows the lineage: **bgi** (section 1) is the framed
resident shape — the loop lives in the child (author-written or a
effector-shipped shim; the fcgi-adapts-cgi move). **exec** is bare cgi —
the parent's per-call spawn is the adapter, and an adapter that
re-launches per request has the php-fpm semantics: stateless by
definition, not by omission. Recorded so nobody files the statelessness
as a bug: iterate on an exec booth is an error value that names the
design, there is no ctx channel to hang calls on, and there is no
residency to evict.

SKILLs stay invoke-only (the established downgrade): generated scripts
need no storage surface, no residency; their bodies are not booths.

### 3. Wire: frames in a DECLARED codec (json | cbor), bgi only — typed host frames (ADR-0037 §2)

One frame codec per session, selected by the booth's declaration
(ADR-0037 §2 — dual-protocol by declaration, user ruling 2026-09-30).
JSON-lines is the default and the stdlib-reachable entry: any language
reaches it with its parser — no library tax for entry, which is the
carrier's whole purpose. CBOR (one self-delimited document per frame, no
length prefix, no line terminator) is the declared upgrade for carriers
that bring a codec; the earlier whole-channel plan died on the measured
fact that nushell's stdlib has no CBOR codec. exec has no wire vocabulary
— one document in (the declared codec), one out (section 2).

Frame vocabulary (the bgi duplex; the shapes below are logical — the
declared codec carries them as JSON text lines or CBOR documents):

```
{ "id": N, "kind": "call", "event": "<handler>", "args": <value> }            → in
{ "id": N, "kind": "iterate_start|iterate_next|iterate_dispose",
  "event": "<handler>", "op": "start|next|dispose",
  "args": <value>, "stream_id": "<sid>" }                                      → in
{ "result": <value> }                                                          ← out
{ "host": {"type": "invoke|iterate|store|interface_schema", …} }               ← out (bgi only: the booth calls the host — TYPED, ADR-0037 §2; the retired free `op: "<ctx-fn-name>"` lookup is gone, a bad discriminator fails at decode)
{ "host_reply": {"ok": <value>} }                                              → in  (bgi only)
```

`result` is a success value only — mid-call failures surface on the
parent's error channel (the outer Result discipline, ADR-0012). Under the
unified envelope (ADR-0036, landed in Phase 4.15) a dispatch frame's reply
is a stream envelope: a plain one-shot script's bare stdout is wrapped to
`{done: true, value: <stdout>}` at the carrier — the script stays
protocol-free (0036 §2), and the stream verbs after it are the named
error value (the cgi shape holds no residency).

Host calls in bgi ride the same typed frame vocabulary as the remote
wire bridge (invoke, iterate, store — ADR-0037 §2, landed 4.16c): a bad
discriminator or verb fails at decode and answers as an error value, the
old free-function-name table miss is gone. `ctx_store_emit` joined the
arm set at Phase 4.14 gate 1 (`HostOp::StoreEmit` — one okm instruction
as DATA, schema-blind on the effector side); ADR-0037 §3 retired its ENTRY
(the typed `store` frame carries the same DATA). stdio is a transport
for the existing ops, not a new surface. The envelope
(ADR-0034, unified by ADR-0036: one shape, `done` always a boolean,
`value` on the terminal round) is CBOR-encodable schema: it crosses
unchanged, and the frame protocol inherits its typing rules (`done` is a
field, never a sentinel).

### 4. Language = spawn spec, not carrier

The carrier is one; the per-language entries are spawn declarations in the
effector registry: `nu` = `["nu", "--no-config-file", "-c", ...]` over the
effector-shipped frame-loop shim; a compiled Rust booth = `["./booth"]`, the
binary implements the loop against a documented frame contract (effector
publishes no guest crate for it; the contract is the ABI, as `aura_alloc`
is for wasm). Registration advertises carried languages unchanged — the
effector's capability list gains spawn entries, not a new kind of thing.

### 5. BGI — Booth Gateway Interface: the wrapper, named

The resident-bridge contract (section 3) has an asymmetric edge: the
parent's side is a loop over frames, the child author's side should be a
handler. The translation layer in between — a per-language outer loop
that turns "process this line" into "dispatch this event, call this
handler, stream this envelope" — deserves a name the same way CGI earned
one: **BGI, Booth Gateway Interface**. Naming it matters because it is
the portability surface: one BGI per language is what makes "any language
that can read a line" mean "any language can be a booth", the same move
that made CGI a universal HTTP story rather than a per-server one.

BGI is the interface; the section-3 line protocol is the wire. The
wrapper's job: open the channels, loop, decode the frame, name-dispatch
to the author's handler, encode the reply. Author-visible shape per
language (design note — recorded, not required to ship in this phase):

```nushell
run_bgi {|e|
    match $e.kind {
        "call" => dispatch $e.event $e.args,
        ...
    }
}
```

```python
import aura_bgi

@aura_bgi.event("order.created")
def on_order(e):
    ...

aura_bgi.run()          # the outer loop; handlers never see frames
```

Rust booths implement the trait (or a `main` that calls the shim's loop);
bash reads `read -r line` cases. The effector ships no guest crate and no
guest SDK — the wire is the contract (section 4's rule), and each BGI
wrapper is either a published shim (effector assets, like the nushell
adapter) or three lines of the author's own loop. The name exists so
those artifacts have one thing to be adapters *of*.

What BGI is not: not a second protocol (it wraps the existing line
frames), not aura-side machinery (the realm never sees it), and not
required for exec — the bare shape has no loop to wrap; its adapter is
the per-call spawn itself.

### 6. Nushell PTY retirement — LANDED, no dual tracks

The PTY carrier (NushellResident, bridge.nu, pump_quiet and its regression
locks) is deleted: bgi carries the `ctx_store_emit` arm (the `HostOp`
`store_emit` variant — wire parity with what the in-process bridge already
provides) and the store-emit round trip passes on the two-fifo shape
(`exec_booth.rs::bgi_nu_booth_store_emit_roundtrip`). The gate was sequenced
ahead of deletion because the store-emit roundtrip is live acceptance —
deleting the PTY before the framed shape provably carried it would turn the
suite red and re-open the double-maintenance door the pass closes. That
ordering is now satisfied; the retirement landed with it. One execution shape
per language, ever: nu's shapes are `exec` (bare one-shot) and `bgi` (the two-
fifo adapter) — the `nushell` language string and its PTY feature are gone from
the carriers, the Cargo feature tree (effector-runtime, aura-realm, aura-engine)
and the wire vocabulary.

### 7. Trust tiers unchanged: exec is the trusted posture, wasm keeps the untrusted one

The exec carrier does not weaken anything because it replaces nothing
that was a sandbox: the bwrap jail is deployment-level isolation for
host-trusted code (same posture as the PTY and embedded carriers), while
wasm remains the only hardware-isolated tier for untrusted delivered
code (ADR-0027's fetch-and-verify path targets it). A Rust author who
wants a booth writes an exec binary; one who wants to run stranger's
code compiles it to wasm. The two tiers are not substitutes and the
"cross-platform" argument for wasm is not its reason — its reason is
import-whitelist capability refusal, which fd-level framing cannot
offer (a child either has a capability in its jail or it does not; a
wasm module has nothing until the host wires it).

## Honest semantic cost

- **Every call crosses a process boundary.** bgi pays a pipe round
  trip per call against in-process carriers; exec pays the full spawn.
  The pricing is the consumer's: hot loops pick the embedded carrier,
  operator-shaped and one-shot work pick exec. This is stated, not
  hidden: the exec carrier's existence does not make any currently-fast
  path faster.
- **The bgi ctx bridge is frame protocol, not native.** Python's
  `ctx_store_emit` is a registered builtin; a bgi child's is a `host`
  frame and a reply. Deeper debug surface, one more serialization seam — the
  standard price of process isolation, paid equally by every exec
  language.
- **Two shapes, not one protocol with a flag.** The ADR's founding line
  ("B is A-without-a-loop") was wrong in the making and is corrected
  here: bgi is framed and resident, exec is unframed and per-call; the
  shared part is only spawn supervision, not the exchange.
- **nushell reaches bgi through a channel adapter, not through stdin.**
  Effector-verified: nu cannot block-read a non-TTY stdin (`input line`
  errors) and its `open` delivers at writer-EOF, so stdin-direct resident
  bgi is impossible. The adapter is a TWO-fifo shape: requests ride `req`
  (parent writes one frame and closes the writer — the batch EOF the
  child's `open --raw $req | lines` wakes on), ctx replies ride a second
  `rep` fifo, result frames ride stdout. The split is what makes reply
  routing deterministic: on one fifo, the author's outer request reader
  and its inline ctx-reply reader race for every written frame (a single-
  fifo shape hung in the effector; wakeup order decides who gets the frame).
  The author's script runs `def main [req rep]` as the loop — nu
  auto-invokes `main` with the script arguments, and because `source`
  rejects a dynamic path at parse and nu has no eval, the entry-side
  dispatch is a hand-written `match` on event names (the single-entry
  table every language without a runtime name lookup uses). The batch
  loop is a `for`, not an `each`: `each`'s closure scope eats the `$env`
  writes the residency rule relies on. The retirement gate (§6) cleared
  on this shape.
- **Capability gating is coarser than wasm's import list.** bwrap grants
  file/net scopes; it has no per-symbol notion. Recorded as tier design,
  not as a defect to fix later.

## Consequences

- **effector:** exec carrier module (spawn, frame loop, shim registry);
  bwrap policy reuse; the nushell PTY machinery retired with the §6 gate
  (the `nushell` language string, the PTY module and its Cargo feature are
  gone — nu's shapes are `exec` and `bgi`/two-fifo).
- **aura:** the frame protocol reuses the existing op vocabulary
  (ToolCall/HostOp); `HostOp` gained the `store_emit` arm (Phase 4.14
  gate 1) — closing the wire-parity gap the remote stdio bridge had
  against the embedded carriers.
  The language string in registration selects a spawn spec. ADR-0011's ctx
  boundary is untouched — host fns gained nothing, a transport grew.
- **gravity:** full-Rust booth path = bgi binary; SKILLs = exec
  (bare one-shot). The provider booth (python, ADR-0034 §6) is unaffected — its
  generator mode stays where the host drives it across an FFI seam
  nobody has to cross.
- **okm:** none — the frame contract lives in effector-protocol.
- **PLAN:** new phase for the exec carrier; the nushell PTY deletion is
  the phase's last item (gate §6), never a standalone change.

## Related records

ADR-0034 (envelope and host-op vocabulary the frames carry), ADR-0031
(outbound behaviour belongs to the booth's own code — an exec binary is
that code at its strongest), ADR-0027 (content-addressed delivery —
exec fetches to a temp path and runs it once), ADR-0015/0016 (node trust
and residency accounting unchanged for bgi instances), effector
ownership ruling (the effector executes delivered code; spawn is execution).

## Update (2026-10-01) — the reply contract, made explicit (from mudra's extension review)

Section 3 lists the frames but nowhere states what a guest must produce.
The implemented answer already exists in `CallSpec` (`crates/booth/src/call.rs`):
**reply semantics are per-target, statically declared** — Tier rides the
booth/target registry beside the carried languages ("static execution
nature … declared at registration, never guessed at runtime"). A **Hot**
target parks the caller: the guest's `result` must arrive within the
deadline, and expiry is itself a failure value — the caller never hangs.
A **Cold** target never parks: dispatch returns Pending and re-enters via
`resolve_call` whenever the guest answers (minutes-scale waits on humans or
external systems). A handler is one shape or the other — a call that is
simultaneously fire-and-forget and request-response does not exist. Also
worth restating: `result` is not the booth's only outbound voice — the
typed `host` frames are how an actor-initiated call flows mid-handler.
The wire section read as plain RPC; this note records that it is an actor
substrate with function-call sugar on top.

Adoption record: mudra's extension protocol
(`~/world/mudra/docs/ADR-extension-protocol.md`) takes this vocabulary as
its wire contract — profile deltas: no `store` arm (extension state lives
with the extension), json-lines codec only, `@on` event subscriptions
declared in the script's interface_schema and carried by the session hello.
