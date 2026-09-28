# 0035 — exec carrier: out-of-process booths in two modes

> **Languages:** [English](0035-exec-carrier.md) (primary) · [中文](0035-exec-carrier.zh-CN.md)

**Status:** Accepted (2026-09-28). Design; implementation pending, see
Consequences. Motivated by gravity's full-Rust wish (wasm limits felt as
needless) and the nushell PTY carrier's cost (the most fragile machinery
among the four residents). Raised by the user as "probe gains a CGI-like
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

Probe spawns the child once per booth instance and keeps it alive; frames
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
probe-shipped shim; the fcgi-adapts-cgi move). **exec** is bare cgi —
the parent's per-call spawn is the adapter, and an adapter that
re-launches per request has the php-fpm semantics: stateless by
definition, not by omission. Recorded so nobody files the statelessness
as a bug: iterate on an exec booth is an error value that names the
design, there is no ctx channel to hang calls on, and there is no
residency to evict.

SKILLs stay invoke-only (the established downgrade): generated scripts
need no storage surface, no residency; their bodies are not booths.

### 3. Wire: newline-delimited JSON frames, bgi only (CBOR is the planned optimization)

One frame codec for the bgi shape, one line per frame. JSON-lines is the
shipped form: any language reaches it with its stdlib parser — no
library tax for entry, which is the carrier's whole purpose. Length-
prefixed CBOR is the planned payload optimization (the wasm carrier
proved the marshal discipline; the line framing upgrades when a fast
path needs it — the Windmill criterion, same rule as ADR-0034's pull
batching). exec has no wire vocabulary — one JSON document in, one out
(section 2).

Frame vocabulary (the bgi duplex):

```
{ "id": N, "kind": "call", "event": "<handler>", "args": <value> }            → in
{ "id": N, "kind": "iterate_start|iterate_next|iterate_dispose",
  "event": "<handler>", "op": "start|next|dispose",
  "args": <value>, "stream_id": "<sid>" }                                      → in
{ "result": <value> }                                                          ← out
{ "host": {"op": "<ctx-fn-name>", "args": <value>} }                          ← out (bgi only: the booth calls the host)
{ "host_reply": {"ok": <value>} }                                              → in  (bgi only)
```

`result` is a success value only — mid-call failures surface on the
parent's error channel (the outer Result discipline, ADR-0012; the
envelope's terminal form is Phase 4.15's merge, ADR-0036).

Host calls in bgi ride the same `HostOp` vocabulary as the wire bridge
(invoke, iterate). `ctx_store_emit` is not yet a wire arm — the in-process
nushell bridge reaches it through the host-fn table, and bgi needs an
explicit `store_emit` variant added to `HostOp` (recorded in Consequences
as the wire-parity gap this carrier closes). stdio is a transport for the
existing ops, not a new surface. The iterate envelope (ADR-0034) is
CBOR-encodable schema: it crosses unchanged, and the frame protocol
inherits its typing rules (`done` is a field, never a sentinel).

### 4. Language = spawn spec, not carrier

The carrier is one; the per-language entries are spawn declarations in the
probe registry: `nu` = `["nu", "--no-config-file", "-c", ...]` over the
probe-shipped frame-loop shim; a compiled Rust booth = `["./booth"]`, the
binary implements the loop against a documented frame contract (probe
publishes no guest crate for it; the contract is the ABI, as `aura_alloc`
is for wasm). Registration advertises carried languages unchanged — the
probe's capability list gains spawn entries, not a new kind of thing.

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
bash reads `read -r line` cases. The probe ships no guest crate and no
guest SDK — the wire is the contract (section 4's rule), and each BGI
wrapper is either a published shim (probe assets, like the nushell
adapter) or three lines of the author's own loop. The name exists so
those artifacts have one thing to be adapters *of*.

What BGI is not: not a second protocol (it wraps the existing line
frames), not aura-side machinery (the realm never sees it), and not
required for exec — the bare shape has no loop to wrap; its adapter is
the per-call spawn itself.

### 6. Nushell PTY retirement, gated — no dual tracks

The PTY carrier (NushellResident, bridge.nu, pump_quiet and its regression
locks) is deleted once bgi carries the `ctx_store_emit` arm
(`HostOp` gains the `store_emit` variant — wire parity with what the
in-process bridge already provides) and the nushell round-trip test
(echo.rs::nushell_store_emit_roundtrip's shape) passes on it. Retirement
is sequenced behind that gate because
the store-emit roundtrip is live acceptance today — deleting the PTY
first would turn the suite red and re-open the double-maintenance door
the same pass is meant to close. One execution shape per language, ever:
the dual track exists only between "landed" and "gate passed".

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
  Probe-verified: nu cannot block-read a non-TTY stdin (`input line`
  errors) and its `open` delivers at writer-EOF. The user's mkfifo +
  `loop { open pipe | lines | each }` shape streams per-writer-session
  batches correctly (the round trip incl. the inline ctx-reply read), so
  the BGI wrapper for nu is a two-fifo adapter, not a rewrite of the
  protocol; until that wrapper ships, nushell rides exec (SKILL
  semantics fit it anyway) — the PTY retirement gate (§6) waits on the
  adapter, not the other way around.
- **Capability gating is coarser than wasm's import list.** bwrap grants
  file/net scopes; it has no per-symbol notion. Recorded as tier design,
  not as a defect to fix later.

## Consequences

- **probe:** exec carrier module (spawn, frame loop, shim registry);
  bwrap policy reuse; retirement of the nushell PTY machinery once the
  §6 gate passes.
- **aura:** the frame protocol reuses the existing op vocabulary
  (ToolCall/HostOp); `HostOp` gains the `store_emit` arm — the wire-parity
  gap between the in-process nushell bridge and the remote stdio bridge.
  The language string in registration selects a spawn spec. ADR-0011's ctx
  boundary is untouched — host fns gained nothing, a transport grew.
- **gravity:** full-Rust booth path = bgi binary; SKILLs = exec
  (bare one-shot). The provider booth (python, ADR-0034 §6) is unaffected — its
  generator mode stays where the host drives it across an FFI seam
  nobody has to cross.
- **okm:** none — the frame contract lives in probe-protocol.
- **PLAN:** new phase for the exec carrier; the nushell PTY deletion is
  the phase's last item (gate §6), never a standalone change.

## Related records

ADR-0034 (envelope and host-op vocabulary the frames carry), ADR-0031
(outbound behaviour belongs to the booth's own code — an exec binary is
that code at its strongest), ADR-0027 (content-addressed delivery —
exec fetches to a temp path and runs it once), ADR-0015/0016 (node trust
and residency accounting unchanged for bgi instances), probe
ownership ruling (the probe executes delivered code; spawn is execution).
