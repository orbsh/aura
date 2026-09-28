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

**RULING — the exec carrier is one mechanism with two modes. Mode A
(resident bridge) carries the booth model; mode B (one-shot) carries
invoke and nothing else.**

### 1. Mode A: resident bridge (the booth mode)

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

### 2. Mode B: one-shot (the SKILL mode)

Spawn per invocation: args in on stdin, result out on stdout, exit.
This is mode A minus the loop — a frame stream whose producer exits at
EOF, hence no resident session, no iterate, no ctx calls. It is the exact
shape of an AI-generated SKILL (run once, return a value), and it is
strictly simpler than the PTY path it replaces for nushell: today's PTY
wrapper already materializes one-shot call files; mode B is that shape
without the REPL it had to share a process with.

SKILLs stay invoke-only (the established downgrade): generated scripts
need no storage surface, no residency; their bodies are not booths.

### 3. Wire: length-prefixed CBOR frames; JSON lines as the debug form

One frame codec for both modes. CBOR is the currency (binary-safe, the
wasm carrier already proved the marshal discipline); JSON-lines is the
documented debug format any language reaches without a CBOR library —
the same "JSON only at seams" rule as the ResidentSession boundary.
Framing: 4-byte big-endian length prefix, then the CBOR document.

Frame vocabulary (the A-mode duplex; B-mode uses the Request/Response pair
alone):

```
{ "t": "call", "id": "...", "event": "<handler>", "args": <value> }   → in
{ "t": "result", "id": "...", "ok": <value> }                          ← out
{ "t": "host", "id": "...", "op": <HostOp> }                           ← out (A only: the booth calls the host)
{ "t": "host_reply", "id": "...", "ok": <value> }                      → in  (A only)
```

Host calls in mode A ride the same `HostOp` vocabulary as the wire bridge
(invoke, iterate). `ctx_store_emit` is not yet a wire arm — the in-process
nushell bridge reaches it through the host-fn table, and mode A needs an
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

### 5. Nushell PTY retirement, gated — no dual tracks

The PTY carrier (NushellResident, bridge.nu, pump_quiet and its regression
locks) is deleted once exec mode A carries the `ctx_store_emit` arm
(`HostOp` gains the `store_emit` variant — wire parity with what the
in-process bridge already provides) and the nushell round-trip test
(echo.rs::nushell_store_emit_roundtrip's shape) passes on it. Retirement
is sequenced behind that gate because
the store-emit roundtrip is live acceptance today — deleting the PTY
first would turn the suite red and re-open the double-maintenance door
the same pass is meant to close. One execution shape per language, ever:
the dual track exists only between "landed" and "gate passed".

### 6. Trust tiers unchanged: exec is the trusted posture, wasm keeps the untrusted one

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

- **Every call crosses a process boundary.** Mode A pays a pipe round
  trip per call against in-process carriers; mode B pays the full spawn.
  The pricing is the consumer's: hot loops pick the embedded carrier,
  operator-shaped and one-shot work pick exec. This is stated, not
  hidden: the exec carrier's existence does not make any currently-fast
  path faster.
- **The A-mode ctx bridge is frame protocol, not native.** Python's
  `ctx_store_emit` is a registered builtin; exec's is a `host` frame and
  a reply. Deeper debug surface, one more serialization seam — the
  standard price of process isolation, paid equally by every exec
  language.
- **Two execution shapes document as one carrier.** A/B share everything
  but the loop; the docs must say "B is A-without-a-loop", not grow two
  carrier narratives.
- **Capability gating is coarser than wasm's import list.** bwrap grants
  file/net scopes; it has no per-symbol notion. Recorded as tier design,
  not as a defect to fix later.

## Consequences

- **probe:** exec carrier module (spawn, frame loop, shim registry);
  bwrap policy reuse; retirement of the nushell PTY machinery once the
  §5 gate passes.
- **aura:** the frame protocol reuses the existing op vocabulary
  (ToolCall/HostOp); `HostOp` gains the `store_emit` arm — the wire-parity
  gap between the in-process nushell bridge and the remote stdio bridge.
  The language string in registration selects a spawn spec. ADR-0011's ctx
  boundary is untouched — host fns gained nothing, a transport grew.
- **gravity:** full-Rust booth path = exec mode A binary; SKILLs =
  mode B. The provider booth (python, ADR-0034 §6) is unaffected — its
  generator mode stays where the host drives it across an FFI seam
  nobody has to cross.
- **okm:** none — the frame contract lives in probe-protocol.
- **PLAN:** new phase for the exec carrier; the nushell PTY deletion is
  the phase's last item (gate §5), never a standalone change.

## Related records

ADR-0034 (envelope and host-op vocabulary the frames carry), ADR-0031
(outbound behaviour belongs to the booth's own code — an exec binary is
that code at its strongest), ADR-0027 (content-addressed delivery —
mode B fetches to a temp path and executes), ADR-0015/0016 (node trust
and residency accounting unchanged for A-mode instances), probe
ownership ruling (the probe executes delivered code; spawn is execution).
