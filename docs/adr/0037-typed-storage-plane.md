# 0037 — Booth storage access: typed host channel + in-process bindings (amends ADR-0026 §3)

> **Languages:** [English](0037-typed-storage-plane.md) (primary) · [中文](0037-typed-storage-plane.zh-CN.md)

**Status:** Accepted (2026-09-28); §2 AMENDED 2026-09-30 by the user's
dual-protocol ruling (below). ALL LANDED: §1 (Phase 4.16a python binding
face 2026-09-30; Phase 4.16b steel Collection face 2026-09-30) and §2
(Phase 4.16c typed host frames + declared dual-encoding 2026-09-30).
Raised by the user's contract challenge: python/steel
have directly-bound Collection surfaces (okm's embedder usage, decided in
ADR-0022), yet ADR-0026 §3 wrote the in-process bridge as a translation to
instruction documents — the correct end state of "the bridging cost is paid
once, in the adapter" is not translating at all.

## Context

ADR-0026 §3's contract: "`ctx.store` exposes exactly ONE interface —
`ctx.store.emit(op)`". As a CAPABILITY contract that holds (one entry, no
second vocabulary); but §3 then wrote python's implementation as "the script
implements okm's `VirtualStorage` adapter, translating each engine call into
one `ctx.store.emit`" — the in-process carriers detour through the document
layer too.

The user pointed out this is not the contract's only implementation, and not
the best one:

1. **okm's established embedder usage is direct binding.** `bindings/okm-python`
   registers `DynamicCollection` (put/get/delete/scan/plan_put/plan_delete) as
   a Python class; `bindings/okm-steel` already registers the
   schema/encode/decode function surface. A binding's type signatures catch
   mistakes at the language boundary; a document layer cannot — a typo in
   `{"op": "get_document"}`, an unknown op, a missing argument: TypeError at
   the binding, versus `from_value` failure (op name) or SILENT SUCCESS (a
   misspelled field lands in the dynamic segment) in JSON.
2. **Byte correctness is independent of encoding.** ADR-0026's no-bypass
   ruling keeps the instruction surface at the Collection semantic layer (raw
   primitives are not in the op set); the binding layer does not expose raw
   either. So "binding beats JSON" is not about byte correctness (both sides
   compensate index/reduce through `DynamicCollection`) — it is about **which
   layer validation happens at**.
3. **Carrier stratification.** In-process carriers share the booth's thread;
   direct binding is zero-translation. Out-of-process carriers (bgi, wasm)
   must move payloads — but the payload should not be JSON. The wasm OpFrame
   byte seam is already right; the bgi
   `{"host":{"op":"ctx_store_emit",…}}` frame is the one to fix.

## Decision

**RULING — in-process: bind; out-of-process: a typed host channel; the
channel's encoding is DECLARED per booth — the dual-protocol shape
(json/cbor, user ruling 2026-09-30 replacing the whole-channel-CBOR
plan; §2) — storage is one type among the channel's frames, not a new
encoding invented for storage.**

### 1. In-process carriers (python / steel): bind, no translation

- **python**: register `okm-python`'s Collection binding directly into the
  session module (alongside the `ctx_store_emit` entry; `add_class::<Collection>()`
  exists today). Zero translations.
- **steel**: `okm-steel` has schema/encode/decode registered; the Collection
  method face is filled (4.16b) aligned with the python binding: two host
  faces of the same `DynamicCollection`, one executor. The handle shape the
  `'static`/thread-local constraints decided: a PER-VM registry (not the
  codec handles' thread_local — a session's VM moves across worker threads)
  of integer-internal collections addressed by the script through six
  fixed-name fns by NAME STRING (`(collection-put! "notes" pkey doc)`).
  Fixed names, not a dotted per-collection shim (`Counters.put` — measured
  to resolve on both define and call sides), because steel resolves free
  identifiers at DEFINE-COMPILE time: the introspection throwaway engine
  must carry the same names (the ctx-stub precedent), and shim names are
  script content — unstubbable there, which would reopen the silent
  schema-drop trap.

### 2. The out-of-process channel = one stream, typed frames; the channel's encoding is declared (dual codec)

The user's model: the host channel is not "one JSON-text seam per operation"
— it is **one typed frame stream**, and storage is one type within it. The
frame vocabulary keeps ADR-0035 §3's shape; the payloads become typed:

```
child → parent   {"host": {"type": "invoke",  "args": <typed>}}
child → parent   {"host": {"type": "iterate", "op": "start|next|dispose", …}}
child → parent   {"host": {"type": "store",   "op": <typed okm instruction>}}
parent → child   {"host_reply": {"ok": <typed>}}
```

- **The encoding is DECLARED per booth (user ruling 2026-09-30 —
  dual-protocol).** The channel ships two codecs selected by the
  declaration — `BoothType::encoded(ChannelEncoding)` (aura-booth, serde
  values `json`/`cbor`, persisted in `BoothDef` as the hot-tail append)
  and `ToolCall.encoding` on the remote wire (in-process: the realm
  passes it down; remote: it rides the call to the node). `Json` =
  newline-delimited lines, the stdlib-reachable default every booth and
  older persisted row rides; `Cbor` = one self-delimited document per
  frame (no line terminator — ciborium reads exactly the declared bytes,
  so sequential decodes on the blocking pipe land document-by-document).
  The child learns the codec through the `BGI_ENCODING` env at spawn —
  not an appended argv (the fifo shape passes exactly `[req rep]`; a
  generated argument would break the author's `def main` arity). The
  measured fact that forced the dual shape: nu 0.115 has no CBOR codec —
  the entrance criterion (ADR-0035 §3: any language reaches the protocol
  with its stdlib parser) is a hard wall for a whole-channel CBOR. A
  CBOR declaration against the nu fifo spec is a spawn-time error, never
  a silent downgrade. The typing (below) applies under BOTH codecs —
  the encoding changes the codec, the typing changes the vocabulary;
  orthogonal axes. The Windmill criterion still gates a third codec:
  none is parsed until one drives an action only parsing gets right.
- **Typing kills document-layer mistakes**: a typo'd `{"op":"put_docment"}`
  passes the text layer silently and explodes at serde; with typed frames the
  op discriminates at the frame-structure layer — a wrong CBOR tag/field
  number = decode failure = error value, no silent path.
- **wasm is untouched**: the wasm OpFrame byte seam (`aura_host.emit`) IS this
  stance already implemented — engine-call bytes, not documents; ADR-0026 §3's
  "no dynamic instruction path for wasm" stands.
- **nushell**: binding is impossible (nu has no okm binding; and the BGI shim
  does NOT bind the storage face either — the channel is typed frames, nu
  just reads them), so nu rides §2's typed seam. The bgi shim therefore does
  NOT need to carry a storage implementation: the shim is frame loop +
  dispatch only; store frames forward per §2 as-is.

### 3. Disposition of the `ctx_store_emit` JSON

The JSON instruction document that gate 1 landed (`HostOp::StoreEmit`,
the `host_bridge_for("ctx_store_emit")` fn) was the declared transitional
shape; §2's landing retired its ENTRY — on the bgi seam the free
`op: "ctx_store_emit"` string lookup is gone, replaced by the typed
`store` frame (the instruction itself still travels as DATA — the probe
stays schema-blind; the typed envelope retires the ENTRY, not the
document). The remote WS's `HostOp::StoreEmit` variant was already a typed
frame (serde-discriminated enum), so nothing retires there. After §1,
python/steel never touch this seam; bgi/nu booths read store frames
typed.

## Honest semantic cost

- **Same-day rework**: gate 1 (the `HostOp::StoreEmit` JSON arm + bridge fn)
  is declared transitional within a day of landing. Accepted — the contract
  ambiguity in §3 was punctured by the user's binding facts, and fixing the
  contract costs less than letting the transitional shape calcify.
- **The probe's dependency surface grows**: registering the python binding =
  probe-runtime's python feature needs `okm-python` (or its logic inlined
  into probe's python carrier). Not a breach of "probe never depends on
  Aura crates" (okm is independent of aura), but okm becomes a transitive
  probe dependency — the registry + typed frame protocol + direct bindings
  are all pushing the probe from "protocol mover" toward "runtime"; watch
  that line continuously.
- **The steel binding's gap closed with a real decision inside it**:
  filling `okm-steel`'s Collection face (4.16b, an okm-repo cut) forced
  the shape ruling the quote had anticipated — per-VM registry + fixed
  name-string fns (see §1), the dotted-shim alternative rejected by the
  define-compile trap, not by taste.
- **Transitional period = two correct shapes coexisting**: after §1,
  python/steel bind while bgi/nu still ride the JSON seam — the ENTRY
  shape is uniform once §2 lands (typed frames under both codecs); the
  binding-vs-frame split is the carrier stratification §2 intends, not
  residue.
- **The dual codec is a measured concession, not a hedge**: the original
  whole-channel-CBOR plan died against nu's stdlib (no CBOR codec, and
  adding one to a language's reach is the library tax the entrance
  criterion forbids). The declaration carries two codecs instead of one
  upgrade — every booth rides exactly ONE for its life, the channel never
  negotiates per frame, and a third codec stays behind the Windmill gate.

## Consequences

- **okm** (implementation): `okm-python` gained the host-injected face —
  `Collection::with_store` over a byte-level `Engine` trait (4.16a,
  commit 58cf72b). 4.16b landed the engine face + entry parsing as the
  SHARED crate `okm-entry` (python rewired onto it — bindings must not
  fork the entry semantics any more than the byte layout) and
  `okm-steel`'s Collection method face over it (put/get/delete/scan +
  reduce reads, per-VM registry, name-string fns). 4.16b also fixed
  `okm-steel`'s standing breakage: `Value::Obj`/`Value::Array` (okm
  0c2a354) never got arms in `value_to_steel` — the binding had not
  compiled since. A latent defect surfaced while wiring the host:
  slatedb's sync facade `block_on`s a held runtime, which PANICS on a
  thread with a tokio context entered (spawn_blocking keeps it — the
  realm drives injections exactly there); the facade now rides a
  dedicated driver thread (okm 92b2551, locks in
  `okm-core/tests/driver_thread_test.rs`).
- **probe** (4.16a landed): `HostBridge` carries a `storage` slot — the
  host's engine behind four byte-closure fns (`StorageEngineFns`: no okm
  types on the seam, so probe and aura build against different okm revs
  freely) + the plan's raw collection entries; the python carrier's load
  step builds one `Collection` per entry over it and `module.add`s it
  under the collection name (the engine handle never crosses a seam
  back — a pyclass carries `*mut PyObject`, not Send); the steel carrier
  consumes the same slot (4.16b): `ClosureEngine` adapts the four byte
  closures to okm-steel's Engine trait, `SteelSession::new` builds the
  per-VM registry at session start and registers the stub arms ONLY in
  the introspection engine (the ctx-stub scoping rule — same-name
  `register_fn` stacking would shadow the real fns in resident sessions).
  Remaining: bgi's `exchange()` implements §2's typed-frame shape — LANDED (4.16c):
 `exchange()` decodes `{"host": {"type": …}}` into a typed enum mapped
 onto the bridge table (decode failure = error value, the silent
 unknown-op miss is gone); the session codec is the declared one (JSON
 lines or self-delimited CBOR documents), crossing to the child as the
 `BGI_ENCODING` spawn env; exec one-shot rides the same field; wasm
 untouched.
- **aura** (4.16a/b): `run_job`'s script arm fills the slot from
  `StorePlan.entries`, engine closures capturing the BARE realm-mq
  handle — the injected `Collection` binds ns itself, so the binding face
  and `ctx_store_emit` land byte-identical (the `ns_raw` shape is the
  wasm plane only). Locks per carrier: `py_injection.rs` (python, 4.16a),
  `steel_injection.rs` (steel, 4.16b — the same cross-check shape:
  binding write ↔ emit read and the reverse, eviction rebuilds the
  per-VM registry over the surviving rows). `host_bridge_for`'s `ctx_store_emit` JSON entry
  retired with §2 (4.16c; the op set was frozen meanwhile, §3); the
  realm-side executor survives whole (`store_exec`, plan resolution) —
  the payload's arrival shape is now a typed `store` frame. The
  declaration surface (4.16c): aura-booth carries its own
  `ChannelEncoding` (the crate stays probe-free), `BoothType::encoded`
  selects it, `PersistedBooth`/`BoothDef` persist it as a hot-tail
  append (old rows reload as json — their actual behavior), and
  `introspect_schema` spawns the throwaway child in the DECLARED codec.
- **Docs**: ADR-0026 §3's python wording is amended per this ADR (bind, no
  adapter); ADR-0035 §3's host frame shape updates with §2; this file
  supersedes both storage-bridge passages.
- **Sequencing**: landed in the planned order — 4.14 gates 2/3 and 4.15
  came first (the nu shim + PTY retirement rode the transitional JSON
  seam; the envelope unification gave the host and call channels one
  shape), then this ADR's §1 (bindings, 4.16a/b) and §2 (typed frames +
  declared dual-encoding, 4.16c). The CBOR half arrived NOT as the
  whole-channel upgrade this file first sketched but as the declared
  dual codec (user ruling 2026-09-30): nu's stdlib wall measured the
  single-upgrade plan dead.
