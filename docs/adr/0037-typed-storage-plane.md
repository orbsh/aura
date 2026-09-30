# 0037 — Booth storage access: typed host channel + in-process bindings (amends ADR-0026 §3)

> **Languages:** [English](0037-typed-storage-plane.md) (primary) · [中文](0037-typed-storage-plane.zh-CN.md)

**Status:** Accepted (2026-09-28). §1's python binding face LANDED
(Phase 4.16a, 2026-09-30); the steel Collection face (4.16b) and the
typed-frame host channel + CBOR (4.16c, §2) remain — see Consequences.
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

**RULING — in-process: bind; out-of-process: a typed host channel; CBOR is
the payload encoding for the WHOLE host channel (storage is one type among
its frames), not a new encoding invented for storage.**

### 1. In-process carriers (python / steel): bind, no translation

- **python**: register `okm-python`'s Collection binding directly into the
  session module (alongside the `ctx_store_emit` entry; `add_class::<Collection>()`
  exists today). Zero translations.
- **steel**: `okm-steel` has schema/encode/decode registered; the Collection
  method face (put/get/scan) is MISSING — an implementation item, not a
  ruling problem. Fill it aligned with the python binding: two host faces of
  the same `DynamicCollection`, one executor.

### 2. The out-of-process channel = one stream, typed frames; CBOR is the channel's encoding (a planned type)

The user's model: the host channel is not "one JSON-text seam per operation"
— it is **one typed frame stream**, and storage is one type within it. The
frame vocabulary keeps ADR-0035 §3's shape; the payloads become typed:

```
child → parent   {"host": {"type": "invoke",  "args": <typed>}}
child → parent   {"host": {"type": "iterate", "op": "start|next|dispose", …}}
child → parent   {"host": {"type": "store",   "op": <typed okm instruction>}}
parent → child   {"host_reply": {"ok": <typed>}}
```

- **The encoding upgrade is the whole channel's**: JSON-lines → length-prefixed
  CBOR. That is the landing shape of ADR-0035 §3's already-recorded CBOR plan
  (the Windmill criterion: build a parser only when parsing drives an action
  only parsing can get right — the CBOR parser becomes worth building the
  moment payloads become byte streams). Do not ride the storage name to
  invent a new encoding.
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

The JSON instruction document that gate 1 just landed (`HostOp::StoreEmit`,
the `host_bridge_for("ctx_store_emit")` fn) is a **transitional shape**: it
retires with the JSON payload when the typed channel (§2) lands; until then
it is the only storage seam for bgi/nu booths, and **no new op is added to
it** (the op set freezes at the Collection semantic set ADR-0026 landed
with). After §1 lands, python/steel stop using the JSON seam; it remains
only for bgi consumers.

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
- **The steel binding has a real gap**: `okm-steel` has no Collection method
  face; filling it is work in the okm repo (a cross-repo cut), and steel's
  `'static`/thread-local constraints (the RegisterFn precedent) will decide
  how the bound handle travels.
- **Transitional period = two correct shapes coexisting**: after §1,
  python/steel bind while bgi/nu still use JSON — the seam is not uniform
  until §2 lands. Accepted: binding-first has standalone value (mistake
  interception + zero translations) and does not need to wait for CBOR.

## Consequences

- **okm** (implementation): `okm-python` gained the host-injected face —
  `Collection::with_store` over a byte-level `Engine` trait (4.16a,
  commit 58cf72b). Remaining: `okm-steel`'s Collection method face
  (put/get/delete/scan, mirroring `okm-python`'s `#[pymethods]` shape).
- **probe** (4.16a landed): `HostBridge` carries a `storage` slot — the
  host's engine behind four byte-closure fns (`StorageEngineFns`: no okm
  types on the seam, so probe and aura build against different okm revs
  freely) + the plan's raw collection entries; the python carrier's load
  step builds one `Collection` per entry over it and `module.add`s it
  under the collection name (the engine handle never crosses a seam
  back — a pyclass carries `*mut PyObject`, not Send). Remaining: bgi's
  `exchange()` implements §2's typed-frame shape (JSON first, payload IS
  the frame-type field; whole-channel CBOR later); wasm untouched.
- **aura** (4.16a partial): `run_job`'s script arm fills the slot from
  `StorePlan.entries`, engine closures capturing the BARE realm-mq
  handle — the injected `Collection` binds ns itself, so the binding face
  and `ctx_store_emit` land byte-identical (the `ns_raw` shape is the
  wasm plane only). `host_bridge_for`'s `ctx_store_emit` JSON entry
  retires with §2 (op set frozen, §3); the realm-side executor survives
  whole (`store_exec`, plan resolution) — only the payload's arrival
  shape changes.
- **Docs**: ADR-0026 §3's python wording is amended per this ADR (bind, no
  adapter); ADR-0035 §3's host frame shape updates with §2; this file
  supersedes both storage-bridge passages.
- **Sequencing**: does not cut ahead of 4.14's remaining gates or 4.15.
  Suggested order: 4.14 gates 2/3 (the nu shim + PTY retirement ride the
  transitional JSON seam — the shim and the seam's shape are decoupled, as
  §3.1 records) → 4.15 envelope unification (the host channel and the call
  channel share one envelope; typed payloads ride along) → this ADR's
  implementation (§1 first, §2 with CBOR).
