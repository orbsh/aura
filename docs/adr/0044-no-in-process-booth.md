# 0044 — The engine ships no in-process booth

> **Languages:** [English](0044-no-in-process-booth.md) (primary) · [中文](0044-no-in-process-booth.zh-CN.md)

**Status:** Accepted (2026-09-25 ruling, Phase 4.5/4.5a; formalized as an ADR 2026-10-12)

## Context

`BoothType::simple` once let Rust code become a booth as an in-process closure
(`Body::Rust`) compiled into the host binary. Under that form every new service
meant repackaging the engine binary and every Agent-app feature meant a rebuild
of aura — the platform collapses into a framework. Phase 4.5 / 4.5a (CLOSED
2026-09-25) deleted the public constructor and migrated the cli demo and all
engine tests onto script booths; this ADR records that ruling as a standing
boundary so it stops living only inside a PLAN checklist.

## Decision

**The engine ships NO in-process booth.**

- Framework mechanics (the evictor class) are plain realm logic, not booths —
  wrapping them as booths is a pointless detour.
- User Rust code becomes a booth through out-of-process channels only: a
  compiled artifact uploaded at runtime (wasm, ADR-0026 §4) or a native child
  process speaking the framed protocol (bgi, ADR-0035); remote effectors
  connect outbound (ADR-0031). Which carriers exist is a roster question
  (tracked in PLAN), not part of this boundary.
- `Body::Rust` remains an enum variant with no public constructor
  (framework-internal future use; engine tests construct it as scaffolding).
- Booth definitions live on the data plane via the upload lifecycle (Phase
  4.5b: `set` persists the definition + introspected metadata) — never the
  compile plane.

## Consequences

- The platform stays an engine: adding a service = uploading a definition at
  runtime, not rebuilding the engine.
- For compiled-language authors, ctx is a WIRE contract, not a process object
  (ADR-0043 §5/§6): the "global ctx" of an out-of-process carrier is a
  shim-side construct, arms mirror the callable surface 1:1. No `static` or
  module-global ctx exists anywhere — in-process ctx is the per-instance
  projection `Realm::ctx_for` assembles (unreachable by users, since no
  in-process booth exists); out-of-process it is the frame vocabulary.
- Test coverage for the engine itself runs on script booths (steel; nushell
  for slow handlers; python behind a feature gate) — the same channels users
  have, so the test path exercises the real surface.
