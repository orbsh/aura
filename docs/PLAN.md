# PLAN

Decisions live in `docs/adr/`; the design in `docs/design/` (`storage.md`, `realm.md`, `partitioning.md`, `booth-api.md`, `event-flow.md`); this file is the index — which phase, when, which record decided it, and what is still open.

## How to gate this repo

`nu scripts/gate.nu` runs two gates: the workspace with default features, then `aura-engine --features python`. The second is not redundant — every test that touches the okm-python face sits behind `#![cfg(feature = "python")]`, so a plain `cargo test` skips those files instead of failing them, and a rename inside that face stays invisible until someone runs the feature.

## Milestone A — the single binary engine

- [x] Phase 0 — Workspace skeleton: `crates/{engine,booth,realm,storage,config,cli}`, one binary, no external dependency
- [x] Phase 1 — The booth runtime: the ctx surface, partition-key routing, scale-to-zero — ADR-0011
- [x] Phase 2 — Embedded languages, by importing the effector runtime's carriers
- [x] Phase 2.5 — The script booth's ctx bridge — ADR-0026
- [x] Phase 2.6 — A resident VM per script instance
- [x] Phase 3 — The realm model: event namespace, exact and wildcard routes, the dead-event ring
- [x] Phase 3.5 — The unified call model (`CallSlot`)
- [x] Phase 3.6 — User namespace isolation, structural at construction
- [~] Phase 4 — Root config and two-instance storage
  - [ ] the slate engine option on both planes — no pressure
- [x] Phase 4.5 — The platform booth model: definitions live on the data plane
- [x] Phase 4.5a — The Rust-closure booth form leaves the public API
- [x] Phase 4.5b — The booth metadata lifecycle: introspect once at upload, persist to the meta store
- [x] Phase 4.5c — Multi-entry booths and the event-queue semantics
- [~] Phase 4.8 — Timers — ADR-0016
  - [ ] the durable timer tier has no home — see "Open items needing a ruling"
- [x] Phase 4.9 — Type-scoped booth storage — ADR-0026
- [x] Phase 4.10 — Effector affinity and realm demotion — ADR-0026, ADR-0028
- [x] Phase 4.11 — Content-addressed code delivery — ADR-0027
- [x] Phase 4.12 — Realm-set terminology — ADR-0028
- [x] Phase 4.13 — Routing's final form: an instance key through okm's access methods — ADR-0042
- [x] Phase 4.14 — The exec carrier: out-of-process booths — ADR-0035
- [x] Phase 4.15 — Envelope unification: invoke is iterate's one stream — ADR-0036
- [x] Phase 4.16 — Booth storage access: the typed host channel and the in-process bindings — ADR-0037
- [x] Phase 4.17 — Event-plane identity and the delivery end state — ADR-0038
- [x] Phase 4.18 — Partition identity and the keyspace bands — ADR-0039, ADR-0040
- [x] Phase 4.19 — The cursor retention promise — ADR-0039
- [x] Phase 4.20 — The ctx object: one ambient execution context — ADR-0043
- [x] Phase 4.21 — The wasm channel retires: compiled languages go bgi-first — 2026-10-12

## Milestone B — agent base

- [ ] Phase 6 — Turn-executor booth hosting — implemented in the gravity repo, out of scope here
- [~] Phase 6.5 — Resident execution windows
  - [ ] the turn-executor type declares its long TTL
  - [ ] same-session consecutive calls fill via the in-memory oneshot; the session persists and the executor is released at turn end or retention expiry
- [x] Phase 6.6 — WITHDRAWN 2026-10-10, the k10r decoupling ruling
- [x] Phase 7 — Effector embedding, REMOTE ONLY (restated 2026-11-09) — ADR-0003
- [x] Phase 8 — Prism hosting: prism keeps the WS gateway and reaches the realm directly (restated 2026-11-09) — ADR-0017

## The resource contract, inside the venue — 2026-11-09 … 2026-11-17

- [x] `ctx.resource` closes inside ONE host call for code the venue contains, through the same `effector_runtime::resource::{ensure, sync}` a node runs; grants are rows (ns 36 grant, ns 38 credential), not config — the tenant is `Instance.owner`, never named by the script
- [x] A cancelled call ends the transfer, not only the waiting — the stop bit answers the same question in-process, a dispatch's deadline now measures SILENCE (`Frame::Progress`) rather than duration, and a cancelled `ensure` removes what it left
- [x] The grant surface is operable: `/admin/resources` and the plane's own page (`resources_ui.rs`); the credential only ever travels in

## The stop primitive — ADR-0047, decided and built 2026-11-09

- [x] SET: one in-memory bit per instance (`realm/src/stop.rs`), cleared on eviction — not storage, not an ns
- [x] INTERRUPT: the realm races the body against a per-call slot and the caller takes the terminal `Interrupted`; a remote body sends `Frame::Interrupt` and the node answers `landed`
- [x] Every carrier answers honestly: `exec` kills its child, `induction` writes the frame, `steel` and `python` report `landed: false` because they cannot reach the work

## Open items needing a ruling — measured 2026-11-09

- [ ] Phase 4.8's durable timer tier has no home: `crates/realm/src/timer.rs:104` registers memory-tier entries only, and the StateStore it points at was retired by ADR-0026 §3
- [x] Phases 7 and 8 promised more than the split allows — restated the same day (see Milestone B)
- [ ] A gated WILDCARD is unreachable from scripts: `wildcard_receives` is read as a plain string array, so no script can declare `auth` on a wildcard

## Ideas — undecided

- [ ] A database as a resource: `ctx.resource({op: "sql", kind: "pg", …})` with SQL text in and JSON out, so a booth names what it wants instead of holding a client. The unanswered question is ordering — row-level security says who may touch a row, not who goes first when two booths write through SQL