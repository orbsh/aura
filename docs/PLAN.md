# PLAN

Design lives in the wiki (stateless-agent-architecture.md / aura-architecture.md) and ADRs; this file only sequences phases. Phases reference ADRs once written.

## Milestone A — Single binary engine

- [ ] Phase 0 — Workspace skeleton: `crates/{engine,actor,realm,storage,config,cli}`; single-binary start, no external deps (no Docker / etcd / DB). Echo Actor: define → invoke → return.
- [ ] Phase 1 — Actor runtime: Rust host + Tokio MPSC pipeline; per-Actor context (in-memory modify, on-disk sleep); partition key routing; on_sleep/on_wake scale-to-zero (state → Fjall).
- [ ] Phase 2 — Embedded languages: Steel (deterministic core, natural sandbox), Python (PyO3), Wasm (Wasmtime) — one language per Actor, Polyglot Bridge, host-function async suspension.
- [ ] Phase 3 — Realm model: event namespace, call (resolve_call, Actor return) vs emit (fire-and-forget), event composition primitives; interface_schema with returns declaration.
- [ ] Phase 4 — Storage engines: Fjall (local, engine=fjall) and SlateDB + S3 (cloud-native, engine=slate); engine/consistency matrix validation (fjall+raft ✓, slate+s3 ✓, else boot error).
- [ ] Phase 5 — Openraft metadata networking: Actor registry / user state / config sync via `--raft-nodes`; Actor data stays local-or-lake; hot reload of Actor definitions.

## Milestone B — Agent base

- [ ] Phase 6 — Turn-executor Actor hosting: Gravity as Actor type (partition key = session_id; same-session serial, cross-session parallel). Out of scope here — implemented in the gravity repo, hosted via this phase's contract.
- [ ] Phase 7 — Probe embedding: container execution base (heavy-isolation end of the Wasmtime lineage) as an in-realm base component; probe repo deploys as remote actuator via outbound registration.
- [ ] Phase 8 — Prism hosting: WS gateway as Aura-resident component (client connections pin here, not on Gravity); turn delivery = realm events. Prism repo owns the protocol, this repo owns the connection plane.

Deferred gates:

- MQ decomposition: no standalone queue component — boundary-queue needs (external delivery, audit log, consumer retry) via S3-as-truth + KV metadata.
- invoke.toml external HTTP endpoints: only after realm-internal calls are complete (address vs program judgment — program/embedded is the default extension unit).
