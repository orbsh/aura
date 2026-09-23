# 0025 — The meta plane's fate: merge into the data plane now, full externalization deferred to Prism

> **Languages:** [English](0025-meta-plane-fate.md) (primary) · [中文](0025-meta-plane-fate.zh-CN.md)

**Status:** Accepted (2026-09-22) — Plan A implemented this revision; Plan B recorded, deferred

## Context

ADR-0018's no-JSON ruling migrated the meta plane (actor definitions) onto its own okm instance. A later question dissolved the premise: why does the meta plane exist as a SEPARATE instance at all? Two pressures push further:

1. **Actor definitions are just data.** A definition (source, language, entry, TTL, schema) has no structural difference from mq rows or state documents — it is an actor's data. A dedicated instance for one table is one more directory, one more engine choice, one more config surface (`meta_engine`/`meta_dir`), none of it earning its keep.
2. **Aura as a pure execution environment.** If aura is the stateless executor of the stack, "definitions survive restart" is not aura's concern at all — definitions belong to the OUTER layer (prism/gravity), which pushes them to nodes. Distributed concerns (replication, availability, where the truth lives) are the control plane's; aura neither knows nor cares.

Two terminal states follow. Both are recorded — one implemented now, one deferred with its trigger condition.

## Plan A (implemented): one storage plane, definitions as a table

**The meta instance is deleted. Actor definitions become an `ActorDef` table in the DATA plane's okm instance** (`realm/src/meta.rs`, ns 41, beside the mq tables and state documents).

- `Engine.meta_store` is gone; `meta_engine`/`meta_dir` config surface is gone; one engine, one directory, one engine choice.
- `register` still persists the definition and boot still reloads (`load_all` → `engine.register`) — but now through the same instance the mq tables ride. The SEMANTICS of 4.5b (upload = own lifecycle; introspect once; execution is schema-free) are unchanged; only the physical placement collapsed.
- The identity model stays the registry pattern INSIDE the plane (TypeName registry + MAX watermark reduce; ids never reused) — the same shape as ActorName and the state table's identity resolution. One pattern, three uses.
- The `aura-storage` crate is deleted (no JSON store remains anywhere in aura).
- The introspected schema keeps riding as verbatim JSON text — an interface artifact (the LLM/script-side contract), not a storage encoding.

**Why not decide Plan B at the same time:** the outer layer Plan B requires does not exist yet (prism is designed, not built). Removing aura's persistence before the outer layer can receive it leaves the stack unrunnable in between — the intermediate state fails Milestone A's zero-dependency startup for no gain. Plan A is a strict subset of Plan B's end state: when B lands, the single remaining instance is replaced by a remote handle and boot reload is deleted.

## Plan B (recorded, deferred): aura holds no storage at all

**Aura becomes a stateless executor: no local engine, no directory, no boot reload.** Definitions, ctx state, and mq live in the OUTER layer's storage (prism/gravity's okm instances); aura mounts them through okm's remote path — `NestStorage`/`RemoteStore` over the wire (okm ADR-0010/0021: the sender/receiver framework and the streaming-scan contract are already landed; the aura side becomes a receiver host).

- **What changes:** realm's mq/state synchronous local-engine calls become wire round trips; `Engine::start` stops opening any engine; boot reload is deleted — the control plane pushes definitions on node start and on definition change (`engine.register` keeps its signature, loses its persistence side effect).
- **What stays:** the node still bears engine-adjacent duties on its own data (write-path execution, watermark compaction, the timer wheel) — "stateless executor" means no STORAGE OWNERSHIP, not no computation. okm ADR-0010's receiver contract already assigns exactly this shape.
- **Trigger condition:** prism's connection plane exists and needs a node to host its storage (ADR-0017 §1: prism is an engine-bearing host). The remote-mount refactor lands as part of prism's implementation — not before, not after.
- **What the deferral costs:** one interim shape where aura persists locally. Accepted: it is the current running system, and Plan A already collapsed it to the minimum surface (one instance) that B will replace wholesale.
- **Distributed concerns resolved outward:** replication, availability, and the truth's location become the control plane's deployment choices (single-node file, replicated store, whatever prism's operator picks) — aura stops having an opinion. The framing: "even if distributed problems really arise, aura need not handle them — the outer prism does".

## Honest semantic cost

- **Plan A keeps a persistence obligation aura will later shed.** Between A and B, a node restart still restores definitions from the local directory. If the control plane's definition copy drifts from aura's local one, aura's local copy wins until B lands — a temporary dual-master window on definitions. Accepted because definitions are idempotent re-pushes (keyed by type name, latest version wins) and the window ends when B lands.
- **Plan B trades latency locality for architectural purity.** Every ctx-state write and mq append becomes a wire round trip. The hot-loop work (Phase 6.5's resident executor) is priced against the REMOTE world, not the local one — a per-turn cost that must be measured when B lands, not assumed away. Local engines may remain a DEPLOYMENT shape for single-node development (Milestone A's zero-dependency property) — B governs the production topology, not the test harness.
- **The group-sentinel workaround in the registry reduce** (`global` constant field, because okm's derive rejects empty group lists) survives both plans; okm ADR-0023/0024's combinators will absorb it when they land.

## Consequences

- Now: one okm instance in aura; `meta_engine`/`meta_dir` removed from config; `actor_defs` beside mq/state; zero JSON storage anywhere.
- At prism time: implement Plan B — remote-mount the one instance, delete boot reload, push definitions from the control plane. The ADR-0017 amendments (§3/§5/§7: identity tables live in prism; delivery carries sender metadata in the payload, aura's Ctx unchanged) and this ADR's Plan B clause execute together.
- The precedent holds: aura owns computation and delivery; the outer layers own identity, definitions, and the truth's location.
