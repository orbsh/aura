# 0025 — The meta plane's fate: merged into the data plane (terminal)

> **Languages:** [English](0025-meta-plane-fate.md) (primary) · [中文](0025-meta-plane-fate.zh-CN.md)

**Status:** Accepted (2026-09-22), revised (2026-09-23) — Plan A is the terminal state; Plan B rejected, not deferred

## Context

ADR-0018's no-JSON ruling migrated the meta plane (booth definitions) onto its own okm instance. A later question dissolved the premise: why does the meta plane exist as a SEPARATE instance at all? Two pressures push further:

1. **Booth definitions are just data.** A definition (source, language, entry, TTL, schema) has no structural difference from mq rows or state documents — it is an booth's data. A dedicated instance for one table is one more directory, one more engine choice, one more config surface (`meta_engine`/`meta_dir`), none of it earning its keep.

The original design positioned aura as compute-storage integrated ("存算一体"): an aura instance carries its own local storage and runs autonomously. Federated operation (ADR-0013) extends the same idea — when distributed requirements arise, an OUTER coordination mechanism handles sharding, routing, and synchronization, and aura itself remains unaware of them. In that light an independent meta plane has no reason to exist.

Component integration is a further motive for compute-storage integration. Aura+probe are positioned as base components, reused across many other scenarios. An independent meta plane would raise integration complexity — especially in scenarios where meta would need many-way synchronization: synchronizing meta alone is useless, while synchronizing everything is too heavy and pointless. Built-in storage lowers integration complexity the same way: the data has to live somewhere, and having the integrator supply storage is both troublesome and uncontrolled — Redis or PostgreSQL each degrade in some dimension, using both together (the common pattern) is an architectural degradation, and keeping one logic consistent across its re-implementations in multiple projects is hard to guarantee.

Compute-storage integration does not mean isolation from the outside: aura needs interfaces for exchanging data with external sources. Caller identity (user_id/device_id) rides in request parameters — request-level data and instance-level data (the instance key) live at two different levels, and conflating them onto ctx creates logical conflicts (see the partition design principles in partitioning.md). Business-level bulk data operations (import/export) take no special channel: a dedicated booth wraps the data import/export — isomorphic to a projection booth: an ordinary event-receiving booth carrying one class of data duty, no extra infrastructure.

The meta plane was originally designed as raft-synchronized data (for example, user authentication information), to serve centralized distributed coordination. But synchronizing authentication data alone is not enough, so the synchronization was dropped entirely: the outer framework decides how to handle it, weighing application shape and causality. That resolution is the federation model, not centralized distributed coordination.

## Plan A (implemented, terminal): one storage plane, definitions as a table

**The meta instance is deleted. Booth definitions become an `BoothDef` table in the DATA plane's okm instance** (`realm/src/meta.rs`, ns 41, beside the mq tables and state documents).

- `Engine.meta_store` is gone; `meta_engine`/`meta_dir` config surface is gone; one engine, one directory, one engine choice.
- `register` still persists the definition and boot still reloads (`load_all` → `engine.register`) — but now through the same instance the mq tables ride. The SEMANTICS of 4.5b (upload = own lifecycle; introspect once; execution is schema-free) are unchanged; only the physical placement collapsed.
- The identity model stays the registry pattern INSIDE the plane (TypeName registry + MAX watermark reduce; ids never reused) — the same shape as BoothName and the state table's identity resolution. One pattern, three uses.
- The `aura-storage` crate is deleted (no JSON store remains anywhere in aura).
- The introspected schema keeps riding as verbatim JSON text — an interface artifact (the LLM/script-side contract), not a storage encoding.

## Rejected alternative: aura holds no storage at all

An earlier draft of this ADR deferred a second terminal state: aura as a stateless executor — no local engine, no directory, no boot reload; definitions, ctx state, and mq living in the outer layer's storage, mounted by aura through okm's remote path. It is rejected, not deferred.

**The premise contradicts aura's identity.** The stateless-executor framing assumes aura holds no storage and the outer layer owns the truth. But aura is compute-storage integrated: it has its own local storage, and an aura instance is autonomous. When distributed requirements appear, the outer coordination mechanism handles sharding, routing, and synchronization — aura does not know these exist. There is nothing in aura to externalize, so there is no terminal state to defer toward.

**The independent meta plane has no role to fill.** The meta plane existed to be raft-synchronized data (authentication information) in service of centralized coordination. Since synchronizing authentication data alone is insufficient, and the coordination question is answered by federation (ADR-0013) at the outer layer, the separate plane loses both its payload and its justification.

**Consequence of the rejection:** aura's single okm instance is permanent, not transitional. Definitions restore from the local directory on node restart; if the outer layer also holds definition copies, they are idempotent re-pushes (keyed by type name, latest version wins) — a push target, not a dual-master conflict.

## Honest semantic cost

- **The group-sentinel workaround in the registry reduce** (`global` constant field, because okm's derive rejects empty group lists) survives; okm ADR-0023/0024's combinators will absorb it when they land.

## Consequences

- Now: one okm instance in aura; `meta_engine`/`meta_dir` removed from config; `booth_defs` beside mq/state; zero JSON storage anywhere.
- Permanently: an aura instance owns its local storage and is autonomous. Distributed needs (sharding, routing, synchronization) are the outer federation mechanism's responsibility; aura stays unaware of them.
- The precedent holds: aura owns computation, delivery, and its own storage; the outer layers own coordination across autonomous instances.
