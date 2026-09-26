# 0032 — Booth terminology: the participant renames actor → booth

> **Languages:** [English](0032-booth-terminology.md) (primary) · [中文](0032-booth-terminology.zh-CN.md)

**Status:** Accepted (2026-09-26) — naming ruling; code rename + docs sweep land
with this ADR; cross-repo alignment (probe/prism/gravity/okm/wiki) lands in
the same batch.

## Context

The word "actor" promised two things Aura does not do.

1. **A communication shape it retired.** The classic actor's defining
   mechanic is the per-actor mailbox — an actor IS its mailbox
   (CARB: Computation + Asynchronous RPC + Mailbox/Behavior). PLAN 4.5c
   replaced per-actor mailboxes with per-(event, partition) queues +
   per-subscriber cursors (ADR-0014); delivery is MQ-shaped (named
   queues, one-to-many, zero fan-out copies). ADR-0014's own rationale
   was that the mailbox model "bakes in 'an event belongs to an actor'
   — wrong for one-to-many." What remains of the actor idea is purely
   scheduling: serial-per-instance-key, state scoped to the type,
   failure-as-value.
2. **An invocation shape it never had.** `ctx.invoke` is request-response
   over the entry function's return value (reply_to correlation,
   Phase 3.5 CallSpec tiers) — RPC semantics, not actor `ask` in any
   classical sense. The actor model's ask/tell was an ActorRef-direct
   addressing story; Aura addresses by (type, instance key) through
   the realm.

The divergence grew past the point a definitional footnote can carry:
ADR-0031 moved *participants* into the same frame as remote services
and browser sessions, where "actor" additionally clashes with the
"remote actor" sense readers bring from Erlang/Akka federation lore.

## Decision

**The realm participant is named booth (Chinese: 摊位).** Everything a
reader previously saw as "actor" — local or remote, Rust-registered or
browser-driven — is a booth: it declares its interface (the stall's
goods), receives routed events, answers invocations, holds its own
state. The tier vocabulary pairs with probe: **probe = remote
execution** (capacity that runs control-plane code, dial-in; unchanged
name, unchanged ADRs), **booth = remote/local participation** (brings
its own code and storage).

1. **Code:** `aura-actor` crate → `aura-booth` (`crates/booth`);
   `ActorType` → `BoothType`, `ActorDef` → `BoothDef`, `PersistedActor`
   → `PersistedBooth`, `ActorName` → `BoothName`; fields and frame
   segments `actor_type`/`actor_id` → `booth_type`/`booth_id`;
   `ACTOR_NS_BASE` → `BOOTH_NS_BASE`; `ActorInstance` → `BoothInstance`.
2. **No compatibility alias (strategy A).** Persisted meta-plane rows
   and frame field names change spelling with no serde alias — existing
   local data is wiped and re-registered. A silent dual spelling is the
   exact drift the rename removes (the ADR-0028 precedent, same ruling).
3. **Docs:** historical ADRs are swept too, not annotated-in-place —
   readers filter by term and may never reach a correcting document;
   the sweep is the correction. External concepts keep the word actor:
   Akka/Erlang/Orleans/Actix/tellus discussion, "the actor model",
   ActorRef, Virtual Actor, Rivet Actors — those name other systems'
   ideas, and realm.md §5.8/§5.9's comparison sections depend on them
   staying put. Probe-side artifact names (`actor-guest`,
   `counter_actor` in ~/world/probe) are not ours to rename here.
4. **"actor" stays legal in prose about the external paradigm** (the
   actor model never demanded determinism — reads as a reference to the
   outside idea; realm.md §5.1's calibration paragraph keeps the
   contrast honest). What is banned is actor as the name of *our*
   participant.

Alternatives considered: `worker` (overoccupied by the thread-pool sense;
would blur probe's execution-capacity idea), `consumer` (MQ-true for
delivery but drops serial-state semantics and implies pull-only),
`nexus` (occupies the hub slot realm already holds),
`beacon`/`transponder` (vivid imagery, but direction-specific images
fight the dial-in/dial-out symmetry). `booth` won on being
direction-free AND carrying ADR-0031's governance vocabulary naturally
(declaration = stating your goods; approval = the market owner's own
decision; vocabulary = the market's naming rules) — the metaphor does
work in the docs, it is not decoration.

## Consequences

- **This sweep:** aura code + aura docs; cross-repo alignment landed in
  the same session — probe (comments/docs; wire had no `actor_type`
  field), prism (`actors.rs` → `booths.rs`, `echo_actors` →
  `echo_booths`, path dep), gravity (PLAN/README wording), okm docs,
  wiki (摊位 as the zh term; external-paradigm pages untouched).
- **ADR-0031** moved/re-titled to `0031-remote-booths.*` as part of
  this sweep; its text reads with booth.
- **Cargo.lock / path deps in sibling repos** move with each repo's own
  commits.
