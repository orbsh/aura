# 0045 — A declaration binds an entry

> **Languages:** [English](0045-a-declaration-binds-an-entry.md) (primary) · [中文](0045-a-declaration-binds-an-entry.zh-CN.md)

**Status:** Accepted (2026-10-29, landed with the prism renderer leg)

## Context

Delivery calls a body at an ENTRY: the job carries `handler`, the remote frame carries `entry` (the
symbol the delivered code addresses). For a compiled body this is a label the host already has a
handle for — the closure is in hand and the name is decoration. A SCRIPT body is different: its
resident session is called per event by name, and the only binding form the carrier has is the
declaration. That declaration (`interface_schema` → `receives`) carried an event and its
resolution and nothing else, so the entry was, by necessity, the event name.

That holds only while every event name is also a name the language can define. It is not. A venue
that marks its UI-facing events (prism's `view:<name>`) hands the body `view:add-to-cart`, and a
script cannot define that function — the character is not part of an identifier. The subscription
registers, the emit matches the route, the queue receives the message, the carrier is asked for
`view:add-to-cart`, finds nothing, and **drops the job with no log line**. Measured on prism's
renderer leg: the booth was never entered and nothing anywhere said why.

There is no `@on("view:x")`-style runtime registration to fall back on: `ctx` exposes `emit` and
`store`, and the schema's flat `{"on": [...]}` spelling is a list of event names, not of function
references. For a script body, the declaration IS the registration.

## Decision

**A subscription declaration says what it binds.** `ReceiveDecl` (and `Route`) carry the entry as
an optional second name, spelled `entry` — the word the remote frame already uses:

    receives: { "view:add-to-cart": { "entry": "add_to_cart" } }

Absent, the event name is the entry — the behaviour of every declaration today. The binding travels
the whole path (declaration → router, from BOTH the engine's introspection arm and the registry's →
the instance's subscription tuple → `QueuedJob.handler`), because a fact that stops at the router
is a fact the dispatcher does not have.

Nothing changes for compiled bodies, which never consulted a name. Script bodies gain the one thing
they lacked: a way to say which function a marked event belongs to.

## Consequences

- A namespace of names becomes usable for routing without being a namespace of callable
  identifiers: prefixes, dots, and other marker syntax stop being a dispatch hazard.
- The marker costs a booth exactly one key of declaration, and nothing at the call site.
- Wildcard subscriptions still deliver the CONCRETE name (there is no per-message declaration to
  consult), so a wildcard subscriber's body must be reachable under the concrete name — which for
  a Rust body is free (it ignores the name) and for a script body means one entry point per
  event, or an exact declaration per event.
- **A failed delivery is no longer silent.** The delivery seam used to drop the reply unread
  (`run_job_queued`), so a body that could not be called — missing entry, carrier fault — left no
  trace at all. The reply is now observed: the failure is logged with the event name, the entry it
  tried, and the booth type, and recorded in the dead ring as `DeadReason::DeliveryFailed`
  carrying `{entry, booth_type, error, payload}`. Reading it back is a deployer's business, not
  aura's: prism serves the ring at `GET /admin/dead`.