# 0053 — the node pool is the venue's, and a placement is a filter

Status: Accepted (2026-11-20)

## Context

`Placement` has ridden the job envelope since effector ADR-0005 and aura has carried it into `JobOutcome` without acting on it, because "node selection needs a node pool, which does not exist yet" (ADR-0051 §Status). Two things were true at once:

- **The connections did exist, per realm.** `Realm.effectors` was a `HashMap` of alias → connection, filled by the gateway that admitted the node. One map per realm.
- **A job's realm inherits nothing.** ADR-0051 mints it an instant before its first op. So the map it saw was EMPTY, and a `Body::Remote` op inside a dispatched job could not find the node that owns the executor — it failed with "effector not connected" about a node that was connected. The per-realm map was not a smaller pool; it was a realm that could not do remote work at all.

The question, then: what is a node — a realm's property or the venue's? And given that a declaration names its node (`Body::Remote { node_alias }`, because a mount is registered per node × language), what can a job-level `Placement` possibly mean?

## Decision

1. **The pool belongs to the venue: one per engine, borrowed by every realm.** `NodePool` holds `NodeRecord { alias, conn, carriers, class, labels }`. `RealmSet` owns it, and every realm it creates gets the same `Arc` — in ONE place (`RealmSet::adopt`), because both creation paths must do it, and a realm that missed it would fail as "effector not connected" in a job dispatched a microsecond earlier, far from the cause.

2. **The node states the box, the admitting party states what it is for.** `NodeRecord::machine` is what the node reported about its own hardware (a serial, or the id it minted once and kept — effector ADR-0006); `labels` are the admitting party's. The split is not a trust gradient and not "privilege vs no privilege" — it is whether a wrong claim can be seen to be wrong. A machine identifier is unique: a box claiming another's serial is a CONTRADICTION, and reporting `same_machine` is that contradiction surfacing. A label is guessable and shared on purpose — many machines are `site:home` — so a wrong claim is indistinguishable from a right one, and nothing but admission can be the check.

3. **Labels are the ADMITTING party's word, and remain so.** Nothing about placement travels in a registration frame: effector ADR-0005 §2b settled that a node cannot name its labels without being able to name someone else's, and this record does not reopen it. The pool therefore takes labels from whoever admits the connection — a grant (prism, the admitting party that owns the registry) or a deployment file. A node admitted by something that knows no grant is UNPLACED: reachable by every job that asks for no place, by none that does. That is why aura's own `serve_effectors` gateway passes an empty set: it has no registry to read, so it is in no position to place anyone, and saying so is better than guessing.

4. **`Placement::Labels` is a FILTER on the job's view of the pool.** Dispatch resolves it before the realm exists to a `Reach`:
   - `Placement::Any`, or `Labels { labels: [] }` (which the envelope documents as `Any`) → `Reach::All`.
   - `Labels { labels }` → `Reach::Only(pool.matching(labels))`, subset matching: every requested label must be on the node.
   The job's realm carries that `Reach`, and the remote-call path checks it before sending. A call naming a node outside it is refused in words — `node 'x' is outside this job's placement — it may reach [a, b]` — not silently served by a machine the job did not name.

5. **A placement that matches no node is refused before the realm exists** (`DispatchError::Nowhere`). A job running in a place with no nodes would fail later, per op, for a reason that reads like the node's fault. And it would have to run somewhere: the only nodes left are ones the job explicitly excluded.

6. **The outcome records what the job could see** (`JobOutcome::nodes`), not only what it asked for. A placement is checkable after the fact: a job that ran with `site:home` and reports two nodes is a job whose placement was satisfiable and was read as intended.

7. **Labels do not select BETWEEN nodes for one call.** A declaration names its node, so the only thing a job-level placement can mean today is which nodes the job may reach. Late binding — a declaration that names a requirement instead of an alias, with dispatch choosing the node — is a different change with its own record: it changes what a mount IS, not where a job points.

## Rejected

- **Keep the map per realm and copy it at creation.** A copy diverges the moment a node reconnects, and "which copy does this call see" becomes a question with no good answer. The pool has one lock and one truth.
- **Let a placement select the node for a call, ignoring the alias in the declaration.** The alias is what the mount was registered under; overriding it silently would run code on a machine the declaration never mentioned, and the node would have to be trusted to have the executor.
- **Treat an empty match as `Reach::Only({})` and refuse per op.** One refusal per op is the same failure told N times, after the realm and its registry were built for nothing.
- **Let the node carry its labels in `Register`.** It is the obvious shape and it is wrong for effector ADR-0005 §2b's reason: a node that can name `site:prod` can name it for work that was not meant for it. Labels travel with admission, not with the node.

## Consequences

- **`Body::Remote` works inside a dispatched job**, which is what makes a job able to run anything outside the venue's own process at all.
- **`Needs.resource` is still a named refusal** (ADR-0048/0049): a resource is venue state, and reachability of a node is not the same question.
- **A node that reconnects under the same alias displaces its predecessor**, and the replacement is reported with both peers (ADR-0015 §7) — the pool returns the displaced record so the gateway can say it.
- **An unplaced node is reachable by every job that does not ask for a place, and by none that does.** Today that describes every node aura's own gateway admits, because it reads no grant. Placement becomes usable in production when the admitting party is prism, which mints labels at admission; until then the tests stand in for that admitting party and insert placed records directly.
- **Selection by load and locality is still not implemented.** `Placement::Any` does not pick the least-loaded node; it exposes the whole pool. Choosing among nodes for one call needs a late-bound declaration, which §7 defers.

## Tests

- `crates/realm/src/pool.rs` — label matching is subset matching and an empty request matches everything; a replacement reports the record it displaced; removal is identity-checked so a reconnect keeps its name; `Reach` filters and can list what it allows.
- `crates/engine/tests/dispatch_job.rs` — `a_placement_no_node_is_in_is_refused_before_a_realm_exists` (refused with the labels it asked for, no realm created), `a_job_reaches_only_the_nodes_its_placement_names` (the node is FOUND — not "not connected" — and refused by the filter; the outcome lists only the node the job could reach), `a_job_that_asks_for_no_place_reaches_every_node` (default placement sees both nodes).
- `crates/engine/tests/remote_effector.rs`, `alias_takeover.rs`, `deadline_stop.rs` — the existing remote path now reads the pool: registration, alias takeover and presence expiry are asserted through `realm_set.pool()`.

## Related records

aura ADR-0051 (dispatch creates the realm; boundary (d) is the inheritance this fills in), ADR-0015 §7 (alias replacement is reported, never silent), ADR-0027 (code by reference), ADR-0048/0049 (resources are venue state), effector ADR-0005 (the envelope's `Placement`, and why the job carries it), effector ADR-0006 (the node states the box; labels are the admitting party's), ADR-0054 (a machine id is an address, and it is what addressing uses once a name has been resolved).