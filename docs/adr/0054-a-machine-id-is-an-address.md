# 0054 — A machine id is an address; a label is a request

> **Languages:** [English](0054-a-machine-id-is-an-address.md) (primary) · [中文](0054-a-machine-id-is-an-address.zh-CN.md)

**Status:** Accepted (2026-11-20)

## Context

Placement can only filter by label (ADR-0053, `Placement::Labels`). Labels are a person's words — `u1:MyLaptop`, `site:home` — minted with a node token (prism ADR-0020) and prefixed at admission. That is the right vocabulary for *asking*: a human or a model says "run this on my laptop", and the venue works out which box that is.

It is the wrong vocabulary for *addressing*, in three situations that already exist:

- **A retry that must land on the same hardware.** A step ran on a box and left a file there. The next call must go to that box, and the label is a word a person can change at any time.
- **A caller that resolved the word already.** An operator has the machine id in hand (prism reports it), or a dispatcher keeps its own table of which machine holds what. It has an id and no reason to learn the alias.
- **"The box that is not the control plane's".** Some placements are about the hardware a person owns rather than anything they named, and an empty label list already means `Any` — so there is no way to say "this specific box" without inventing a label for it.

Meanwhile the machine id already exists and is already reported: the node states the box it is (effector ADR-0006), the registry binds a token to the first machine that presents it (prism ADR-0020), and the pool can answer which aliases run on a given id.

## Decision

### 1. `Placement::Machine { machine }` — one box, by the id it reports

The machine id from effector ADR-0006 is what addressing uses. The job envelope carries another placement variant; the realm resolves it the same way it resolves labels, before the realm exists (ADR-0053's rule, unchanged).

An id addresses the box, not a process: **every alias running on that machine is reachable.** Two nodes on one laptop (a resident one and a container one, say) are the same hardware behind two doors, and choosing between them here would be this filter deciding something it cannot see.

### 2. An empty id addresses nothing

`Placement::Machine { machine: "" }` is refused, by name, before any realm exists. It is not widened to `Any`, and it does not become "the nodes that could not say who they are" — that reading would turn a missing identity into a group, and it would be a way to ask for hardware without saying which.

### 3. A refusal lists ids, because the caller asked by id

`DispatchError::NoSuchMachine` carries `alias=machine` for every node in the venue. A caller that asked by id needs the ids that ARE here; the labels it never mentioned would be noise in the one message that has to tell it what to try next.

### 4. Neither variant is a superset of the other

A label may match several machines — that is a filter, and matching several is correct. An id matches one box or nothing — that is an address. Collapsing the two would force one of them to behave like the other: ids would become fuzzy, or labels would become exact.

## Rejected

- **`Placement::Alias { alias }`.** The alias is the door's name, and a rename is exactly what it is for. An address that changes when somebody renames their laptop is not an address; it is a label with fewer characters.
- **Put the machine id in `ToolCall.executor`.** The executor names an *instance the job declared* (`type/key`, ADR-0042); hardware is not part of a type. It would also make the declaration set depend on what the pool happens to hold, which is the coupling ADR-0051 exists to avoid.
- **Resolve labels to machine ids inside the venue and treat both as ids.** Then two nodes' identical label on two machines would stop being a legitimate filter result, and every rename would silently re-point every address that used the word.
- **Match an empty id against unidentified nodes.** A missing identity would gain the power of a group: `machine: ""` would be the one placement that reaches every box that cannot be told apart.

## Consequences

- **Retries can be pinned.** A dispatcher that knows the work left a file on a box can say so, and cannot be silently moved by a rename.
- **Users never see ids.** They type and read names; the admin surface reports the id (prism ADR-0020's listing), and nothing a person composes needs it. That asymmetry is the point: names are for people, ids are for machines that must not guess.
- **A machine that comes back under a new alias is still the same address** — the alias is the door, the id is the box.
- **`JobOutcome.nodes` still lists aliases** — the outcome is about doors, and the caller can join to the ids through the registry if it needs them.

## Tests

- `crates/engine/tests/dispatch_job.rs` — `a_machine_id_addresses_the_box_whatever_its_alias_is_now` (the reach is built from the id: the other box is refused by the filter) and `a_machine_no_node_is_on_is_refused_and_names_the_ids_that_are_here` (refused pre-realm, listing `alias=machine`).
- `crates/realm/src/pool.rs` — `aliases_of_machine` returns every process of the box, sorted; an empty id returns nothing.

## Related records

effector ADR-0006 (the node states the box), prism ADR-0020 (the token is bound to it, and the listing reports it), aura ADR-0053 (the pool, the prefix, and the filter), aura ADR-0051 (a job's realm is its own).