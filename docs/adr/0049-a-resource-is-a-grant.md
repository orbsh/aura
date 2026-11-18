# ADR-0049 — A resource is a grant, not a snapshot

Status: Accepted (2026-11-17). Supersedes ADR-0048's declaration, registry and version points (§1–§3)
and every "the venue holds the pointer" statement in it.

## Context

ADR-0048 gave the venue three jobs: carry a declaration, keep a registry of resources keyed by
`(owner, kind, seq)`, and record a published version per resource so a bad write could be undone by
not advancing a pointer. Each of the three turned out to be the wrong shape for what the thing
actually is.

**A declaration fixed at start cannot be filled.** A booth's data need is decided by the code while
it runs — effector ADR-0003 §5 says exactly that, in those words ("a task discovers what it needs
while it runs"). A list riding the call was therefore a table nobody could write, and a script that
wanted a second file had to be re-declared and re-dispatched.

**A version pointer duplicates the store and can disagree with it.** A git ref, an object version and
a rustic snapshot are each already a history with a name. Keeping a second pointer in the venue adds
a thing that can be wrong (the two can drift, and the wrong one wins at rollback time) and buys
nothing the store does not already do better.

**A row per resource makes the registry a slower bucket.** With an S3-shaped address space, a script
naturally names its own objects (`org/repo`, `builds/out.tar`). Registering each of those turns an
API into an approval queue, and the operator, not the script, becomes the bottleneck on what a task
may name.

## Decision

**The venue grants; the node moves; the store remembers.**

1. **One grant per kind.** A grant is `(kind) → { base, mode, credential }`. The key is the kind; the
   tenant is not a row, it is a prefix this venue composes. A shared store therefore serves everyone
   with one row.

2. **The kinds are mechanisms, not stores.** `KINDS = ["git", "object", "archive"]`. `object` is one
   blob over an S3-shaped wire protocol; `archive` is an incremental repository (rustic today, any
   equivalent tomorrow); `git` is a repository with its own reference vocabulary. `image` is gone —
   what a container IS belongs to the environment a node declares (effector ADR-0003 §1, and its
   "the image is the mount" consequence), not to the data a script reads. `volume` is gone — "a directory that outlives the call" is a node-local cache
   policy, and a row cannot describe a machine.

3. **The effective root is composed, never named by the caller.** `material(tenant, kind)` answers
   `<base>/<tenant>/<kind>`. The node joins the script's own `remote` (`org/repo`, `a/b.bin`,
   `project-x`) onto that root after checking that it is a relative name with no `..`, no leading
   slash and no backslash. The prefix exists because the NODE builds it; a credential cannot be
   scoped to a prefix by the store, so this is a naming rule, not an isolation boundary.

4. **The script names, the node transfers.** `ctx.resource({op, kind, remote, mount?, ref?})`:
   `get` fetches to `mount` (default `<root>/<remote>`), `publish` pushes the same path back and
   answers the reference the store minted (`branch@sha`, an ETag). The node runs git and curl; the
   script installs no client, and the credential never reaches it.

   **In-process, the venue is the node.** Effector ADR-0003 §1 makes "what contains the code" the
   only containment story, and the venue contains its own scripts — so for them there is nobody to
   ask and no wire to wait on: `realm::resource_bridge` mints from the same registry row and calls the
   same `effector_runtime::resource::{ensure, sync}`, and the root is the venue's own
   (`EFFECTOR_RESOURCES_ROOT`, else `<cwd>/resources`). The data lands beside the venue, which is what
   the in-process class claims rather than a shortcut around it. Grants are declared at boot from
   `aura.kdl` (`resources { kind "git" { base … mode … secret-env … } }`, the credential named by
   environment variable) — a grant a script could create would make "a resource is a grant" a
   sentence about nothing.

5. **No version table, no `Synced` frame, no pointer.** A publish's reference goes back to the
   script. Undo is the store's own: a git revert, a rustic snapshot, a bucket with versioning on.

6. **Cleanup follows the node, not the clock.** What a node materialised is recorded and removed when
   the process leaves (a `Drop` guard, so every exit — lease, disconnect, error — cleans up).

## Consequences

- **Three tables become two** (`grants` at ns 36, `secrets` at ns 38); ns 37 is retired. The registry
  holds one row per kind plus its credential, and the admin face has two routes: declare a grant,
  read a grant.
- **A script may name anything under its slice.** That is the point, and it is also the cost: the
  venue no longer knows what a tenant has written, so "what does this owner have?" is a question for
  the store, not for us.
- **Leaking a credential leaks the store, not a slice.** The prefix protects against collision, not
  against access. The honest bound is stated here so nobody later reads "cannot escape" as
  "isolated".
- **Object writes lose undo unless versioning is on.** That is an operational switch on the store,
  which is cheaper than a pointer table and cannot disagree with the store about itself.
- **`archive` is declared but not yet movable.** The node refuses it by name until its transport
  exists; a grant nobody can use would only be discovered at the first task.
- **A database as a resource stays parked** (aura PLAN, Ideas): SQL text in and JSON out would fit
  the shape, but a second state store with no ordering between booths is a different decision.

## Alternatives rejected

- **Keep the declaration but widen it (`resources` on the call and on `ctx`).** Two ways to name one
  thing; the runtime one is strictly more capable, so the static one is dead weight.
- **Keep `seq` and add `name` beside it.** A number nobody can read plus a name that must stay in
  step is two identifiers for one row.
- **`kind: "s3"` plus an `incremental: bool`.** The flag is not an axis: rustic IS an object store
  used incrementally, so the node would translate the flag back into a mechanism — the same thing
  under two names. Credential shapes differ per mechanism too (`object` signs `ak:sk`, rustic also
  wants a repository password), which is why the kind has to say which one it is.
- **A network filesystem kind.** Worst reproducibility of the options: the data is whatever the mount
  said at the moment of reading, and nothing about the run is repeatable.
- **Per-executor refcounting for cleanup.** A shared resource deleted the moment one executor leaves
  breaks another that is still reading; the process lifetime is the honest unit.