# 0048 — Resources are a venue primitive

Status: Accepted (2026-11-23)

## Context

Data is already the venue's: a booth declares a schema, the realm owns the
namespace, the store holds the rows. Everything an execution needs *besides*
its own arguments — a repository to read, a bucket to publish into, a scratch
directory, an image to pull — had no home. Each booth that needed one invented
it: gravity grew a resource table, and a second booth (a build pipeline, say)
would have had to grow the same table plus the same git handling.

A resource is not a transport detail. `git`, a bucket, a snapshot repository and
a registry serve different shapes, and a scratch workspace is not like any of
them: it exists for one execution and is gone. What they share is what makes
them one concept — an owner, a mode (readable or writable), a version or a
lifetime, readiness before the code runs, and disposal after.

Keys were the objection: the venue must run without them. It does. A venue with
no credentials is a venue where resource-bearing calls are refused by name at
dispatch; nothing else about it changes.

## Decision

**Resources are an aura primitive.** The venue owns three things and nothing
more:

1. **The declaration.** An activation declares what it needs, in its own start
   args (`resources: [{kind, seq}]`). The realm stamps the OWNER from the
   turn's envelope and hands the declaration to every dispatch of that
   activation. A task never names an owner — one that could would borrow
   another account's data. The environment follows the call: a booth the
   activation invokes (a mount, a sub-booth) inherits the declaration and the
   account, because it is the one that dispatches to the node, and a callee that
   declares resources of its own still wins over what it inherited.
2. **The registry.** Rows in the venue's store — the same place an owner's data
   lives — in a framework band of its own (ns 36, ADR-0040's meta band; versions at 37 and
   secrets at 38 — one table per ns, because okm's structural stamp is one row per
   ns and a shared number would let one table's layout change speak for another's). The key
   is the store's TYPED key (a discriminator and a number), not a byte layout of
   `owner/kind/seq`: okm keys are fixed-width typed fields and only the last may
   be a variable-width string, so identity parts that are text are DECLARED
   FIELDS, with `by_owner` as the index that makes "everything I own" one range
   read. A row carries what is fixed (`desc`, `mode`, `created_at`, and the
   pointer to the current version) and what its kind adds. Versions are rows of
   their own with a `previous` link, so undo is one read and needs no index.
3. **The interface.** Built into the realm, not a booth:
   `resource.register`, `resource.secret`, `resource.material`,
   `resource.published`. A plane (prism) manages it the way it manages nodes
   and tokens.

**Keys live in the venue too**, in a namespace no booth declares — so booth
store plans cannot address it at all. That is structural, not a naming
convention: the isolation is the absence of a schema, not a rule someone has to
remember. A row names the secret it uses; the secret is never in a row. At rest
they are plaintext for now; sealing them is okm's job when okm has the
primitive.

**Transfers are the node's.** The venue authorises one transfer — an address
and a one-shot credential, minted per call, read or write separately, so a
leaked read credential cannot publish. Cloning, fetching, GET/PUT, pulling an
image are the executor's work and no line of them belongs in the realm.

## Kinds

    git       file tree, persistent, versions are branch@sha
    volume    file tree, persistent or per-activation
    s3        whole objects, persistent; no prefix scanning — a resource is
              identified, not searched
    rustic    snapshot stream, persistent
    image     immutable blob in a registry, persistent

Executions that need none of these declare nothing, and that is normal, not an
error.

**Ephemeral resources (workspaces) are not registered.** They appear only in a
declaration, the realm allocates their sequence within the activation, and the
executor makes and removes the directory. A name that outlives its execution
would say something false about it.

## Consequences

- gravity's resource table becomes a client of this interface rather than its
  owner; its `material`/`published` ops reduce to calls. A second booth gets
  the same service without touching git, s3 or the store.
- The venue knows how to **authorise** a transfer and nothing about performing
  one. `kind` stays an identity (`git` as a repository and `git` as a snapshot
  are two resources), so a new kind arrives as its own rows and its own node
  adapter, never as a new axis.
- A venue without credentials still boots and still runs everything that
  declares no resources: the refusal is at declaration, by name, before the
  call — not a discovery in the middle of a transfer.
- Owner is derived from the turn, so an executor cannot ask for another
  owner's resource by claiming one: the plane resolves it from the call it
  dispatched.