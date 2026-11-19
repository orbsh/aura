# 0050 — A realm is a row: an id in the key, a name in a table

> **Languages:** [English](0050-a-realm-is-a-row.md) (primary) · [中文](0050-a-realm-is-a-row.zh-CN.md)

**Status:** Accepted (2026-11-19). Supersedes §1 of ADR-0028 only in what a realm is *identified by*; the vocabulary ruling (outer axis is "realm", its collection is "realm-set", "ns" is the key segment) stands unchanged.
**Date:** 2026-11-19

## Context

A realm is a prefix. `MqStore::for_realm(inner, name)` prepends `[u16 BE len][name]` to every key, and isolation is that binding: a handle built for one realm cannot address another (ADR-0028, `realm_set.rs`). That was enough while a realm was a deployment-time word. It stops being enough the moment realms are created and destroyed at runtime, which is what a job-scoped temporary realm requires.

Three properties of the current arrangement are the problem, and none of them is a bug in the prefix scheme:

1. **The name is the identity AND the location.** Renaming a realm would move every key; two realms cannot be told apart except by their names; and a name is as long as someone typed.
2. **The default realm has no prefix at all.** `Engine::new` builds it as `Realm::with_mq(mq.clone())` — the raw engine handle — while named realms go through `for_realm(name)`. So "the system realm" is not a member of the realm set; it is the unprefixed remainder of the keyspace. That is the real reason the code reads as having two tiers.
3. **A prefix is not an object.** Nothing enumerates realms from storage (`RealmSet::live` lists the in-memory map), nothing can measure one, and nothing can delete one: "destroy this realm" has no handle to hold.

## Decision

**RULING — a realm is identified by an ID. The id is what lives in the key; the name is a column in a table. Persistent and temporary realms are the same kind of thing, differing only in where their row lives and whether they are ever written.**

1. **Two shapes of id, one type.**

```rust
enum RealmId {
    Persisted(u32),    // allocated from the realms table, monotonic, never reused
    Ephemeral(u128),   // a uuidv7, minted in-process; never reaches storage
}
```

The variant is explicit, not inferred from width. Width is a property of the variant, not its discriminator: a uuid's numeric value can fall inside `u32`'s range, and a serializer that writes the number rather than the bytes would erase the distinction. The tag is what makes "look in the memory table, then in the persistent one" decidable.

2. **The storage prefix is the id, not the name.** `[u32 BE id]` instead of `[u16 len][name]`. A persisted realm's keys are therefore shorter, stable across a rename, and free of an encoding that has to be parsed to be understood. An ephemeral realm never writes, so its u128 never becomes a prefix; it exists as an in-memory map key and as something a log line can name.

3. **Two tables, one lookup order.** `realms` (a fixed root table: `id → (name, created_at)`) for the persistent kind; a process-local map for the ephemeral kind. Resolving an id consults the memory table first and the persistent table second — the ephemeral kind is by definition only ever found in memory.

4. **There is one kind of realm, and no special case for the first one.** The default realm becomes an ordinary persisted realm with a name — `main` — and its keys are prefixed like every other realm's. `Engine::new`'s unprefixed handle is retired: no code path may write a key that belongs to no realm.

5. **Creating is memory-only unless persistence is asked for.** `create` defaults to `Ephemeral`; a caller that wants durability says so. The default is the one that leaves nothing behind, which is the right bias for a call made at runtime. prism creates exactly one persisted realm at boot (`main`) and opens it by name.

6. **A temporary realm is fully isolated, including its type table.** Its registry is not written to `interface_schema`, its storage namespaces come from an in-memory allocator rather than the persisted `BoothName` rows, and its mq store is an in-memory stand-in. Types registered there are visible only inside it.

7. **Realms do not nest.** A temporary realm cannot create another realm. Jobs may nest (effector ADR-0005) — that is a fact about work, not about isolation — but the isolation axis is one level deep: many realms, no parents.

## The migration (one pass, no compatibility mode)

The unprefixed keyspace becomes `main`'s:

1. Create the `realms` table at its fixed root; insert `1 → ("main", now)`.
2. Walk every key in the existing keyspace, write it back with `[u32 BE 1]` prepended. fjall has no in-place key rewrite, so this is a copy into a fresh keyspace followed by a swap of the open handles — offline, one shot, with a key count before and after as the check.
3. Known rows are spot-checked after the swap: the booth-type registry, one type's storage collections, one mq table.
4. No unprefixed write path remains, so a stale writer cannot re-create the old space: the engine no longer has a handle that writes without an id.

## Consequences

- **Ids are never reused.** A destroyed realm's id is retired even though its rows are deleted; recycling one would make a new realm inherit an old realm's data, which reads as a bug in isolation and is not one — it is the id doing exactly what it was told.
- **A raw key dump is no longer self-describing.** Reading storage directly now requires the `realms` table to render `1` as `main`. That is the cost of a short prefix, paid once in the tooling and never again per key.
- **Destroy has to mean something.** With a row and a prefix, `destroy` is: drop the live Realm (instances, executors, streams, in-flight calls), delete the row, and delete the prefix range. An ephemeral realm's destroy is only the first step, because it wrote nothing.
- **`live()` becomes a query, not a map listing.** The persistent half of the answer comes from the table; the ephemeral half from memory.
- **The two-tier reading disappears from the code.** There is no "system realm plus named realms" — there is the realm set, and `main` is in it.

## Related records

ADR-0028 (the outer axis is a realm, its collection is the realm set — vocabulary unchanged), ADR-0026 (a booth's storage is type-scoped: the ns a realm gives each type), ADR-0018 (mq tables ride the byte engine), effector ADR-0005 (a job is an envelope — the reason temporary realms exist at all).