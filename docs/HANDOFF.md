# HANDOFF — after the event-plane landing (aura + prism)

> **Languages:** [English](HANDOFF.md) (primary) · [中文](HANDOFF.zh-CN.md)

Written 2026-10-02; updated 2026-10-08 after the event-plane internal-layout
landing (ADR-0041). Scope: what is left to do in **aura** and in **prism**. The
sibling projects that could be affected are named at the end.

**Landed and committed (working trees clean):**

| repo | commit | what |
|---|---|---|
| aura | `2d1fafb` | `feat(realm,engine,config,docs): land the event-plane terminal form (ADR-0038/0039/0040; Phases 4.17/4.18/4.19)` — 34 files |
| probe | `b56faa3` | `refactor(runtime): rename the ctx_skip_to_now steel stub to ctx_skip_to_head` — 1 file |
| aura | `74221de` | `feat(realm,docs): event-plane internal layout — instance-key vocabulary, issuer on MqData, one physical partition (ADR-0041)` |
| aura | `e3e28b7` | `docs(design): event-flow clarity pass — dedupe §7, repair §1/§6.1, PLAN session record` |

Ruling text: `aura/docs/adr/0038-event-plane-identity.md`,
`0039-partition-encoding-and-retention.md`, `0040-keyspace-bands.md`,
`0041-event-plane-internal-layout.md` (each with a `.zh-CN.md` twin). Keyspace
authority: `aura/docs/design/event-flow.md` §7 (key and value layouts detailed in
§2's landing lines). Session records: `aura/docs/PLAN.md` → "会话记录（2026-10-02b）"
and "会话记录（2026-10-08）".

**Landed since 2026-10-02 (ADR-0041) — names in the list below that predate it are
superseded:** the slice segment is the instance key (`part_id` →
`instance_key_id`, `PartitionName` → `InstanceKeyRegistry`, `mq::Partition` →
`mq::InstanceKey`, `SINGLETON_PART` → `SINGLETON_KEY_ID`, `bound_partition` →
`bound_instance_key`); **MqHead (ns 24) is retired** — the write head is MqData's
`HighWater(seq)` reduce, ns 24 vacated (event plane's live tables: 20, 21, 22, 23,
25); `#[ok_partition(2)]` left MqCursor (MqData's partition 1 is the only physical
partition); the event plane is documented in two layers (contract / internal
layout). okm `MODELING` gained two sections in the same window (open-vocabulary
proxy-id registries; reading a reduce; commit `559d031`) and trigger joined the
event-consumer split (commit `d3f4fde`).

## What landed (so the next reader does not re-derive it)

- **Identity/delivery (4.17)**: a key-less (wildcard) subscription delivers to the
  type's SINGLETON instance; the event plane keeps no subscriber dictionary of its
  own — routes and cursors key on the meta plane's BoothName id (ns 30), so the
  cursor key's third segment is `booth_id` and `split_once('/')` is gone. The
  `__default__` fallback is retired: a payload missing its declared key field is a
  malformed event → dead ring with `DeadReason::MissingKeyField`.
- **Partition identity + bands (4.18)**: `PartitionName` (ns 21) is a proxy
  dictionary (`by_name` + `HighWater` + reverse resolution); the FNV-1a hash family
  is deleted; `mq::Partition` (`Singleton | Named`) marks key-less delivery
  structurally, `SINGLETON_PART = 0` is unreachable by issuance. The framework low
  block is renumbered: event plane **20–25**, meta plane **30–32** (ADR-0040);
  key widths tightened (MqData 20→16 B, MqCursor 16→12 B, MqHead 12→8 B).
- **Retention (4.19)**: one global `cursor_ttl` (KDL `mq { cursor_ttl "30d" }`,
  default 30 days, decoupled from `idle_ttl`). Expiry = the row leaves the
  compaction denominator, never a delete; only rows already below the watermark are
  reclaimed (`drop_cursor`). `MqCursor.last_active_ms` is the wall-clock input.
- **Two corrections found while landing**: the denominator's test is
  `mq::booth_subscribes` (MATCHING routes, exact or wildcard) — the old
  exact-event_id lookup silently dropped wildcard consumers out of the denominator
  and let compaction eat their backlog; and MqData's third segment is a plain
  SEQUENCE (`seq` = `last + 1`, `MqHead.last_seq`) — not a timestamp. Uniqueness
  rests on the emit path holding the realm lock (one writer), not on the clock.
- **Renames**: `type_id` → `booth_id`, `by_type` → `by_booth`, `type_subscribes` →
  `booth_subscribes`, `type_id_of` → `booth_id_of`, `mq::skip_to_now` →
  `mq::skip_to_head`; the script-facing host fn was renamed with it
  (`ctx_skip_to_now` → `ctx_skip_to_head`) in aura AND in probe's steel stub list.
  The zero-caller passthrough `mq::booth_name_of` was deleted.

## AURA — remaining work

**A1. Phase 4.13 — the routing end form (the event plane's last open item).**
PRIORITY; **precondition = dynamic schema**. This is the other half of ADR-0038
§3, the part that deliberately did NOT land with 4.17: today the EventRoute row
carries `key_field` (a per-event payload-field name) and a `wildcard` flag; the end
state is `resolve` given per event as (type, event) → one of THREE shapes —
singleton / payload-field / **index scan** — discriminated at the row structure
layer, with references stored by NAME (never a slot/ns number, because position is
not identity). Acceptance per the PLAN entry (`docs/PLAN.md` line ~200): the
declaration is per event, absence = singleton, and the three shapes are mechanisms,
not sentinels. Nothing interim was built for it — do not invent a transitional
shape; it lands with the scan face.

**A2. Structural instance identity (recorded residual, needs its own decision).**
The INSTANCE key space still uses the sentinel string `"__singleton__"`
(`InstanceId { key: ... }`), so a payload key literally equal to that text still
aliases the singleton INSTANCE (a different bug from the partition aliasing that
4.18 fixed). Making it structural touches `InstanceId` across the whole call model
and the probe seam — filed in ADR-0038's "Residual" paragraph. No ADR yet.

**A3. Ops act for existing deployments (ADR-0040, extended by ADR-0041).** An
existing store must have the low block WIPED: the new meta band (30–32) lands on
numbers the old event plane used, and without the wipe a new `BoothName` decodes
old `EventName` rows as booth names ("reading old data as new data"). Cost is
higher than "mq bytes are transient": persisted booth definitions and code blobs go
with it, so the deployment re-registers its types. ADR-0041's renames, the MqHead
retirement and the MqCursor partition removal are absorbed by the SAME wipe — no
extra migration step. A fresh store has nothing to wipe — this repo's tests build
fresh stores per run.

**A4. probe USAGE bilingual frame-shape sync (4.16c leftover).** Not touched this
session: probe's docs contain no reference to the renamed host fns, so the scope of
"USAGE sync" needs a call before anyone edits it (it may be a doc-only sweep of the
frame shapes landed in 4.16c).

**A5. The `python` carrier cannot be exercised on this machine.** `cargo test
--features python` fails to link `okm-python` (pyo3 0.25.1 does not support the
host's Python 3.14.7). This is environmental, not a code defect — but it means the
python binding face was NOT re-verified after this session's changes, and any gate
that says `--workspace --features python` (PLAN 4.16/4.15 gates, prism's default
features) needs a machine with Python ≤ 3.13.

**A6. Bookkeeping (tiny docs commit).** The status lines of ADR-0038/0039/0040 and
the PLAN entries for 4.17/4.18/4.19 plus the 2026-10-02b session record still say
"提交待指令 / commit pending". They should cite §`2d1fafb`.

**A7. `~/world/aura-base` is a stale duplicate** of this repo (HEAD `3b30466`,
clean tree, no references found anywhere). If it participates in any build (image
base?), it is a second, older code line — decide keep or delete. (Note: a
TRANSIENT worktree at `~/world/aura-baseline` was used for this session's
pre-existing-failure baseline and already removed; `aura-base` is a different,
older thing.)

## PRISM — remaining work

**P1. The nushell feature cleanup (blocks everything else).** prism is the only
local project that path-depends on aura (`aura-booth`, `aura-engine`,
`aura-realm`). It currently CANNOT build, for a reason that predates this session:

```
package `prism` depends on `aura-engine` with feature `nushell`
but `aura-engine` does not have that feature
```

PTY/nushell was retired in Phase 4.15 (ADR-0035 §6) — nu now rides the bgi
carrier's fifo adapter and needs NO feature gate (confirmed in probe:
`runtime/src/carrier/exec.rs`). Fix in three places:

1. `crates/prism/Cargo.toml` — delete the line `nushell = ["aura-engine/nushell"]`
   and drop `"nushell"` from `default = [...]`.
2. `crates/prism/src/booths.rs` — remove `#[cfg(feature = "nushell")]` from the nu
   echo booth: keep the booth UNCONDITIONALLY (the language still works; deleting it
   would drop a capability).
3. `crates/prism/tests/echo_e2e.rs` — same cfg removal on the nu tests and in the
   list around line 181.

Then build. prism has not compiled since 2026-09-29, so expect further drift; the
first successful `cargo check` is the real gate, not the three edits above.

**P2. `python` is in prism's `default` features** (`default = ["steel", "python",
"nushell", "wasmtime", "fjall"]`). On this host that makes the default build
impossible for the same pyo3/Python-3.14 reason as A5. Either drop `python` from
`default` (keep it opt-in) or pin a ≤3.13 interpreter; otherwise local checks always
need `--no-default-features --features steel,wasmtime,fjall`.

**P3. After it builds (prism's own PLAN, `prism/docs/PLAN.md`).** Phase 1 (WS
gateway: auth + parse + turn submission as realm events, streaming back via event
subscription) is partially landed — identity half LANDED 2026-09-25, Phase 1.9
(static code export) LANDED 2026-09-26; finish Phase 1, then Phase 2 (CLI wrapping
the same WS protocol — no second RPC surface). Phase 1.5 is a scope note with no
code.

**P4. Dev store before testing against the new aura.** If prism has an existing
aura store directory, wipe the low block first (A3) or point the test at a fresh
dir — otherwise the first run reads old rows as the new tables.

**P5. Verified non-issues as of 2026-10-02 (re-check against ADR-0041).** prism
touches only `aura_realm::meta::{code_hash, code_hex, get_blob}`,
`aura_realm::mq::MqStore`, `aura_booth::{BoothType, InstanceId, call::Waited}` and
`aura_engine::Engine`. MqStore's TYPE names changed with ADR-0041
(`mq::InstanceKey`, `instance_key_id`), so a prism build against current aura may
need touching if it names those types directly — the first `cargo check` after P1
answers it. No prism script calls `ctx_queue_depth`/`ctx_skip_to_*` (a repo-wide
grep of `~/world` found no callers outside aura/probe), and the host-fn set is
unchanged by ADR-0041.

## Cross-cutting rules that keep biting

- **The host-fn contract is two files.** aura's ctx bridge (`crates/realm/src/ctx.rs`)
  and probe's steel introspection stubs (`crates/runtime/src/carrier/steel.rs`) must
  name the same set — the stub list is what lets a steel script resolve `ctx_*`
  identifiers at LOAD time. Current set: `ctx_invoke`, `ctx_store_emit`,
  `ctx_interface_schema`, `ctx_queue_depth`, `ctx_skip_to_head`, `ctx_timer_*`,
  `ctx_iter_*`. Rename in one repo only = a script that fails to load.
- **Numbers are never reused.** Event plane tables live at 20, 21, 22, 23, 25
(ns 24 vacated by ADR-0041), meta plane 30–32, booth data ns from 100; the old
`30–35`/`40–42` set is retired for good.
- **Only prism depends on aura today.** gravity is docs-only (its Phase 0 needs no
  aura dep; Milestone B's Phase 4 Booth binding is where it starts using the
  contract); k10r/krystallizer, mudra, fluxora and klaw have no aura/probe
  dependency (okm only) and are unaffected.

## Verification recipe

```sh
# aura — must be green except exec_booth::bgi_booth_ctx_invoke_to_sibling,
# which is PRE-EXISTING (reproduced against a read-only git-archive copy of HEAD).
cd ~/world/aura
cargo check -p aura-realm -p aura-engine -p aura-config --all-targets
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets          # only proc-macro-error2 future-compat note

# probe — the stub list's own test, plus the exec/bgi carriers.
cd ~/world/probe
cargo test -p probe-runtime --test steel_introspect

# prism — after P1; python excluded because of A5.
cd ~/world/prism
cargo check -p prism --no-default-features --features steel,wasmtime,fjall

# the cross-repo end-to-end proof of the renamed host fn (aura script -> probe steel
# stub -> aura host bridge): passes today.
cd ~/world/aura && cargo test -p aura-engine --test queue_relief
```