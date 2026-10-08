# PLAN

Design lives in the wiki (summaries) and ADRs; detailed design moved into this repo: `docs/design/storage.md` (storage architecture), `docs/design/realm.md` (field model), `docs/design/partitioning.md` (data partitioning), `docs/design/booth-api.md` (script booth reference), `docs/design/event-flow.md`/`event-flow-en.md` (the emit/on mechanics end to end — declaration, registration, matching, delivery, persistent queues, cursors, retention — with the okm keyspace layout §7 merged from the former ns-layout page, and the Phase 4.13 open-questions list §8; `ns-layout.md`/`ns-layout-en.md` are pointers), cross-referenced with the wiki. This file only sequences phases.

## Milestone A — Single binary engine

- [x] Phase 0 — Workspace skeleton: `crates/{engine,booth,realm,storage,config,cli}`; single-binary start, no external deps (no Docker / etcd / DB). Echo Booth: define → invoke → return.
- [x] Phase 1 — Booth runtime: Rust host + Tokio MPSC pipeline; per-Booth context (in-memory modify, on-disk sleep) — ctx surface per ADR-0011 (state/metadata/invoke only; emit/on, contracts, hooks stay off ctx); partition key routing; on_sleep/on_wake scale-to-zero (state → Fjall).
- [x] Phase 2 — Embedded languages: implemented by importing the probe runtime's carriers (steel/python/wasmtime/nushell, feature-forwarded) instead of an in-tree Polyglot Bridge — one carrier implementation serves the remote actuator and embedded booths. `BoothType::script(language, source, entry)`; script bodies run via spawn_blocking.
- [x] Phase 2.5 — Script-booth ctx bridge (CLOSED 2026-09-25: all four carriers bridge now — the nushell PTY file bridge landed, see 遗留节同题条目; ctx_state_* were later retired by ADR-0026 §3 — the bridge carries ctx_invoke / ctx_store_emit / ctx_interface_schema)
  - [x] Host functions exposed into carrier scripts: probe-runtime gains `HostBridge`/`HostFn` (`ExecRequest.host`); carriers marshal one JSON arg in / native value out
    - steel: builtins via `register_fn`, native hash/number marshal
    - python: `PyCFunction::new_closure` closures
    - nushell: subprocess cannot call back — bridge absent, carrier errors if demanded; TTL via interface_schema introspection works (one spawn)
  - [x] Realm `host_bridge_for`: `ctx_state_get/set/delete` (scoped to the instance's own state — cross-instance reach not expressible) and `ctx_invoke` (blocks on the unified call model inside spawn_blocking)
  - [x] Acceptance: steel/python scripts store+read state, invoke other booths, state survives eviction
  - [x] Remaining (LANDED 2026-09-23, revised shape): dynamic schema → the EventRoute PERSISTED registry + logical-time MQ keys — the revised landing replaces the sketched "event name maps to an okm ns" shape (per-event dynamic ns REJECTED: ns space is the application budget, an open event vocabulary would consume it unboundedly; the route registry is one fixed ns with the event resolved through the existing EventName registry)
    - EventRoute table (ns 35): pkey (event_id, booth_id), payload = booth_id mirror (index fields must be payload fields) + key_field + wildcard u8 sentinel; by_booth index — subscription facts survive restart, readable by ops without introspecting scripts; register_type persists as a side effect; the in-memory EventRouter stays the emit-matching hot path, the registry is the durable source of truth and the watermark denominator (compact_queue_locked reads routes_of_event)
    - MqData sort key: [event_id][part_id][time] — LOGICAL time (ms) monotonic per partition via the MqHead row (max(now_ms, last+1)), append O(1) (the max-scan is gone), skip-to-now reads the head; wall truth rides the payload
    - part_id: FNV-1a hash retained (partition values are user-data scale — a registry would grow unbounded); 0 RESERVED for the singleton partition (part_id_of; hash collision maps to 1)
    - not landed (deferred until a real one-to-many consumer appears): index-scan-based fan-out where key VALUES enter index entries (PLAN:19's original sketch) — the current MqData primary key IS the access method for per-event/per-partition scans; cursor-per-subscriber already covers one-to-many delivery
    - **[2026-10-02 update]** the two lines above describe the 2026-09-23 shape; both were superseded and LANDED in Phase 4.18: EventRoute is now ns 25 keyed `[event_id][booth_id]` with a `by_booth` index (ADR-0038 §2), and `part_id` is no longer an FNV-1a hash — the PartitionName dictionary (ns 21) issues it (ADR-0039 §1). Authoritative table: `docs/design/event-flow.md` §7.
- [x] **Phase 2.6 — Resident VM per script instance (PRIORITY, closes the memory-state gap)** (CLOSED 2026-09-25 — acceptance items all test-locked: shared in-VM globals + VM drop on evict = echo.rs::idle_eviction_drops_the_resident_session; state survives eviction = state_survives_scale_to_zero; probe disconnect flips presence + in-flight fails as error value = remote_probe.rs::remote_probe_roundtrip extended with the abort/unregister/not-connected assertion)
  - Problem: spawn-per-job — every message re-loads source, builds a fresh VM, runs the entry, drops it
    - script globals never survive between messages
    - idle_ttl eviction loses nothing → per-type TTL / retention-window semantics meaningless for script booths
  - DONE (2026-09-16): one-shot execution REMOVED — all carriers resident
    - probe `carrier/session.rs`: `ResidentSession` trait (load/call) + `Sessions` registry; per-instance slot locks (registry lock never held across a call — `ctx_invoke` re-entry safe)
    - steel: engine cached per instance; python: module (interpreter namespace) cached per instance; nushell: resident PTY REPL (reedline CPR answering, file-based result protocol) — replaces the one-shot subprocess path
    - aura: `Realm` owns `Sessions`; `run_job` calls `with_session(instance_key)` — cold start loads, later calls reuse; sessions die with the realm (test isolation), hot replacement can evict selectively
    - eviction = drop the session; rebuild = re-instantiate + reload source (same as activation)
  - Remaining:
    - interim shim: REMOVED (2026-09-23) — event delivery addresses the handler by its concrete event name; no execute fallback
    - wasm: DONE (2026-09-23) — `Module` compiled once at session spawn + resident `Store`/`Instance` (`WasmSession`, see Phase 4.5 wasm carrier completion)
  - Phase 3 wire (2026-09-16): ctx bridge over the wire landed — Frame::Host round trip, gateway resolves host calls via pending_remote (call_id → instance), state/invoke scoped to the remote booth instance (engine/src/host_wire.rs); E2E: probe script's ctx_state_set/get + ctx_invoke round-trip through the wire.
  - Phase 3 wire (2026-09-16): `Body::RemoteProbe` — realm holds probe connections by node alias (`probes` + `pending_remote`); `run_job` sends Frame::Call over the wire and awaits the correlated reply; engine `probes.rs` gateway accepts probe dial-ins, registers by alias, correlates Result frames. E2E: real probe dials the gateway, invoke round-trips through the probe's resident session.
  - Lifecycle ownership: AURA owns the policy, PROBE owns the mechanics
    - aura decides WHEN a residency (and its VM) dies: realm-wide default TTL + per-type override + script-introspected value
    - eviction is instance-level (per-instance queue + residency set + partition serial semantics live in aura)
    - probe NEVER self-expires an booth VM session (two owners of one lifecycle = drift)
    - probe-side independent expiry applies only to probe-internal operations aura does not track
  - Affinity + offline (remote probes)
    - registry records the booth→probe binding: affinity is metadata, not a routing hop
    - requests follow the data (Phase 5 invariant); the registry answers "where is the residency now"
    - probe offline: (a) connection drop flips registry presence; (b) in-flight invocations fail with the call model's normal error-value semantics; (c) residency declared lost, not silently kept — recovery = re-activation on next connection (VM rebuilt, working set re-fetched); (d) while offline, messages queue in the realm's per-instance queue (bounded) or fail per the caller's tier
  - Probe parallelism: control-plane concern, not a probe threading model
    - same-node-serial queue semantics stay the default (two ops writing one file is a policy violation, not a scheduling bug)
    - parallelism = control plane expresses it as separate partitions/instances or explicit operation-declared concurrency; conflict responsibility at the caller/plane level
  - Acceptance: two consecutive invocations of a python/steel script booth share in-VM global state (counter in globals, not ctx_state); state still survives eviction via ctx_state; evicted instance's VM is dropped; probe disconnect flips registry presence and in-flight calls fail as error values
- [x] Phase 3 — Realm model: event namespace landed
  - emit routing: exact + wildcard-prefix (dotted names, `order.created`; wildcard requires the dot)
  - partition key extracted from event data; singleton `__singleton__` for wildcards
  - emits whitelist enforced at the Realm boundary (undeclared emit = error value; system/None bypasses — the whitelist constrains booths, not the host)
  - bounded dead-event ring for unmatched events
  - follow-ups: RouteMode composition primitives (on_join/on_batch/on_debounce); interface_schema dynamic (script-declared) form arrives with the Phase 2.5 ctx bridge
- [x] Phase 3.5 — Unified call model (CallSlot): `ctx.invoke()` with oneshot + `pending_calls` + `reply_to` is the single call mode for HTTP / realm Booth / remote Probe targets
  - Two-tier waiting split at the entry by static tool declaration (never mid-wait)
    - hot: task parks on the oneshot (memory-only, no thread held, no persistence)
    - cold: wait never enters park — transcript persisted, task ends, suspension recorded as a session event; re-entry from transcript on result (completed calls never replayed)
  - Timeout = failure value (Result via oneshot); no suspend/continue instruction (not feasible in Rust, unnecessary under entry-split)
  - Cold-call declaration rides Gravity's tool registration
- [x] Phase 3.6 — User namespace isolation: structural at construction
  - each namespace owns a Realm (types/instances/router/pending_calls) AND a namespace-qualified state store
  - PrefixStore wraps the shared engine per okm's nesting model: `[2B BE len(ns)][ns]` prepended to every key the inner store produces (raw ops get/set/del/scan prefix after inner key_for)
  - no textual separators; inner format unknown to the wrapper → cross-namespace state/events/targets not expressible
  - surfaces: `register_in` / `call_in` / `emit_in`; namespaces lazy + observable
  - probe registration credential = user credential → namespace derived at registration; tool target resolution = user namespace + node alias + operation
  - loose inbound message cap (anomaly guard only — large artifacts never enter the control plane)
- [~] Phase 4 — Root config + two-instance storage
  - [x] Root config `aura.kdl` (KDL via knus, krystallizer ADR-0007 pattern: no secrets in file, env-var names only): `node` / `data` / `meta` top-level nodes, `TryFrom<RootConfig> for EngineConfig` with unknown-engine rejection; slate engine shape (S3 endpoint block) parses now
  - [x] Two-instance okm model: data and metadata are TWO separate okm instances, engines independently selectable (fjall | slate); single-node runs both on fjall in different directories; ns isolation per-instance → ns reuse never collides, okm needs zero changes
  - [x] Fjall data-plane (`FjallStateStore`, per-field native LSM writes, boot error on feature mismatch — never silent fallback; acceptance: state survives engine restart)
  - [ ] Remaining: slate engine option on both planes
- [x] **Phase 4.5 — Platform booth model (PRIORITY): Booth definitions live on the data plane, never the compile plane** (CLOSED 2026-09-25 — all work items landed; the platform's booth forms are script source + wasm artifact, definitions persist via the 4.5b upload lifecycle)
  - Ruling: the engine ships NO in-process Rust booth
    - framework mechanics (the evictor class) are plain realm logic, not booths; wrapping them as booths is a pointless detour
    - k10r/gravity-class Rust services ship as `.wasm` artifacts uploaded at runtime (`set(lang="wasm", bytes)`)
    - compiling them into the host binary would fork the platform per app (every new service = repackage; Agent apps adding features = rebuild aura), collapsing the platform into a framework
  - Work items
    - [x] wasm carrier completion (LANDED 2026-09-23): `WasmSession` (probe-runtime `carrier/wasmtime.rs`) — resident session, module compiled at spawn; CBOR over linear memory (host writes args via the guest's `aura_alloc`, calls `handler(ptr, len) -> i64`, unpacks `(ptr:u32)<<32|len:u32`); handlers = function exports named after their events; `interface_schema` explicit export wins else export-list derivation; ctx-bridge host imports under `aura_host` namespace, uniform `(i32, i32) -> i64` packed ABI, undeclared import = instantiation failure (capability refusal); source = WAT text or base64 `.wasm`; `ResidentSession` gained `as_any` for carrier-specific introspection. Tests: probe `tests/wasm_session.rs` (WAT fixtures, 7 tests)
    - [x] metadata declaration unified on the type: `BoothType.receives` (ReceiveDecl: event + key_field + wildcard) with `.on(event, key_field)` / `.on_wildcard(pattern)` builders; introspection writes onto the type at register; `register_type` assembles routes as a side effect — one declaration surface per type (emits: none, per ADR-0012). Hot-swap = replacement semantics LANDED 2026-09-25 (see 遗留节 热替换条目)
    - [x] remove the Rust-closure booth form (`BoothType::simple`) from the public API — deleted; cli demo + all engine tests migrated to script booths (steel; nushell for the slow handler). Body::Rust remains in the enum with no public constructor (framework-internal future use)
      - rewrite cli echo demo + engine tests onto script booths (wasm/steel/python) as the acceptance path
    - [x] NUSHELL RESOLVED → PTY residency (ruled 2026-09-15, prototype-verified; ctx bridge + residency fully landed 2026-09-25)
      - resident session: one PTY per booth instance running a long-lived `nu` REPL (`--no-config-file`); idle_ttl eviction = close the PTY
      - multi-entry: `use 'operation.nu' *` imports all exports; delivery addresses the handler by event name (exported fn names = event names)
      - session state: `$env` variables persist across calls within the resident process — the memory tier (vanishes at eviction); durable state goes through the ctx bridge (landed 2026-09-25, see below)
      - verified in prototype: env vars persist across sequential calls in one nu process; reedline emits `ESC[6n` cursor queries the host MUST answer (`ESC[row;colR`) or input hangs; ANSI/OSC output needs stripping (`--no-config-file` + winsize reduces noise)
      - implementation (carrier PTY mode): LANDED — long-lived PTY session per instance, per-call wrapper eval, reedline query answering, ANSI strip
      - capability position after this: nu booths = stateful-resident (2026-09-25: the PTY ctx bridge landed — durable state via ctx-store-emit like the other script carriers; cross-call $env is the memory tier)
- [x] **Phase 4.5a — PRIORITY CLEANUP: remove the Rust-closure booth form (`BoothType::simple`) from the public API, immediately after Phase 4.5 lands its replacement**
  - the engine ships NO in-process Rust booth — framework mechanics (the evictor class) are plain realm logic, not booths; wrapping them as booths is a pointless detour
  - Rust code becomes an booth through exactly one channel: compile to wasm and upload
  - concrete removals
  - cross-node semantics resolved (ADR-0013): federation, not metadata consensus — metadata is per-node (meta okm, control-plane single writer), user data stays home, inter-node identity via well-known protocol auth; Location Transparency deliberately rejected
    - delete `BoothType::simple` (or confine it to `#[cfg(test)]` scaffolding)
    - rewrite the cli echo demo (`crates/cli/src/main.rs` — two `BoothType::simple` registrations) onto a steel script booth
    - migrate engine tests (`echo.rs` / `events.rs` / `callslot.rs` / `fjall_state.rs` / `namespaces.rs`) onto script booths (wasm/steel/python) as the acceptance path
  - done = no `BoothType::simple` outside `#[cfg(test)]`; public docs describe one booth form per language (script source or wasm artifact)
- [x] **Phase 4.5b — PRIORITY: booth metadata lifecycle — introspect-once at upload, persist to the meta store, never re-introspect on the execution path**
  - Lifecycle model (authoritative)
    - UPLOAD (`set`) is its own lifecycle and may never execute
    - at upload the host introspects `interface_schema()` ONCE (a pure function; one carrier load, one call, result extracted, load discarded)
    - extracted metadata (receives / emits / lifecycle.idle_ttl / returns) is PERSISTED into the meta okm instance, keyed alongside the booth definition (`booth_defs`-adjacent; same versioning, content-hash dedup and rollback semantics as the script bytes)
    - EXECUTION never calls `interface_schema` — the schema is a static record in the metadata store; message handling loads the script (latest version) and calls the entry only
    - VERSION CHANGE (a new `set`) re-introspects once and updates the persisted metadata; until then the old metadata governs
  - Carrier-uniform: python/steel (loaded module), nushell (one spawn per call — introspection is one spawn running a generated `interface_schema` wrapper; the current "not wired for nushell" limitation is a TODO, not a capability gap), wasm (one module instantiation — same one-shot shape as nushell)
  - Corrections to the current implementation
    - [x] (a) `booth/src/persist.rs`: `PersistedBooth` record (language/source/entry/idle_ttl/schema) persisted via `engine.register` (now returns `Result`); meta okm instance constructed per config (`meta_engine`/`meta_dir`); boot reload re-registers types from the meta store — metadata survives node restart
    - [x] (b) execution path introspection-free, locked by acceptance (definition + TTL survive restart on the same meta dir, immediately invocable)
    - [x] (c) nushell introspection works through the generic carrier path (generated wrapper calls `interface_schema(args)`; zero carrier changes) — acceptance: nu script declares `idle_ttl: 5m`, register seeds it
    - [x] (d) docs: realm.md §script persistence + booth-api.md state the three-lifecycle model (upload/introspect+persist — execute/read metadata — version change/re-introspect), replacing any "load once, call schema then entry" phrasing
  - Done = metadata survives node restart from the meta store; execution path provably schema-free; all four carriers (python/steel/nushell/wasm) deliver declared metadata through the same upload-time introspection contract
- [x] **Phase 4.5c — PRIORITY: multi-entry booths + event-queue semantics (corrects the single-entry model in the current implementation)** — CLOSED 2026-09-27 (relief-valve items below; the model, queues, multi-entry, and docs landed earlier)
  - Problem: the current model is asymmetric and wrong on two axes
    - definition: one `execute` entry, but emits are multiple exits — an booth with several handlers must split into several booths, duplicating shared logic
    - delivery: events are fanned out into per-booth mailboxes, which bakes in "an event belongs to a booth" — wrong for one-to-many (several booths listen to one event)
  - Ruling (restores the realm.md §on-decorator design + fixes delivery semantics)
    - MULTI-ENTRY: handlers declare `@on(event, key=...)` per handler (python decorator / steel `on` fn / wasm export convention) — one booth type, many handlers; shared logic stays in one place. NO standalone single-entry mode: a direct call (`ctx.invoke` / engine `invoke(target, handler, args)`) DECLARES the handler name in its payload — no reserved names, no implicit entry; event delivery addresses the handler by the event name
    - `interface_schema` is assembled IMPLICITLY and merged with an explicit partial declaration (not "derived, hand-written wins"): decorators own receives/wildcard_receives; the script contributes lifecycle etc.; the merged single function is the only thing aura calls — uniform across languages (rust/wasm: `#[on(x)]` generates the same implicit fn; steel/nushell hand-write it). EMITS are never collected or validated (ADR-0012): receiver set is a runtime fact — dead ring + delivery log are the observation surface; source parsing deferred until it serves a real consumer (Windmill criterion)
    - partition key declared on the decorator (`@on("add_to_cart", key="user_id")`), bound to the handler — not in a separate schema block
    - EVENT QUEUES replace per-booth mailboxes for event delivery: each queue holds one event family; @on-declared `key` → queue is per-(event, partition); no key declared → queue is per-event (singleton consumers)
      - an event belongs to NO booth; a queue may have multiple subscribers (one-to-many delivery is structural, not a fan-out simulation)
      - booth instances subscribe to queues per their @on declarations; serial-per-(booth-instance) semantics preserved by per-subscription cursor, not by owning the queue
      - PERSISTENT QUEUE RULING (2026-09-16): queues are embedded persistent partitions in okm, not broadcast channels — `[mq-data][event][part_id][time]` for events, `[mq-cursor][event][part_id][booth]{cursor}` for cursors (okm composite keys, zero new components). Emits are passively persisted on write (the passive-save half of the ctx.state dual-track); cursors advance per subscriber. Scale-to-zero no longer drops triggers: an evicted instance's backlog is delivered on re-activation. Slow consumers accumulate visible backlog (better than broadcast's silent Lagged loss); time-based catch-up (`skip-to-now`) is the backward-compatible relief valve — jump the cursor forward to the newest message, discard the stale backlog. Multi-subscriber fan-out stores the event ONCE per queue (N booths = N cursors into one partition), not N mailbox copies — the deduplication that per-booth mailboxes fundamentally cannot do. Backlog lifecycle follows the instance/namespace; permanently-departed instances' backlog is reclaimable by prefix scan. Replaces the interim tokio::broadcast implementation (broadcast semantics: late subscribers get nothing, slow subscribers get Lagged = silent segment loss — inconsistent with passively-persisted event data).
      - PARTITION ANNOTATION (2026-09-16): mq tables carry `#[kv_partition]` — `MqData` partition 1, `MqCursor` partition 2 (okm `#[kv_partition(N)]`, ADR-0014 §5: 2-byte `[0xFF][N]` escape key segment structurally disjoint from unpartitioned tables; Fjall routes handles per id so the append/watermark-delete and point-write compaction profiles never share a tree). `EventName`/`BoothName` registries stay unpartitioned (small, point-lookup). Atomic domain of a batch = one partition — emit append and ctx.state writes are deliberately not co-atomic (events are triggers, ctx_state is the truth).
      - RETENTION = min-watermark over ACTIVE subscribers (2026-09-16): a [ev][part] queue keeps only the range every active subscriber's cursor still needs — data before the minimum cursor is deleted on write-path compaction. The watermark's denominator comes from the ROUTE REGISTRY (4.5b-persisted @on metadata), never from the raw cursor keys: a permanently-departed booth's stale cursor must not pin the watermark forever. Deregistering an booth removes its cursor row; its backlog then falls below the watermark and vanishes with ordinary compaction — no separate reaper. Backlog depth is a live okm reduce count over the mq-data prefix (insert +1, watermark-compaction -1 unfold) — zero-scan operational surface, and the skip-to-now decision reads it directly.
      - HONEST SEMANTIC COST: the queue is a buffer, not a store — "at-least-once" holds only while every subscriber stays registered and consuming. A subscriber permanently deregistered with unconsumed backlog loses it. Consistent with the layering: ctx_state is the durable truth; the queue guarantees "alive = can catch up", nothing more.
    - emits: no hand-written whitelist, no static collection, no registration validation (ADR-0012) — an emit with no subscribers lands in the dead ring; that ring is the audit surface
  - Scope / order: touches realm delivery path (router → queues), booth type definition (decorators → derived metadata feeding 4.5b introspection), carrier contract (handlers are addressed by event name, not a single entry) — lands with/after 4.5 metadata unification so the derived schema feeds the same meta-store persistence
  - Progress
    - [x] step 1 — decorator-derived metadata + router seeding: python carrier injects an `@on` collector at import (identity decorator, `(event, key)` registry) and ASSEMBLES an implicit `interface_schema` merged field-wise with an explicit partial declaration (decorators own receives/wildcard_receives; the script contributes lifecycle etc.) — aura calls `interface_schema` uniformly across languages, no python branch; `engine.register` reads the full schema and seeds `router.on(event, type, key)` / `on_wildcard` per declaration (acceptance: exact + key-less + wildcard routes derive from decorators; merged schema carries decorator receives AND explicit lifecycle)
    - [x] step 2 (INTERIM, broadcast) — delivery path: per-(event, partition) broadcast queues replaced per-route submit-to-instance-mailbox; `@on` key → (event, partition) queue, no key → per-event singleton queue; emit activates matched route targets BEFORE sending; acceptance: one event, two subscriber types, both receive exactly once
    - [x] step 2b (persistent queues) — broadcast swapped for okm tables per the ruling: `realm/src/mq.rs` declares EventName/BoothName registries (open-ended names → numeric ids, resolved through the `by_name` text index) + MqData `[event_id][part_id][seq]` / MqCursor `[event_id][part_id][booth_id]` tables (`#[kv_ns]`, Table API over a StateStore→VirtualStorage bridge); payload is CBOR (new okm `FieldType::Bytes` — variable-length TLV raw bytes, added to okm RowEncode); emit persists passively (dead ring only for route-less events), consumer loop = backlog scan → run → cursor advance, re-activation replays. Follow-ups landed: min-watermark retention compaction (watermark denominator = route registry, evicted instances still count — their backlog replays; deregistered types fall out; runs on the emit path; 2026-09-23 the denominator now reads the PERSISTED EventRoute registry, not the in-memory router); logical-time keys + MqHead O(1) append + skip-to-now via the head row (2026-09-23, see Phase 2.5 Remaining). Remaining (LANDED 2026-09-27): reduce-based backlog depth — see the close-out item below.
    - [x] step 3 — steel collector: carrier binds an `on` builtin before the run (`(on "event" "key" handler)` records the declaration, returns the handler); `carrier::introspect(language, source)` dispatches — steel/python assemble the merged implicit+explicit schema, languages without collectors (nushell) fall through to the generic entry call on a hand-written `interface_schema`; wasm `#[on]` convention stays with Phase 4 link payloads (PLAN 4.5c wasm row)
    - [x] step 4 — DROPPED (ADR-0012): static emit collection + registration validation rejected — the lint catches only a subset the dead ring already reports better, warm-up/placement on an unguaranteed graph wastes runtime resources, and collected emits re-duplicate the emit call site (the same drift the whitelist had); may_emit whitelist check removed from the emit path
    - [x] step 5 — docs: booth-api.md bilingual rewritten to multi-entry model (lifecycle + event-queue semantics + @on examples per language) — landed; realm.md session-queue section + wiki §6.2/§mailbox updated (wiki aura-architecture §5 bullet + §6.2 lifecycle, stateless-agent-architecture probe adapter wording); 2026-09-23 realm.md emit walkthrough rewritten to the persistent-queue shape (concrete-name queue identity, wildcard concrete-name expansion via events_matching, MqHead logical time, min-watermark compaction on the emit path; broadcast-era code sketch removed)
  - Close-out (2026-09-27, the two ruling items the code lacked):
    - [x] reduce-based backlog depth — `MqData` carries `#[ok_reduce(Count { group(event_id, part_id) })]` (okm ADR-0023 preset × ADR-0024 key-field group): append folds +1, watermark compaction's delete unfolds −1 on the write path; `mq::depth(event, part)` = one point read, never a scan (the zero-scan operational surface realm.md's retention paragraph promised).
    - [x] skip-to-head wired to consumers — `advance` is monotonic by contract (a lower seq never rewinds: without it a skip re-surfaced the backlog on the next drain pass); `mq::rewind_cursor` is the test-only exception; the relief valve reaches scripts as host fns `ctx_queue_depth(event)` / `ctx_skip_to_head(event)` (renamed with the mq function; probe's steel carrier stub list updated in the same batch) (ctx bridge, bound to the instance; the queue resolves through the PERSISTED route registry — `mq::bound_partition`, exact id or wildcard-prefix, same rule as the consumer loop; an unbound event is an error value, never a silent no-op; no bypass guard). e2e: `engine/tests/queue_relief.rs` (handler read == store read; skip moves the cursor; post-skip events still flow); unit: `realm/tests/mq_okm.rs` depth fold/unfold + skip durability.
  - Docs status: realm.md §on-decorator matches the ruling; booth-api.md bilingual rewritten (multi-entry lifecycle + event-queue semantics + @on/merge examples per language); wiki aura-architecture §5/§6.2/§6.3 and stateless-agent-architecture probe-adapter wording updated to event-queue semantics (2026-09-15)

- [~] Phase 4.8 — Timers (ADR-0016, docs/adr/0016-timers-timer-wheel-cron.md en+zh; ADR-0011 amended — blocking/self-scheduling stays rejected, delivery scheduling passes the criterion): timer wheel scanned by the evictor tick; due entries deliver as ordinary `__on_timer` queue jobs (同目标到期合并一次唤醒); delivery counts as activity, re-arm explicit (投递不隐式自我重排); memory tier (dies with eviction) + durable tier (StateStore reserved namespace, restored via on_wake); declarative `lifecycle.cron` in `interface_schema` (注册时内省翻译为持久定时器，运行时从不解释 cron 表达式，错过策略 = skip-and-jump-to-next) + imperative `ctx.timer.register/cancel`; ctx-bridge host fns move to dot-namespaced introspectable groups (`ctx.store.*`, `ctx.timer.*`); gravity 按 channel 一实例（ADR-0016 ruling）。
  - RECONCILED WITH CODE (2026-09-27 audit; the [x] had recorded the ruling, not the landing): LANDED — reclaim tier end-to-end (DelayQueue driver + command channel; idle-TTL eviction measured from job COMPLETION, per-type `idle_ttl` override; watchdog = max_exec budget, unconditional cancel at completion; `cancel_target` on eviction; empty-queue parking fix; locked by `tests/timer_reclaim.rs`). DELIVERY MACHINERY EXISTS, ZERO CALLERS — `register_deliver`/`Entry::Deliver`/`deliver_timer`→`__on_timer` path is complete but nothing registers a deliver entry. REMAINING: (1) `ctx.timer.register/cancel` host fns (probe's steel stub table carries the names; aura never registers them — scripts cannot reach the wheel); (2) declarative `lifecycle.cron` (no cron surface in code; register-time introspection translation unbuilt); (3) durable tier (entries die with eviction; the 50-min compression wake + cron cross-eviction survival needs the restore path timer.rs's header already flags); (4) coalescing due deliver entries per target into one wake; (5) dot-namespaced host-fn groups. Work items here are wiring + the two entry surfaces, not new machinery — the wheel semantics (explicit re-arm, no implicit self-schedule) hold as built.
- [~] **Phase 4.9 — Type-scoped booth storage (PRIORITY, ADR-0026, docs/adr/0026-type-scoped-booth-storage.md en+zh)**
  - Ruling: storage isolation moves from the instance level to the TYPE level; the instance key keeps answering "who serially processes this message" and stops deciding storage layout. Terminology: routing-side `partition key` renames to **instance key** (aligns with `InstanceId`/`instance_key` in the state registry — one concept, one name; "partition" was the storage fact this ADR supersedes). Cross-node sharding vocabulary (Phase 5 shard map) keeps `shard` — node placement, not instance identity
    - each booth TYPE occupies one real okm ns (bounded declared vocabulary — satisfies the closed-vocabulary ns ruling; events/partitions stay registry+hash); low ns block reserved for aura (mq 30–35, meta/state 40–41), booth types allocate from a fixed base; allocation = `register_type` side effect via the type registry (the existing type_id assigner); ids never reused
    - instances are documents inside the type's ns (okm collection/document terminology; the type's ns plays a SQL-schema role): the `InstanceState` one-document-per-instance isolation shape is superseded; cross-instance aggregation inside one type = ordinary scan/reduce over the type's ns — projection booths retire for same-type aggregation, remain for cross-type precomputation
    - ctx.store rises to okm capability level: put / get / scan / reduce over the type's declared tables (exact op names set at implementation); field-level get/set/delete point model replaced; handles structurally bound to the type's ns at registration (cross-type access not expressible); nested invoke resolves target ns through the type registry, never caller-supplied keys
    - per-user realm isolation (Phase 3.6 mechanism — the axis ADR-0028 renamed namespace → realm; PrefixStore since retired, `MqStore::for_realm` is the layer) stays, orthogonal — it prefixes the whole engine beneath the type nss
    - serialization/partition routing/timer/mq semantics untouched; existing instance-state bytes discarded, no migration (ADR-0018 precedent)
  - Schema declaration (rides the 4.5b upload lifecycle; execution path never regenerates):
    - python: decorator over the okm type definitions derives the schema, merged into `interface_schema` (same implicit+explicit merge as @on)
    - steel/nushell: hand-written schema literal in interface_schema; handlers call ctx store functions directly (nushell's PTY host bridge landed 2026-09-25 — file-round-trip req/resp, same op set as steel; see PLAN 4.5 NUSHELL item)
    - wasm: same op set over CBOR per the landed carrier ABI
    - merged schema persists on BoothDef (dynamic segment) at registration; `ctx.interface_schema` reads the persisted copy (reflection for handlers; dev-time completion served by the LLM receiving interface_schema)
  - Protocol naming (aura side docs-only; prism side lands with Phase 8): the wire/concept layer carries only `ev` in BOTH directions — no direction field; emit/on are per-end implementation details (aura @on+emit; prism client ws.send/ws.on); prism = aura event semantics extended to the user end
  - Work items
    - [x] terminology rename: routing-side `partition key` → `instance key` — code LANDED earlier (c800a3b: Route.instance_key_field, ReceiveDecl key fields, router/emit naming); design docs' live passages swept 2026-09-25 (partitioning en+zh, realm, modeling en+zh, booth-api en+zh, ADR-0025 identity sentence); historical PLAN phase entries stay unedited
    - [~] op set narrowing: protocol types landed (`aura-booth/src/store_emit.rs` — StoreOp/StoreOpKind, JSON wire, Collection-semantic layer); put/get/delete_document + put/get/delete FIELDS (dynamic-segment bridge: okm-dynamic gains put_fields/get_fields/delete_fields mirroring the typed Collection — byte-identical entries incl. the shared field-name dictionary, locked by a cross-mode test; okm-dynamic Value carries Obj/Array so composite field values ride the dynamic segment natively, schema-declared fixed-width paths reject them upstream) + SCAN (schema-declared AccessMethod) + REDUCE GET (count/high_water/lowwater presets) EXECUTE through okm-dynamic DynamicCollection (`realm/src/store_exec.rs`); host-language reduce logic objects register through bindings, not schema data. No bypass guard: the developer has full control, primitive misuse is self-sabotage
    - [x] ctx bridge host fns + `ctx.interface_schema` read (LANDED 2026-09-24, pulled ahead of the per-language schema item): `Ctx` carries a store-emit handle + the persisted schema copy (injected by `ctx_for` as DATA — the emit handle clones the plan and the mq handle, no realm deref at emit time, no blocking_lock in spawn_blocking); `register_type` resolves the plan once from the type registry + the persisted interface_schema (failure = no plan = no ctx.store surface, error values never panics); host fns `ctx_store_emit` (StoreOp serde form) + `ctx_interface_schema` (persisted copy) registered in the bridge; engine.register persists the introspected schema with the definition.steel e2e: declare `storage.collections` → put/get round trip → schema read back (store_emit_roundtrip_and_interface_schema_read). Cross-repo prerequisites landed: okm 0c2a354 (nTLV composites everywhere — Obj-in-Array encode/decode threaded the name resolver; previously an Obj inside an Array encoded empty and decoded None, silently dropping the persisted interface_schema) + probe 2397149 (steel introspection registers ctx-fn stubs scoped to the throwaway engine — steel resolves free identifiers at define-compile, so any ctx-using script failed to load during introspection; stubs never shadow the real host fns in resident sessions)
    - [x] registry→ns allocation: `TypeName.ns` assigned at first registration (`BOOTH_NS_BASE=100` + id, never reused); `meta::ns_of` resolve; unit test locks stability/distinctness (b0013b7)
    - [x] InstanceState retirement (LANDED 2026-09-24, probe bbefac8 + aura 7828031): `realm/src/state.rs` deleted whole (StateDocumentStore/InstanceState registry, by_key index, watermarks); `StateStore` trait + `SharedStore` + `Ctx.state` die in aura-booth (storage addressing leaves Ctx — ctx carries self_id + invoke + store-emit handle + interface_schema only); `PrefixStore` dies with the trait (4.10: if it revives it wraps MqStore); `ctx_state_get/set/delete` host fns + engine host_wire `HostOp::State` arms deleted (probe side: protocol variants + remote.rs bridge arms). No backward-compat sugar — the ruling: retirement is justified by the MODEL (point document superseded by the collection surface), and "external users already exist" is as invalid as "no external users yet" as an argument either way; correctness of the model is the only input. `register_in` fixed in the same pass: it bypassed register()'s introspection+persist path, so namespaced types could never get a ctx.store plan (invisible while ctx_state_* needed no schema). Tests migrated onto the collection surface (events.rs pattern: declared collection + RMW + assert through an invoke read-back); `script_state_survives_eviction` deleted as an exact duplicate of the rewritten `state_survives_scale_to_zero`; remote_probe keeps invoke-only (execution nodes hold no state; ctx_store_emit needs a plan = 4.5b)
    - [x] per-language schema declaration (LANDED 2026-09-24, okm 80327a1..a64a9e0 + probe d5a8b54 + aura 3873240): the python DSL lives in okm (bindings/okm-python okm_schema.py — @KeyEncode/@DocumentEncode classes mirror the Rust derive, type annotations drive the layout; @ok_ref/@ok_ns/@ok_layout/@ok_index metadata; assembler emits the exact CollectionSchema serde, cross-checked byte-equal against CollectionSchema::of); okm-python publishes it as OKM_SCHEMA_PY (pyo3 extension feature-gated so rlib consumers embed the module without linking the extension); probe's python carrier execs the DSL into booth scripts and merges assemble_module's storage block into interface_schema (decorator storage wins over explicit; empty block omitted so explicit-only declarations survive merge_schema's or_insert); no @ok_ns = booth-side auto allocation (aura injects the registry-allocated ns into the plan — the declaration never carries it); index slots follow SOURCE declaration order (stamp list is bottom-up reversed, assemble reverses it); variable-width fields locate at most once and only LAST in an index fields/includes list; steel/nushell hand-written literal form already exercised by the aura tests; aura e2e: decorator-declared collection → ctx_store_emit RMW → persisted BoothDef schema (py_schema.rs). wasm (okm compiles INTO the module, static derives, no dynamic schema form) split to its own item
    - [x] wasm schema path (LANDED 2026-09-25, probe actor-guest crate + aura raw emit arm): okm compiles INTO the module — `actor-guest` provides `EmitStore` (VirtualStorage whose every primitive is one `aura_host.emit` host round trip carrying okm-wire OpFrame/OpResponse bytes; no new wire format) + `collection_entry::<K,R>()` serializing the compiled CollectionSchema into the interface_schema storage block at upload; `counter_actor` example is the rustc-compiled fixture. Carrier: `WasmSession` gains the `emit` RAW-BYTE host arm (request/reply skip the CBOR value marshal; JSON number-array carries the bytes losslessly) and `carrier::introspect`'s wasm branch satisfies the import with a STUB emit (steel register_ctx_stubs precedent — the schema export is pure, the plan doesn't exist at introspection). aura: `MqStore::ns_raw(inner, ns)` (2-byte type-ns prefix raw handle) + `host_bridge_for` gains `wasm_raw` — the emit arm executes OpFrames against the type's RAW ns engine plane (no Collection-op layer; the trusted static-mode writer IS the module, no-bypass-guard ruling). e2e: register → introspect persists compiled schema → in-module Collection RMW through the bridge → document survives eviction (aura wasm_guest_storage.rs); probe wasm_guest_storage.rs runs the same bytes against TestStore
    - [x] `ctx.interface_schema` read of the persisted copy (LANDED with the ctx-bridge item 2026-09-24, see that entry — echo.rs `store_emit_roundtrip_and_interface_schema_read` locks it)
    - [x] docs: storage.md/partitioning.md rewrite LANDED 2026-09-24 (018ff00 — instance-document passages superseded across booth-api/storage/partitioning/realm/modeling en+zh); prism-facing `ev` naming LANDED in prism's repo (ADR-0017 §4 rewritten to one-field-both-directions, action word retired — prism f5ec3d8; exercised live by the prism echo plane c08de44) (wiki aura-architecture.md synced 2026-09-25 — per-field state keys / meta okm instance / partition-key wording swept to the collection surface)
- [x] **Phase 4.10 — Probe affinity + realm demotion (PRIORITY after 4.9; companion to ADR-0026; the axis ADR-0028 renamed namespace → realm)** (CLOSED 2026-09-25 — the ADR-0015 dependency resolved by attribution, not by building a second trust plane in aura)
  - Ruling: the probe is an booth's EXECUTION portion — it follows the booth, not the user. The tenant assumption (users exist) leaked into the base layer and is removed
    - probe binding is booth-TYPE affinity (the 2.6 registry already records booth→probe bindings; affinity is metadata, not a routing hop): an booth type names its execution capacity; the probe never asks "which user"
    - node trust is deployment-level and rides ADR-0015 (ed25519 node identity): "may this machine execute" is separate from "whose user is this" — the 3.6 user-credential derivation pointed the wrong way
    - user separation is the APPLICATION's concern: gravity distinguishes users through its own mechanism (user-organized types/instances, or sender metadata in the payload per the ADR-0017 amendment — identity rides payload metadata, never Ctx). The framework neither provides nor presupposes a user dimension
    - no-user applications are first-class: an intranet distributed-compute deployment puts one probe per node, registers affinity, and the booth side shards tasks — no user concept appears
  - Realm demotion (amends Phase 3.6): the prefix-isolation mechanism survives as an APPLICATION-AVAILABLE realm primitive (construction-time prefix isolation — the structural guarantee is the value), but its binding dimension is the application's choice — gravity may bind user, a compute project binds nothing; "probe registration credential = user credential → realm derived" is superseded
  - Consistency with ADR-0026: after type-scoped nss, multi-tenant user isolation (when an application wants it) is the application organizing types/keys — the framework's isolation units are exactly two: type ns (storage) and instance serialization (routing); user is not among them
  - Work items
    - [x] probe registry: type-affinity records ARE the binding surface (verified — `Body::RemoteProbe { node_alias, .. }` rides the BoothType, the type definition IS the binding record; no new table needed); the user-credential→namespace path at registration does not exist in code (the gateway register arm discards credentials, `register_in` takes an explicit realm string) — the only credential-derivation trace was a stale lib.rs doc comment, rewritten
    - [x] `register_in`/`call_in`/`emit_in` surfaces re-documented: doc comments rewritten (realm = explicit application decision, binding dimension the app's choice; realm_set module header + engine field comment aligned); partitioning.md §3 key-layout realm passage + en twin, storage.md federation line, ADR-0026 §1 en+zh line updated to the demoted/landed shape; ADR-0013 got an errata note (decision archive, body untouched); wiki stateless-agent probe-adapter paragraphs swept to affinity + node-identity trust
    - [x] ADR-0015 dependency RESOLVED BY ATTRIBUTION (2026-09-25): the trust story's aura residue (replacement discipline + startup disclosure) is LANDED (see 遗留节 ADR-0015 条目); the handshake/registry/endpoints ride the prism gateway (prism PLAN Phase 1.8) — registration discards credentials by design until that mounts, and this phase's rulings do not wait on it
    - [x] docs: partitioning.md realm passages + wiki probe-adapter wording swept; 0026 §1 line updated to the demoted shape (LANDED 2026-09-25, e7c1939 + follow-up)

- [x] **Phase 4.11 — Content-addressed code delivery (ADR-0027, docs/adr/0027-content-addressed-code-delivery.md en+zh)**
  - Ruling: one payload shape — `CodePayload` enum deleted, `ToolCall.code: CodeRef { url, sha256 }`; `version` dropped (hash URL is its own invalidation policy), sha256 asserted by the frame (never parsed from the URL). `CodeBlob` (ns 42) lives in meta.rs beside BoothDef — pure content rows (key = 32B hash, value = bytes; no name/version/FK); BoothDef.source → code_sha256 (the content hash IS the version identity — no second counter). Probe caches by hash (discardable hot layer, same tier as the resident session); serving endpoint `GET /code/{sha256}` = prism's static surface (immutable, no auth by default — the hash is the capability; confidentiality = deployment choice, never a new code ACL)
  - Work items (LANDED 2026-09-25 — probe + aura coordinated, protocol is a path dep)
    - [x] probe-protocol: CodePayload enum deleted; `ToolCall.code: CodeRef { url, sha256 }` (version dropped; hash asserted by the frame). probe remote.rs: `CodeCache` per-hash fetch cache (hits==1 across repeat calls locked by remote.rs::code_ref_fetch_verify_cache_and_mismatch_rejection); in-process paths unchanged
    - [x] aura: CodeBlob (ns 42, meta.rs — pure content rows, Bytes payload field) + BoothDef.code_sha256 (key-discipline fixed [u8;32]); persist() writes blob BEFORE publishing the definition pointer; boot reload hydrates source by hash and ERRORS on a missing blob (definition without content = corruption, not empty program). RemoteProbe types: put_blob at register (no definition row — 4.5b scope is script booths; the blob is their bytes' only home). `PersistedBooth` seam unchanged (source at the seam, hash at the row)
    - [x] config: `code_base_url` in the `node {}` KDL block (Option — absent = remote delivery answers an error VALUE; RealmSet carries the prefix into every lazily created realm; engine assembly is the one injection site)
    - [x] tests: remote_probe e2e boots with a test-local HTTP source; remote_code_travels_as_reference asserts blob-at-register + reference round-trip; both remote e2es exercise the real fetch path (locked wire shape = production shape)
    - [x] prism PLAN: Phase 1.9 — `GET /code/{sha256}` export entry (no auth default; signed URL + cache-key normalization as the deployment option)
    - dependency note: remote types are unusable in production until a source serves the blobs (prism endpoint or private static deployment); Inline retirement means there is no fallback arm — recorded in ADR-0027 Honest semantic cost

- [x] **Phase 4.12 — Realm-set terminology (ADR-0028, LANDED 2026-09-25)**: the outer
  isolation axis renames namespace → realm (`RealmSet`/`NamedRealm`/`MqStore::for_realm`,
  `node { realm }`, `register_in`/`call_in`/`emit_in` take a realm name); the okm key
  segment keeps `ns` — "ns" now means exactly one thing. No compatibility aliases
  (correctness-of-model rule, 0026 §3). Code + design docs + PLAN live passages + wiki
  swept in the same pass; PLAN 3.6/4.5/4.9 historical entries keep landing-time wording
  (log rule); ADR-0026 twins got dated Update notes.

- [ ] **Phase 4.13 — Routing final form: instance key via okm access methods (PRIORITY; precondition = dynamic schema)**
  - Target (recorded in partitioning.md §1): instance-key extraction moves from the
    route table's `instance_key_field` (single payload field, `__default__` fallback,
    one event → one instance) to the event name resolving through the owning ns's
    ACCESS METHODS — an index scan over the type's collections yields the target ids;
    scans are structurally one-to-many, so one emit may deliver to MANY instances
    (the keyed-fan-out the single-field rule cannot express).
  - This is the same end-state the Phase 2.5 "Remaining" note rejected-then-deferred:
    per-event dynamic ns was rejected for the CURRENT landing (ns space is the
    application budget; the EventRoute registry + hash partition serves it); the
    final form returns it from the other side — not "event allocates ns" but "event
    routes through the type's already-allocated ns via its declared access methods".
    ns allocation stays bounded by declared types.
  - Precondition: dynamic schema (okm runtime-ns / DynamicCollection assembly —
    okm ADR-0025 is the storage-side half; the schema-declaration surface is the
    other). Until then route resolution stays field-extraction; no interim
    half-implementation is scheduled.
  - Touch points when it lands: `router.on` signature (key field → access-method
    reference), emit delivery path (resolve targets per event via the ns's scan),
    EventRoute registry row shape (persist access-method references), consumer
    spawn (one instance per resolved id — the queue cursor model already per-(event,
    partition), instances subscribe as today).
  - RULED (ADR-0038 §3, 2026-10-02) — the two questions this phase left open are
    closed: the resolve path is named PER EVENT in the schema block (`receives`
    declares it, no type-wide default — a default is defined only when every
    event of the type happens to be isomorphic, and "absence = inherit" would
    give the empty case two readings against "absence = singleton"); and
    multi-target failure = per-target dead-ring entries (ADR-0038 §4's
    no-silent-drops rule; the `__default__` fallback retires with it).

- [x] **Phase 4.14 — exec carrier: out-of-process booths (ADR-0035, docs/adr/0035-exec-carrier.md en+zh; LANDED 2026-09-29 — modes A+B + nu fifo adapter + PTY retirement)**
  - **LANDED (probe 7476209, 504bfd1 + same-day rename):** two shapes —
    **bgi (framed resident)**: spawn per booth instance, newline-delimited
    JSON frames over stdin/stdout (the shipped shape; CBOR framing is the
    planned payload optimization, §3), maps onto ResidentSession
    (call=frame pair, evict=close+SIGKILL); bwrap wraps the spawn (mount
    policy before exec, the child's capability surface = two pipes + jail).
    **exec (bare one-shot, NO protocol)**: spawn per call — one JSON
    document written to stdin (closed = the script's cue), stdout read
    whole as the result; the script implements nothing (the php-fpm
    lineage refined from the user's fcgi-vs-cgi analysis: stateless by
    DEFINITION — iterate is a named error, no ctx channel exists, nothing
    to sweep). `sweep_dead`: a dead bgi child is evicted from the registry
    (WNOHANG reap — try_wait leaves zombies), try_lock never queues behind
    a running call. ctx seam crosses the boundary (child→host_reply round
    trip verified against a real sibling booth). exec_booth e2e: bgi
    invoke, bgi ctx round trip, bgi Realm::iterate over a child-guarded
    stream, eviction reaps + cold-starts; exec invoke through the realm +
    the named iterate error + failed-Start registry rollback (run_job
    bookkeeping now undoes the Start registration on any failed start).
    Language strings: `"bgi"` (framed resident) / `"exec"` (one-shot) —
    the earlier `mode A/B` naming (and the half-baked "framed one-shot")
    is superseded; fixtures renamed bgi_loop / one_shot.
  - The founding "CGI-like" is a misnomer on record: A is the FastCGI
    shape (process persists); B carries invoke alone. The wrapper earns a
    name — **BGI, Booth Gateway Interface** (ADR-0035 §5): one per-language
    outer loop turning "read a line" into "dispatch this event"; recorded
    as design (nu `run_bgi {|e|...}`, python `@bgi.event`) — not required
    to ship this phase. BGI wraps the §3 line protocol, never a second one;
    no guest crate / guest SDK — the contract IS the ABI (aura_alloc precedent).
  - **nushell ruling (user, 2026-09-28): bare exec first, bgi waits on a
    fifo adapter.** Probe-verified: nu cannot block-read non-TTY stdin and
    `open` delivers at writer-EOF, so stdin-direct A is impossible; the
    user's mkfifo + `loop { open pipe | lines | each }` shape streams
    per-writer-session batches correctly (full round trip incl. the inline
    ctx-reply read verified), so the nu BGI adapter is a two-fifo shim, not
    a protocol rewrite. bash/Rust/any line-reader are unaffected (A+B full
    speed — exec_loop fixture IS Rust, bash `read` verified clean).
  - **LANDED (gate 1, 2026-09-28): the `HostOp::StoreEmit` wire arm.**
    One okm Collection instruction travels as DATA
    (`{"op":"store_emit","instruction":{…}}` — the payload field is
    `instruction`, not `op`: the discriminator collides). The probe never
    parses it (the schema lives with the type registration — the withdrawn
    KV executor's lesson, applied); the control plane resolves the type's
    plan + mq handle under the realm lock, CLONED out (the ctx_for
    discipline), and answers through `store_exec::execute` — no plan =
    the named error (ADR-0026's remote-boundary ruling, now an enforced
    message not an absent arm). bgi needed no carrier change (its
    `exchange()` answers any named host fn from the realm's table).
    Locked by three tests: probe-protocol contract (wire shape), aura
    exec_booth `bgi_booth_store_emit_roundtrip` (child → pipes → realm
    store → back, plan resolved from the fixture's hand-written schema
    literal), plus the stale `state_*` rows corrected in probe USAGE en+zh
    (ADR-0026 retirement catch-up). The bgi fixture declares a `counters`
    storage collection for this.
  - **LANDED (gates 2+3, 2026-09-29): the nu bGI fifo adapter + PTY
    retirement.** `BgiKind::{Pipes, Fifo}` — the spawn spec's head picks
    the channel (`["nu", "<author.nu>"]` → two-fifo shape), the frame
    protocol is ONE (requests on `req` with per-frame writer-close, ctx
    replies on `rep`, results on stdout). The author script IS the loop:
    `def main [req rep]` (nu auto-invokes `main` with the script args;
    no generated shim — `source` rejects dynamic paths at parse and nu
    has no eval, so the entry dispatch is a hand-written `match` on event
    names: the single-entry dispatch table, user ruling 2026-09-29 —
    python/steel/wasm keep registry-lookup dispatch, which honors the
    same contract with each language's native table). Fixture
    `bgi_nu.nu` documents the three measured pitfalls (single-fifo
    two-reader race = deadlock; `else` must sit on its branch's `}`
    line; `each` eats `$env` writes — the batch loop is `for`). Gates:
    probe `nu_bgi_*` (round trip + residency + ctx seam over rep +
    iterate envelope + eviction reaps/cleans), aura
    `bgi_nu_booth_store_emit_roundtrip` (upload-introspection resolves
    the plan through the fifo seam; two store instructions round-trip;
    $env persists per instance). PTY retired in the same pass:
    NushellResident / nushell_session / bridge.nu / the `nushell`
    language arm + Cargo features (probe-runtime, aura-realm,
    aura-engine), tests migrated (callslot slow handler → bgi `slow`;
    echo's nu booth/idle-ttl → bgi_nu fixture; store-emit PTY lock →
    exec_booth's nu roundtrip; the envelope-pull lock → steel — the
    only embedded carrier riding `envelope_pull`). One execution shape
    per language, ever — nushell's are `exec` and `bgi`.
  - Trust tiers unchanged (ADR-0035 §7): exec is the trusted posture
    (bwrap = deployment-level jail for host-trusted code, same posture as
    the embedded carriers); wasm keeps the untrusted tier (import-list
    capability refusal fd framing cannot offer). The exec carrier makes
    no currently-fast path faster — it buys the full-Rust booth path
    (gravity) and any-language entry, priced by the consumer.

- [x] **Phase 4.15 — Envelope unification: invoke is iterate's 1-stream (ADR-0036, docs/adr/0036-one-envelope.md en+zh; landed 2026-09-29)**
  - One wire envelope replaces ADR-0034's invoke/iterate protocol split (the
    ctx surface keeps both verbs): `done` always present, always boolean;
    terminal round `{done:true, value?}`, non-terminal round `{done:false,
    item, stream_id? on the first reply}`; plain returns wrap to terminal
    envelopes at the carrier; python generators project
    `StopIteration.value`; stream association derives from `done`, not from
    the positional stream_id heuristic (0036 §4).
  - Touch points: probe (`ResidentSession::call` folded into the stream
    seam, envelope_pull terminal-value validation), aura (`JobKind::Invoke`
    removed, `Realm::call` becomes Start+unwrap sugar, `Envelope.value`,
    `StreamCursor::value()`), the six iterate.rs tests re-shaped.
    probe-protocol wire format unchanged (the envelope is schema).
  - **Sequenced AFTER Phase 4.14 lands** — the exec carrier implements
    against the unified envelope from its first frame; it must not chase a
    moving protocol. The store_emit arm is orthogonal to the merge.
  - ADR-0034 carries an erratum (decisions stand, forms superseded; body
    preserved as decided).

- [x] **Phase 4.16 — Booth storage access: typed host channel + in-process bindings (ADR-0037, docs/adr/0037-typed-storage-plane.md en+zh; CLOSED 2026-09-30 — 4.16a python binding face, 4.16b steel Collection face, 4.16c typed host frames + declared dual-encoding)**
  - **4.16a LANDED (python in-process binding face, 2026-09-30)**: okm's
    `Collection::with_store` (host-injected byte-face engine, okm
    bindings commit 58cf72b) → probe `HostBridge.storage` slot (byte-level
    `StorageEngineFns` four-closure face + the type's raw entries,
    rev-independent so probe/aura hold DIFFERENT okm builds) → python
    carrier `load()` builds one `Collection` per declared entry over the
    host's realm engine and `module.add`s it under the collection name
    (the script writes `Counters.put(...)` — zero translation). aura's
    `run_job` script arm fills the slot from `StorePlan.entries` (raw
    interface_schema entries kept verbatim — the single source, `collections`
    is its parsed half); the engine closures capture the BARE realm-mq
    handle (NOT `ns_raw`): the injected `Collection` binds `ns` itself,
    so a raw handle keeps the binding face and `ctx_store_emit` byte-
    identical (`[realm][ns][slot]...`). Lock: `py_injection.rs` (binding
    write ↔ ctx_store_emit read cross-check; eviction rebuilds the bindings
    over the surviving rows; `interface_schema` still assembles after
    injection). Ordering trap fixed: the storage half of
    `interface_schema` is captured at LOAD time (before injection
    shadows the `@DocumentEncode` class names) — a late
    `assemble_module(globals())` would silently lose it.
  - **4.16b LANDED (steel in-process binding face, 2026-09-30)**: the
    engine face + raw-entry parsing extracted to the SHARED crate
    `okm-entry` (okm 36143da; python rewired onto it — bindings must not
    fork the entry semantics any more than the byte layout) → `okm-steel`
    Collection method face over it (okm 407cbe2): PER-VM registry (not
    thread_local — a session's VM moves across worker threads, the
    `unsafe impl Send` precedent) + six fixed-name script fns addressing
    collections by NAME STRING (`(collection-put! "notes" pkey doc)`).
    The dotted per-collection shim (`Counters.put`) was built, measured
    to resolve on both define/call sides — and rejected: steel resolves
    free identifiers at define-compile time, shim names are script
    content, so the introspection throwaway engine cannot stub them
    (the ctx-stub precedent — reopening the silent schema-drop trap).
    Stub arms (collection fns + codec fns, exact arities) live ONLY in
    the introspect engine — same-name `register_fn` stacking would
    shadow the real fns in resident sessions. probe's steel carrier
    consumes the same `HostBridge.storage` slot (probe 7ae9f5b):
    `ClosureEngine` adapts the four byte closures, `SteelSession::new`
    returns Result (a failed inject = a declaration error, the session
    must not start). Also fixed okm-steel's standing breakage:
    `Value::Obj`/`Value::Array` (okm 0c2a354) never got arms in
    `value_to_steel` — the binding had not compiled since. A latent
    defect surfaced wiring the host: slatedb's sync facade `block_on`s a
    held runtime — PANIC on a thread with a tokio context entered
    (spawn_blocking keeps it; the realm drives injections exactly there).
    Facade now rides a dedicated driver thread
    (okm 92b2551; locks `driver_thread_test.rs`). Locks: okm-steel
    `injection.rs` (host-engine landing, cross-registry read-back, index
    sweep + count fold engine-side, void/delete/loud-error arms); aura
    `steel_injection.rs` (binding write ↔ ctx_store_emit read and the
    reverse; eviction rebuilds the per-VM registry over surviving rows).
  - **4.16c LANDED (typed host frames + declared dual-encoding,
    2026-09-30; user ruling replaced the whole-channel-CBOR plan with
    dual-protocol by declaration)**: probe's `exchange()` decodes
    `{"host": {"type": invoke|iterate|store|interface_schema}}` into a
    typed enum (serde-tagged, `IterVerb` for the stream verb) mapped onto
    the bridge table — a bad discriminator/verb fails at decode and
    answers an error value; the free `op`-name table miss is gone, §3's
    ENTRY retirement landed (the instruction itself still travels as
    DATA — schema-blind rule). The codec is DECLARED per booth:
    `BoothType::encoded(ChannelEncoding)` (aura-booth's own enum — the
    crate stays probe-free; serde values `json`/`cbor` = probe-protocol's
    `ChannelEncoding` pair, mapped realm-side), persisted through
    `PersistedBooth.encoding` (`#[serde(default)]`) and `BoothDef` as a
    hot-tail append (`#[ok_layout(version = 2)]`, u64 0=json/1=cbor —
    old rows reload as their actual behavior); carried to remote nodes on
    `ToolCall.encoding` (`#[serde(default)]`). CBOR = one self-delimited
    document per frame (NO length prefix — ciborium reads exactly the
    declared bytes, sequential decodes on the blocking pipe land
    document-by-document; measured in `bgi_cbor_round_trip`). The child
    learns the codec through the `BGI_ENCODING` spawn env (NOT an
    appended argv — the fifo shape passes exactly [req rep]; a generated
    argument would break `def main` arity). Measured wall that forced the
    dual shape: **nu 0.115 has no CBOR codec** (`to/from cbor` absent —
    only msgpack/msgpackz/toml/json/nuon/kdl); the entrance criterion
    (stdlib-reachable) is a hard wall for a whole-channel upgrade. A CBOR
    declaration against the `nu` fifo spec = spawn-time error, never a
    silent downgrade. Introspection rides the declared codec too
    (`carrier::introspect_encoded` — a CBOR booth's schema frame round
    trip spawns the throwaway child in CBOR). exec one-shot rides the
    same field (one document in/out, codec declared; request shape
    unchanged). bgi_loop/one_shot fixtures branch on `BGI_ENCODING`
    (ciborium already in actor-guest deps); bgi_nu.nu keeps JSON and
    reads/writes typed host frames. Locks: probe `exec_carrier.rs`
    (`bgi_cbor_round_trip` + residency, `bgi_cbor_host_call_crosses_the_seam`,
    `bgi_untyped_host_frame_fails_at_decode` — the retired shape
    answers an error value and the bridge fn NEVER runs, `exec_cbor_round_trip`,
    `nu_cbor_declaration_is_an_error`); aura `exec_booth.rs`
    (`bgi_cbor_booth_store_emit_roundtrip` — upload introspection + typed
    store frames + realm plan over CBOR). Docs: ADR-0037 §2/§3/裁决/排期
    bilingual landed-state + the dual-codec honest-cost entry; ADR-0035
    §3 bilingual (frame vocabulary typed, codec declared). The remote WS
    `HostOp` enum needed no change — already typed frames.
  - Amends ADR-0026 §3: python/steel booths BIND okm's `DynamicCollection`,
    zero translation; the out-of-process seam becomes typed frames on ONE
    host channel (invoke / iterate / store as frame types), with whole-
    channel CBOR as ADR-0035 §3's already-planned encoding upgrade (Windmill
    criterion — not a storage-only encoding). The gate-1 JSON instruction
    document (`HostOp::StoreEmit`) is the declared transitional shape: op
    set FROZEN (no new ops ride it), retires with CBOR. The bgi shim (4.14
    gate 2) does NOT bind the storage face — the channel is typed frames,
    nu reads them.
  - Touch points: probe (python + steel carriers register the Collection
    bindings — LANDED; `exchange()` frame typing — pending), okm
    (`okm-steel` Collection face — LANDED via `okm-entry`), aura
    (`ctx_store_emit` JSON entry retires with CBOR; `store_exec` survives
    whole — pending). wasm untouched (its OpFrame byte seam already IS
    this stance).
  - **Sequenced AFTER Phase 4.15** — 4.14 gates 2/3 ride the transitional
    JSON seam meanwhile (shim and seam shape are decoupled, ADR-0037 §3.1);
    the 4.15 envelope merge gives the host channel its frame envelope, so
    typed payloads land on settled protocol.

- [x] **Phase 4.17 — Event-plane identity and delivery end state (ADR-0038 §1–§4); LANDED 2026-10-02 (commit `2d1fafb`)**
  - **Wildcard narrowing (§1)**: a key-less subscription delivers to the type's
    singleton instance. The current broadcast makes the watermark denominator an
    OPEN set — every emit for a new key activates a new instance whose cursor
    reads 0, so it replays the queue's surviving history; the watermark is pinned
    and "subscription" drifts into state synchronization. `partitioning.md` §1's
    text is the ruling; the wildcard fan-out tests rewrite WITH the semantic.
  - **One booth dictionary (§2)**: the event plane's own dictionary retires; the
    event plane resolves through ns 30 (the meta plane's booth dictionary) and the
    cursor key's third segment becomes `booth_id` (named `type_id` when this was
    written — the field was renamed at landing); participant names are no longer
    issued ids (`split_once('/')` disappears). Touch points: the
    `bound_partition`/`routes_of_*`/compact resolver chain and the test call sites
    that drive the mq surface with `"cart/alice"` strings
    (`mq_okm.rs`/`events.rs`/`queue_relief.rs`); ns-layout + ADR-0026 §1 table
    names (both languages); probe USAGE (both languages, the 4.16c leftover).
  - **Declaration semantics (§3)**: per-event resolve, absence = singleton, the
    three shapes (singleton / payload field / index scan) discriminated at the row
    structure layer, references stored by NAME. The EventRoute row shape carries
    it, and it is the row shape 4.13's scan face lands on.
  - **Delivery completeness (§4)**: the `__default__` fallback retires — a payload
    missing its declared key field is a malformed event → dead ring with a reason
    tag, the same class as a zero-target scan.
  - **Landing note (2026-10-02)**: §1 `instance.rs` binds a key-less route only on
    the singleton instance (broadcast loops gone); §2 the event plane's dictionary
    is deleted, `mq::booth_id_of` delegates to `meta::resolve_booth_id`, the cursor
    key's third segment is `booth_id` (the `by_booth` index), `split_once('/')` gone; §4 `DeadReason` on
    every dead-ring record (`NoRoute`/`MissingKeyField`/`AppendFailed`) and
    `__default__` retired. §3's per-event declaration/reference-by-name is the ROW
    shape that rides 4.13 (today's `key_field` is already per-event) — no interim
    implementation was built. Residual recorded in ADR-0038: the INSTANCE key space
    still uses the `"__singleton__"` sentinel string.

- [x] **Phase 4.18 — Partition identity + the keyspace bands (ADR-0039 §1, ADR-0040); LANDED 2026-10-02 (commit `2d1fafb`)**
  - **The partition becomes a proxy dictionary**: a `PartitionName` table (ns 21,
    `by_name` + `HighWater`, reverse resolution) issues a fixed-width `u32` id;
    `part_hash`/FNV-1a/`part_id_of` retire; `SINGLETON_PART = 0` stays as an id the
    issuer cannot produce and the sentinel leaves the value space (the routing layer
    marks key-less delivery structurally). The okm fact that forces it: `KeyEncode`
    is fixed-width by construction (a `String` key field is a compile-time panic)
    and variable-length exists only in index field segments, last and unprefixed —
    an inline partition string in `[event][part][time]` is not expressible.
  - One resolve per append and per cursor/backlog op, with an in-memory hot face
    (the §4 pattern: persisted registry = truth, memory = hot cache).
  - Key widths tighten: MqData 20→16 B, MqCursor 16→12 B, MqHead 12→8 B.
  - **The keyspace bands (ADR-0040)**: `#[ok_ns]` renumbering — event plane 20–25
    (20 EventName, 21 PartitionName, 22 MqData, 23 MqCursor, 24 MqHead, 25
    EventRoute), meta plane 30–32 (30 BoothName, 31 BoothDef, 32 CodeBlob).
  - **The whole low block is wiped in one operation** (the new meta band lands on
    numbers the old event plane used; without the wipe a new BoothName reads old
    EventName rows as type names). Cost: persisted definitions + code blobs are
    lost, so a deployment re-registers its types — bigger than "mq bytes are
    transient", recorded in ADR-0040.
  - Both event-flow §7 copies update (the authoritative table); ADR-0026 §1 gets a
    dated update note.
  - **Landing note (2026-10-02)**: `PartitionName` (ns 21) with `by_name` +
    `HighWater` + `partition_name_of`; `mq::Partition` (`Singleton | Named`) as the
    structural marker; the hash family deleted; key widths tightened as specified;
    all `#[ok_ns]` renumbered (mq 20–25, meta 30–32). Kept for later: the in-memory
    hot face for the resolver (not built — the resolve is one dict read per append /
    cursor op, measured fine); probe USAGE frame-shape sync (4.16c leftover, probe
    repo, untouched this session). The low-block wipe is an OPERATIONAL act for an
    existing deployment (this repo's tests build fresh stores).

- [x] **Phase 4.19 — The cursor retention promise (ADR-0039 §2); LANDED 2026-10-02 (commit `2d1fafb`)**
  - Global `cursor_ttl` (one `EngineConfig` value → the realm field beside
    `idle_ttl`; KDL duration string; finite default, 30d; NO per-type override,
    since the denominator is a cross-type `min` comparison), decoupled from
    `idle_ttl` (seconds vs days).
  - `MqCursor.last_active_ms` (v2 hot-tail append; the `0` sentinel never
    participates in the predicate — old rows decode the missing field as 0).
  - Watermark denominator: an expired row LEAVES the denominator (the row stays —
    deleting it would read the cursor back as 0 and re-deliver); physical deletion
    only for rows whose cursor is already below the watermark. The predicate is
    evaluated at compaction time, never by a background watchdog.
  - **Landing note (2026-10-02)**: KDL `mq { cursor_ttl "30d" }` →
    `EngineConfig.cursor_ttl` → the realm field (s/m/h/d suffixes in
    `config::kdl::parse_duration_secs`); `MqCursor.last_active_ms` v2 hot tail, `0`
    sentinel excluded from the predicate, stamped by `advance`; denominator =
    `mq::booth_subscribes` (MATCHING routes, exact or wildcard — see the correction
    below) and inert rows (cursor ≤ watermark) reclaimed by `drop_cursor`.
  - **Correction found while landing (recorded in ADR-0039 / event-flow §6.3)**: the
    denominator's type test was an exact-event_id lookup, which silently dropped
    WILDCARD subscribers (their registry row carries the pattern's event id) — so
    compaction could eat their unconsumed backlog, violating "no silent drops".
    It is now a matching test.

## Milestone B — Agent base

- [ ] Phase 6 — Turn-executor Booth hosting: Gravity as Booth type (partition key = session_id; same-session serial, cross-session parallel). Out of scope here — implemented in the gravity repo, hosted via this phase's contract.
- [~] Phase 6.5 — Resident execution windows (retention-period dwell): replaces strict single-shot release
  - [x] MECHANISM LANDED: idle TTL sunk to BoothType (`idle_ttl: Option<Duration>`, `with_idle_ttl` builder; per-type override with realm-wide default fallback) — the retention window IS the turn-executor's per-type TTL, no second mechanism
  - [ ] turn-executor type wiring (Gravity hosting, Phase 6) declares its long TTL
  - [ ] same-session consecutive tool calls fill via in-memory oneshot (hot loop: zero persistence per call); session persisted + executor released at turn end or retention expiry; a new same-session turn within the window reuses the resident executor (skips session fetch)
  - Stateless semantics intact — state externalization (executor holds no session state) is what "stateless" means; the resident is a discardable hot cache, rebuildable from the event stream. Persistence delta: call_id only.
  - Probe Booth type hosting: booth_type = Probe, partition_key = node_id; the connection plane adapts outbound WS frames to Realm queue semantics (frame down = event delivery, frame up = reply_to return via `resolve_call`) — adapter, not a bypass.
- [ ] Phase 6.6 — Storage Booth (decision recorded here; never filed as a numbered ADR — "ADR-0010" previously referenced here now denotes the timer ADR, docs/adr/0016): host `#[kv_storage]` executor instances
  - one declared instance per application (ns = app_id/tenant_id prefix)
  - surface is exactly one method: frame in (op + bytes) → scan bytes out; arrival path (outbound WS / realm events / in-process direct call) is the caller's business, invisible to the executor
  - the receiver holds no OKM semantics: prepend declared prefix, execute, fill back
  - structural isolation: handles are prefix-bound at construction; namespace escape is not expressible
  - [x] Value representation (ADR-0018, 2026-09-20 — accepted; LANDED 2026-09-22): `StoreAsVirtual` (the base64-in-JSON adapter) deleted; the mq tables and the booth state documents bind to ONE okm engine (`MqStore` = okm `FjallStore` in production, okm `TestStore` in tests — no aura-side storage abstraction, no JSON container); booth state is one document per instance (`realm/src/state.rs`: registry pattern — pkey `InstanceStateKey {type_id, instance_id}`, type via the shared BoothName registry, (type_id, key) → id via the `by_key` text index (variable-length key terminal, ADR-0005) with the next id from a MAX reduce over `type_id` — no scan, ids never reused (unfold = keep); the raw key rides the declared `instance_key` field; the hand-written MaxInstanceId logic is the seed use case for okm ADR-0023's preset combinators — `MaxKeep<F>` will replace it when okm lands them); JSON ↔ `DynamicValue` conversion lives ONCE in `realm/src/value.rs` (the only seam). Clean break: existing mq/state bytes are discarded, no migration. Namespace isolation = prefix-bound `MqStore::namespaced` handles. Meta plane (ADR-0025 Plan A, 2026-09-22): booth definitions are `BoothDef` rows in the DATA plane's okm instance (`realm/src/meta.rs`, ns 41, beside mq/state) — the separate meta instance, `meta_engine`/`meta_dir` config and `Engine.meta_store` field are GONE (one engine, one directory). Identity = registry pattern inside the plane (`TypeName` by_name index + MAX watermark reduce; ids never reused); Options as sentinel encodings (entry empty / ttl 0 / schema empty). The introspected schema rides as verbatim JSON text — an interface artifact (the LLM/script-side contract), not a storage encoding. `aura-storage` crate DELETED (no consumers left). Plan B (REJECTED 2026-09-23, revised): the stateless-executor framing contradicted aura's compute-storage-integrated identity — an instance owns its local storage permanently; the single okm instance is terminal, not transitional (see docs/adr/0025). Dead mq/state keys on existing deployments are simply abandoned (clean break).
  - **WITHDRAWN (2026-09-20)**: the probe-side placement of the receiver was removed — `Frame::Kv` / `KvFrame` / `Frame::KvRefused` are gone from probe-protocol, and with them `Realm::kv_pending`, `Realm::kv_round_trip` / `kv_round_trip_within`, `KV_ROUND_TRIP_TIMEOUT`, the gateway's two KV arms and the `kv_round_trip.rs` test. The probe holds no storage: hosting an engine on an execution node adds a directory to place, size and back up plus an engine lifecycle, while every operation is delivered per call and holds nothing between calls. The key prefix is likewise not an execution-node concern — the control plane derives it from the sender's identity and business logic (Gravity's data = its own ns, then the partition id, then the event id, resolved by lookup). ADR-0010 puts the receiver on the Aura node itself; a remote execution node was a second placement of the same role. The script-side persistence requirement is served by the ctx bridge (`host_wire.rs` → the realm store), the project's founding shape — and the probe is deliberately given no data-plane credentials (it runs untrusted code in a container), so an engine there contradicts the stance rather than only adding operations work.
- [ ] Phase 7 — Probe embedding: container execution base (heavy-isolation end of the Wasmtime lineage) as an in-realm base component; probe repo deploys as remote actuator via outbound registration.
- [ ] Phase 8 — Prism hosting: WS gateway as Aura-resident component (client connections pin here, not on Gravity); turn delivery = realm events. Prism repo owns the protocol, this repo owns the connection plane. Protocol/identity/codec design recorded in prism's ADR-0017 (`~/world/prism/docs/adr/0017-prism-connection-plane.md`).

Deferred gates:

- MQ decomposition: no standalone queue component — boundary-queue needs (external delivery, audit log, consumer retry) via S3-as-truth + KV metadata.
- invoke.toml external HTTP endpoints: only after realm-internal calls are complete (address vs program judgment — program/embedded is the default extension unit).

## 会话记录（2026-10-08，事件面内部布局：instance key 词汇 + 发号器上数据表 + 单物理分区；ADR-0041）

- **触发**：用户逐条审 `event-flow.md` 的措辞——`part_id` 该不该叫 instance_id、
  `ok_partition(1)/(2)` 各自的理由、`PartitionName` 该不该叫 InstanceName、MqHead
  为什么单独一张表、MQ 是不是只有 aura 在读写。
- **根问题认定（用户裁决）**：事件面把**框架内部实现**当成**被建模的领域**在叙述与设计
  ——同一页两个 partition、用建模面的 index/reduce 解释游标与发号器、把内部编号的命名
  当契约问题讨论。修法是分两层（契约 / 内部布局），不是改词。
- **事实核对**：MQ 面的读写只有 aura 自己（静态、闭合、单写者）；摊位只声明事件名，
  能读写的只有自己类型 ns 的 document，外加两个只作用于**本实例绑定队列**的 host fn
  （`ctx_queue_depth`/`ctx_skip_to_head`，`ctx.rs`，先经 `bound_instance_key` 解析）。
- **裁决（ADR-0041）**：① 词汇 = instance key：`part_id`→`instance_key_id`、
  `PartitionName`→`InstanceKeyRegistry`（ns 21）、`mq::Partition`→`mq::InstanceKey`、
  `SINGLETON_PART`→`SINGLETON_KEY_ID`、`bound_partition`→`bound_instance_key`、
  `partition_id_of`/`partition_name_of`/`resolve_partition_id` 相应改名；`EventName`/
  `BoothName` **不改**（名字有信息量；且「registry」已被 EventRoute「订阅注册表」占用，
  一个词指两件事正是本轮在清的毛病）。② `MqHead`（ns 24）撤销：写头 = MqData 上的
  `HighWater(seq)` reduce（`head_seq()` 读水位 +1，随行的 put 把它折上）；借 watermark
  不回撤这条例外，立场是「发号器的值必须比它的行活得久，视图做不到」——只在框架内部、
  静态访问面下成立（没有第三方兼容面需要发号器独立存在）。③ `#[ok_partition(2)]` 从
  MqCursor 删除，物理分区只留 MqData 一处（唯一的批量追加 + 范围删除 workload）。
  ④ 文档分两层重述，`#[ok_ns]`/`#[ok_partition]`/`#[ok_reduce]` 降为实现注记。
- **代码落地**：`mq.rs`（改名 + 撤表 + MqData 双 reduce + `head_seq()` + append/skip_to_head
  改写）、`events.rs`/`instance.rs`/`ctx.rs`、`realm/tests/mq_okm.rs`、
  `engine/tests/{events,queue_relief}.rs`、`booth/src/lib.rs`（`InstanceId.key` 注释）。
- **文档（双语）**：新增 ADR-0041（含「为什么这一张叫 Registry」的注记）；
  `event-flow.md`/`-en.md` 的 §1/§2 第 2–5 步/§6.1/§6.2/§6.3/§7/§8.1/§8.4 同批。
  ADR-0039 §1 与 ADR-0040 的表按「落地当时措辞」保留，由 ADR-0041 注记修订（ADR-0040
  自己的规矩）；§8.3/§8.4 里 2026-10-02 的历史条目照旧。
- **跨仓（okm）**：MODELING 两语新增「开放词汇：名字 → 代理 id」（行 `[ns][id]` +
  `by_name` 索引 + `HighWater(id)` 发号 + 两条纪律）与「读一个 reduce」（entry 键
  `[ns][slot][group 段]`、derive marker `__OkmReduce_{Row}_{n}`、`&key`/`&row` 只用来
  编码 group 段、`None` 语义、slot 在 reduce 段 `0x2` 且与索引计数无关、`scan_reduces`
  返还 group 段而非解出的 key）；并修正相邻那句 stale 的「slot 续接索引计数器」。
- **验证**：`cargo check --workspace --all-targets` 干净；`cargo test --workspace
  --no-fail-fast` 除既有失败 `exec_booth::bgi_booth_ctx_invoke_to_sibling` 外全绿。
  基线取证：`git worktree add`——**必须放在 `~/world` 下**（`Cargo.toml` 里 `../probe`
  是相对 path 依赖，放进 scratch 会解析失败）；或沿用 2026-10-02b 那次的
  `git archive HEAD` + 软链法。
- **提交**：aura `74221de`、okm `559d031`。`docs/HANDOFF.md`/`.zh-CN.md` 在本轮开始前
  已在暂存区，未纳入这两笔（仍 staged，待用户处置）。本会话记录本身待提交。

## 会话记录（2026-10-02b，4.17/4.18/4.19 落地：事件面身份 + 分区代理字典 + 游标保留）

- **代码落地（ADR-0038/0039/0040，全部在一个 working tree 内，未提交）**：
  `crates/realm/src/mq.rs` 重写为 ns 20–25（EventName 20 / PartitionName 21 / MqData 22 /
  MqCursor 23 / MqHead 24 / EventRoute 25）+ `PartitionName` 表（`by_name` + `HighWater` +
  `partition_name_of`）+ `mq::Partition`（`Singleton | Named`）——`part_hash`/FNV-1a/
  `part_id_of`/`part_hash_of` 全删，键宽收紧（MqData 20→16 B、MqCursor 16→12 B、
  MqHead 12→8 B）；`meta.rs` ns 30–32（BoothName 30 / BoothDef 31 / CodeBlob 32），
  `resolve_booth_id` 公开，`mq::booth_id_of` 委托它（游标键第三段 = `booth_id`，
  `split_once('/')` 消失）；`events.rs` 移除 `__default__` 兜底、新增 `instance_of()`，
  压缩分母改用 `mq::booth_subscribes`；`event.rs` `DeadReason` 增 `MissingKeyField`；
  `instance.rs` 只为单例实例绑定无 key 路由；`ctx.rs`/`lib.rs`/`remote.rs`/`realm_set.rs`
  适配（`DEFAULT_CURSOR_TTL` = 30 天，realm 携 `cursor_ttl` 字段）；
  `crates/config`（`Root.cursor_ttl_secs` + `kdl.rs` 的 `MqConfig`/`parse_duration_secs`，
  支持 s/m/h/d）+ `crates/engine/src/lib.rs` 注入。
- **测试适配**：`crates/realm/tests/mq_okm.rs` 重写（Partition API、单例/命名分区、
  游标 TTL 过期，新增 `age_cursor` 测试支持——`rewind_cursor` 现在落 NOW 而不过期）；
  `engine/tests/events.rs`（含新增通配收窄用例 `wildcard_delivers_to_the_singleton_instance_only`）、
  `queue_relief.rs` 适配。
- **一处超出裁决文本的修正（已写入 ADR-0039 与 event-flow §6.3）**：分母原先只按**精确
  event_id** 查表，会把**通配**订阅者静默排除出分母（注册行带的是模式的事件 id），
  于是压缩会吃掉通配消费者未消费的积压——违反 ADR-0038 §4「无静默丢弃」。现判据为
  `mq::booth_subscribes`（匹配精确或通配）。
- **第二批改名与语义收紧（同日，用户裁决后追加）**：`type_id` → `booth_id`（mq/meta 两面的
  字段、参数、局部量全扫，含派生索引名 `by_type` → `by_booth`；`type_subscribes` →
  `booth_subscribes`、`type_id_of` → `booth_id_of`、`type_name_of` → `booth_name_of`）。
  MqData 的第三段从「逻辑时间」改为**纯序列**：字段 `time` → `seq`，`MqHead.last_time` →
  `last_seq`，append 由 `max(now_ms, last+1)` 改为 `last + 1`（墙钟彻底离开写路径，
  只留在 `last_active_ms` 这个 TTL 输入上），`mq::skip_to_now` → `mq::skip_to_head`。
  理由：该值是排序键兼行身份，必须全序唯一——唯一性由「emit 路径持 realm 锁 = 单写者」
  保证；把墙钟读数与 +1 混在一个数里（旧 `max`）只会冒充时间戳：同毫秒会算出相等主键
  （覆盖 = 丢数据），回拨会落到已消费游标之下（静默丢弃）。脚本面 host fn 名
  `ctx_skip_to_now` → `ctx_skip_to_head`（**两边一起改名**：aura ctx bridge 的 host fn 名与
  probe `runtime/src/carrier/steel.rs` 的 introspection stub 列表；脚本面契约变更，已确认）。
- **验证**：`cargo check`（realm/config/engine）通过；`cargo clippy` 三包新代码零警告
  （修掉两条自引入：mq.rs 文档列表缩进、events.rs 冗余 `let _ =`）。测试：workspace 各
  套件全绿，**唯一失败 `exec_booth::bgi_booth_ctx_invoke_to_sibling` 经取证为预先存在**
  ——用 `git archive HEAD`（只读，未动工作树）导出 HEAD 内容到 scratch、软链 `probe`/`okm`
  后在同一测试复现同样失败（左侧 `Null`），与本次改动无关。
- **文档同步（双语）**：ADR-0038/0039/0040 状态行由「已裁未实施」改为「已落地
  （2026-10-02，提交待指令）」并写入落地形态、连带与残余；`event-flow.md`/`-en.md`
  的 §1/§2/§5/§6.1/§6.2/§6.3/§7/§8 全部翻为已落（§6.1 伪码块改为
  `Partition::Singleton`/`Named` + `MissingKeyField`；§8 导语改为「8.3/8.4 已落、
  8.1 待决（前置 = 动态 schema）、8.5 开放」）。
- **残余（记录在 ADR-0038 与 event-flow §8.3）**：实例键空间仍用 `"__singleton__"`
  哨兵字符串——payload 里字面等于它的 key 仍会别名到单例**实例**（与已修掉的分区别名
  不是同一个 bug）；让实例身份结构化会牵动整个 call model 与 probe 缝上的 `InstanceId`，
  留给独立裁决。4.18 的 in-memory resolve 热面**未建**（每次 append/游标一次字典读，
  实测无需）；probe 仓 USAGE 帧形同步（4.16c 遗留）本次未动。
- **提交**：`2d1fafb`（2026-10-02b 批次）；后续 ADR-0041 批次见 2026-10-08 会话记录。

## 会话记录（2026-10-02，事件面终态裁决 + event-flow 重写为推导式）

- **文档重写（已落）**：`docs/design/event-flow.md`/`-en.md` 从流水账改为推导式组织
  ——新增 §2「从约束推导持久面」（11 步，每步 = 约束 → 落点表 + ns；收束为「事件是
  事实、订阅是关系、两者都不许在键里带名字、摊位自己的东西各有其表」）；§6 把投递/
  消费/保留合成「一次 emit 的一生」（6.1/6.2/6.3）；§7 键空间表标注为推导落点；§8
  从「待决问题」改为「终态裁决 + 实施清单」。§7/§8 编号与全部外部引用（ns-layout
  指针、PLAN、partitioning、ADR-0026 §1）保持不变。
- **审核发现（文档侧，已修）**：§8.3「已落地（未提交）」陈旧（dbc5d60 已提交）；
  MqData 的 live `Count` reduce（`depth()` 点读来源、skip-to-now 的决策输入）全文
  缺失；dead ring 漏记「`mq::append` 失败」这条入口；「backlog 批读」不实（实为分区
  前缀全扫、无批上限）；漏记 `(event, partition)` 去重导致的多类型单次入队；未写明
  meta 表与 mq 表同住一个 okm 实例。
- **审核发现（代码注释陈旧，已修）**：`crates/realm/src/event.rs` 模块头仍写「`emits`
  是白名单、未声明即被 Realm 拒绝」（与 ADR-0012 及实际代码冲突）；`meta.rs` 模块头写
  booth 定义在「另一个 okm 实例（独立 engine/目录，跨实例查询不存在）」（ADR-0025 已
  并入数据面，函数实际吃 realm 的 `MqStore`）；`meta.rs` 的低位块注释漏 42。
  `cargo check -p aura-realm` 通过（确认重编译）。
- **新 ADR（已落，双语）**：ADR-0038「事件面的消费者集合与身份」（通配收窄为单例
  投递、单字典、声明逐事件且缺失=单例、无静默丢弃）；ADR-0039「分区身份与游标保留承诺」
  （分区改为代理字典、哈希退役；`cursor_ttl` 全局配置、过期退出分母不删行）；ADR-0040
  「框架键空间分段」（事件面 2x、meta 面 3x，段内概念序）。三条均初裁未实施——**本会话
  稍后即落地，见下一条会话记录**。
- **中途修正（重要）**：ADR-0039 §1 初稿裁定「分区段改自描述长度前缀」，随后核对 okm
  发现**不可实现**——`KeyEncode` 主键在构造上定宽（`String` 字段编译期 panic），变长字段
  只存在于索引 fields 段（至多一个、必须最后、且不带长度前缀），`Count` reduce 的 group
  字段同样受定宽约束。改为：分区成为**代理词汇**（`PartitionName` 字典发 `u32` id），
  也就是本文档 §7 早已写明的「开放词汇走代理 id」规则——哈希才是那个不合群的例外。
  副产：碰撞即错误投递、保留值与值空间共用名字空间、哈希不可反查这三个缺陷全部变成
  构造性不可能，键宽还缩了（MqData 20→16 B、MqCursor 16→12 B、MqHead 12→8 B）。
- **分段与清除代价（用户裁决：不用 4x 段）**：ns 段改为事件面 20–29 / meta 面 30–39。
  新 meta 段（30–32）正落在旧事件面的号上，所以重新编号**只在低位块被彻底清除的前提下
  安全**（否则新 `BoothName` 会把旧 `EventName` 行读成类型名）；代价比「mq 字节转瞬即逝」
  大一圈——已持久化的摊位定义与代码 blob 一并作废，部署方要重新注册类型（ADR-0040
  记录在案）。
- **推导所得的结构性发现**（写进 ADR）：① 广播使消费者集合成为开集——新实例游标从 0
  起会重放该队列现存全部历史，水位被永久钉住、订阅漂移成状态同步；② 定宽哈希分区有
  三个副作用：碰撞即错误投递（同类型两个 key 共享队列与游标行）、保留值 `0` 与值空间
  共用名字空间（key 字面等于哨兵的有 key 实例别名进单例队列）、哈希不可反查（运维
  列不出人类可读的队列）；③ 游标过期必须「退出分母」而非删行（删行 → 游标读回 0 →
  `backlog(after=0)` 重放现存行 → 若游标原本领先水位就是重复投递）。
- **PLAN**：4.13 遗留的两个问题由 ADR-0038 §3 关闭并就地标注（逐事件命名、逐目标
  dead-ring）；新增 4.17（事件面身份与投递终态）、4.18（分区身份 + 键空间分段）、4.19
  （游标保留承诺）。
- **提交**：`b523a5f`（2026-10-02 批次）。

## 会话记录（2026-09-30c，4.16c 落地：typed 宿主帧 + 声明式双编码）

- **裁决落点（用户"采用双协议，bgi/exec 配置中添加编码字段"）**：原
  §2"整通道 CBOR"计划被实测事实判死——nu 0.115 stdlib 没有 CBOR 编
  解码（`to/from cbor` 不存在，只有 msgpack/msgpackz/toml/json/nuon/
  kdl），入口判据（任何语言 stdlib 可达）是硬墙。改为每摊位声明一种
  编码（json|cbor），终身一 codec、不按帧协商；第三种编码仍关在
  Windmill 判据后。声明面：aura-booth 自有 `ChannelEncoding`（crate
  保持 probe-free，serde 值与 probe-protocol 同名对，realm `map_encoding`
  映射）+ `BoothType::encoded()`；`script()` 三参不变、默认 Json，
  50+ 既有调用点零破坏。远程路径编码随 `ToolCall.encoding`
  （`#[serde(default)]`）到节点；probe 下行 `with_session_encoded`。
- **类型帧**：`exchange()` 把 `{"host":{"type":…}}` 反序列化进
  serde 标签枚举（invoke / iterate+`IterVerb` / store / interface_schema）
  再映射回桥表——判别符/动词错 = 解码失败 = 错误值回 child，自由
  `op` 名查表落空的静默路径消失（§3 的"退役"兑现为**入口**退役：
  store 指令本身仍按数据搬运，schema-blind 铁律不动；远程 WS `HostOp`
  本就是类型帧，无退役对象）。
- **两个实测形状**：CBOR 分帧用自定界文档而非计划里的长度前缀——
  ciborium 恰好读声明的字节数，阻塞管道上连续 `from_reader` 逐帧落位
  （`bgi_cbor_round_trip` 锁住多帧顺序 + 持驻）；编码经 `BGI_ENCODING`
  spawn env 传子进程，不追加 argv——fifo 形恰好传 `[req rep]`，生成参
  数会破作者 `def main` 的 arity。nu 头声明 Cbor = 启动期点名错误
  （`nu_cbor_declaration_is_an_error`），绝不静默降级。
- **持久化兼容（布局规则先行查证）**：`BoothDef` 的 `encoding: u64`
  追加在热段尾 + `#[ok_layout(version = 2)]`——先读 okm-derive
  `emit_payload_decode` 确认截断尾读声明默认（append-only 规则），旧
  v1 行重读为 0=json（其实际行为），字段插中间会移动 `code_sha256`
  偏移的路线否决。`introspect_schema` 按声明编码起抛却子进程
  （`introspect_encoded`）——CBOR 摊位的 schema 帧往返也骑 CBOR。
- **验收矩阵**：probe workspace 全绿（新锁 5 条：CBOR 往返+持驻、CBOR
  ctx 缝、未类型帧=解码错误且桥 fn 不执行、exec CBOR、nu+Cbor=错误）；
  aura workspace 全绿（新锁 `bgi_cbor_booth_store_emit_roundtrip`：上传
  introspection + 类型 store 帧 + realm plan 全走 CBOR）；两仓 clippy
  零警告——顺手清 echo.rs 存量 6 条（5×register 结果 `.unwrap()`、1×
  被遮蔽绑定），`too_many_arguments` 以属性+why 抵制（参数组是
  `with_session` 既有形态，结构化会翻全部调用点）。
- **文档**：ADR-0037 双语（状态 ALL LANDED、§2 声明式双协议、裁决行、
  §3 入口退役落地态、诚实代价加"双编码是实测让路非对冲"、排期落地
  态）；ADR-0035 §3 双语（帧词汇类型化、编码声明化）；PLAN 4.16 标
  CLOSED。夹具 bgi_loop/one_shot 按 env 分双编码、宿主帧类型化；
  bgi_nu.nu 保持 JSON 读类型帧。
- **提交**：aura `d87bd18`（4.16c typed 帧缝 e2e）；probe 侧同批提交号见 probe 仓。

## 会话记录（2026-09-30b，4.16b 落地：steel 绑定面注入 + slatedb 驱动线程）

- **共享切口兑现（用户裁决"共享"）**：引擎面（`Engine` 四字节方法 +
  `EngineBox` 单执行体分派）与条目解析（`collection_from_entry`/
  `plain_access_method`/`preset_reduce`）从 okm-python 抽出为 standalone
  crate `okm-entry`，python 改骑它（re-export + `with_store` 委托），
  steel 的方法面建其上——绑定复刻条目语义，一如复刻字节布局，都是被否
  决的那类债。python 的 3 条注入锁原样过共享路径，语义未变。
- **句柄形态的实测裁决**：先建点号 shim（`(Counters.put ...)`）并实测
  steel 的 define 与调用两侧确实把点号名解析为单一标识符——然后否决它
  自己：steel 在 define 编译期解析自由标识符，shim 名是脚本内容，
  introspect 的临时引擎里无法打桩，会重演 ctx stub 抓过的"声明静默丢
  失"陷阱。定案：六个固定名全局 fns + 脚本按名字字符串寻址集合，stub
  臂（集合 fns + codec fns，精确 arity）只进临时引擎（同名
  `register_fn` 叠加会遮蔽常驻会话真函数——ctx stub 同规）。注册表为
  PER-VM（非 codec 句柄那种 thread_local：会话 VM 经 `unsafe impl
  Send` 跨 worker 线程迁移，thread_local 会丢绑定；per-VM 也贴合驱逐
  生命周期——drop 会话即 drop 绑定）。
- **slatedb 潜在缺陷（接线宿主时挖出，用户批准方向 1 根治）**：同步门面
  持 runtime 逐调用 `block_on`——在已进入 tokio context 的线程上必 panic
  （spawn_blocking 保留 context，realm 恰在其中构造并驱动注入；open 一
  炸，修完 open 再炸 scan：全部同步方法都在雷区；4.16a 的锁当时没走到
  这条路径，原因未深究，记录留白）。改为专用驱动线程持
  runtime+Db，同步操作 = 通道命令（消费侧只阻塞 mpsc，永不 block_on）；
  惰性扫描=逐项通道流（ADR-0027 惰性契约保形：驱动随消费产出、消费者
  drop 即 send 失败停驱）；`next_back` 阻塞抽干到终结哨兵——第一版用
  `try_iter` 被既有 `lazy_range_scan` 锁抓住（驱动滞后时静默截断）。中
  途引入的手写 `Cmd::Stop` 优雅关闭自删：mpsc 在最后一个 sender drop
  后 recv 自然 Err 退出，手写信号需要 racy 的 strong_count 判断才有意
  义，是冗余。锁：`driver_thread_test.rs`（tokio context 内全操作 +
  100 行反向抽干）。
- **载体接线**：probe steel 载体消费同一 `HostBridge.storage` 槽——
  `ClosureEngine` 适配四字节闭包到 okm-steel 的 Engine trait，
  `SteelSession::new` 改返回 Result（注入失败=声明错误，会话不得启动，
  对齐 python 载体的 load 签名）；okm 锁统一 bump 407cbe26（branch=main
  与 plain-URL 两种 source 拼写解析到同一 rev，非分裂锁）。验收矩阵：
  okm-core/dynamic 全绿（含新锁）、okm-steel 6 锁、okm-python 3 锁、
  probe 13 运行、aura engine 全特性 17 binary（含 `steel_injection`：
  绑定写↔emit 读双向互证 + evict 后注册表在幸存行上重建）全绿，clippy
  零警告。
- **提交**：okm 92b2551（驱动线程 fix + 锁）→ 36143da（okm-entry 共享
  crate + python 改骑）→ 407cbe2（okm-steel 方法面 + Obj/Array 缺臂修
  复 + 锁）；probe 7ae9f5b（steel 载体接线 + okm 依赖 bump）；aura
  3f61522（`steel_injection.rs` e2e 锁）。ADR-0037 双语状态/§1/诚实成
  本/后果改落地态（4.16c 留名），随本文档批提交。

## 会话记录（2026-09-30，4.16a 落地：python 绑定面注入）

- **接线形状（两仓原子批 + okm 前置批）**：Collection 方法面（python 对
  作者零翻译）→ okm-dynamic plan 纯函数 → 字节 ops → 注入的字节面引擎。
  跨仓 rev 问题（probe 与 aura 各指不同 okm rev）在字节面上自然消解——
  缝上只走 `Vec<u8>`/`&[u8]`，无 okm 类型。布局事实源留 okm-dynamic，
  binding 不复刻（B 方案兑现）。probe 的 `StorageSlot` 是纯数据（ns +
  原始条目 JSON + 四闭包），python `load()` 内直接 `with_store` 结果
  `module.add`——**没有中转层**：曾试 `Vec<(String, Collection)>` 经
  `Box<dyn Any + Send>` 跨缝回传再 downcast，`*mut PyObject`（pyclass
  注册数据）不满足 Send，正解是根本不过缝。
- **ns_raw 纠偏（用户指令的修正，实测驱动）**：接线指示原写"捕获
  `MqStore::ns_raw(plan.ns)`"，落地改捕**裸 realm-mq**——注入的
  `Collection` 自绑 ns（DynamicCollection 的 key 自带 `[ns BE]` 段），
  ns_raw 会叠出 `[ns][realm][ns]…` 双 ns，绑定面与 ctx_store_emit
  互读必挂；`ns_raw` 是 wasm 平面专属形状（guest 内模块不绑 ns，宿主
  补）。同字节验收（`py_injection.rs`：绑定写↔emit 读双向 + evict 重建
  幸存）只有裸句柄成立。
- **序坑（实测抓到，勿重新发明）**：python 载体的 interface_schema
  storage 半原为 schema 调用时晚评估 `assemble_module(globals())`——
  注入把 pyclass 实例注册进**同名**命名空间（作者绑定面就是它）后，晚
  评估看不到 `@DocumentEncode` 类、静默装配空块、storage 半失踪。改
  load 期（body 跑完、注入发生前）捕获为数据。同理 introspect 走同一
  load_module，语义一致无需分支。
- **字节平面（与 ns-layout 键空间图对账）**：绑定面与 emit 面同平面
  `[realm 前缀][ns][slot]…`；wasm full-power 独立平面 `[ns][...]`
  （无 realm 段，trusted-writer 语义）。"跨类型 ns 在绑定面不可表达"
  是结构事实：槽只建 plan 声明的 collections，ns 构造期绑定，DSL 规则
  （ns 不进条目）。
- **闸门**：aura `--workspace --features python` 全绿（含新
  `py_injection.rs` 两条：互读/重建 + schema 装配锁）；probe workspace
  （steel,python,wasmtime）全绿；clippy 新代码零警告（`StorageEngineFns`
  四闭包提为 PutFn/GetFn/DelFn/ScanRangeFn 别名消 type_complexity）；
  两仓 Cargo.lock bump okm → ce08c24。ADR-0037 双语状态行 + 后果节改为
  落地态（4.16b/4.16c 留名）。

## 会话记录（2026-09-29b，Phase 4.15 落地：统一信封）

- **协议塌缩（两仓原子批）**：probe `CallKind::Invoke` 删除（IterateStart
  成 default——wire 形状不变，帧词汇表瘦身）、aura `JobKind::Invoke` 删除
  （`{Start,Next,Dispose}`，`Job.stream` 必填）；dispatch 全部走 stream
  缝（aura run_job 的 script 臂 fold 进 `s.iterate`，remote.rs 同理）。
  事件投递=Start、回复由 drop 丢弃（0036 §4 逐字），`Realm::call` 热臂=
  mint+注册+回信封+**realm 侧 terminal-unwrap**（§1"realm unwraps `value`
  into the parked caller"——surfaces 保持值形态：ctx.invoke/engine.invoke/
  host_wire/dispatch_call 签名与语义零改动；冷臂 resolve_call 前同样解包，
  parked-value 规则热冷一致）。
- **载体包装点（§2 plain returns wrap AT THE CARRIER）**：python
  StopIteration 投影 `.value`（非生成器返回→`{done:true,value}`）；
  envelope_pull 统一校验（跨字段规则强制执行；Start 裸回复包 terminal、
  Next 缺 done=错误不是静默终止；子侧 `{"error":…}` 约定透传外层错误值）；
  bgi 过 validate_envelope（夹具 dispatch 按 `(kind,event)` 双 kind 骑
  同臂——`call` 留作载体内部原语：内省+probe 直驱测试）；exec OneShot 的
  Start=跑一次+包 stdout terminal（脚本协议零改），Next/Dispose 保持
  具名错误——statelessness 锁移到尾随拉取（probe exec_carrier + aura
  exec_booth 两测同形态 reshape）。
- **关联从 done 导出（§4）**：`StreamCursor::next` 不再按 stream_id 字段
  位置认 Start——非 terminal 首帧缺 id=协议错误；terminal 首帧不合并 id
  且 run_job 立即撤销注册（mint-and-discard，单码路无"invoke 跳过注册"
  特例）。`StreamCursor::value()` 落地为显式访问器（§3 文档化：原生
  迭代糖不消费它）；python `ctx_iterate` 尾值落 `last_value` 属性。
- **实测坑（勿重新发明）**：envelope-mode 的 `iterate` 注入 tag 是框架
  词汇——bare handler 回显 args 会把注入键带进结果（echo 测试首跑抓到），
  validate_envelope 的 Start 包装支剥 `iterate` 键（保留字规则入注）；
  非对象 args 无法承载注入（steel `(ctx_store_emit … 7)` 这类数字参数
  booth 的 Start 走 bare-call 形态——不注入不剥离，Next/Dispose 才要求
  对象 args）。
- **闸门**：iterate.rs 六测 reshape（rust_body 锁"Start=terminal invoke+
  注册撤销、尾随 Next=not live"；steel_envelope 断言原样存活）；probe
  workspace（steel,python,wasmtime）全绿；aura engine+realm
  （steel,python,wasmtime,fjall）全绿；clippy 两仓零新增（aura 存量=
  echo.rs 6 条，本批 instance.rs 曾引入 1 条 match-single-pattern 已改
  if-let 消除；probe 零警告）。ADR-0034 erratum 双语已在位（上一批落）。

## 会话记录（2026-09-29，闸门 2+3 落地：nu bgi 双 fifo + PTY 退役）

- **分派表裁决（用户，2026-09-29）**：事件帧进单入口、入口内部按事件名派发——有运行时查表的语言用原生机制（python/steel 装饰器收集进 dict、host 按名 getattr——load 期收集即注册表，派发零额外开销、零字符串化；wasm 导出表寻址），没有的（nushell，bgi/exec 通用形态）作者手写 `main` + match 字面名（nu 无 eval、`source` 拒动态路径，实测 not_a_constant——手写不是妥协，是这类语言的契约形态）。`@on` 若走 bgi 需 py 实现的另一套收集逻辑（装饰器住脚本侧），未实施——嵌入式 python 已覆盖。bgi 保留显式循环、其它形态不带——循环的存在理由=免逐调用 spawn（跨请求状态是顺带，不靠它）。
- **闸门 2（probe）**：`BgiKind::{Pipes, Fifo}`——spawn spec 头选通道（`["nu","<author.nu>"]` → 双 fifo），线协议一套不多造。请求 `req`（逐帧写后即关=批次 EOF 唤醒）、ctx 应答 `rep`、结果 stdout。父侧零生成（无 shim——main 即入口，nu 用脚本参数自动调用）。夹具 `bgi_nu.nu` 头注记三个实测坑：**单 fifo 双读者竞态=实测死锁**（外层循环与内联应答读抢帧，早期单 fifo 探针通过纯属唤醒顺序运气）、`else` 须与分支 `}` 同行、`each` 闭包吞 `$env` 写（批次循环必须 `for`）。nu 侧 `print` 逐条 flush 实测成立（500ms 间隔两行各到）。
- **闸门 3（两仓）**：PTY 整删——probe NushellResident/nushell_session/bridge.nu/nu_session+nu_bridge 测试/`nushell` feature/语言臂；aura feature 链（realm、engine）与 wire 词汇。迁移路线（每把 PTY 锁移等价活锁，无裸删）：callslot slow handler→bgi 夹具新 `slow` 臂（500ms 真睡眠，feature-free）；echo nu booth/idle-ttl→bgi_nu 夹具（新 `sum` 臂 + `lifecycle idle_ttl`）；echo PTY store 锁→指向 exec_booth 的 nu 往返锁；envelope_pull 锁→steel（唯一 ride 该路的嵌入式载体——顺带补上它此前的零锁定）；python iterate 测试的 `all(python,nushell)` 门无历史依据→`python`。新锁：probe `nu_bgi_*` 四条 + aura `bgi_nu_booth_store_emit_roundtrip`（注册期 schema 帧过缝解析 plan、双 store 指令往返、$env 逐实例跨调用）。ADR-0035 双语 §6/诚实成本 nu 条/Consequences 就地改写为落地态。Phase 4.14 勾 [x]。
- **教训入档**：steel 布尔字面量 `#f/#t`（写 `false` 是 FreeIdentifier 解析错，首跑抓到）；PTY 每轮"命令行文本"式 handler 寻址退役后，"inline 脚本文本"对 nu 不再存在——源=文件/argv，bash/Rust-bin 同构，一语言一形态兑现。

## 会话记录（2026-09-28c，闸门 1 落地 + ADR-0037 裁决）

- **闸门 1 已提交（本条同批）**：`HostOp::StoreEmit` 线臂——一条 okm
  Collection 指令作为 DATA 过线（载荷字段名 `instruction`，不叫 `op`：
  与内部 tag 判别符碰撞）；probe 端透传不解析（schema 住类型注册处），
  aura 端 `resolve_host_call` 锁下取 plan+mq 克隆执行（ctx_for 纪律），
  无 plan=点名错误（0026"远程 ctx_store_emit 需 resolved plan"从缺席臂
  变成有信息的错误值）。bgi 载体零改动（`exchange()` 查表即答）。三测
  试锁定：probe-protocol 线形状契约、aura exec_booth
  `bgi_booth_store_emit_roundtrip`（child→管道→realm store→回程，夹具
  手写 storage literal 解析出 plan）、remote_probe 过期注释更新。
  USAGE 双语的 `state_*` 残留行随 0026 退役一并修正（勘误非扩面）。
- **ADR-0037 裁决（typed 存储面 + 进程内绑定）**：用户的 okm 绑定事实
  戳破 0026 §3 的实现措辞——python/steel 进程内**直接绑 DynamicCollection**
  （零翻译；steel 缺 Collection 方法面是实施项），进程外=一条 host 通道
  上的**类型化帧**（invoke/iterate/store 是帧类型，bgi 垫片【不】绑存储
  面——垫片只做帧循环+派发），CBOR 是整通道编码（0035 §3 既有计划，
  Windmill 判据落点，非为存储新造）。闸门 1 的 JSON 指令文档=点名的过渡
  形态：op set 冻结、随 CBOR 退役。JSON 缝的真实缺陷校准：不破坏不变量
  （两边都走 Collection 补偿），是 schema 拼写错误静默落动态段——校验
  层从语言边界降级到 from_value。0026 §3 双语已挂 erratum。排期 4.16，
  在 4.14 闸门 2/3（走过渡缝，垫片与缝形态解耦）与 4.15 之后。
- **教训入档**：`instruction` 字段命名=serde 内部 tag 碰撞检查要过一遍
  所有新增 HostOp 载荷字段；python feature 门控的既有测试（sibling 用
  python booth）单跑必红——按 feature 组合跑是默认，不是例外。

## 会话记录（2026-09-28b，iterate 落地 + ADR-0035/0036 裁决）

- **已提交**：aura 70b61a5（iterate 代码+测试）、36c2d35（0034 修订 docs）、
  probe e85c933（iterate carrier+协议）；probe 7476209 + aura 504bfd1
  （exec/bgi 载体首刀，含 GIL 修复与 stream bookkeeping 上提修复）；
  aura 15fa114（pull(n) 措辞降级）。
- **裁决链**：用户推翻 0034"wasm 仅消费侧"（不能 HTTP ≠ 不能生成器）→
  0034 修订；invoke/iterate 对称性 → **ADR-0036**（一套信封，
  `done` 恒布尔、终止轮 `{done:true,value?}`；ctx 动词保留两个；
  关联从 done 导出废位置性启发式）→ PLAN Phase 4.15（排 4.14 后）。
  exec 载体 → **ADR-0035**；同日 fcgi-vs-cgi 分析把"两模式一套帧"更正为
  **两形态**：bgi（带帧常驻，循环住子进程——作者自写或 probe 垫片）/
  exec（裸 cgi 一次性，【无协议】：一进一出各一个 JSON，stateless by
  definition——php-fpm 血统点名，iterate 是点名错误值）。BGI 名字采纳。
- **nushell 实证（探针，勿重新发明）**：非 TTY stdin 不能阻塞读（`input`
  报错）；`open` 在写方 EOF 交付（父常持 O_RDWR fd 会饿死 `lines`）；
  用户的 mkfifo + `loop { open pipe | lines | each }` 逐写方会话派发成立
  （含内联 ctx 应答读的双 fifo 验证）。裁决：nushell 先走裸 exec，
  bgi 等 fifo 垫片；PTY 退役闸门等垫片。
- **未提交（检查点待审）**：两仓的 exec→bgi/exec 重构（fixture 改名
  bgi_loop/one_shot、OneShotSession 无 ResidentSession 帧、aura run_job
  的 failed-Start 注册回滚、0035 双语 §1/§2/§3/§5/§6/§7 词汇更正、
  PLAN 4.14 改写）；0036 双语 + 0034 erratum + PLAN 4.15 已随
  0ccc064/57884f0 提交（本条更正：0036/erratum 在 0ccc064，0035+BGI
  的 0035 首次落地在 57884f0，本轮重构是 0035 的同日更正）。
- **闸门与残余**：4.14 剩 `HostOp::store_emit` 臂 + nu BGI fifo 垫片 +
  PTY 退役（闸门=nu store-emit 往返过 bgi）；4.15=信封合并；iterate
  残余=pull(n) 旋钮、Rust body 生产方。GIL 教训已入 aura-dev skill。

## 会话记录（2026-09-28，ADR-0031：远程摊位不采用）

- ADR-0031 经复核与初始设定冲突，整篇改写为**「远程摊位：为什么不采用」**（同一编号，
  不新开 ADR）：对外访问是摊位代码的事，不是 realm 基础设施——逻辑归摊位、访问配置
  非全局，摊位内部自选客户端（脚本体 HTTP 库；wasm 经 wasi-http host，属 carrier
  能力任务非 realm 配置）。tier-1 远程执行（probe + ADR-0027 内容寻址）不受影响，
  是唯一的远程形态；浏览器会话归 prism 会话平面（会话≠摊位）。草稿的两个观察
  （actor 模型不要求确定性、调用模型目标无关）作为「什么幸存」留档。代码零改动，
  仅两处措辞修正（kdl.rs 注释、instance.rs probe 臂错误串 "remote booth"→"remote probe"）。
- prism docs/PLAN.md Phase 1.8 的 DESIGN CONSTRAINTS 块按该裁决改写：拨出/CBOR/远程
  摊位注册删除；站得住的部分（Node 保持 transport-only、auth block + 前缀戳、Phase 1
  流式 = fluxen 接收面）本就是 prism 自有能力，不依赖远程摊位概念。
- 追加（同日）：wasm 出网需求复核后定案 ADR-0034——`ctx.iterate` 成为与 emit/on/
  invoke 并列的第四原语（生成器语义：类型化 `{item,done,error?}` 信封、dispose 对偶、
  pull 重置 idle 计时、流停止经 timer API 重武装、只做 hot 层）；wasi-http 与
  aura_host 代理两条路都撤销（前者要 component model 迁移，后者把外部访问上移框架层，
  违背 0031 精神）。provider 摊位（python 生成器适配 OpenAI SSE）是第一个应用实例。

## 会话记录（2026-09-26，booth 改名 + 消费者饥饿修复）

- ADR-0032：参与者 actor → booth（中文 摊位）全链路改名落地（aura 代码/文档 +
  probe/prism/gravity/okm/wiki 同批对齐；策略 A 无兼容别名——meta 持久行清空重注册）。
- **真 bug 修复**：instance.rs 消费者任务旧形如 `for subs { loop }`——多队列实例
  只排第一条队列，其余饥饿（clippy::never_loop 是表象，非误报）。重写为单循环扫
  全部队列；回归锁 `multi_route_instance_drains_every_queue`（旧码 RED：count 1，
  新码 GREEN：count 2，实测验证）。修复顺带暴露并从根上堵住旧形态的 Arc 泄漏
  （无订阅实例的 poll loop 永不退出、持 store 引用 → fjall "Locked"）：消费者对
  空 subs 直接 return。
- 存量测试卫生：engine/realm 测试 22× unused-Result（register().await; →
  .unwrap()）、4× redundant mut、1× redundant pattern 清零，工作树 clippy 警告
  回到 HEAD 基线（仅余 okm derive 宏展开处的 3 个存量命名/ptr_arg 警告）。
- callslot 两超时测试 + wasm_guest e2e 按 echo.rs 惯例补 feature 门控
  （nushell/wasmtime 缺席时此前是假跑/必挂）。
- 全绿口径：默认 features + `--features "steel python wasmtime nushell fjall"`
  全量；prism cargo test 全绿。

## 会话记录（2026-09-23，自 HANDOFF 简报合并）

起点 `ffb9a5c`（timer wheel 批次收尾），终点 aura `8a92122` / probe `9da8ed8`，
全量测试通过（`cargo test -p aura-engine --features "fjall,nushell,steel"` 36 个 +
`aura-realm --features fjall`）。已知失败清单：**空**（fjall_state 真 bug 已修、
callslot deadline 属 feature 组合误判——见下）。

### 已完成（全部已提交）

- [x] fjall_state 修复（a7a5b9d）：`ctx_for` dispatch 闭包持强 SharedRealm → 进入
      resident session（steel `register_fn` 'static）→ 引用环钉死 fjall Database，
      Engine drop 后重开 `Locked`。修复：闭包持 `Arc::downgrade`，`Weak::upgrade`
      每次调用。standing rule：任何活得比单个 job 久的闭包/task 持 realm Weak。
- [x] callslot 假失败澄清：deadline 测试用 nushell slow handler，feature 不全时
      job 瞬时错误早于 50ms deadline。规则：engine 测试用全语言 feature 跑。
- [x] MQ 投递层演进（388bbea + 86a255e）：MqData 键改逻辑时间
      `[event_id][part_id][time]`（MqHead 行保单调，append O(1)，skip-to-now 读
      head）；新 MqHead 表（ns 34）；singleton 结构化（part_id 0 保留）；水位分母
      切持久 EventRoute 注册表。设计否决记录：per-event 动态 ns、Part_id↔seq
      registry、partition registry。
- [x] EventRoute 持久注册表（388bbea 内）：ns 35，`register_type` 落表；durable
      事实源 + ops 面 + 水位分母；内存 EventRouter 保留为 emit 匹配热路径。
- [x] meta schema 动态段化（a0b0c03）：`BoothDef` 删 `schema: String`，schema 以
      `DynamicValue::Obj` 走 dynamic segment；存储层不再有任何 JSON 文本。
- [x] shim 移除（aura 68877c2 + probe 9da8ed8）：四层 entry 全删；handler 只按
      事件名寻址；25 处测试/CLI 迁移。
- [x] wildcard 队列身份修复（c249ac2）：emit 一律以具体事件名落队列；通配订阅经
      `events_matching` 展开具体名、逐名 cursor。规则：队列身份与 handler 名永远
      是具体事件名，模式串只在 router 与展开步骤。
- [x] docs（ec89966 + 8a92122 + 493720d）：modeling.md 游戏房间高频状态形态节；
      realm.md emit 分发路径重写为持久队列现状；wasm carrier ABI 设计文档落地 →
      Phase 4.5 主项闭环（PLAN:79 勾选，4.5c step 5 勾选）。

### 遗留待办（未动）

- [x] **iterate 原语实施（ADR-0034，LANDED 2026-09-28）**：与 emit/on/invoke
      并列的流式调用。范围：① ctx 表面（aura-booth `Ctx::iterate` 游标 +
      `StreamCursor::next/dispose`，ADR-0011 errata 加条目）；② 帧管道
      （`Realm::iterate` 骑 hot 调用机件——Job 加 kind/stream 两字段，
      stream_id 由 realm call_seq 铸造、registry（`streams`）解析
      Next/Dispose 的 target——id 即路由，不再重呈地址；Start 应答合并
      stream_id 进首个信封）；③ 信封 `{item,done}`——host 可驱动生成器的
      carrier（python `yield`：框架停放 generator、StopIteration 编码 done、
      dispose 走 `close()`）框架驱动；无宿主可驱动生成器的（steel/nushell/
      wasm）handler 显式返回信封（框架注入 `{stream_id,op}` 并校验布尔
      `done`；Rust-wasm guest 在模块状态内映射 `Iterator`，ABI 不变）；
      消费方 wrapper：python 载 entry `ctx_iterate`（原生 generator，
      GeneratorExit→finally 自动 dispose），Rust 经 `StreamCursor`，其余
      手拉循环；wasm 两侧都服务（生产 = 信封、消费 = 拉到 done 循环——
      本 PLAN 条目按 ADR 当日修订口径落地，推翻"只做消费侧"）；④ 驻留计时
      零新代码：每次 pull 就是 Job→run_job 的 last_activity 重置 + idle
      重武装，done/失败 pull 时 realm registry 移除条目；⑤ tier 只做 hot。
      验收（engine tests/iterate.rs，6 项全绿）：python 生成器生产→python
      消费、中途 break 的 dispose 断言（registry 排空）、未知 stream 失败值
      + 幂等 dispose、流中途 raise 的 error 值断言、nushell 信封往返、Rust
      body 的 iterate 错误值（Rust 生产方=记录残余：closure body 无常驻
      状态可持迭代器）。probe-protocol：ToolCall 加 kind/stream（远程生产
      腿）、HostOp 加 iterate/next/dispose（远程消费腿）。
      **残余**：① `pull(n)` 批量旋钮（ADR-0034 措辞已同日降级为
      "planned"——Windmill 判据，快生产者把跳数变成真实成本时再建）；
      ② Rust body 生产方（closure 无常驻状态可持迭代器）。
      **GIL 修复（e2e 逼出的真 bug）**：python host fn 原持 GIL block_on——
      两 python 摊位互调必死锁；现
      `py.allow_threads` 释放 GIL 再阻塞。
- [x] **wasm 出网 HTTP——已撤销（2026-09-28 当日，被 ADR-0034 取代）**：立项后复核
      两条路都不可取：wasi-http 主机钩子要求 component model（wasmtime 34 不支持
      core module，carrier 迁移是平台级工程且破 4.5b 落地面）；aura_host 族新主机
      函数则把外部访问逻辑上移框架层——违背 ADR-0031「访问方式由摊位代码决定」的
      精神。定案：wasm 摊位的对外 HTTP = **消费兄弟摊位**——provider 摊位（python
      生成器，httpx.stream + yield）经新原语 `ctx.iterate` 被消费（ADR-0034，
      生成器语义流式调用）。实施任务随 ADR-0034 落到 iterate 原语本身。
- [ ] events_matching 增量化（可选小优化）：通配订阅每轮 50ms 全量重扫；可缓存
      上轮展开 + EventName registry 水位，registry 不变即跳过。通配订阅多/词汇大
      时才值得（Windmill 判据）。
- [x] probe 侧 nushell 的 ctx 桥（LANDED 2026-09-25）：bridge.nu 把每个 host fn 物化为
      `ctx-<dash-name>` 自定义命令（nu 禁点号），nu 侧写 req-*.json 轮询 resp-*.json，
      Rust `call` 的 poll 循环 sweep 会话目录应答（HostBridge 同步口）。回归锁：结果
      文件出现时 REPL 仍在重绘提示符，立即返回会让下一桥回合的 source 吞进半截提示符
      ——`call` 交还 session 前须 pump 至 PTY 流静默（nu_bridge.rs 两回合测试锁住）。
      aura 侧：`pure_nushell` 特判删除，nu 与其余语言同走桥；e2e =
      echo.rs::nushell_store_emit_roundtrip（手写 schema 字面量 → ctx-store-emit →
      真实 realm store 往返）。
- [ ] cold call over the wire（依赖 Phase 6，勿单独实施）：重进入路径的前提是
      调用方有挂起/恢复契约——gravity 的 transcript 持久化 + 脚本侧约定 resume
      handler（如 `__call_resolved`）。今天无任何 cold tier 消费者，现在建
      pending marker + 事件重进入 = 给不存在的消费者铺管道，且 gravity 落地时
      形态会变（终态前提纪律）。实施时 probe 不改：marker 是数据非新协议帧。
- [ ] ADR-0015 三步实施：归属已按 2026-09-25 修订拆分（见 ADR-0015 Update 节）——
      ①的 aura 残余（顶替必须可见 + 启动如实披露）已落地（replacement 事件点名
      alias 与新旧 peer、PresenceGuard 身份核对，alias_takeover.rs 锁）；identity
      模式开关、密钥对握手、节点登记表、四端点、并入账号——全部住 **prism**
      （prism PLAN Phase 1.8），aura 不重复建设（同一决定两个家 = 第二真相源）。
      `credential_env` 删除随 prism 握手线落地时执行（probe 仓，同一协调提交）。
      身份归属修订（2026-09-22）：认证数据住 **prism**，aura 只在投递载荷里收到
      sender 元数据（Ctx 不变）——与 ADR-0017 §3/§5/§7 修订一起在 prism 侧执行
      （aura 侧无远程挂载改造——ADR-0025 的 Plan B 已否决，2026-09-23）。
- [ ] Phase 4 Remaining：slate engine option on both planes（无压力）。
- [x] 热替换路由更新（4.5，LANDED 2026-09-25）：register_type 改替换语义——同名类型
      再注册 = 先在内存 router（drop_booth）与持久 EventRoute 表（routes_drop_booth）
      丢弃旧路由再按新 receives 装配，并回收该类型全部驻留实例/session（旧 source
      不再应答，下条消息按新代码冷启动；实例表是可丢弃热缓存，无数据损失）。审计顺
      带修出的真 bug：routes_drop_booth 原实现把 booth_id 拼在主键 [event_id][booth_id]
      的 event 段做前缀扫——只有 id 恰好相等才删对行，会误删他人路由（旧测试侥幸通过
      纯属 id 相撞）；改为走 by_booth 索引，错开 id 的回归锁 =
      mq_okm.rs::routes_drop_booth_targets_only_its_own_rows（旧代码红、新代码绿验过）。
      引擎侧锁 = echo.rs::re_register_replaces_routes（路由无重复、退役事件停止投递、
      持久表与 router 一致、执行换到 v2 代码）。PLAN:80 "needs a set() versioning path"
      以替换语义解决（同 ns 的 BoothDef put = 最新版本赢，无需版本链）。

### 跨仓备忘（prism 侧）

- 双编码调试姿态：JSON 保留为解码路径不作为线上默认；一个 action 模型、两个
  codec impl、一致性测试 round-trip 每个 Frame 变体过两种编码；不加第三种编码。
- 游戏服务端方向：房间制/回合制现在就能搭；实时动作类需先解决帧驱动 + 房间内
  并发（未立项）。

## 会话记录（2026-09-22，自 HANDOFF 简报合并）

背景：krystallizer 的会话存储原语统一为 channel 日志（人的聊天与 LLM 对话共用一个
容器），agent 上下文是它自己的投影（checkpoint + coverage 增量 + 未读）；消息键用
gravity 打的时间戳（不是 seq）；压缩是启发式策略引擎（缓存时钟 / 50% 预算 /
max-gap 分界），参数化 + 决策日志，回归调参推迟；多人模式独立成 ADR（成员身份两层、
kind=profile 画像、插话策略）；aura 侧补 timer wheel + cron 原语，gravity 按
channel 一实例。

### aura（本仓）——已完成

- [x] ADR-0016 timers 批次（ADR 文件 + ADR-0011 修订 + 本 PLAN 4.8 勾选）：已提交。
- [x] ADR-0018 两步存储方案（Value representation 条目，见 Phase 6.6）：已提交。
  实现要点与偏差（无 aura 侧 ByteStore 抽象、测试直接用 okm TestStore、
  SHA-256 key 方案被否改为 registry + MAX reduce、pkey 必须带 type 段、
  `set` 全字段 RMW）已记于该条目。
- 消费侧同步：okm ADR-0024（reduce 钩子接收解码后的 key）落地后，aura 已
  `cargo update` okm-core 并补 `scan_range` 转发；`MaxInstanceId` 等待 okm
  ADR-0023 预置组合子落地后收缩为 `MaxKeep` 声明（镜像字段随之退役）。

### krystallizer（~/world/krystallizer）——已完成

- [x] ADR-0008 unified channel log + ADR-0009 multi-party participation（en+zh）；
      ADR-0001/0002/0006 dated Update；PLAN Phase 2 重写 + 2.5/2.6/2.7。
      已按建议单提交落地（krystallizer 3660597）。

### 备注

- 规则留存位置：okm ADR-0018（EN+中文）＋ `okm-project-conventions` 技能（细则在
  `references/storage-value-model.md`）＋ `aura-dev` 技能；两仓 PLAN 有对应条目。
- 明确不做（本会话记录）：内部时间 epoch（okm）、补偿性 cron 连跑、无标签回归
  调参、session 第二容器、channel 域画像。
