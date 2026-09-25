# PLAN

Design lives in the wiki (summaries) and ADRs; detailed design moved into this repo: `docs/design/storage.md` (storage architecture), `docs/design/realm.md` (field model), `docs/design/partitioning.md` (data partitioning), `docs/design/actor-api.md` (script actor reference), cross-referenced with the wiki. This file only sequences phases.

## Milestone A — Single binary engine

- [x] Phase 0 — Workspace skeleton: `crates/{engine,actor,realm,storage,config,cli}`; single-binary start, no external deps (no Docker / etcd / DB). Echo Actor: define → invoke → return.
- [x] Phase 1 — Actor runtime: Rust host + Tokio MPSC pipeline; per-Actor context (in-memory modify, on-disk sleep) — ctx surface per ADR-0011 (state/metadata/invoke only; emit/on, contracts, hooks stay off ctx); partition key routing; on_sleep/on_wake scale-to-zero (state → Fjall).
- [x] Phase 2 — Embedded languages: implemented by importing the probe runtime's carriers (steel/python/wasmtime/nushell, feature-forwarded) instead of an in-tree Polyglot Bridge — one carrier implementation serves the remote actuator and embedded actors. `ActorType::script(language, source, entry)`; script bodies run via spawn_blocking.
- [x] Phase 2.5 — Script-actor ctx bridge (CLOSED 2026-09-25: all four carriers bridge now — the nushell PTY file bridge landed, see 遗留节同题条目; ctx_state_* were later retired by ADR-0026 §3 — the bridge carries ctx_invoke / ctx_store_emit / ctx_interface_schema)
  - [x] Host functions exposed into carrier scripts: probe-runtime gains `HostBridge`/`HostFn` (`ExecRequest.host`); carriers marshal one JSON arg in / native value out
    - steel: builtins via `register_fn`, native hash/number marshal
    - python: `PyCFunction::new_closure` closures
    - nushell: subprocess cannot call back — bridge absent, carrier errors if demanded; TTL via interface_schema introspection works (one spawn)
  - [x] Realm `host_bridge_for`: `ctx_state_get/set/delete` (scoped to the instance's own state — cross-instance reach not expressible) and `ctx_invoke` (blocks on the unified call model inside spawn_blocking)
  - [x] Acceptance: steel/python scripts store+read state, invoke other actors, state survives eviction
  - [x] Remaining (LANDED 2026-09-23, revised shape): dynamic schema → the EventRoute PERSISTED registry + logical-time MQ keys — the revised landing replaces the sketched "event name maps to an okm ns" shape (per-event dynamic ns REJECTED: ns space is the application budget, an open event vocabulary would consume it unboundedly; the route registry is one fixed ns with the event resolved through the existing EventName registry)
    - EventRoute table (ns 35): pkey (event_id, actor_id), payload = actor_id mirror (index fields must be payload fields) + key_field + wildcard u8 sentinel; by_actor index — subscription facts survive restart, readable by ops without introspecting scripts; register_type persists as a side effect; the in-memory EventRouter stays the emit-matching hot path, the registry is the durable source of truth and the watermark denominator (compact_queue_locked reads routes_of_event)
    - MqData sort key: [event_id][part_id][time] — LOGICAL time (ms) monotonic per partition via the MqHead row (max(now_ms, last+1)), append O(1) (the max-scan is gone), skip-to-now reads the head; wall truth rides the payload
    - part_id: FNV-1a hash retained (partition values are user-data scale — a registry would grow unbounded); 0 RESERVED for the singleton partition (part_id_of; hash collision maps to 1)
    - not landed (deferred until a real one-to-many consumer appears): index-scan-based fan-out where key VALUES enter index entries (PLAN:19's original sketch) — the current MqData primary key IS the access method for per-event/per-partition scans; cursor-per-subscriber already covers one-to-many delivery
- [x] **Phase 2.6 — Resident VM per script instance (PRIORITY, closes the memory-state gap)** (CLOSED 2026-09-25 — acceptance items all test-locked: shared in-VM globals + VM drop on evict = echo.rs::idle_eviction_drops_the_resident_session; state survives eviction = state_survives_scale_to_zero; probe disconnect flips presence + in-flight fails as error value = remote_probe.rs::remote_probe_roundtrip extended with the abort/unregister/not-connected assertion)
  - Problem: spawn-per-job — every message re-loads source, builds a fresh VM, runs the entry, drops it
    - script globals never survive between messages
    - idle_ttl eviction loses nothing → per-type TTL / retention-window semantics meaningless for script actors
  - DONE (2026-09-16): one-shot execution REMOVED — all carriers resident
    - probe `carrier/session.rs`: `ResidentSession` trait (load/call) + `Sessions` registry; per-instance slot locks (registry lock never held across a call — `ctx_invoke` re-entry safe)
    - steel: engine cached per instance; python: module (interpreter namespace) cached per instance; nushell: resident PTY REPL (reedline CPR answering, file-based result protocol) — replaces the one-shot subprocess path
    - aura: `Realm` owns `Sessions`; `run_job` calls `with_session(instance_key)` — cold start loads, later calls reuse; sessions die with the realm (test isolation), hot replacement can evict selectively
    - eviction = drop the session; rebuild = re-instantiate + reload source (same as activation)
  - Remaining:
    - interim shim: REMOVED (2026-09-23) — event delivery addresses the handler by its concrete event name; no execute fallback
    - wasm: DONE (2026-09-23) — `Module` compiled once at session spawn + resident `Store`/`Instance` (`WasmSession`, see Phase 4.5 wasm carrier completion)
  - Phase 3 wire (2026-09-16): ctx bridge over the wire landed — Frame::Host round trip, gateway resolves host calls via pending_remote (call_id → instance), state/invoke scoped to the remote actor instance (engine/src/host_wire.rs); E2E: probe script's ctx_state_set/get + ctx_invoke round-trip through the wire.
  - Phase 3 wire (2026-09-16): `Body::RemoteProbe` — realm holds probe connections by node alias (`probes` + `pending_remote`); `run_job` sends Frame::Call over the wire and awaits the correlated reply; engine `probes.rs` gateway accepts probe dial-ins, registers by alias, correlates Result frames. E2E: real probe dials the gateway, invoke round-trips through the probe's resident session.
  - Lifecycle ownership: AURA owns the policy, PROBE owns the mechanics
    - aura decides WHEN a residency (and its VM) dies: realm-wide default TTL + per-type override + script-introspected value
    - eviction is instance-level (per-instance queue + residency set + partition serial semantics live in aura)
    - probe NEVER self-expires an actor VM session (two owners of one lifecycle = drift)
    - probe-side independent expiry applies only to probe-internal operations aura does not track
  - Affinity + offline (remote probes)
    - registry records the actor→probe binding: affinity is metadata, not a routing hop
    - requests follow the data (Phase 5 invariant); the registry answers "where is the residency now"
    - probe offline: (a) connection drop flips registry presence; (b) in-flight invocations fail with the call model's normal error-value semantics; (c) residency declared lost, not silently kept — recovery = re-activation on next connection (VM rebuilt, working set re-fetched); (d) while offline, messages queue in the realm's per-instance queue (bounded) or fail per the caller's tier
  - Probe parallelism: control-plane concern, not a probe threading model
    - same-node-serial queue semantics stay the default (two ops writing one file is a policy violation, not a scheduling bug)
    - parallelism = control plane expresses it as separate partitions/instances or explicit operation-declared concurrency; conflict responsibility at the caller/plane level
  - Acceptance: two consecutive invocations of a python/steel script actor share in-VM global state (counter in globals, not ctx_state); state still survives eviction via ctx_state; evicted instance's VM is dropped; probe disconnect flips registry presence and in-flight calls fail as error values
- [x] Phase 3 — Realm model: event namespace landed
  - emit routing: exact + wildcard-prefix (dotted names, `order.created`; wildcard requires the dot)
  - partition key extracted from event data; singleton `__singleton__` for wildcards
  - emits whitelist enforced at the Realm boundary (undeclared emit = error value; system/None bypasses — the whitelist constrains actors, not the host)
  - bounded dead-event ring for unmatched events
  - follow-ups: RouteMode composition primitives (on_join/on_batch/on_debounce); interface_schema dynamic (script-declared) form arrives with the Phase 2.5 ctx bridge
- [x] Phase 3.5 — Unified call model (CallSlot): `ctx.invoke()` with oneshot + `pending_calls` + `reply_to` is the single call mode for HTTP / realm Actor / remote Probe targets
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
- [x] **Phase 4.5 — Platform actor model (PRIORITY): Actor definitions live on the data plane, never the compile plane** (CLOSED 2026-09-25 — all work items landed; the platform's actor forms are script source + wasm artifact, definitions persist via the 4.5b upload lifecycle)
  - Ruling: the engine ships NO in-process Rust actor
    - framework mechanics (the evictor class) are plain realm logic, not actors; wrapping them as actors is a pointless detour
    - k10r/gravity-class Rust services ship as `.wasm` artifacts uploaded at runtime (`set(lang="wasm", bytes)`)
    - compiling them into the host binary would fork the platform per app (every new service = repackage; Agent apps adding features = rebuild aura), collapsing the platform into a framework
  - Work items
    - [x] wasm carrier completion (LANDED 2026-09-23): `WasmSession` (probe-runtime `carrier/wasmtime.rs`) — resident session, module compiled at spawn; CBOR over linear memory (host writes args via the guest's `aura_alloc`, calls `handler(ptr, len) -> i64`, unpacks `(ptr:u32)<<32|len:u32`); handlers = function exports named after their events; `interface_schema` explicit export wins else export-list derivation; ctx-bridge host imports under `aura_host` namespace, uniform `(i32, i32) -> i64` packed ABI, undeclared import = instantiation failure (capability refusal); source = WAT text or base64 `.wasm`; `ResidentSession` gained `as_any` for carrier-specific introspection. Tests: probe `tests/wasm_session.rs` (WAT fixtures, 7 tests)
    - [x] metadata declaration unified on the type: `ActorType.receives` (ReceiveDecl: event + key_field + wildcard) with `.on(event, key_field)` / `.on_wildcard(pattern)` builders; introspection writes onto the type at register; `register_type` assembles routes as a side effect — one declaration surface per type (emits: none, per ADR-0012). Hot-swap = replacement semantics LANDED 2026-09-25 (see 遗留节 热替换条目)
    - [x] remove the Rust-closure actor form (`ActorType::simple`) from the public API — deleted; cli demo + all engine tests migrated to script actors (steel; nushell for the slow handler). Body::Rust remains in the enum with no public constructor (framework-internal future use)
      - rewrite cli echo demo + engine tests onto script actors (wasm/steel/python) as the acceptance path
    - [x] NUSHELL RESOLVED → PTY residency (ruled 2026-09-15, prototype-verified; ctx bridge + residency fully landed 2026-09-25)
      - resident session: one PTY per actor instance running a long-lived `nu` REPL (`--no-config-file`); idle_ttl eviction = close the PTY
      - multi-entry: `use 'operation.nu' *` imports all exports; delivery addresses the handler by event name (exported fn names = event names)
      - session state: `$env` variables persist across calls within the resident process — the memory tier (vanishes at eviction); durable state goes through the ctx bridge (landed 2026-09-25, see below)
      - verified in prototype: env vars persist across sequential calls in one nu process; reedline emits `ESC[6n` cursor queries the host MUST answer (`ESC[row;colR`) or input hangs; ANSI/OSC output needs stripping (`--no-config-file` + winsize reduces noise)
      - implementation (carrier PTY mode): LANDED — long-lived PTY session per instance, per-call wrapper eval, reedline query answering, ANSI strip
      - capability position after this: nu actors = stateful-resident (2026-09-25: the PTY ctx bridge landed — durable state via ctx-store-emit like the other script carriers; cross-call $env is the memory tier)
- [x] **Phase 4.5a — PRIORITY CLEANUP: remove the Rust-closure actor form (`ActorType::simple`) from the public API, immediately after Phase 4.5 lands its replacement**
  - the engine ships NO in-process Rust actor — framework mechanics (the evictor class) are plain realm logic, not actors; wrapping them as actors is a pointless detour
  - Rust code becomes an actor through exactly one channel: compile to wasm and upload
  - concrete removals
  - cross-node semantics resolved (ADR-0013): federation, not metadata consensus — metadata is per-node (meta okm, control-plane single writer), user data stays home, inter-node identity via well-known protocol auth; Location Transparency deliberately rejected
    - delete `ActorType::simple` (or confine it to `#[cfg(test)]` scaffolding)
    - rewrite the cli echo demo (`crates/cli/src/main.rs` — two `ActorType::simple` registrations) onto a steel script actor
    - migrate engine tests (`echo.rs` / `events.rs` / `callslot.rs` / `fjall_state.rs` / `namespaces.rs`) onto script actors (wasm/steel/python) as the acceptance path
  - done = no `ActorType::simple` outside `#[cfg(test)]`; public docs describe one actor form per language (script source or wasm artifact)
- [x] **Phase 4.5b — PRIORITY: actor metadata lifecycle — introspect-once at upload, persist to the meta store, never re-introspect on the execution path**
  - Lifecycle model (authoritative)
    - UPLOAD (`set`) is its own lifecycle and may never execute
    - at upload the host introspects `interface_schema()` ONCE (a pure function; one carrier load, one call, result extracted, load discarded)
    - extracted metadata (receives / emits / lifecycle.idle_ttl / returns) is PERSISTED into the meta okm instance, keyed alongside the actor definition (`actor_defs`-adjacent; same versioning, content-hash dedup and rollback semantics as the script bytes)
    - EXECUTION never calls `interface_schema` — the schema is a static record in the metadata store; message handling loads the script (latest version) and calls the entry only
    - VERSION CHANGE (a new `set`) re-introspects once and updates the persisted metadata; until then the old metadata governs
  - Carrier-uniform: python/steel (loaded module), nushell (one spawn per call — introspection is one spawn running a generated `interface_schema` wrapper; the current "not wired for nushell" limitation is a TODO, not a capability gap), wasm (one module instantiation — same one-shot shape as nushell)
  - Corrections to the current implementation
    - [x] (a) `actor/src/persist.rs`: `PersistedActor` record (language/source/entry/idle_ttl/schema) persisted via `engine.register` (now returns `Result`); meta okm instance constructed per config (`meta_engine`/`meta_dir`); boot reload re-registers types from the meta store — metadata survives node restart
    - [x] (b) execution path introspection-free, locked by acceptance (definition + TTL survive restart on the same meta dir, immediately invocable)
    - [x] (c) nushell introspection works through the generic carrier path (generated wrapper calls `interface_schema(args)`; zero carrier changes) — acceptance: nu script declares `idle_ttl: 5m`, register seeds it
    - [x] (d) docs: realm.md §script persistence + actor-api.md state the three-lifecycle model (upload/introspect+persist — execute/read metadata — version change/re-introspect), replacing any "load once, call schema then entry" phrasing
  - Done = metadata survives node restart from the meta store; execution path provably schema-free; all four carriers (python/steel/nushell/wasm) deliver declared metadata through the same upload-time introspection contract
- [~] **Phase 4.5c — PRIORITY: multi-entry actors + event-queue semantics (corrects the single-entry model in the current implementation)**
  - Problem: the current model is asymmetric and wrong on two axes
    - definition: one `execute` entry, but emits are multiple exits — an actor with several handlers must split into several actors, duplicating shared logic
    - delivery: events are fanned out into per-actor mailboxes, which bakes in "an event belongs to an actor" — wrong for one-to-many (several actors listen to one event)
  - Ruling (restores the realm.md §on-decorator design + fixes delivery semantics)
    - MULTI-ENTRY: handlers declare `@on(event, key=...)` per handler (python decorator / steel `on` fn / wasm export convention) — one actor type, many handlers; shared logic stays in one place. NO standalone single-entry mode: a direct call (`ctx.invoke` / engine `invoke(target, handler, args)`) DECLARES the handler name in its payload — no reserved names, no implicit entry; event delivery addresses the handler by the event name
    - `interface_schema` is assembled IMPLICITLY and merged with an explicit partial declaration (not "derived, hand-written wins"): decorators own receives/wildcard_receives; the script contributes lifecycle etc.; the merged single function is the only thing aura calls — uniform across languages (rust/wasm: `#[on(x)]` generates the same implicit fn; steel/nushell hand-write it). EMITS are never collected or validated (ADR-0012): receiver set is a runtime fact — dead ring + delivery log are the observation surface; source parsing deferred until it serves a real consumer (Windmill criterion)
    - partition key declared on the decorator (`@on("add_to_cart", key="user_id")`), bound to the handler — not in a separate schema block
    - EVENT QUEUES replace per-actor mailboxes for event delivery: each queue holds one event family; @on-declared `key` → queue is per-(event, partition); no key declared → queue is per-event (singleton consumers)
      - an event belongs to NO actor; a queue may have multiple subscribers (one-to-many delivery is structural, not a fan-out simulation)
      - actor instances subscribe to queues per their @on declarations; serial-per-(actor-instance) semantics preserved by per-subscription cursor, not by owning the queue
      - PERSISTENT QUEUE RULING (2026-09-16): queues are embedded persistent partitions in okm, not broadcast channels — `[mq-data][event][part_id][time]` for events, `[mq-cursor][event][part_id][actor]{cursor}` for cursors (okm composite keys, zero new components). Emits are passively persisted on write (the passive-save half of the ctx.state dual-track); cursors advance per subscriber. Scale-to-zero no longer drops triggers: an evicted instance's backlog is delivered on re-activation. Slow consumers accumulate visible backlog (better than broadcast's silent Lagged loss); time-based catch-up (`skip-to-now`) is the backward-compatible relief valve — jump the cursor forward to the newest message, discard the stale backlog. Multi-subscriber fan-out stores the event ONCE per queue (N actors = N cursors into one partition), not N mailbox copies — the deduplication that per-actor mailboxes fundamentally cannot do. Backlog lifecycle follows the instance/namespace; permanently-departed instances' backlog is reclaimable by prefix scan. Replaces the interim tokio::broadcast implementation (broadcast semantics: late subscribers get nothing, slow subscribers get Lagged = silent segment loss — inconsistent with passively-persisted event data).
      - PARTITION ANNOTATION (2026-09-16): mq tables carry `#[kv_partition]` — `MqData` partition 1, `MqCursor` partition 2 (okm `#[kv_partition(N)]`, ADR-0014 §5: 2-byte `[0xFF][N]` escape key segment structurally disjoint from unpartitioned tables; Fjall routes handles per id so the append/watermark-delete and point-write compaction profiles never share a tree). `EventName`/`ActorName` registries stay unpartitioned (small, point-lookup). Atomic domain of a batch = one partition — emit append and ctx.state writes are deliberately not co-atomic (events are triggers, ctx_state is the truth).
      - RETENTION = min-watermark over ACTIVE subscribers (2026-09-16): a [ev][part] queue keeps only the range every active subscriber's cursor still needs — data before the minimum cursor is deleted on write-path compaction. The watermark's denominator comes from the ROUTE REGISTRY (4.5b-persisted @on metadata), never from the raw cursor keys: a permanently-departed actor's stale cursor must not pin the watermark forever. Deregistering an actor removes its cursor row; its backlog then falls below the watermark and vanishes with ordinary compaction — no separate reaper. Backlog depth is a live okm reduce count over the mq-data prefix (insert +1, watermark-compaction -1 unfold) — zero-scan operational surface, and the skip-to-now decision reads it directly.
      - HONEST SEMANTIC COST: the queue is a buffer, not a store — "at-least-once" holds only while every subscriber stays registered and consuming. A subscriber permanently deregistered with unconsumed backlog loses it. Consistent with the layering: ctx_state is the durable truth; the queue guarantees "alive = can catch up", nothing more.
    - emits: no hand-written whitelist, no static collection, no registration validation (ADR-0012) — an emit with no subscribers lands in the dead ring; that ring is the audit surface
  - Scope / order: touches realm delivery path (router → queues), actor type definition (decorators → derived metadata feeding 4.5b introspection), carrier contract (handlers are addressed by event name, not a single entry) — lands with/after 4.5 metadata unification so the derived schema feeds the same meta-store persistence
  - Progress
    - [x] step 1 — decorator-derived metadata + router seeding: python carrier injects an `@on` collector at import (identity decorator, `(event, key)` registry) and ASSEMBLES an implicit `interface_schema` merged field-wise with an explicit partial declaration (decorators own receives/wildcard_receives; the script contributes lifecycle etc.) — aura calls `interface_schema` uniformly across languages, no python branch; `engine.register` reads the full schema and seeds `router.on(event, type, key)` / `on_wildcard` per declaration (acceptance: exact + key-less + wildcard routes derive from decorators; merged schema carries decorator receives AND explicit lifecycle)
    - [x] step 2 (INTERIM, broadcast) — delivery path: per-(event, partition) broadcast queues replaced per-route submit-to-instance-mailbox; `@on` key → (event, partition) queue, no key → per-event singleton queue; emit activates matched route targets BEFORE sending; acceptance: one event, two subscriber types, both receive exactly once
    - [x] step 2b (persistent queues) — broadcast swapped for okm tables per the ruling: `realm/src/mq.rs` declares EventName/ActorName registries (open-ended names → numeric ids, resolved through the `by_name` text index) + MqData `[event_id][part_id][seq]` / MqCursor `[event_id][part_id][actor_id]` tables (`#[kv_ns]`, Table API over a StateStore→VirtualStorage bridge); payload is CBOR (new okm `FieldType::Bytes` — variable-length TLV raw bytes, added to okm RowEncode); emit persists passively (dead ring only for route-less events), consumer loop = backlog scan → run → cursor advance, re-activation replays. Follow-ups landed: min-watermark retention compaction (watermark denominator = route registry, evicted instances still count — their backlog replays; deregistered types fall out; runs on the emit path; 2026-09-23 the denominator now reads the PERSISTED EventRoute registry, not the in-memory router); logical-time keys + MqHead O(1) append + skip-to-now via the head row (2026-09-23, see Phase 2.5 Remaining). Remaining: reduce-based backlog depth.
    - [x] step 3 — steel collector: carrier binds an `on` builtin before the run (`(on "event" "key" handler)` records the declaration, returns the handler); `carrier::introspect(language, source)` dispatches — steel/python assemble the merged implicit+explicit schema, languages without collectors (nushell) fall through to the generic entry call on a hand-written `interface_schema`; wasm `#[on]` convention stays with Phase 4 link payloads (PLAN 4.5c wasm row)
    - [x] step 4 — DROPPED (ADR-0012): static emit collection + registration validation rejected — the lint catches only a subset the dead ring already reports better, warm-up/placement on an unguaranteed graph wastes runtime resources, and collected emits re-duplicate the emit call site (the same drift the whitelist had); may_emit whitelist check removed from the emit path
    - [x] step 5 — docs: actor-api.md bilingual rewritten to multi-entry model (lifecycle + event-queue semantics + @on examples per language) — landed; realm.md session-queue section + wiki §6.2/§mailbox updated (wiki aura-architecture §5 bullet + §6.2 lifecycle, stateless-agent-architecture probe adapter wording); 2026-09-23 realm.md emit walkthrough rewritten to the persistent-queue shape (concrete-name queue identity, wildcard concrete-name expansion via events_matching, MqHead logical time, min-watermark compaction on the emit path; broadcast-era code sketch removed)
  - Docs status: realm.md §on-decorator matches the ruling; actor-api.md bilingual rewritten (multi-entry lifecycle + event-queue semantics + @on/merge examples per language); wiki aura-architecture §5/§6.2/§6.3 and stateless-agent-architecture probe-adapter wording updated to event-queue semantics (2026-09-15)

- [x] Phase 4.8 — Timers (ADR-0016, docs/adr/0016-timers-timer-wheel-cron.md en+zh; ADR-0011 amended — blocking/self-scheduling stays rejected, delivery scheduling passes the criterion): timer wheel scanned by the evictor tick; due entries deliver as ordinary `__on_timer` queue jobs (同目标到期合并一次唤醒); delivery counts as activity, re-arm explicit (投递不隐式自我重排); memory tier (dies with eviction) + durable tier (StateStore reserved namespace, restored via on_wake); declarative `lifecycle.cron` in `interface_schema` (注册时内省翻译为持久定时器，运行时从不解释 cron 表达式，错过策略 = skip-and-jump-to-next) + imperative `ctx.timer.register/cancel`; ctx-bridge host fns move to dot-namespaced introspectable groups (`ctx.store.*`, `ctx.timer.*`); gravity 按 channel 一实例（ADR-0016 ruling）。
- [~] **Phase 4.9 — Type-scoped actor storage (PRIORITY, ADR-0026, docs/adr/0026-type-scoped-actor-storage.md en+zh)**
  - Ruling: storage isolation moves from the instance level to the TYPE level; the instance key keeps answering "who serially processes this message" and stops deciding storage layout. Terminology: routing-side `partition key` renames to **instance key** (aligns with `InstanceId`/`instance_key` in the state registry — one concept, one name; "partition" was the storage fact this ADR supersedes). Cross-node sharding vocabulary (Phase 5 shard map) keeps `shard` — node placement, not instance identity
    - each actor TYPE occupies one real okm ns (bounded declared vocabulary — satisfies the closed-vocabulary ns ruling; events/partitions stay registry+hash); low ns block reserved for aura (mq 30–35, meta/state 40–41), actor types allocate from a fixed base; allocation = `register_type` side effect via the type registry (the existing type_id assigner); ids never reused
    - instances are documents inside the type's ns (okm collection/document terminology; the type's ns plays a SQL-schema role): the `InstanceState` one-document-per-instance isolation shape is superseded; cross-instance aggregation inside one type = ordinary scan/reduce over the type's ns — projection actors retire for same-type aggregation, remain for cross-type precomputation
    - ctx.store rises to okm capability level: put / get / scan / reduce over the type's declared tables (exact op names set at implementation); field-level get/set/delete point model replaced; handles structurally bound to the type's ns at registration (cross-type access not expressible); nested invoke resolves target ns through the type registry, never caller-supplied keys
    - per-user realm isolation (Phase 3.6 mechanism — the axis ADR-0028 renamed namespace → realm; PrefixStore since retired, `MqStore::for_realm` is the layer) stays, orthogonal — it prefixes the whole engine beneath the type nss
    - serialization/partition routing/timer/mq semantics untouched; existing instance-state bytes discarded, no migration (ADR-0018 precedent)
  - Schema declaration (rides the 4.5b upload lifecycle; execution path never regenerates):
    - python: decorator over the okm type definitions derives the schema, merged into `interface_schema` (same implicit+explicit merge as @on)
    - steel/nushell: hand-written schema literal in interface_schema; handlers call ctx store functions directly (nushell's PTY host bridge landed 2026-09-25 — file-round-trip req/resp, same op set as steel; see PLAN 4.5 NUSHELL item)
    - wasm: same op set over CBOR per the landed carrier ABI
    - merged schema persists on ActorDef (dynamic segment) at registration; `ctx.interface_schema` reads the persisted copy (reflection for handlers; dev-time completion served by the LLM receiving interface_schema)
  - Protocol naming (aura side docs-only; prism side lands with Phase 8): the wire/concept layer carries only `ev` in BOTH directions — no direction field; emit/on are per-end implementation details (aura @on+emit; prism client ws.send/ws.on); prism = aura event semantics extended to the user end
  - Work items
    - [x] terminology rename: routing-side `partition key` → `instance key` — code LANDED earlier (c800a3b: Route.instance_key_field, ReceiveDecl key fields, router/emit naming); design docs' live passages swept 2026-09-25 (partitioning en+zh, realm, modeling en+zh, actor-api en+zh, ADR-0025 identity sentence); historical PLAN phase entries stay unedited
    - [~] op set narrowing: protocol types landed (`aura-actor/src/store_emit.rs` — StoreOp/StoreOpKind, JSON wire, Collection-semantic layer); put/get/delete_document + put/get/delete FIELDS (dynamic-segment bridge: okm-dynamic gains put_fields/get_fields/delete_fields mirroring the typed Collection — byte-identical entries incl. the shared field-name dictionary, locked by a cross-mode test; okm-dynamic Value carries Obj/Array so composite field values ride the dynamic segment natively, schema-declared fixed-width paths reject them upstream) + SCAN (schema-declared AccessMethod) + REDUCE GET (count/high_water/lowwater presets) EXECUTE through okm-dynamic DynamicCollection (`realm/src/store_exec.rs`); host-language reduce logic objects register through bindings, not schema data. No bypass guard: the developer has full control, primitive misuse is self-sabotage
    - [x] ctx bridge host fns + `ctx.interface_schema` read (LANDED 2026-09-24, pulled ahead of the per-language schema item): `Ctx` carries a store-emit handle + the persisted schema copy (injected by `ctx_for` as DATA — the emit handle clones the plan and the mq handle, no realm deref at emit time, no blocking_lock in spawn_blocking); `register_type` resolves the plan once from the type registry + the persisted interface_schema (failure = no plan = no ctx.store surface, error values never panics); host fns `ctx_store_emit` (StoreOp serde form) + `ctx_interface_schema` (persisted copy) registered in the bridge; engine.register persists the introspected schema with the definition.steel e2e: declare `storage.collections` → put/get round trip → schema read back (store_emit_roundtrip_and_interface_schema_read). Cross-repo prerequisites landed: okm 0c2a354 (nTLV composites everywhere — Obj-in-Array encode/decode threaded the name resolver; previously an Obj inside an Array encoded empty and decoded None, silently dropping the persisted interface_schema) + probe 2397149 (steel introspection registers ctx-fn stubs scoped to the throwaway engine — steel resolves free identifiers at define-compile, so any ctx-using script failed to load during introspection; stubs never shadow the real host fns in resident sessions)
    - [x] registry→ns allocation: `TypeName.ns` assigned at first registration (`ACTOR_NS_BASE=100` + id, never reused); `meta::ns_of` resolve; unit test locks stability/distinctness (b0013b7)
    - [x] InstanceState retirement (LANDED 2026-09-24, probe bbefac8 + aura 7828031): `realm/src/state.rs` deleted whole (StateDocumentStore/InstanceState registry, by_key index, watermarks); `StateStore` trait + `SharedStore` + `Ctx.state` die in aura-actor (storage addressing leaves Ctx — ctx carries self_id + invoke + store-emit handle + interface_schema only); `PrefixStore` dies with the trait (4.10: if it revives it wraps MqStore); `ctx_state_get/set/delete` host fns + engine host_wire `HostOp::State` arms deleted (probe side: protocol variants + remote.rs bridge arms). No backward-compat sugar — the ruling: retirement is justified by the MODEL (point document superseded by the collection surface), and "external users already exist" is as invalid as "no external users yet" as an argument either way; correctness of the model is the only input. `register_in` fixed in the same pass: it bypassed register()'s introspection+persist path, so namespaced types could never get a ctx.store plan (invisible while ctx_state_* needed no schema). Tests migrated onto the collection surface (events.rs pattern: declared collection + RMW + assert through an invoke read-back); `script_state_survives_eviction` deleted as an exact duplicate of the rewritten `state_survives_scale_to_zero`; remote_probe keeps invoke-only (execution nodes hold no state; ctx_store_emit needs a plan = 4.5b)
    - [x] per-language schema declaration (LANDED 2026-09-24, okm 80327a1..a64a9e0 + probe d5a8b54 + aura 3873240): the python DSL lives in okm (bindings/okm-python okm_schema.py — @KeyEncode/@DocumentEncode classes mirror the Rust derive, type annotations drive the layout; @ok_ref/@ok_ns/@ok_layout/@ok_index metadata; assembler emits the exact CollectionSchema serde, cross-checked byte-equal against CollectionSchema::of); okm-python publishes it as OKM_SCHEMA_PY (pyo3 extension feature-gated so rlib consumers embed the module without linking the extension); probe's python carrier execs the DSL into actor scripts and merges assemble_module's storage block into interface_schema (decorator storage wins over explicit; empty block omitted so explicit-only declarations survive merge_schema's or_insert); no @ok_ns = actor-side auto allocation (aura injects the registry-allocated ns into the plan — the declaration never carries it); index slots follow SOURCE declaration order (stamp list is bottom-up reversed, assemble reverses it); variable-width fields locate at most once and only LAST in an index fields/includes list; steel/nushell hand-written literal form already exercised by the aura tests; aura e2e: decorator-declared collection → ctx_store_emit RMW → persisted ActorDef schema (py_schema.rs). wasm (okm compiles INTO the module, static derives, no dynamic schema form) split to its own item
    - [x] wasm schema path (LANDED 2026-09-25, probe actor-guest crate + aura raw emit arm): okm compiles INTO the module — `actor-guest` provides `EmitStore` (VirtualStorage whose every primitive is one `aura_host.emit` host round trip carrying okm-wire OpFrame/OpResponse bytes; no new wire format) + `collection_entry::<K,R>()` serializing the compiled CollectionSchema into the interface_schema storage block at upload; `counter_actor` example is the rustc-compiled fixture. Carrier: `WasmSession` gains the `emit` RAW-BYTE host arm (request/reply skip the CBOR value marshal; JSON number-array carries the bytes losslessly) and `carrier::introspect`'s wasm branch satisfies the import with a STUB emit (steel register_ctx_stubs precedent — the schema export is pure, the plan doesn't exist at introspection). aura: `MqStore::ns_raw(inner, ns)` (2-byte type-ns prefix raw handle) + `host_bridge_for` gains `wasm_raw` — the emit arm executes OpFrames against the type's RAW ns engine plane (no Collection-op layer; the trusted static-mode writer IS the module, no-bypass-guard ruling). e2e: register → introspect persists compiled schema → in-module Collection RMW through the bridge → document survives eviction (aura wasm_guest_storage.rs); probe wasm_guest_storage.rs runs the same bytes against TestStore
    - [x] `ctx.interface_schema` read of the persisted copy (LANDED with the ctx-bridge item 2026-09-24, see that entry — echo.rs `store_emit_roundtrip_and_interface_schema_read` locks it)
    - [ ] docs: storage.md/partitioning.md rewrite LANDED 2026-09-24 (018ff00 — instance-document passages superseded across actor-api/storage/partitioning/realm/modeling en+zh); remaining: prism-facing `ev` naming noted in ADR-0017's prism-side scope (wiki aura-architecture.md synced 2026-09-25 — per-field state keys / meta okm instance / partition-key wording swept to the collection surface)
- [x] **Phase 4.10 — Probe affinity + realm demotion (PRIORITY after 4.9; companion to ADR-0026; the axis ADR-0028 renamed namespace → realm)** (CLOSED 2026-09-25 — the ADR-0015 dependency resolved by attribution, not by building a second trust plane in aura)
  - Ruling: the probe is an actor's EXECUTION portion — it follows the actor, not the user. The tenant assumption (users exist) leaked into the base layer and is removed
    - probe binding is actor-TYPE affinity (the 2.6 registry already records actor→probe bindings; affinity is metadata, not a routing hop): an actor type names its execution capacity; the probe never asks "which user"
    - node trust is deployment-level and rides ADR-0015 (ed25519 node identity): "may this machine execute" is separate from "whose user is this" — the 3.6 user-credential derivation pointed the wrong way
    - user separation is the APPLICATION's concern: gravity distinguishes users through its own mechanism (user-organized types/instances, or sender metadata in the payload per the ADR-0017 amendment — identity rides payload metadata, never Ctx). The framework neither provides nor presupposes a user dimension
    - no-user applications are first-class: an intranet distributed-compute deployment puts one probe per node, registers affinity, and the actor side shards tasks — no user concept appears
  - Realm demotion (amends Phase 3.6): the prefix-isolation mechanism survives as an APPLICATION-AVAILABLE realm primitive (construction-time prefix isolation — the structural guarantee is the value), but its binding dimension is the application's choice — gravity may bind user, a compute project binds nothing; "probe registration credential = user credential → realm derived" is superseded
  - Consistency with ADR-0026: after type-scoped nss, multi-tenant user isolation (when an application wants it) is the application organizing types/keys — the framework's isolation units are exactly two: type ns (storage) and instance serialization (routing); user is not among them
  - Work items
    - [x] probe registry: type-affinity records ARE the binding surface (verified — `Body::RemoteProbe { node_alias, .. }` rides the ActorType, the type definition IS the binding record; no new table needed); the user-credential→namespace path at registration does not exist in code (the gateway register arm discards credentials, `register_in` takes an explicit realm string) — the only credential-derivation trace was a stale lib.rs doc comment, rewritten
    - [x] `register_in`/`call_in`/`emit_in` surfaces re-documented: doc comments rewritten (realm = explicit application decision, binding dimension the app's choice; realm_set module header + engine field comment aligned); partitioning.md §3 key-layout realm passage + en twin, storage.md federation line, ADR-0026 §1 en+zh line updated to the demoted/landed shape; ADR-0013 got an errata note (decision archive, body untouched); wiki stateless-agent probe-adapter paragraphs swept to affinity + node-identity trust
    - [x] ADR-0015 dependency RESOLVED BY ATTRIBUTION (2026-09-25): the trust story's aura residue (replacement discipline + startup disclosure) is LANDED (see 遗留节 ADR-0015 条目); the handshake/registry/endpoints ride the prism gateway (prism PLAN Phase 1.8) — registration discards credentials by design until that mounts, and this phase's rulings do not wait on it
    - [x] docs: partitioning.md realm passages + wiki probe-adapter wording swept; 0026 §1 line updated to the demoted shape (LANDED 2026-09-25, e7c1939 + follow-up)

- [x] **Phase 4.11 — Content-addressed code delivery (ADR-0027, docs/adr/0027-content-addressed-code-delivery.md en+zh)**
  - Ruling: one payload shape — `CodePayload` enum deleted, `ToolCall.code: CodeRef { url, sha256 }`; `version` dropped (hash URL is its own invalidation policy), sha256 asserted by the frame (never parsed from the URL). `CodeBlob` (ns 42) lives in meta.rs beside ActorDef — pure content rows (key = 32B hash, value = bytes; no name/version/FK); ActorDef.source → code_sha256 (the content hash IS the version identity — no second counter). Probe caches by hash (discardable hot layer, same tier as the resident session); serving endpoint `GET /code/{sha256}` = prism's static surface (immutable, no auth by default — the hash is the capability; confidentiality = deployment choice, never a new code ACL)
  - Work items (LANDED 2026-09-25 — probe + aura coordinated, protocol is a path dep)
    - [x] probe-protocol: CodePayload enum deleted; `ToolCall.code: CodeRef { url, sha256 }` (version dropped; hash asserted by the frame). probe remote.rs: `CodeCache` per-hash fetch cache (hits==1 across repeat calls locked by remote.rs::code_ref_fetch_verify_cache_and_mismatch_rejection); in-process paths unchanged
    - [x] aura: CodeBlob (ns 42, meta.rs — pure content rows, Bytes payload field) + ActorDef.code_sha256 (key-discipline fixed [u8;32]); persist() writes blob BEFORE publishing the definition pointer; boot reload hydrates source by hash and ERRORS on a missing blob (definition without content = corruption, not empty program). RemoteProbe types: put_blob at register (no definition row — 4.5b scope is script actors; the blob is their bytes' only home). `PersistedActor` seam unchanged (source at the seam, hash at the row)
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

## Milestone B — Agent base

- [ ] Phase 6 — Turn-executor Actor hosting: Gravity as Actor type (partition key = session_id; same-session serial, cross-session parallel). Out of scope here — implemented in the gravity repo, hosted via this phase's contract.
- [~] Phase 6.5 — Resident execution windows (retention-period dwell): replaces strict single-shot release
  - [x] MECHANISM LANDED: idle TTL sunk to ActorType (`idle_ttl: Option<Duration>`, `with_idle_ttl` builder; per-type override with realm-wide default fallback) — the retention window IS the turn-executor's per-type TTL, no second mechanism
  - [ ] turn-executor type wiring (Gravity hosting, Phase 6) declares its long TTL
  - [ ] same-session consecutive tool calls fill via in-memory oneshot (hot loop: zero persistence per call); session persisted + executor released at turn end or retention expiry; a new same-session turn within the window reuses the resident executor (skips session fetch)
  - Stateless semantics intact — state externalization (executor holds no session state) is what "stateless" means; the resident is a discardable hot cache, rebuildable from the event stream. Persistence delta: call_id only.
  - Probe Actor type hosting: actor_type = Probe, partition_key = node_id; the connection plane adapts outbound WS frames to Realm queue semantics (frame down = event delivery, frame up = reply_to return via `resolve_call`) — adapter, not a bypass.
- [ ] Phase 6.6 — Storage Actor (decision recorded here; never filed as a numbered ADR — "ADR-0010" previously referenced here now denotes the timer ADR, docs/adr/0016): host `#[kv_storage]` executor instances
  - one declared instance per application (ns = app_id/tenant_id prefix)
  - surface is exactly one method: frame in (op + bytes) → scan bytes out; arrival path (outbound WS / realm events / in-process direct call) is the caller's business, invisible to the executor
  - the receiver holds no OKM semantics: prepend declared prefix, execute, fill back
  - structural isolation: handles are prefix-bound at construction; namespace escape is not expressible
  - [x] Value representation (ADR-0018, 2026-09-20 — accepted; LANDED 2026-09-22): `StoreAsVirtual` (the base64-in-JSON adapter) deleted; the mq tables and the actor state documents bind to ONE okm engine (`MqStore` = okm `FjallStore` in production, okm `TestStore` in tests — no aura-side storage abstraction, no JSON container); actor state is one document per instance (`realm/src/state.rs`: registry pattern — pkey `InstanceStateKey {type_id, instance_id}`, type via the shared ActorName registry, (type_id, key) → id via the `by_key` text index (variable-length key terminal, ADR-0005) with the next id from a MAX reduce over `type_id` — no scan, ids never reused (unfold = keep); the raw key rides the declared `instance_key` field; the hand-written MaxInstanceId logic is the seed use case for okm ADR-0023's preset combinators — `MaxKeep<F>` will replace it when okm lands them); JSON ↔ `DynamicValue` conversion lives ONCE in `realm/src/value.rs` (the only seam). Clean break: existing mq/state bytes are discarded, no migration. Namespace isolation = prefix-bound `MqStore::namespaced` handles. Meta plane (ADR-0025 Plan A, 2026-09-22): actor definitions are `ActorDef` rows in the DATA plane's okm instance (`realm/src/meta.rs`, ns 41, beside mq/state) — the separate meta instance, `meta_engine`/`meta_dir` config and `Engine.meta_store` field are GONE (one engine, one directory). Identity = registry pattern inside the plane (`TypeName` by_name index + MAX watermark reduce; ids never reused); Options as sentinel encodings (entry empty / ttl 0 / schema empty). The introspected schema rides as verbatim JSON text — an interface artifact (the LLM/script-side contract), not a storage encoding. `aura-storage` crate DELETED (no consumers left). Plan B (REJECTED 2026-09-23, revised): the stateless-executor framing contradicted aura's compute-storage-integrated identity — an instance owns its local storage permanently; the single okm instance is terminal, not transitional (see docs/adr/0025). Dead mq/state keys on existing deployments are simply abandoned (clean break).
  - **WITHDRAWN (2026-09-20)**: the probe-side placement of the receiver was removed — `Frame::Kv` / `KvFrame` / `Frame::KvRefused` are gone from probe-protocol, and with them `Realm::kv_pending`, `Realm::kv_round_trip` / `kv_round_trip_within`, `KV_ROUND_TRIP_TIMEOUT`, the gateway's two KV arms and the `kv_round_trip.rs` test. The probe holds no storage: hosting an engine on an execution node adds a directory to place, size and back up plus an engine lifecycle, while every operation is delivered per call and holds nothing between calls. The key prefix is likewise not an execution-node concern — the control plane derives it from the sender's identity and business logic (Gravity's data = its own ns, then the partition id, then the event id, resolved by lookup). ADR-0010 puts the receiver on the Aura node itself; a remote execution node was a second placement of the same role. The script-side persistence requirement is served by the ctx bridge (`host_wire.rs` → the realm store), the project's founding shape — and the probe is deliberately given no data-plane credentials (it runs untrusted code in a container), so an engine there contradicts the stance rather than only adding operations work.
- [ ] Phase 7 — Probe embedding: container execution base (heavy-isolation end of the Wasmtime lineage) as an in-realm base component; probe repo deploys as remote actuator via outbound registration.
- [ ] Phase 8 — Prism hosting: WS gateway as Aura-resident component (client connections pin here, not on Gravity); turn delivery = realm events. Prism repo owns the protocol, this repo owns the connection plane. Protocol/identity/codec design recorded in prism's ADR-0017 (`~/world/prism/docs/adr/0017-prism-connection-plane.md`).

Deferred gates:

- MQ decomposition: no standalone queue component — boundary-queue needs (external delivery, audit log, consumer retry) via S3-as-truth + KV metadata.
- invoke.toml external HTTP endpoints: only after realm-internal calls are complete (address vs program judgment — program/embedded is the default extension unit).

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
- [x] meta schema 动态段化（a0b0c03）：`ActorDef` 删 `schema: String`，schema 以
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
      再注册 = 先在内存 router（drop_actor）与持久 EventRoute 表（routes_drop_actor）
      丢弃旧路由再按新 receives 装配，并回收该类型全部驻留实例/session（旧 source
      不再应答，下条消息按新代码冷启动；实例表是可丢弃热缓存，无数据损失）。审计顺
      带修出的真 bug：routes_drop_actor 原实现把 actor_id 拼在主键 [event_id][actor_id]
      的 event 段做前缀扫——只有 id 恰好相等才删对行，会误删他人路由（旧测试侥幸通过
      纯属 id 相撞）；改为走 by_actor 索引，错开 id 的回归锁 =
      mq_okm.rs::routes_drop_actor_targets_only_its_own_rows（旧代码红、新代码绿验过）。
      引擎侧锁 = echo.rs::re_register_replaces_routes（路由无重复、退役事件停止投递、
      持久表与 router 一致、执行换到 v2 代码）。PLAN:80 "needs a set() versioning path"
      以替换语义解决（同 ns 的 ActorDef put = 最新版本赢，无需版本链）。

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
