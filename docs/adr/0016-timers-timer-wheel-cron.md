# ADR-0016: Timers — Timer-Wheel Delayed Emit and Cron Semantics

**Status**: Partially landed (2026-09-23: DelayQueue driver, two entry kinds, idle-from-completion, `max_exec` watchdog; 2026-10-09: the imperative `ctx_timer_register/cancel` host fns — memory tier only; 2026-11-09: the durable tier LANDED — ns 27 `TimerEntry`, ADR-0040, with `ctx.timer.register {durable: true}` writing the row and the driver rebuilding the wheel from it at boot; declarative `lifecycle.cron` still pending)
**Date**: 2026-09-22
**Chinese**: [0016-timers-timer-wheel-cron.zh-CN.md](0016-timers-timer-wheel-cron.zh-CN.md)

## Context

Aura booths are event-driven: work arrives as calls, emits, or queued
event jobs, and an instance with nothing arriving is evicted by the
idle-TTL evictor after its residency expires (per-type `idle_ttl`,
measured from job completion). The runtime has NO delayed-wake primitive:
signals exist only for "something happened", never for "nothing
happened for N seconds". Two krystallizer-driven needs expose this gap
(ADR-0008/0009 on the k10r side define the policies; the delivery
mechanism is aura's):

1. **Passive interrupt** (ADR-0009): an agent in a channel must decide
   "should I speak" after N seconds of silence — but silence emits
   nothing, so no event reaches the booth. The decision needs a signal
   that arrives BECAUSE nothing arrived.
2. **Compression trigger** (ADR-0008): the cache-clock check (e.g.
   "50 idle minutes of a 1-hour cache") needs a wake-up at a computed
   future instant.

The correction to the residency story: a gravity instance is
**per-channel, not per-user** — the unit that accumulates context and
makes speak/compress decisions is bound to one channel's log
(ADR-0008's projection is per (channel, member); keying gravity by
channel lets one user's different channels carry independent
checkpoints and cursors, and one agent serving several channels runs
as several independent instances). Partition-key routing
(`InstanceId = (booth_type, key)`, `@on(event, key_field)`), per-
instance TTL eviction, and per-instance state namespacing all already
support this shape — gravity registers with `key = channel_id` and an
`idle_ttl` strictly longer than its longest timer window.

## Decision

### 1. Per-realm scheduling on tokio-util `DelayQueue`

A per-realm **timer queue** built on `tokio_util::time::DelayQueue`: a
public hashed wheel exposing `insert_at(value, when) -> Key`,
`remove(&Key)` (O(1) cancel), and `poll_expired()` as a stream. ONE
driver task per realm owns the queue by value
(`while let Some(expired) = queue.poll_expired().await`) and acts on
each expired entry; the task count stays bounded. The wheel behind it
is hierarchical — 6 levels × 64 slots, level-0 slots 1 ms wide over a
64 ms range, each level multiplying the slot width by 64 (~4 s,
~4 min, ~4 hr, ~12 day), the top level reaching ~2 years — and an
entry tiers down one level per expiry sweep until level 0 hands it
out. Aura never touches the levels: it only calls `insert_at` /
`remove` / `poll_expired`, so no hierarchy semantics (and no per-level
tuning) live on our side. An expiry is observed as soon as the driving
`Sleep` wakes, at level-0 resolution (1 ms quantisation, subject to
scheduler latency) — not on the 5-second evictor tick, and without a
tick-shaped bound. There is no sub-millisecond or exact-wall-clock
guarantee; no consumer needs better than that.

The queue holds two entry kinds:

- **Deliver entries** `(deliver_at, target, tag, durable)` — wake a
  booth by delivering a `__on_timer` job (interrupt checks,
  compression wakes, cron). Durable entries are written to the
  StateStore's reserved namespace and re-`insert_at`-ed in the
  `on_wake` restore path.
- **Reclaim entries** `(deliver_at, target, kind)` — nobody is
  coming; take the resources back (idle-TTL eviction, execution
  watchdog). The expiry action is NOT a job delivery but
  `evict_instance` (on_sleep + executor drop + reply-with-timeout for
  the watchdog's target). Reclaim entries are always in-memory; they
  need CANCEL (new work arrived → abort the pending idle timer),
  which is `DelayQueue::remove` — the same cancel/re-arm API booths
  use.

Deliver entries follow the ordinary job path:

- **Delivery = a normal queue job.** A due timer is indistinguishable
  from an event job at the booth side (`QueuedJob` with a reserved
  handler name, e.g. `__on_timer`, payload carrying the timer's
  caller-chosen tag). No new callback concept.
- **Delivery counts as activity.** A timer job refreshes
  `last_activity` like any job — with one guard: a timer delivery does
  NOT reschedule itself implicitly. An booth that wants a recurring
  wake re-arms explicitly (see cron below); an booth that forgets to
  re-arm is evicted by TTL as usual. Timers keep an booth alive only
  as long as the booth keeps asking — no "self-feeding immortal"
  residency.
- **Persistence question — timers do NOT survive eviction, by default.** A timer is in-memory scheduling state like a queue, and a booth's durable truth lives in its declared collections. But two krystallizer cases need timers to outlive eviction: the 50-minute compression wake and cron. Therefore a timer may be registered as **durable**: a row in the realm's timer table `TimerEntry` at ns 27, in the event plane's band (ADR-0040), keyed by due time so that one range read rebuilds the wheel at boot and a due scan after a restart finds everything that came due while nothing was running. This is where the durability that this record once hung on `StateStore` lives — that store no longer exists (ADR-0026 §3), and the timer row is plane data (when to wake an instance), not booth data, so it does not belong in any type's ns either. In-memory timers die with eviction — the cheap default for short windows (interrupt checks) where a cold re-arm on the next real event is correct.
- **Coalescing**: multiple due timers for one target coalesce into one
  job with a list of due tags — a burst of scheduled checks costs one
  wake, not N.

### 2. Cancel and re-arm

A timer handle (`Key`) is returned at registration; `cancel(id)` removes
it through `DelayQueue::remove`, O(1). Re-arm = register a new timer
(cancel + register). The interrupt pattern is: on channel activity →
cancel pending silence-timer → evaluate → register a new silence-timer
for the idle threshold. The pattern is entirely booth-driven; the
runtime provides no implicit re-arming.

### 3. Cron semantics: declared in schema, computed by the booth, re-armed on delivery

Cron has TWO entry points, and the engine's role is deliberately narrow
in both:

- **Declarative** (`@cron` in `interface_schema`): the script declares
  a cron spec under `lifecycle.cron`; the Host introspects it at
  registration (the same path `lifecycle.idle_ttl` already takes,
  realm/src/lib.rs `extract_idle_ttl`) and translates the declaration
  into durable one-shot timers. Recurring engineering-style tasks
  ("loop engineering") are the direct consumer.
- **Imperative** (`ctx.timer.register`): wake times computed at
  runtime from conversation state (gravity's orthogonally-scheduled
  wakes) are not expressible as a static spec — the booth computes the
  next instant itself and registers it.

In both cases the booth owns the schedule SEMANTICS; the engine owns
only the delivery mechanics:

- The next-fire computation is a pure function over the schedule spec
  and `now`, executed booth-side (or by the Host at registration for
  the declarative form's FIRST fire — subsequent fires re-arm
  booth-side).
- On each delivery, the next instant is recomputed from **now** (not
  from the previous fire — a misfire/eviction gap skips missed
  schedules rather than bursting catch-up runs) and re-armed.
- Why the engine never interprets cron expressions at runtime: the
  expression set is the booth's policy, not the runtime's; the
  runtime's entire cron support is: durable one-shot timers survive
  eviction and are restored on wake, plus the registration-time
  translation of the declared spec. Everything cron-shaped reduces to
  that.
- Missed-fire policy is explicit: skip-and-jump-to-next (default; the
  compute-next-from-now rule above yields it for free). Catch-up
  bursts (fire once per missed tick) would require the runtime to
  track fire history — rejected: audit needs are served by the booth
  logging its own fires, and a catch-up burst after downtime is
  exactly the "compress everything at once" failure shape ADR-0008
  rejected.

### 3b. The ctx timer surface and host-fn namespacing

The timer API lives on ctx — ADR-0011's criterion decides it (instance-
identity-bound, Host-controlled); its Update note narrows the old
rejection to the blocking/self-scheduling forms. Surface:

- `ctx.timer.register(at, tag, durable) -> TimerId`
- `ctx.timer.cancel(id)`

The script-side ctx bridge (HostBridge host functions) stops using
flat `ctx_state_*` names and switches to dot-namespaced groups —
`ctx.store.get/set/delete`, `ctx.timer.register/cancel` — with the
injected group set discoverable by introspection (the carrier
enumerates the `ctx.*` groups it exposes; the schema can declare
which groups an instance actually carries). New injected capabilities
(metadata, effector targets) join as groups, not as flat-name accretion.

### 3c. Idle reclaim and the execution budget

`last_activity` keeps its meaning — the observation surface: last job
arrival — but no longer drives eviction. `run_job` cancels the
instance's pending idle reclaim entry on entry (work arrived) and
re-registers one at completion (`deliver_at = now + idle_ttl`). A long
turn (an LLM call eating the whole TTL) cannot expire the instance
mid-execution: the reclaim entry does not exist while the job runs.

`lifecycle.max_exec` (per-type, declared in `interface_schema` beside
`idle_ttl`) is the execution watchdog: it registers a reclaim entry at
job start (`deliver_at = now + max_exec`) and cancels it at completion.
Expiry means the instance exceeded its execution budget — evict
(on_sleep, executor drop) and fail the pending reply with a timeout
error. This is maximum-duration control, not idleness measurement; the
two concerns split cleanly along the two entry kinds.

The evictor task remains only for `pending_calls` deadline sweeps
(itself a future reclaim-entry migration); `evict_idle`'s linear scan
is deleted — eviction is timer-driven, O(expiry) not O(instances).

### 4. What does NOT change

- The per-instance eviction semantics (on_sleep hook, executor drop)
  are untouched; only the trigger moved — the idle reclaim entry,
  re-armed at job completion, replaces the linear scan on the
  5-second tick.
- `last_activity` remains the last-arrival observation surface; it no
  longer decides eviction (see §3c). Timers are ordinary jobs at the
  booth side.
- No async runtime timer per entry (no `tokio::spawn` + `sleep` per
  timer): one driver task per realm serves every entry, and durable
  entries restore through `on_wake`.

## Why Not

- **Per-user gravity instances**: splits one user's agent across
  channels; the projection, checkpoint, and cursor are per (channel,
  member) — keying by user would re-merge what ADR-0008 split, and a
  user in two active channels would contend over one context.
  Rejected.
- **Runtime-native cron (engine parses cron expressions)**: puts a
  scheduling DSL in the engine and forces fire-history tracking for
  catch-up semantics. The booth-side pattern needs only durable
  one-shot timers — everything else is the booth's policy. Rejected.
- **`tokio::sleep`-per-timer tasks**: unbounded task count, and timers
  silently vanish with the process (no durable registration path).
  One driver task serves ALL timers; the rejected shape is one task
  per timer. Rejected.
- **Implicit recurring timers**: an booth registering once and being
  woken forever hides residency cost and defeats idle-TTL. Re-arm
  stays explicit.

## Consequences

- New runtime surface: `register_timer(target, deliver_at, tag,
  durable: bool) -> TimerId`, `cancel_timer(id)`, a reserved
  `__on_timer` delivery path, and durable-timer restore in the
  activation/`on_wake` sequence. The same queue carries the reclaim
  entries behind idle-TTL eviction and the `lifecycle.max_exec`
  watchdog. Booth-facing: `ctx.timer.register / cancel` (ADR-0011);
  declarative `lifecycle.cron` in `interface_schema`
  translates to durable timers at registration. The ctx bridge host-fn
  table moves from flat `ctx_state_*` names to dot-namespaced groups
  (`ctx.store.*`, `ctx.timer.*`), introspectable.
- Gravity (per-channel instances) registers its interrupt and
  compression wakes through this surface; the krystallizer policy
  parameters (ADR-0008/0009) drive the durations, the aura timer
  drives the delivery.
- Due entries fire the moment they expire: the queue self-drives with
  one driver task per realm, and the 5-second evictor tick no longer
  scans a wheel.
- Timer resolution is level-0 (1 ms quantisation, scheduler latency on
  top); there is no tick bound and no sub-millisecond or
  exact-wall-clock guarantee.
