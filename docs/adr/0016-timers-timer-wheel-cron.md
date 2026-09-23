# ADR-0016: Timers — Timer-Wheel Delayed Emit and Cron Semantics

**Status**: Accepted (design; implementation pending)
**Date**: 2026-09-22
**Chinese**: [0016-timers-timer-wheel-cron.zh-CN.md](0016-timers-timer-wheel-cron.zh-CN.md)

## Context

Aura actors are event-driven: work arrives as calls, emits, or queued
event jobs, and an instance with nothing arriving is evicted by the
idle-TTL evictor after its residency expires (per-type `idle_ttl`,
5-second evictor tick). The runtime has NO delayed-wake primitive:
signals exist only for "something happened", never for "nothing
happened for N seconds". Two krystallizer-driven needs expose this gap
(ADR-0008/0009 on the k10r side define the policies; the delivery
mechanism is aura's):

1. **Passive interrupt** (ADR-0009): an agent in a channel must decide
   "should I speak" after N seconds of silence — but silence emits
   nothing, so no event reaches the actor. The decision needs a signal
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
(`InstanceId = (actor_type, key)`, `@on(event, key_field)`), per-
instance TTL eviction, and per-instance state namespacing all already
support this shape — gravity registers with `key = channel_id` and an
`idle_ttl` strictly longer than its longest timer window.

## Decision

### 1. Timer wheel + eviction-time deadline computation

A per-realm **timer wheel**: each entry is
`(deliver_at, target: InstanceId, payload)`. The existing evictor tick
(5s) additionally scans the wheel and delivers due entries as ordinary
queued jobs to the target instance.

- **Delivery = a normal queue job.** A due timer is indistinguishable
  from an event job at the actor side (`QueuedJob` with a reserved
  handler name, e.g. `__on_timer`, payload carrying the timer's
  caller-chosen tag). No new callback concept.
- **Delivery counts as activity.** A timer job refreshes
  `last_activity` like any job — with one guard: a timer delivery does
  NOT reschedule itself implicitly. An actor that wants a recurring
  wake re-arms explicitly (see cron below); an actor that forgets to
  re-arm is evicted by TTL as usual. Timers keep an actor alive only
  as long as the actor keeps asking — no "self-feeding immortal"
  residency.
- **Persistence question — timers do NOT survive eviction, by
  default.** A timer is in-memory scheduling state like a queue; the
  actor's durable truth lives in its StateStore. But two krystallizer
  cases need timers to outlive eviction: the 50-minute compression
  wake and cron. Therefore: a timer may be registered as **durable**
  (written to a reserved StateStore field namespace, restored on
  activation via `on_wake`). In-memory timers die with eviction — the
  cheap default for short windows (interrupt checks) where a cold
  re-arm on the next real event is correct.
- **Coalescing**: multiple due timers for one target coalesce into one
  job with a list of due tags — a burst of scheduled checks costs one
  wake, not N.

### 2. Cancel and re-arm

A timer handle (id) returned at registration; `cancel(id)` removes it.
Re-arm = register a new timer (cancel + register). The interrupt
pattern is: on channel activity → cancel pending silence-timer →
evaluate → register a new silence-timer for the idle threshold. The
pattern is entirely actor-driven; the runtime provides no implicit
re-arming.

### 3. Cron semantics: declared in schema, computed by the actor, re-armed on delivery

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
  wakes) are not expressible as a static spec — the actor computes the
  next instant itself and registers it.

In both cases the actor owns the schedule SEMANTICS; the engine owns
only the delivery mechanics:

- The next-fire computation is a pure function over the schedule spec
  and `now`, executed actor-side (or by the Host at registration for
  the declarative form's FIRST fire — subsequent fires re-arm
  actor-side).
- On each delivery, the next instant is recomputed from **now** (not
  from the previous fire — a misfire/eviction gap skips missed
  schedules rather than bursting catch-up runs) and re-armed.
- Why the engine never interprets cron expressions at runtime: the
  expression set is the actor's policy, not the runtime's; the
  runtime's entire cron support is: durable one-shot timers survive
  eviction and are restored on wake, plus the registration-time
  translation of the declared spec. Everything cron-shaped reduces to
  that.
- Missed-fire policy is explicit: skip-and-jump-to-next (default; the
  compute-next-from-now rule above yields it for free). Catch-up
  bursts (fire once per missed tick) would require the runtime to
  track fire history — rejected: audit needs are served by the actor
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
(metadata, probe targets) join as groups, not as flat-name accretion.

### 4. What does NOT change

- The idle-TTL evictor and its 5-second tick gain the wheel scan; the
  eviction semantics (per-instance, on_sleep hook, session drop) are
  untouched.
- `last_activity` semantics stay "last job arrival"; timers are just
  jobs.
- No async runtime timers per entry (no `tokio::spawn` + `sleep` per
  timer): the wheel is scanned by the existing tick — bounded memory,
  one task, and the 5-second granularity is fine for both consumers
  (seconds-scale interrupt windows and minute-scale compression/cron
  wakes).

## Revision (2026-09-23): unified scheduling on tokio-util DelayQueue; reclaim-type timers; idle measured from completion

Implementation review of the residency/timeout surface (driven by the
gravity turn-executor) amends three rulings; the original text above is
preserved as decided.

**1. The wheel is tokio-util's `DelayQueue` — not hand-rolled, not
tick-scanned.** `DelayQueue` is a public hashed-wheel (the same
`Wheel` structure this ADR sketched) exposing exactly the needed
trio: `insert_at(value, when) -> Key`, `remove(&Key)` (cancel,
O(1)), and `poll_expired()` as a stream. It drives itself (one
internal `Sleep`), so the 5-second evictor tick no longer needs to
scan the wheel: due entries fire the moment they expire, not on the
next tick. The task count stays bounded — ONE driver task per realm
(`while let Some(expired) = queue.poll_expired().await`). This
supersedes §4's "no per-timer async task / wheel scanned by the
existing tick" clause: that clause was the price of hand-rolling;
the library removes the price. §4's Why-Not entry ("one tokio::sleep
task per timer") remains rejected for the reason given (unbounded
task count, no persistence path) — the driver is one task for ALL
timers, which that entry never considered.

**2. Two entry kinds.** The scheduling surface turned out to have two
shapes, and the wheel holds both:

- **Deliver entries** `(deliver_at, target, tag, durable)` — wake an
  actor by delivering a `__on_timer` job (interrupt checks,
  compression wakes, cron). Durable entries are written to the
  StateStore's reserved namespace and re-`insert_at`-ed in the
  `on_wake` restore path, exactly as §1 ruled.
- **Reclaim entries** `(deliver_at, target, kind)` — nobody is coming;
  take the resources back (idle-TTL eviction, execution watchdog).
  The expiry action is NOT a job delivery but `evict_instance`
  (on_sleep + session drop + reply-with-timeout for the watchdog's
  target). Reclaim entries are always in-memory; they need CANCEL
  (new work arrived → abort the pending idle timer), which is
  `DelayQueue::remove` — the same cancel/re-arm API actors use.

**3. idle_ttl is measured from job COMPLETION, not arrival.**
`last_activity` keeps its meaning (observation surface: last job
arrival) but no longer drives eviction. Instead: `run_job` cancels the
instance's pending idle reclaim entry on entry (work arrived), and
re-registers one at completion (deliver_at = now + idle_ttl). A long
turn (an LLM call eating the whole TTL) can no longer expire the
instance mid-execution — the reclaim entry simply does not exist while
the job runs. The eviction-race clause this supersedes never existed
in the original text; it was the flaw found when wiring residency to
this ADR.

**4. The execution watchdog is a reclaim entry with a declared
budget.** `lifecycle.max_exec` (per-type, declared in
`interface_schema` beside `idle_ttl`) registers a reclaim entry at job
start (deliver_at = now + max_exec) and is cancelled at completion.
Expiry = the instance exceeded its execution budget: evict (on_sleep,
session drop) and fail the pending reply with a timeout error. This
is maximum-duration control, not idleness measurement — the two
concerns split cleanly along the two entry kinds.

The evictor task remains only for `pending_calls` deadline sweeps
(itself a future reclaim-entry migration); `evict_idle`'s linear scan
is deleted — eviction is now timer-driven, O(expiry) not
O(instances).

## Why Not

- **Per-user gravity instances**: splits one user's agent across
  channels; the projection, checkpoint, and cursor are per (channel,
  member) — keying by user would re-merge what ADR-0008 split, and a
  user in two active channels would contend over one context.
  Corrected to per-channel.
- **Runtime-native cron (engine parses cron expressions)**: puts a
  scheduling DSL in the engine and forces fire-history tracking for
  catch-up semantics. The actor-side pattern needs only durable
  one-shot timers — everything else is the actor's policy. Rejected.
- **`tokio::sleep`-per-timer tasks**: unbounded task count, and timers
  silently vanish with the process (no durable registration path).
  The wheel scan shares the existing tick. Rejected.
- **Implicit recurring timers**: an actor registering once and being
  woken forever hides residency cost and defeats idle-TTL. Re-arm
  stays explicit.

## Consequences

- New runtime surface: `register_timer(target, deliver_at, tag,
  durable: bool) -> TimerId`, `cancel_timer(id)`, a reserved
  `__on_timer` delivery path, and durable-timer restore in the
  activation/`on_wake` sequence. Actor-facing: `ctx.timer.register /
  cancel` (ADR-0011 amended); declarative `lifecycle.cron` in
  `interface_schema` translates to durable timers at registration.
  The ctx bridge host-fn table moves from flat `ctx_state_*` names to
  dot-namespaced groups (`ctx.store.*`, `ctx.timer.*`), introspectable.
- The evictor tick gains a wheel scan (same 5-second loop; the wheel
  is checked before eviction so a due delivery preempts an eviction
  that same tick).
- Gravity (per-channel instances) registers its interrupt and
  compression wakes through this surface; the krystallizer policy
  parameters (ADR-0008/0009) drive the durations, the aura timer
  drives the delivery.
- Timer granularity is bounded by the evictor tick (±5s); sub-tick
  precision is a non-goal (no consumer needs it).
