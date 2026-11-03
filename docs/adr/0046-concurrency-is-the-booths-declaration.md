# ADR-0046 — Concurrency is the booth's declaration, and a queue is only one of its answers

**Status:** Accepted. Landed in the realm's dispatch (`realm/src/instance.rs`), the session
carrier's registry (`effector-runtime` `carrier/session.rs`), the induction carrier's reader
(`carrier/exec.rs`), the type definition (`aura-booth`), the schema's introspection path
(`aura-engine`), and the guest's ctx labelling (`induction-protocol` `HostFrame::call`,
`induction-guest` `Ctx`).

## Context

An actor model is a way to HAVE concurrency, not a way to avoid it: the mailbox is what lets
a caller hand work over without waiting, and the instance is what keeps that work's state in
one place. A lock on the instance is therefore not the model — it is a workaround for one
specific case: a body that cannot be re-entered while its resident state is kept around for
speed.

We were paying that workaround everywhere. Every script booth's calls went through one
mutex, held for the whole handler: `Sessions::with_session_encoded` locked the instance's
slot and ran the call inside it. For a Python booth with one VM behind a GIL that is right.
For a Rust guest that handles each call on its own stack it was pure loss — and it was
measurable: two rounds of 1.5 s and 0.8 s against one gravity turn instance took **2.35 s**
instead of the 1.5 s they would take overlapped. The lock was buying nothing and costing the
thing the actor model is for.

The other half of the same mistake: the venue could not even tell the realm that a booth
*can* be re-entered, so the realm had to assume none can.

## Decision

**The booth declares how its instance serves calls that arrive together, and the realm builds
its residency to match.**

1. **The declaration is part of the type.** `BoothType::concurrency` — a `Concurrency` value
   with three shapes — and, for a booth whose type arrives from a script, the same fact in
   its introspected schema under `lifecycle.concurrency` (a number is a pool size, the string
   `unbounded` is the third shape). The host's builder wins over the script's own word, the
   rule every other lifecycle fact here already follows.

2. **`Queued` — the default — is one call at a time, in arrival order.** One resident session.
   A booth that says nothing keeps exactly the behaviour every existing booth had. This is the
   answer for a body that is not re-entrant: an interpreter holding one VM behind a GIL, a
   session that carries mutable state from call to call, anything whose correctness assumes
   "nobody else is inside me".

3. **`Pool { slots }` is up to `slots` resident sessions running calls side by side** — the
   php-fpm shape. A call takes a free slot; a call that finds them all busy opens a new session
   while the pool is under `slots`; a call past `slots` waits. The count is the parallelism AND
   the residency the booth is willing to pay for (a process each, on a process carrier).

4. **`Unbounded` is ONE resident session with any number of calls in flight — no queue, no
   cap.** This is the shape a booth wants when its own process can already serve several
   requests at once: the parallelism is the process's, and a second process buys nothing. What
   makes it possible is the wire, not the runtime: every request frame carries an id, the child
   answers by that id, and a ctx frame from the child names the request it serves
   (`HostFrame::call`) — so a tool call made inside round A is answered from round A's bridge,
   never round B's. `Unbounded` is therefore only for carriers with a **concurrent surface**
   (`SharedSession`); a carrier without one is a declaration error, not a silent queue, because
   a booth that promised overlap must not look like it has it.

5. **The carrier became a router, not a conversation.** An induction session used to exchange
   request→reply inside the caller's own stack, which made the process serial no matter what
   the child could do. It now owns a reader THREAD that takes frames off the child's stdout, a
   `pending` map that wakes each request's waiter by id, `bridges` keyed by request id for ctx
   attribution, one write mutex for the channel, and one death reason that wakes every waiter
   when the child exits or answers garbage. A call is a send plus a wait on its own channel —
   nothing is shared between calls except the channel itself.

6. **The value belongs to the booth because only the booth knows.** gravity's turn declares
   `unbounded`: its state lives on the call's stack and in its own streams, and its guest runs
   each request on its own thread (`Guest::concurrent(true)`), so one process serves every round
   of every device. An EMBEDDED Python booth declares `Queued` — one VM inside the realm's
   process, and its threads would not run Python concurrently anyway — and buys parallelism from
   its carrier instead. The Python RESIDENT runner is the other shape entirely: an induction
   child that dispatches each request on its own thread and stamps `call` on its ctx frames, so
   its schema declares `unbounded` and one process serves every call. An IO-bound async body
   declares a number and lets the async runtime interleave. The realm does not guess and does
   not grow a per-language rule table: it reads one value.

## Consequences

- **A lock now exists only where a booth asked for one.** The realm stops serializing work it
  has no reason to serialize, and a device's next message is served while the previous round is
  still streaming — in the same process.
- **`Unbounded` puts the limit where it belongs.** There is no admission cap in the realm for
  such a booth; what bounds it is the guest's own runtime — its threads, its fd's, the model's
  latency. A booth that wants a cap declares `Pool { slots }` and pays for slots.
- **Parallelism and residency are one declaration.** "How much may run at once" and "how much
  residency am I willing to keep" are the same value, because a slot IS a resident session;
  `Unbounded` is the case where a single session is enough for any number of calls.
- **A widened declaration replaces the pool; it does not mutate a running one.** Sessions in
  the old pool finish their in-flight calls and are dropped; the next call lands in a pool
  sized for the new declaration.
- **Eviction and the queue do not fight.** A call past capacity waits on a slot, not on the
  instance, so idle-reclaim still sees "work arrived" and the watchdog still arms per job.
- **Eviction now ends the process.** The induction session kills and reaps its child on drop;
  without that, a realm that evicts booths leaks children for the life of the host.

## Tests

- gravity `a_turn_takes_the_next_round_while_it_is_still_running`: two rounds (1.5 s and
  0.8 s of wire latency) on ONE turn instance — one process — finish in under 1.9 s. It failed
  at 2.35 s before the declaration existed, and it fails again if the guest stops declaring
  concurrency or the host stops echoing ctx ids.
- aura's engine suite: unchanged behaviour for every booth that declares nothing (the whole
  suite rides `Concurrency::Queued`).
- The rest of the stack (effector, gravity, prism) is green with the reader thread in place:
  the serial path runs through the same code every other booth still takes.