# 0052 — a job's results are frames, and the frames are the stream

Status: Accepted (2026-11-20)

## Context

ADR-0051 §4 allows exactly one thing through a temporary realm's boundary: the frames the job produces, addressed to the booth that dispatched it. That landed (commit `531a8c6`) carrying only the op's own `ctx.emit`. What it did not carry was the job's results.

So the answers a dispatcher needs — did op 2 run, with what value, why did the job stop — existed only in `JobOutcome`, returned to whoever called `Engine::dispatch` in the same process. A submission over a wire could not read them: the dispatcher that filled `Job::booth` and attached the declarations could no longer learn what became of the work. That makes the door usable only from inside the venue, which excludes k10r and gravity — the parties the mechanism exists for.

The question this record answers: is a job's result stream a thing to BUILD, or a thing that already exists and needs a face?

## Decision

1. **The frame channel carries the job's whole observable life.** One payload per event, discriminated by a `kind` field (`frame_path::kind`): `emit` (what the op handed the venue), `op.done` (`value`), `op.skipped` (`error`, `attempts`), `op.failed` (`error`, `attempts`), `op.not_run` (`reason`), and `job.end` (`destroyed`, `ops`). A reader learns the shape from the kind, not from which fields happen to be present.

2. **There is no separate result stream. The queue rows ARE the stream.** They are already ordered, already sequence-numbered, already addressed by a partition, and already have a retention rule (ADR-0039). A second buffer would hold the same data with a second lifecycle to get wrong — starting with "what happens to it when the job's realm dies", which the queue answers by construction: the rows are in the PARENT, so they outlive the realm that produced them.

3. **`job.end` is posted after the destroy, and it is the last frame.** It carries `destroyed`, so "the job is over" and "nothing of it is left" are one sentence rather than two a reader has to correlate with a timeout. A job that fails before its op list runs still posts it (`ops: 0`) — otherwise a reader waiting for the end would hang on exactly the failure it most needs to hear about.

4. **A frame's path says who spoke: `[{job, op}]` for an op, `[{job}]` for the job.** `op` is optional for that reason. Without the distinction, a reader counting op frames would count the job's own bookends as work.

5. **Reading needs the address, not just the job id.** The rows live in the dispatching booth's queue, so `job_frames(store, dispatcher, job, after_seq)` and `Engine::job_frames(dispatcher, job, after_seq)` take both. Whoever dispatched the job filled in `Job::booth`, so it has both; a reader that knows only the job id would have to search every booth's queue.

6. **Retention is the queue's business, unchanged.** A reader that never advances its sequence forfeits the rows on the same terms as any other subscription (ADR-0039 §2). The stream does not get its own policy.

## Rejected

- **A `JobStream` registry in the parent realm** — a stream id, a buffer, a dispose. It is a second copy of the rows plus a lifetime to define against the job's realm death, for nothing: the queue already is that, with a cursor discipline the rest of the platform uses.
- **Streaming the ops through `iterate` (ADR-0034).** Iterate's producer is an INSTANCE with a handler re-addressed per `Next`; a job is not an instance in the parent realm. Inventing a synthetic one to read a queue would be a shim in front of the row store that already holds the data.
- **Pushing frames into the dispatcher as routed events.** A dispatcher is not a subscriber to an event, and a route it never declared would send its own results to the dead ring — the silent loss this whole line of records exists to prevent.

## Consequences

- **A dispatcher can be a poller.** `job_frames(after)` is the whole protocol: no callback, no connection affinity, no session. That is what lets a cross-process submitter use dispatch at all.
- **A job's debugging story is "read its frames".** The trace is the same one the in-process caller sees in `JobOutcome`, so there is one account of what happened rather than a per-transport one.
- **`JobOutcome` and the frames are two spellings of one fact** for the in-process caller. Both stay: one is the synchronous answer, the other is the observable trace. Folding them is possible later and is not needed for a wire reader.
- **A busy dispatcher's queue grows with its jobs' frames** until its sequence advances. That is a retention question shared with every other subscription, not a new one.
- **`kind` is a closed set for now.** A new phase means a new constant and a record; a consumer that meets an unknown kind must treat it as information, not as an error.

## Tests

- `crates/engine/tests/dispatch_job.rs` — `a_job_reports_what_happened_as_frames_the_dispatcher_can_read` (a job that emits: the frame sequence is `emit, op.done, job.end`, the emit is untouched, the end names no op and says `destroyed`, and the frames are readable after the realm is gone), `every_op_is_reported_and_one_job_does_not_read_as_another` (three ops including a skipped one: `op.done / op.skipped / op.done / job.end` with the op index on each, and a second job's frames not mixed in), `a_job_with_no_channel_out_cannot_emit_at_all`.
- `crates/realm/tests/dispatch_realm.rs` — `a_job_leaves_the_parent_keyspace_byte_identical` (the parent's state namespaces unchanged; its queue plane gaining exactly the job's three frames, all addressed to the dispatcher).

## Related records

aura ADR-0051 (dispatch creates the realm; §4 is the boundary this record fills in), ADR-0034 (a call is a stream Start), ADR-0039 (retention and the watermark), effector ADR-0005 (a job is an envelope).