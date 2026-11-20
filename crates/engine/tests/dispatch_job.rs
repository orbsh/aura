//! ADR-0051 step 2a: a job's ops run in a temporary realm built from the job's
//! OWN declarations, honouring `needs` and `on_error`, and the realm is gone
//! afterwards.
//!
//! The declarations go through the ENGINE's registration door (`Engine::dispatch`
//! → `Registrar for Engine` → `register_into_with`), so these tests exercise the
//! production path: a declaration is registered by the same body a registered
//! booth is, into the job's own realm.
//!
//! What a job envelope CANNOT say, and therefore what these tests do not pretend:
//! `effector_protocol`'s `Need` is the set of ctx SERVICES an op is granted
//! (effector ADR-0005), not a list of op dependencies. Order IS the dependency
//! here — the op list is a line — and `needs` is asserted as the capability gate
//! it actually is.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::Body;
use aura_engine::Engine;
use aura_realm::dispatch::{BoothDecl, Dispatch, DispatchError, JobOutcome, OpResult};
use effector_protocol::{Job, OnError};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------
/// These tests hold declarations in hand (a Rust body can record its own runs, which is how the
/// retry policies are observable), so they enter at the seam below the door: `Dispatch::run`. The
/// door itself — declarations decoded from the job's attachment — is covered by
/// `a_job_whose_declarations_ride_its_attachment_runs` and the refusal cases beside it.
async fn dispatch(
    engine: &Engine,
    job: &Job,
    decls: &[BoothDecl],
) -> anyhow::Result<JobOutcome> {
    Dispatch::new(engine.realm_set.as_ref(), engine)
        .run(job, decls)
        .await
}



/// A booth that records every run by its `step` label and fails when asked to.
/// The log IS the attempt counter: one entry per run, in order.
fn worker(log: Arc<Mutex<Vec<String>>>) -> Body {
    Body::Rust(Arc::new(move |_ctx, args: Value| {
        let log = log.clone();
        Box::pin(async move {
            let label = args
                .get("step")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            log.lock()
                .map_err(|_| anyhow::anyhow!("the log lock was poisoned"))?
                .push(label.clone());
            if args.get("fail").and_then(Value::as_bool) == Some(true) {
                anyhow::bail!("step `{label}` refused to run");
            }
            Ok(args)
        })
    }))
}

/// The storage declaration a type upload carries — one collection, key `id`, one
/// hot field `count` (the shape `StorePlan::from_schema` parses).
fn storage_schema() -> Value {
    json!({
        "storage": { "collections": { "counters": { "schema": {
            "key_len": 8,
            "key_fields": [
                { "name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "layout_version": 1,
            "hot_width": 8,
            "payload_header_len": 3,
            "hot_fields": [
                { "name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "cold_fields": [],
            "slots": { "primary": 0, "dynamic": 1, "field_vocab_id": 2,
                       "field_vocab_name": 3, "declared_index_base": 16385,
                       "declared_reduce_base": 20481, "junction_base": 12288 }
        } } } }
    })
}

/// One op, in the wire shape a job arrives in: the executor is `type/key`
/// (ADR-0042), the kind defaults to a Start, and both policy fields default.
fn op(executor: &str, args: Value, needs: Value, on_error: Value) -> Value {
    json!({
        "call": {
            "call_id": format!("call-{executor}"),
            "executor": executor,
            "entry": "execute",
            "language": "rust",
            "args": args,
            "code": { "url": "http://plane.invalid/code/00", "sha256": "00" }
        },
        "needs": needs,
        "on_error": on_error
    })
}

/// A job built from JSON: the envelope is a wire type, so the tests use the wire
/// form rather than a struct literal that could drift from it.
fn job(id: &str, ops: Vec<Value>) -> Job {
    serde_json::from_value(json!({
        "id": id,
        "booth": "gravity/turn-1",
        "source": { "kind": "ops", "ops": ops }
    }))
    .expect("the job is a valid envelope")
}

fn failed_policy() -> Value {
    json!({ "kind": "fail" })
}

/// Every check on one op's outcome, at the index it ran at.
fn outcome(job: &JobOutcome, at: usize) -> &aura_realm::dispatch::OpOutcome {
    job.ops
        .get(at)
        .unwrap_or_else(|| panic!("the outcome must list op {at}: {:?}", job.ops))
}

// ---------------------------------------------------------------------------
// Order, and `needs` as the capability gate ADR-0005 says it is.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_job_runs_its_ops_in_order_and_needs_store_is_granted_when_declared() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![
        BoothDecl::new("step", worker(log.clone())).with_storage(storage_schema()),
    ];
    // Op 2 asks for `ctx.store`, which the declaration supports. Both ops
    // address the same type under different instance keys — order is the
    // dependency, and the second op runs only after the first answered.
    let job = job(
        "job-order",
        vec![
            op(
                "step/a",
                json!({ "step": "one" }),
                json!({}),
                failed_policy(),
            ),
            op(
                "step/b",
                json!({ "step": "two" }),
                json!({ "store": true }),
                failed_policy(),
            ),
        ],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec!["one".to_string(), "two".to_string()],
        "the op list is a line: op 1 runs after op 0, never before it"
    );
    assert_eq!(out.ops.len(), 2, "every op is reported: {:?}", out.ops);
    assert!(out.succeeded(), "both ops answered: {:?}", out.ops);
    for (index, expected) in [(0, "one"), (1, "two")] {
        let o = outcome(&out, index);
        assert_eq!(o.op, index);
        assert_eq!(o.attempts, 1, "a succeeding op runs once");
        match &o.result {
            OpResult::Done { value } => assert_eq!(value["step"], expected),
            other => panic!("op {index} must be Done, got {other:?}"),
        }
    }
    // The realm the job ran in is gone from the set's memory, and its name — the
    // id's rendering, the only thing that ever held it — resolves to nothing.
    assert!(
        !engine
            .realm_set
            .live()
            .await
            .unwrap()
            .contains(&out.realm.to_string()),
        "the job's realm is still listed after the job ended"
    );
    assert_eq!(engine.realm_set.name_of(out.realm).await.unwrap(), None);
    assert!(out.destroyed);
}

#[tokio::test]
async fn needs_store_is_refused_before_any_op_runs_when_the_type_declared_no_storage() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    // No `.with_storage(..)`: the type keeps nothing, so it has no ctx.store.
    let decls = vec![BoothDecl::new("naked", worker(log.clone()))];
    let job = job(
        "job-needs",
        vec![op(
            "naked/a",
            json!({ "step": "should-not-run" }),
            json!({ "store": true }),
            failed_policy(),
        )],
    );

    let err = dispatch(&engine, &job, &decls)
        .await
        .expect_err("a ctx service the realm cannot grant is refused");

    let refusal = err
        .downcast_ref::<DispatchError>()
        .unwrap_or_else(|| panic!("the refusal must be named (DispatchError), got {err:#}"));
    match refusal {
        DispatchError::Need { op, need, .. } => {
            assert_eq!((*op, *need), (0, "store"));
        }
        other => panic!("expected a `needs` refusal, got {other:?}"),
    }
    assert!(
        log.lock().unwrap().is_empty(),
        "the op ran even though its declared need was refused"
    );
}

// ---------------------------------------------------------------------------
// on_error: Fail / Skip / Retry / Never.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn skip_records_the_failure_and_carries_on() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];
    let job = job(
        "job-skip",
        vec![
            op(
                "step/a",
                json!({ "step": "bad", "fail": true }),
                json!({}),
                json!({ "kind": "skip" }),
            ),
            op(
                "step/b",
                json!({ "step": "good" }),
                json!({}),
                failed_policy(),
            ),
        ],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    match &outcome(&out, 0).result {
        OpResult::Skipped { error } => {
            assert!(error.contains("step `bad` refused"), "got: {error}");
        }
        other => panic!("a skipped op must say so, got {other:?}"),
    }
    assert!(
        matches!(outcome(&out, 1).result, OpResult::Done { .. }),
        "Skip continues: the next op must run, got {:?}",
        outcome(&out, 1).result
    );
    assert_eq!(*log.lock().unwrap(), vec!["bad", "good"]);
    assert!(!out.succeeded(), "a skipped op means the job did not fully succeed");
}

#[tokio::test]
async fn fail_stops_the_job_and_its_successors_are_reported_not_run() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];
    let job = job(
        "job-fail",
        vec![
            op(
                "step/a",
                json!({ "step": "bad", "fail": true }),
                json!({}),
                failed_policy(),
            ),
            op(
                "step/b",
                json!({ "step": "never" }),
                json!({}),
                failed_policy(),
            ),
        ],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    match &outcome(&out, 0).result {
        OpResult::Failed { error } => assert!(error.contains("step `bad` refused"), "got: {error}"),
        other => panic!("a failed op must say so, got {other:?}"),
    }
    // The successor is listed, with the reason — not silently missing from a
    // shorter list, which would read as "the job had one op".
    match &outcome(&out, 1).result {
        OpResult::NotRun { reason } => assert!(
            reason.contains("op 0 failed"),
            "the reason must name the op that stopped the job, got: {reason}"
        ),
        other => panic!("a successor of a failed op must be NotRun, got {other:?}"),
    }
    assert_eq!(outcome(&out, 1).attempts, 0);
    assert_eq!(*log.lock().unwrap(), vec!["bad"], "Fail must not run op 1");
}

#[tokio::test]
async fn retry_two_runs_the_step_three_times_and_no_more() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];
    // `times: 2` counts ADDITIONAL attempts (effector ADR-0005): one run plus
    // two retries = three, and never a fourth.
    let job = job(
        "job-retry",
        vec![op(
            "step/a",
            json!({ "step": "retry", "fail": true }),
            json!({}),
            json!({ "kind": "retry", "times": 2 }),
        )],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    assert_eq!(
        log.lock().unwrap().len(),
        3,
        "retry{{times: 2}} is three runs (the first plus two retries)"
    );
    let o = outcome(&out, 0);
    assert_eq!(o.attempts, 3);
    assert_eq!(o.policy, OnError::Retry { times: 2 });
    assert!(matches!(o.result, OpResult::Failed { .. }));
}

#[tokio::test]
async fn never_runs_exactly_once_even_when_a_later_step_fails() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];
    let job = job(
        "job-never",
        vec![
            // The side effect that must not happen twice.
            op(
                "step/a",
                json!({ "step": "push" }),
                json!({}),
                json!({ "kind": "never" }),
            ),
            // It fails, and its policy stops the job.
            op(
                "step/b",
                json!({ "step": "write", "fail": true }),
                json!({}),
                failed_policy(),
            ),
            // A second Never step that must never even start.
            op(
                "step/c",
                json!({ "step": "second-push" }),
                json!({}),
                json!({ "kind": "never" }),
            ),
        ],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec!["push", "write"],
        "the Never step ran once; the failure after it re-ran nothing"
    );
    let never = outcome(&out, 0);
    assert_eq!(never.attempts, 1);
    assert!(never.succeeded());
    assert!(matches!(outcome(&out, 1).result, OpResult::Failed { .. }));
    assert_eq!(outcome(&out, 2).attempts, 0);
    assert!(
        matches!(&outcome(&out, 2).result, OpResult::NotRun { reason } if reason.contains("op 1 failed")),
        "the Never step after the failure is reported NotRun, got {:?}",
        outcome(&out, 2).result
    );
    // And it is non-replayable by name: the policy is part of the outcome, so a
    // reader can tell this step may not be run again on redelivery.
    assert_eq!(never.policy, OnError::Never);
}

#[tokio::test]
async fn a_failing_never_step_is_one_attempt_and_is_not_retried() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];
    let job = job(
        "job-never-fail",
        vec![op(
            "step/a",
            json!({ "step": "push", "fail": true }),
            json!({}),
            json!({ "kind": "never" }),
        )],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    assert_eq!(
        log.lock().unwrap().len(),
        1,
        "Never is one attempt: nothing may re-run a step with a side effect"
    );
    assert_eq!(outcome(&out, 0).attempts, 1);
    assert!(matches!(outcome(&out, 0).result, OpResult::Failed { .. }));
}

// ---------------------------------------------------------------------------
// Job-level refusals: before any op, before any realm.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_job_naming_a_type_it_did_not_declare_fails_by_name_before_any_op_runs() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("declared", worker(log.clone()))];
    let before = engine.realm_set.live().await.unwrap();
    // Op 0 is fine; op 1 addresses a type the job never brought. The whole job is
    // refused, so op 0 must not have run either.
    let job = job(
        "job-ghost",
        vec![
            op(
                "declared/a",
                json!({ "step": "one" }),
                json!({}),
                failed_policy(),
            ),
            op(
                "ghost/b",
                json!({ "step": "two" }),
                json!({}),
                failed_policy(),
            ),
        ],
    );

    let err = dispatch(&engine, &job, &decls)
        .await
        .expect_err("an undeclared type is a job defect, not an op failure");
    let refusal = err
        .downcast_ref::<DispatchError>()
        .unwrap_or_else(|| panic!("the refusal must be named, got {err:#}"));
    match refusal {
        DispatchError::UndeclaredType { op, name, declared } => {
            assert_eq!((*op, name.as_str()), (1, "ghost"));
            assert_eq!(declared, &vec!["declared".to_string()]);
        }
        other => panic!("expected UndeclaredType, got {other:?}"),
    }
    assert!(err.to_string().contains("`ghost`"), "the failure names it: {err}");
    assert!(
        log.lock().unwrap().is_empty(),
        "no op may run when the job names a type it does not declare"
    );
    assert_eq!(
        engine.realm_set.live().await.unwrap(),
        before,
        "a refused job must not leave a realm behind"
    );
}

#[tokio::test]
async fn a_plan_job_and_a_shared_scope_job_are_refused_by_name() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log.clone()))];

    let plan: Job = serde_json::from_value(json!({
        "id": "job-plan",
        "booth": "gravity/turn-1",
        "source": { "kind": "plan", "plan_hash": "deadbeef", "run": "run-7" }
    }))
    .unwrap();
    let err = dispatch(&engine, &plan, &decls).await.expect_err("plan jobs are out of scope");
    assert!(matches!(err.downcast_ref::<DispatchError>(), Some(DispatchError::PlanSource)));

    let shared: Job = serde_json::from_value(json!({
        "id": "job-shared",
        "booth": "gravity/turn-1",
        "scope": "shared",
        "source": { "kind": "ops", "ops": [
            op("step/a", json!({ "step": "one" }), json!({}), failed_policy())
        ] }
    }))
    .unwrap();
    let err = dispatch(&engine, &shared, &decls)
        .await
        .expect_err("a shared-scope job cannot run in a realm that dies with it");
    assert!(matches!(err.downcast_ref::<DispatchError>(), Some(DispatchError::SharedScope)));
    assert!(log.lock().unwrap().is_empty());
}
// ---------------------------------------------------------------------------
// The door: declarations ride the job's attachment (ADR-0051 §Attachments)
// ---------------------------------------------------------------------------

/// A declaration as it travels: the same record a registration writes, so there is no second
/// declaration format — and the one body that cannot travel (`Body::Rust`, a linked function) is
/// excluded by construction.
fn wire_decl(name: &str, language: &str) -> aura_booth::persist::PersistedBooth {
    aura_booth::persist::PersistedBooth {
        name: name.to_string(),
        language: language.to_string(),
        source: "(define (execute args) args)".to_string(),
        encoding: Default::default(),
        idle_ttl_secs: None,
        schema: Some(storage_schema()),
        concurrency: Default::default(),
    }
}

fn boothset(decls: &[aura_booth::persist::PersistedBooth]) -> Value {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&decls.to_vec(), &mut bytes).expect("the declarations encode");
    // An attachment is bytes; JSON has one spelling for them, an array of numbers.
    json!({
        "name": "boothset",
        "media": "application/cbor",
        "bytes": bytes,
    })
}

fn job_with(id: &str, ops: Vec<Value>, attachments: Vec<Value>) -> Job {
    serde_json::from_value(json!({
        "id": id,
        "booth": "gravity/turn-1",
        "source": { "kind": "ops", "ops": ops },
        "attachments": attachments,
    }))
    .expect("the job is a valid envelope")
}

#[tokio::test]
async fn a_job_whose_declarations_ride_its_attachment_reaches_the_same_door() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    // A language no carrier implements: the op is guaranteed to fail AT THE CALL, which is what
    // tells us the declaration was registered and addressed rather than refused up front.
    let decls = vec![wire_decl("step", "no-such-carrier")];
    let job = job_with(
        "job-attached",
        vec![op(
            "step/a",
            json!({}),
            json!({}),
            failed_policy(),
        )],
        vec![boothset(&decls)],
    );

    let out = engine.dispatch(&job).await.expect("the declarations decoded");

    assert!(
        out.destroyed,
        "the job's realm is destroyed whatever the op did"
    );
    let first = outcome(&out, 0);
    let OpResult::Failed { error } = &first.result else {
        panic!("the op cannot have succeeded without a carrier: {:?}", first.result);
    };
    assert!(
        error.contains("no-such-carrier"),
        "the failure names the language the attachment declared: {error}"
    );
    assert_eq!(
        engine.realm_set.live().await.unwrap(),
        vec!["main".to_string()],
        "nothing about the job outlives it"
    );
}

#[tokio::test]
async fn a_job_that_brought_no_declarations_is_refused_before_a_realm_exists() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let job = job_with(
        "job-bare",
        vec![op("step/a", json!({}), json!({}), failed_policy())],
        Vec::new(),
    );
    let err = engine.dispatch(&job).await.expect_err("no declarations arrived");
    assert!(matches!(
        err.downcast_ref::<DispatchError>(),
        Some(DispatchError::Attachment { .. })
    ));
    assert_eq!(engine.realm_set.live().await.unwrap(), vec!["main".to_string()]);
}

// ---------------------------------------------------------------------------
// Frames: the one thing that crosses (ADR-0051 §4)
// ---------------------------------------------------------------------------

/// A body that emits from inside the op — the same `ctx.emit` a script gets, so
/// the test drives the real path and not a stand-in for it.
fn reporter() -> Body {
    Body::Rust(Arc::new(|ctx, args: Value| {
        Box::pin(async move {
            ctx.emit("progress", json!({ "at": args["step"] })).await?;
            Ok(json!({ "done": true }))
        })
    }))
}

#[tokio::test]
async fn a_job_reports_what_happened_as_frames_the_dispatcher_can_read() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let decls = vec![BoothDecl::new("step", reporter())];
    // `Job::booth` is the dispatcher's address — `gravity/turn-1` in this fixture.
    let job = job(
        "job-frames",
        vec![op(
            "step/a",
            json!({ "step": "one" }),
            json!({ "emit": true }),
            failed_policy(),
        )],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();
    assert!(out.succeeded(), "the op answered: {:?}", out.ops);

    // The job's whole observable life, in order, read back through the pull face:
    // what the op emitted, what became of the op, and that the job is over.
    let frames = engine
        .job_frames("gravity/turn-1", "job-frames", 0)
        .await
        .expect("the dispatcher's frames");
    let kinds: Vec<&str> = frames
        .iter()
        .map(|(_, f)| f["kind"].as_str().unwrap_or("?"))
        .collect();
    assert_eq!(kinds, ["emit", "op.done", "job.end"], "frames: {frames:?}");

    // The op's own emit, untouched: the path names the job and the op that spoke.
    assert_eq!(
        frames[0].1["path"],
        json!([{ "job": "job-frames", "op": 0 }])
    );
    assert_eq!(frames[0].1["event"], "progress");
    assert_eq!(frames[0].1["data"]["at"], "one", "the payload is the op's own");

    // The op's answer, so a reader over a wire does not have to wait for the end.
    assert_eq!(frames[1].1["value"]["done"], true);
    assert_eq!(frames[1].1["path"], json!([{ "job": "job-frames", "op": 0 }]));

    // The end: the job spoke, not an op, and the realm is gone by the time it says so.
    assert_eq!(frames[2].1["path"], json!([{ "job": "job-frames" }]));
    assert_eq!(frames[2].1["destroyed"], true);
    assert_eq!(frames[2].1["ops"], 1);
    assert_eq!(
        engine.realm_set.live().await.unwrap(),
        vec!["main".to_string()],
        "the frames outlive the realm that produced them"
    );

    // A reader resumes where it stopped: the sequence number is the cursor.
    let last = frames.last().expect("at least one frame").0;
    assert!(
        engine
            .job_frames("gravity/turn-1", "job-frames", last)
            .await
            .unwrap()
            .is_empty(),
        "nothing new after the last frame read"
    );
}

#[tokio::test]
async fn every_op_is_reported_and_one_job_does_not_read_as_another() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let decls = vec![BoothDecl::new("step", worker(log))];
    // Op 0 answers, op 1 fails under `skip` (so the job carries on), op 2 answers.
    let many = job(
        "job-many",
        vec![
            op("step/a", json!({ "step": "one" }), json!({}), failed_policy()),
            op(
                "step/b",
                json!({ "step": "two", "fail": true }),
                json!({}),
                json!({ "kind": "skip" }),
            ),
            op("step/c", json!({ "step": "three" }), json!({}), failed_policy()),
        ],
    );
    let other = job(
        "job-other",
        vec![op(
            "step/a",
            json!({ "step": "aside" }),
            json!({}),
            failed_policy(),
        )],
    );

    dispatch(&engine, &many, &decls).await.unwrap();
    dispatch(&engine, &other, &decls).await.unwrap();

    let frames = engine
        .job_frames("gravity/turn-1", "job-many", 0)
        .await
        .unwrap();
    let kinds: Vec<(&str, Option<u64>)> = frames
        .iter()
        .map(|(_, f)| {
            (
                f["kind"].as_str().unwrap_or("?"),
                f["path"][0]["op"].as_u64(),
            )
        })
        .collect();
    assert_eq!(
        kinds,
        [
            ("op.done", Some(0)),
            ("op.skipped", Some(1)),
            ("op.done", Some(2)),
            ("job.end", None),
        ],
        "every op reports, in order, and the end names no op: {frames:?}"
    );
    assert_eq!(frames[1].1["attempts"], 1, "a skipped op ran once");

    // The other job's frames share the queue (one partition per booth) and are not
    // mixed into this one's — the path is the discriminator.
    let other_frames = engine
        .job_frames("gravity/turn-1", "job-other", 0)
        .await
        .unwrap();
    assert_eq!(other_frames.len(), 2, "{other_frames:?}");
    assert!(other_frames
        .iter()
        .all(|(_, f)| f["path"][0]["job"] == "job-other"));
}

#[tokio::test]
async fn a_job_with_no_channel_out_cannot_emit_at_all() {
    // The channel is installed by dispatch. A temporary realm built any other way
    // has none, and its emits are refused rather than dropped: "the job emitted
    // nothing" is the failure a boundary exists to prevent.
    let set = aura_realm::realm_set::RealmSet::with_mq(aura_realm::mq::MqStore::mem());
    let eph = set.create_ephemeral().await.unwrap();
    let err = aura_realm::Realm::emit(&eph.realm(), Some("step"), "progress", json!({}))
        .await
        .expect_err("a temporary realm with no channel out must refuse the emit");
    assert!(
        err.to_string().contains("no outbound channel"),
        "the refusal says why: {err:#}"
    );
}

// ---------------------------------------------------------------------------
// Placement (ADR-0053): a filter on the pool, resolved before the realm exists.
// ---------------------------------------------------------------------------

/// A node in the venue's pool, named and placed, with a connection nobody answers on.
/// Calls sent to it fail at the channel — which is how a test can tell "the realm
/// found the node" from "the realm never saw it", without a websocket in the way.
fn place(engine: &Engine, alias: &str, labels: &[&str]) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    drop(rx);
    engine.realm_set.pool().insert(aura_realm::pool::NodeRecord {
        alias: alias.to_string(),
        conn: aura_realm::ActuatorConn {
            sender: tx,
            peer: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
        },
        carriers: vec!["steel".into()],
        class: effector_protocol::NodeClass::Host,
        machine: format!("box-{alias}"),
        labels: labels.iter().map(|l| l.to_string()).collect(),
    });
}

/// The failure text of one op, whichever way it failed.
fn why(out: &JobOutcome, at: usize) -> String {
    match &outcome(out, at).result {
        OpResult::Done { value } => format!("answered: {value}"),
        OpResult::Skipped { error } | OpResult::Failed { error } => error.clone(),
        OpResult::NotRun { reason } => reason.clone(),
    }
}

fn remote_step(node_alias: &str) -> BoothDecl {
    BoothDecl::new(
        "step",
        Body::Remote {
            encoding: aura_booth::Encoding::Json,
            node_alias: node_alias.into(),
            language: "steel".into(),
            source: "fn run() {}".into(),
            requires: None,
        },
    )
}

fn placed(id: &str, ops: Vec<Value>, labels: &[&str]) -> Job {
    serde_json::from_value(json!({
        "id": id,
        "booth": "gravity/turn-1",
        "placement": { "kind": "labels", "labels": labels },
        "source": { "kind": "ops", "ops": ops }
    }))
    .expect("the job is a valid envelope")
}

#[tokio::test]
async fn a_placement_no_node_is_in_is_refused_before_a_realm_exists() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    place(&engine, "office-box", &["site:office"]);
    let decls = vec![BoothDecl::new("step", worker(Arc::new(Mutex::new(Vec::new()))))];
    let job = placed(
        "job-nowhere",
        vec![op("step/a", json!({ "step": "one" }), json!({}), failed_policy())],
        &["site:home"],
    );

    let err = dispatch(&engine, &job, &decls)
        .await
        .expect_err("a place with no nodes cannot be honoured");
    let refusal = err
        .downcast_ref::<DispatchError>()
        .expect("the refusal keeps its type through anyhow");
    assert!(
        matches!(refusal, DispatchError::Nowhere { labels, .. } if labels == &["site:home".to_string()]),
        "asked for site:home, venue has office-box: {refusal}"
    );
    assert_eq!(
        engine.realm_set.live().await.unwrap(),
        vec!["main".to_string()],
        "refused before the realm was created — nothing to clean up"
    );
}

#[tokio::test]
async fn a_job_reaches_only_the_nodes_its_placement_names() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    place(&engine, "home-box", &["site:home"]);
    place(&engine, "office-box", &["site:office"]);
    let decls = vec![remote_step("office-box")];
    let job = placed(
        "job-filter",
        vec![op("step/a", json!({}), json!({}), failed_policy())],
        &["site:home"],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    // The op found the node (a lookup that failed would say "not connected") and was
    // refused by the filter, in words, before anything was sent to the machine.
    let why = why(&out, 0);
    assert!(
        why.contains("outside this job's placement"),
        "the placement must refuse the node by name, got: {why}"
    );
    assert_eq!(
        out.nodes,
        vec!["home-box".to_string()],
        "the outcome records what the job could see, not what it asked for"
    );
}

#[tokio::test]
async fn a_job_that_asks_for_no_place_reaches_every_node() {
    let engine = Engine::start(&Default::default()).await.unwrap();
    place(&engine, "home-box", &["site:home"]);
    place(&engine, "office-box", &["site:office"]);
    let decls = vec![remote_step("office-box")];
    // No `placement` in the envelope at all: the default is `Any`.
    let job = job(
        "job-any",
        vec![op("step/a", json!({}), json!({}), failed_policy())],
    );

    let out = dispatch(&engine, &job, &decls).await.unwrap();

    let why = why(&out, 0);
    assert!(
        !why.contains("outside this job's placement") && !why.contains("not connected"),
        "an unplaced job reaches every node, and fails later for a later reason: {why}"
    );
    assert_eq!(
        out.nodes,
        vec!["home-box".to_string(), "office-box".to_string()],
        "every connected node"
    );
}
