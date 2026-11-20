//! The path a job's frames carry home (ADR-0051 §4).
//!
//! A temporary realm is isolated in every direction but one: the frames the job
//! produces leave, addressed to the booth that dispatched it. Addressing is the
//! whole content of this module — a frame that cannot say WHICH job and WHICH op
//! produced it is a message from nowhere, and a dispatcher running three skills
//! at once cannot tell its own step's output from a grandchild's.
//!
//! The path is a CHAIN rather than a pair because jobs nest (effector ADR-0005):
//! a nested job's frames carry the chain that reached it, so the outermost
//! dispatcher reads `[{job: a, op: 2}, {job: b, op: 0}]` and knows the frame came
//! out of op 0 of the job it started at op 2 of its own plan.
//!
//! One-way is structural, not a rule: a job's realm receives frames only from
//! its own `@on` routes, and nothing writes into those queues but the realm
//! itself. There is no inbound door to close because none was built.

use serde::{Deserialize, Serialize};

/// Where a frame came from: one job's one op.
///
/// `job` is the envelope's id (effector ADR-0005) — the same string the job was
/// submitted under, so a log line or a dispatch record names it the same way.
/// `op` is the op's index in the list the job ran, which is also the order the
/// ops happened in (the op list is a line).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathStep {
    pub job: String,
    /// Which op spoke. `None` means the JOB spoke — its start, or its end — and the
    /// difference matters: a reader counting op frames should not count the job's own
    /// bookends as work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<u32>,
}

impl PathStep {
    pub fn at(job: impl Into<String>, op: u32) -> Self {
        Self {
            job: job.into(),
            op: Some(op),
        }
    }

    /// The job as a whole, no op.
    pub fn whole(job: impl Into<String>) -> Self {
        Self {
            job: job.into(),
            op: None,
        }
    }
}

/// The chain, oldest job first. Empty is a bug in the caller, not a state:
/// there is no frame without a producer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FramePath(Vec<PathStep>);

impl FramePath {
    /// The path a job's frames start from: the job, at the op about to run.
    pub fn root(job: impl Into<String>, op: u32) -> Self {
        Self(vec![PathStep::at(job, op)])
    }

    /// The path of a frame the JOB itself posts — its end. Same chain, no op step.
    pub fn whole(job: impl Into<String>) -> Self {
        Self(vec![PathStep::whole(job)])
    }

    /// The same chain, addressing another op of the SAME job. Ops run in order,
    /// one at a time, so a dispatched job updates its last step rather than
    /// growing the chain.
    #[must_use]
    pub fn at_op(&self, op: u32) -> Self {
        let mut steps = self.0.clone();
        match steps.last_mut() {
            Some(last) => last.op = Some(op),
            None => return Self(vec![PathStep::at(String::new(), op)]),
        }
        Self(steps)
    }

    /// The chain a job nested under this one inherits: the parent's path, then
    /// the child's own root. Nesting appends; it never replaces, or an inner
    /// subscription would look like it came from the outer job's op.
    #[must_use]
    pub fn nested(&self, job: impl Into<String>, op: u32) -> Self {
        let mut steps = self.0.clone();
        steps.push(PathStep::at(job, op));
        Self(steps)
    }

    pub fn as_slice(&self) -> &[PathStep] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The wire form: a JSON array of `{job, op}`. A frame's addressing rides
    /// inside the frame's own payload, because the envelope it travels in
    /// (effector's) knows jobs and ops but not realms, and this crate does not
    /// get to add fields to it.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(&self.0).unwrap_or(serde_json::Value::Null)
    }
}

/// The one channel a temporary realm has out (ADR-0051 §4).
///
/// It holds the PARENT's realm handle — not a route, not a store handle. The
/// job cannot write the parent's storage (that is boundary (a)), but it can hand
/// the parent a frame it will post itself, which is the difference between
/// "nothing crosses" and "only frames cross".
#[derive(Clone)]
pub struct FramesOut {
    /// Where the frames are posted: the realm the job was dispatched from.
    pub parent: crate::SharedRealm,
    /// The booth that dispatched, as `Job::booth` spells it (`type/key`). The
    /// frame is addressed to it, so a venue running several dispatchers needs no
    /// guessing at delivery time.
    pub dispatcher: String,
    /// Which job and which op is speaking. Dispatch rewrites the op step before
    /// each call (ops run one at a time), and a nested job appends its own step.
    pub path: FramePath,
}

/// What a frame says.
///
/// One queue, and one field that says which shape the rest is. A job's frames are
/// its whole observable life — the events its ops emit AND what became of each op —
/// so a reader that had to infer the kind from which fields happen to be present
/// would be reading a second protocol, in prose.
pub mod kind {
    /// The op called `ctx.emit`. Carries `event` and `data`, exactly what the op
    /// handed the venue.
    pub const EMIT: &str = "emit";
    /// The op answered. Carries `value`.
    pub const OP_DONE: &str = "op.done";
    /// The op failed and its policy said to carry on. Carries `error` and `attempts`.
    pub const OP_SKIPPED: &str = "op.skipped";
    /// The op failed for the last time: the job stops here. Carries `error` and `attempts`.
    pub const OP_FAILED: &str = "op.failed";
    /// The op never ran, because an earlier one stopped the job. Carries `reason`.
    pub const OP_NOT_RUN: &str = "op.not_run";
    /// The job is over and its realm is gone. Carries `destroyed` and `ops`.
    ///
    /// Last by construction: a reader that has it needs no timeout to know the job
    /// ended, and a reader that never gets one knows something went wrong that the
    /// frames could not report.
    pub const JOB_END: &str = "job.end";
}

/// The queue a job's frames land in, on the PARENT realm's plane.
///
/// One reserved name, not a family: a dispatcher declares one route (or reads
/// one queue) and every job it runs reports there, told apart by the path it
/// carries. A name per job would make the parent's router grow one route per
/// dispatch, which is a registry that has to be cleaned up — the thing a
/// temporary realm exists to avoid.
pub const JOB_FRAME_EVENT: &str = "job.frame";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nested_job_appends_and_an_op_step_rewrites_in_place() {
        let outer = FramePath::root("job-a", 1);
        assert_eq!(outer.as_slice(), &[PathStep::at("job-a", 1)]);

        // Ops run in order, one at a time: the same chain, the next op.
        let later = outer.at_op(3);
        assert_eq!(later.as_slice(), &[PathStep::at("job-a", 3)]);
        assert_eq!(
            outer.as_slice(),
            &[PathStep::at("job-a", 1)],
            "updating a path does not mutate the one it was made from"
        );

        // A nested job inherits the chain that reached it.
        let inner = later.nested("job-b", 0);
        assert_eq!(
            inner.as_slice(),
            &[PathStep::at("job-a", 3), PathStep::at("job-b", 0)]
        );
        assert_eq!(
            inner.to_value(),
            serde_json::json!([{ "job": "job-a", "op": 3 }, { "job": "job-b", "op": 0 }])
        );
    }
}