//! Unified call model (Phase 3.5): one call mode for every target —
//! realm Actor, remote Probe, future HTTP — built on oneshot +
//! `pending_calls` + `reply_to`.
//!
//! Two-tier waiting, split at the ENTRY by static declaration (never
//! mid-wait; PLAN Phase 3.5):
//!
//! - **Hot path** (`Tier::Hot`): the caller parks on the oneshot — task
//!   parked, no thread held, zero persistence.
//! - **Cold path** (`Tier::Cold`): wait never enters park — the call is
//!   registered in `pending_calls`, `CallSlot::wait()` returns
//!   `Waited::Pending(call_id)` immediately, the caller's task ends. The
//!   result arrives later via `resolve_call` (framework re-enters the
//!   suspended turn; completed calls never replay). What the framework
//!   persists to re-enter is the session transcript — Gravity's job, not
//!   the realm's.
//!
//! Timeout on either tier = failure value through the same channel; no
//! suspend/continue instruction exists (continuation snapshots are not
//! feasible in Rust and unnecessary under entry-split).

use serde_json::Value;
use std::time::{Duration, Instant};

/// Opaque call id: correlates a pending call to its eventual result.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CallId(pub String);

/// Static execution nature of a target — declared at registration, never
/// guessed at runtime (PLAN: "升级由静态声明驱动，不是运行时猜测").
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Tier {
    /// Fast-returning, idempotent-ish: park the caller on the oneshot.
    #[default]
    Hot,
    /// Touches a human or an external system (minutes-scale): never park —
    /// register, return Pending, task ends.
    Cold,
}

/// Per-target call declaration. Part of the actor/target registry (the
/// same registration that names carried languages for probes).
#[derive(Clone, Debug)]
pub struct CallSpec {
    pub tier: Tier,
    /// Deadline for the hot path (failure value on expiry). Cold calls
    /// have no deadline: humans take minutes; re-entry is event-driven.
    pub timeout: Option<Duration>,
}

impl CallSpec {
    pub fn hot(timeout: Duration) -> Self {
        Self { tier: Tier::Hot, timeout: Some(timeout) }
    }
    pub fn cold() -> Self {
        Self { tier: Tier::Cold, timeout: None }
    }
}

/// What `CallSlot::wait()` yields.
#[derive(Debug)]
pub enum Waited {
    /// Hot path completed: the value (or failure) is here.
    Done(anyhow::Result<Value>),
    /// Cold path: the call is registered; the caller's task ends now.
    /// Framework re-enters on `resolve_call(call_id, result)`.
    Pending(CallId),
}

/// The single wait surface — always the same one line at the call site:
/// `slot.wait().await`. Which tier runs was decided at entry.
pub enum CallSlot {
    Hot {
        rx: tokio::sync::oneshot::Receiver<anyhow::Result<Value>>,
        deadline: Option<(tokio::time::Instant, Duration)>,
    },
    Cold {
        call_id: CallId,
    },
}

impl CallSlot {
    pub async fn wait(self) -> anyhow::Result<Waited> {
        match self {
            CallSlot::Hot { rx, deadline } => {
                // Timeout/dropped = failure VALUE (Result inside Done),
                // never an outer error: "timeout = failure value" (PLAN).
                let result = match deadline {
                    Some((deadline, _)) => match tokio::time::timeout_at(deadline, rx).await {
                        Ok(Ok(result)) => result,
                        Ok(Err(_)) => Err(anyhow::anyhow!("call dropped (sender gone)")),
                        Err(_) => Err(anyhow::anyhow!("call timed out")),
                    },
                    None => match rx.await {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!("call dropped (sender gone)")),
                    },
                };
                Ok(Waited::Done(result))
            }
            CallSlot::Cold { call_id } => Ok(Waited::Pending(call_id)),
        }
    }
}

/// A registered pending call (cold tier, or hot-but-in-flight bookkeeping).
#[derive(Debug)]
pub struct PendingEntry {
    pub registered_at: Instant,
    /// Cold calls: None deadline (re-entry is event-driven).
    /// Hot in-flight: Some — the deadline scan converts expiry into a
    /// failure value delivered through the oneshot.
    pub deadline: Option<Instant>,
    pub reply: Option<tokio::sync::oneshot::Sender<anyhow::Result<Value>>>,
    /// The suspended turn/session this call belongs to (cold re-entry
    /// routing: the framework reads this to know which transcript to wake).
    pub session: Option<String>,
}
