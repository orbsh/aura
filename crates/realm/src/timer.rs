//! Timer wheel (ADR-0016, revised 2026-09-23): unified scheduling surface
//! on tokio-util's DelayQueue — a public hashed wheel with insert / cancel
//! / poll_expired. ONE driver task per realm owns the queue by value;
//! registration and cancellation flow through a command channel, so the
//! driver may park on the queue without holding any lock other callers
//! need. Two entry kinds share the wheel:
//!
//! - Deliver: wake an actor via a `__on_timer` queue job (interrupt
//!   checks, compression wakes, cron). Durable delivery entries are a
//!   StateStore restore concern (ctx.timer wiring lands separately).
//! - Reclaim: nobody is coming; take resources back. Idle-TTL eviction
//!   (measured from job COMPLETION — the entry only exists while the
//!   instance is idle) and the execution watchdog (max_exec budget,
//!   cancelled at completion, expiry = evict).

use crate::InstanceId;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;

/// Opaque timer handle. Cancellation is by id; unknown ids are
/// already-fired or already-cancelled timers — `cancel` is idempotent by
/// contract (the run_job completion path cancels unconditionally).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TimerId(pub u64);

/// What happens when a reclaim entry fires.
#[derive(Clone, Debug)]
pub enum ReclaimKind {
    /// Idle expiry: evict the instance (on_sleep, session drop).
    Idle,
    /// Execution budget exceeded: evict (the in-flight caller observes
    /// the eviction as a dropped reply).
    Watchdog,
}

#[derive(Clone)]
enum Entry {
    /// Deliver-type (ADR-0016 §1): `__on_timer` queue job with a tag.
    Deliver { target: InstanceId, tag: String },
    /// Reclaim-type (revision §2): evict / watchdog expiry action.
    Reclaim { target: InstanceId, kind: ReclaimKind },
}

impl Entry {
    fn targets(&self, target: &InstanceId) -> bool {
        match self {
            Entry::Deliver { target: t, .. } => t == target,
            Entry::Reclaim { target: t, .. } => t == target,
        }
    }
}

enum Command {
    /// Pre-allocated id: the caller learns nothing back — ids come from a
    /// shared atomic so register is fully synchronous on the caller side.
    Register { id: TimerId, entry: Entry, at: Duration },
    Cancel(TimerId),
    CancelTarget(Box<InstanceId>),
}

/// The caller-facing half: cheap clones, never blocks (unbounded command
/// channel — a timer registration is not worth backpressure).
#[derive(Clone)]
pub struct TimerHandle {
    tx: mpsc::UnboundedSender<Command>,
    seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl TimerHandle {
    /// A handle whose command channel is closed: every command is a
    /// no-op. Used for the construction window before the driver spawns.
    pub fn detached() -> Self {
        let (tx, _rx) = mpsc::unbounded_channel();
        Self {
            tx,
            seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn alloc_id(&self) -> TimerId {
        TimerId(self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
    }

    /// Delivery timer (cron / future ctx.timer face; tag identifies the
    /// wake). The id is immediately cancelable — cancellation of a
    /// not-yet-processed registration removes the entry before it can
    /// fire (commands drain before expiries in the driver loop).
    pub fn register_deliver(&self, target: InstanceId, tag: String, at: Duration) -> TimerId {
        let id = self.alloc_id();
        let _ = self.tx.send(Command::Register {
            id,
            entry: Entry::Deliver { target, tag },
            at,
        });
        id
    }

    /// Reclaim timer (idle / watchdog). Cancelled unconditionally at job
    /// completion — idempotent by the driver's contract.
    pub fn register_reclaim(&self, target: InstanceId, kind: ReclaimKind, at: Duration) -> TimerId {
        let id = self.alloc_id();
        let sent = self.tx.send(Command::Register {
            id,
            entry: Entry::Reclaim { target, kind },
            at,
        });
        sent.ok();
        id
    }

    pub fn cancel(&self, id: TimerId) {
        let _ = self.tx.send(Command::Cancel(id));
    }

    /// Cancel every timer targeting an instance (eviction path: a dead
    /// instance's pending idle/watchdog entries must not fire).
    pub fn cancel_target(&self, target: &InstanceId) {
        let _ = self.tx.send(Command::CancelTarget(Box::new(target.clone())));
    }
}

/// The driver-owned wheel. Lives inside the driver task; the world talks
/// to it through `TimerHandle`'s command channel.
pub struct TimerDriver {
    queue: DelayQueue<(TimerId, Entry)>,
    /// id → queue key (cancellation) and id → entry (target matching).
    index: HashMap<TimerId, (tokio_util::time::delay_queue::Key, Entry)>,
    commands: mpsc::UnboundedReceiver<Command>,
    realm: crate::SharedRealm,
}

impl TimerDriver {
    /// Spawn the driver task; returns the caller-facing handle. One task
    /// per realm — task count is bounded by the realm count.
    pub fn spawn(realm: crate::SharedRealm) -> TimerHandle {
        let (tx, commands) = mpsc::unbounded_channel();
        let driver = Self {
            queue: DelayQueue::new(),
            index: HashMap::new(),
            commands,
            realm,
        };
        tokio::spawn(driver.run());
        TimerHandle {
            tx,
            seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                // Commands drain before expiries: a cancel arriving in the
                // same wakeup as its timer's expiry wins — the entry is
                // removed before the expiry poll sees it (cancellation
                // determinism for the run_job completion path).
                cmd = self.commands.recv() => {
                    match cmd {
                        Some(cmd) => self.apply(cmd).await,
                        None => break,
                    }
                }
                expired = std::future::poll_fn(|cx| self.queue.poll_expired(cx)) => {
                    // Ready(None) = the queue is EMPTY (Stream termination
                    // semantics), not a dead wheel: park on the command
                    // channel until the next registration re-populates it.
                    // (The original `break` here killed the driver on the
                    // first poll of an empty queue — every timer became a
                    // silent no-op.)
                    if expired.is_none() {
                        match self.commands.recv().await {
                            Some(cmd) => self.apply(cmd).await,
                            None => break,
                        }
                        continue;
                    }
                    let Some(expired) = expired else { unreachable!("handled above") };
                    let (id, entry) = expired.into_inner();
                    self.index.remove(&id);
                    self.fire(entry).await;
                }
            }
        }
    }

    /// Apply one command (shared by the select arm and the empty-queue
    /// parking path).
    async fn apply(&mut self, cmd: Command) {
        match cmd {
            Command::Register { id, entry, at } => {
                let key = self.queue.insert_at(
                    (id, entry.clone()),
                    tokio::time::Instant::now() + at,
                );
                self.index.insert(id, (key, entry));
            }
            Command::Cancel(id) => self.cancel(&id),
            Command::CancelTarget(target) => self.cancel_target(&target),
        }
    }

    fn cancel(&mut self, id: &TimerId) {
        if let Some((key, _)) = self.index.remove(id) {
            self.queue.remove(&key);
        }
    }

    fn cancel_target(&mut self, target: &InstanceId) {
        let ids: Vec<TimerId> = self
            .index
            .iter()
            .filter(|(_, (_, e))| e.targets(target))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.cancel(&id);
        }
    }

    async fn fire(&self, entry: Entry) {
        match entry {
            Entry::Deliver { target, tag } => {
                crate::Realm::deliver_timer(self.realm.clone(), target, tag).await;
            }
            Entry::Reclaim { target, kind } => {
                match kind {
                    ReclaimKind::Idle => {
                        crate::Realm::evict_instance(self.realm.clone(), &target).await;
                    }
                    ReclaimKind::Watchdog => {
                        crate::Realm::watchdog_expiry(self.realm.clone(), &target).await;
                    }
                }
            }
        }
    }
}
