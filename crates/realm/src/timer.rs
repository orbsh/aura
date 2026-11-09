//! Timer wheel (ADR-0016, revised 2026-09-23): unified scheduling surface
//! on tokio-util's DelayQueue — a public hashed wheel with insert / cancel
//! / poll_expired. ONE driver task per realm owns the queue by value;
//! registration and cancellation flow through a command channel, so the
//! driver may park on the queue without holding any lock other callers
//! need. Two entry kinds share the wheel:
//!
//! - Deliver: wake an booth via a `__on_timer` queue job (interrupt
//!   checks, compression wakes, cron). Durable delivery entries are a
//!   StateStore restore concern (ctx.timer wiring lands separately).
//! - Reclaim: nobody is coming; take resources back. Idle-TTL eviction
//!   (measured from job COMPLETION — the entry only exists while the
//!   instance is idle) and the execution watchdog (max_exec budget,
//!   cancelled at completion, expiry = evict).

use crate::mq::MqStore;
use crate::timer_entry::{self, TimerEntryKey, TimerTable};
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
    /// Idle expiry: evict the instance (on_sleep, executor drop).
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
    Reclaim {
        target: InstanceId,
        kind: ReclaimKind,
    },
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
    Register {
        id: TimerId,
        entry: Entry,
        at: Duration,
        /// Also write the row: this entry must survive the process (ADR-0016
        /// as amended 2026-11-09 — the table is the schedule, the wheel the
        /// fast path).
        durable: bool,
    },
    Cancel(TimerId),
    /// Reclaim entries only: the job-entry path voids pending idle/
    /// watchdog timers (work arrived) but must NOT touch the instance's
    /// delivery timers (ctx.timer registrations survive a job — they
    /// are the instance's own scheduled wakes, not residency bookkeeping).
    CancelReclaimTarget(Box<InstanceId>),
    /// Every entry kind: eviction kills the instance outright (a dead
    /// instance's pending deliveries must not fire either).
    CancelAllTarget(Box<InstanceId>),
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
            durable: false,
        });
        id
    }

    /// A delivery timer that OUTLIVES the process: the same wake, plus a row in
    /// the ns-27 table. A row that cannot be written is a timer that would be
    /// silently forgotten on the next boot, so the caller still gets an id —
    /// what it must not do is believe the timer is durable.
    pub fn register_durable(&self, target: InstanceId, tag: String, at: Duration) -> TimerId {
        let id = self.alloc_id();
        let _ = self.tx.send(Command::Register {
            id,
            entry: Entry::Deliver { target, tag },
            at,
            durable: true,
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
            // Reclaim is residency bookkeeping: it must die with the process.
            durable: false,
            at,
        });
        sent.ok();
        id
    }

    pub fn cancel(&self, id: TimerId) {
        let _ = self.tx.send(Command::Cancel(id));
    }

    /// Cancel the instance's RECLAIM timers (the job-entry path: work
    /// arrived, so the idle/watchdog bookkeeping is void — the ctx.timer
    /// deliveries stay armed).
    pub fn cancel_reclaim_target(&self, target: &InstanceId) {
        let _ = self
            .tx
            .send(Command::CancelReclaimTarget(Box::new(target.clone())));
    }

    /// Cancel EVERY timer targeting an instance (eviction path: a dead
    /// instance's pending deliveries must not fire either).
    pub fn cancel_target(&self, target: &InstanceId) {
        let _ = self
            .tx
            .send(Command::CancelAllTarget(Box::new(target.clone())));
    }
}

/// The driver-owned wheel. Lives inside the driver task; the world talks
/// to it through `TimerHandle`'s command channel.
pub struct TimerDriver {
    queue: DelayQueue<(TimerId, Entry)>,
    /// id → queue key (cancellation) and id → entry (target matching).
    index: HashMap<TimerId, (tokio_util::time::delay_queue::Key, Entry)>,
    /// The durable rows behind the entries that have one: id → the row's key.
    /// A row is the schedule; this map is only "which of the armed entries owes
    /// a delete when it fires or is cancelled".
    durable: HashMap<TimerId, TimerEntryKey>,
    /// The ns-27 table, bound at spawn so registration never has to lock the
    /// realm (the driver runs while the realm is busy).
    timers: TimerTable,
    commands: mpsc::UnboundedReceiver<Command>,
    /// Weak: the driver must never keep the realm (and its engine file
    /// lock) alive — same discipline as the evictor task (ADR-0016 §4).
    /// Expiry actions upgrade; a dropped realm simply never fires.
    realm: std::sync::Weak<tokio::sync::Mutex<crate::Realm>>,
}

impl TimerDriver {
    /// Spawn the driver task; returns the caller-facing handle. One task
    /// per realm — task count is bounded by the realm count.
    pub fn spawn(realm: crate::SharedRealm, store: MqStore) -> TimerHandle {
        let (tx, commands) = mpsc::unbounded_channel();
        let driver = Self {
            queue: DelayQueue::new(),
            index: HashMap::new(),
            durable: HashMap::new(),
            timers: timer_entry::table(store),
            commands,
            realm: std::sync::Arc::downgrade(&realm),
        };
        tokio::spawn(driver.run());
        TimerHandle {
            tx,
            seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    async fn run(mut self) {
        // BOOT: the table is the schedule. Every row is armed where it is owed
        // (a row that came due while nothing ran is armed at zero and fires on
        // the first pass), so a restart resumes rather than forgets.
        self.restore();
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
                    let Some(expired) = expired else {
                        match self.commands.recv().await {
                            Some(cmd) => self.apply(cmd).await,
                            None => break,
                        }
                        continue;
                    };
                    let (id, entry) = expired.into_inner();
                    self.index.remove(&id);
                    self.fire(id, entry).await;
                }
            }
        }
    }

    /// Rebuild the wheel from the table (ADR-0016: the table is the schedule,
    /// the wheel only the fast path). A read failure arms nothing rather than
    /// guessing: the rows are still there for the next boot.
    fn restore(&mut self) {
        let rows = match timer_entry::all(&self.timers) {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("aura: timer restore could not read the table: {error}");
                return;
            }
        };
        let now = now_ms();
        for (key, document) in rows {
            let Some(document) = document else { continue };
            let id = TimerId(key.id);
            let entry = Entry::Deliver {
                target: InstanceId {
                    booth_type: document.booth_type.clone(),
                    key: instance_key_of(&document.instance_key),
                },
                tag: document.tag.clone(),
            };
            let wait = Duration::from_millis(key.due_ms.saturating_sub(now));
            let slot = self
                .queue
                .insert_at((id, entry.clone()), tokio::time::Instant::now() + wait);
            self.index.insert(id, (slot, entry));
            self.durable.insert(id, key);
        }
    }

    /// Apply one command (shared by the select arm and the empty-queue
    /// parking path).
    async fn apply(&mut self, cmd: Command) {
        match cmd {
            Command::Register { id, entry, at, durable } => {
                if durable {
                    let key = TimerEntryKey {
                        due_ms: now_ms().saturating_add(at.as_millis() as u64),
                        id: id.0,
                    };
                    if let Entry::Deliver { target, tag } = &entry {
                        if let Err(error) = timer_entry::put(
                            &self.timers,
                            key.clone(),
                            &target.booth_type,
                            &target.key.render().to_string(),
                            tag,
                        ) {
                            eprintln!("aura: durable timer row not written ({id:?}): {error}");
                        } else {
                            self.durable.insert(id, key);
                        }
                    }
                }
                let key = self
                    .queue
                    .insert_at((id, entry.clone()), tokio::time::Instant::now() + at);
                self.index.insert(id, (key, entry));
            }
            Command::Cancel(id) => self.cancel(&id),
            Command::CancelReclaimTarget(target) => self.cancel_kind(&target, false),
            Command::CancelAllTarget(target) => self.cancel_kind(&target, true),
        }
    }

    fn cancel(&mut self, id: &TimerId) {
        if let Some((key, _)) = self.index.remove(id) {
            self.queue.remove(&key);
        }
        // A durable entry that is cancelled owes nothing to the next boot.
        if let Some(row) = self.durable.remove(id) {
            if let Err(error) = timer_entry::purge(&self.timers, &row) {
                eprintln!("aura: cancelled timer row not purged ({id:?}): {error}");
            }
        }
    }

    /// Cancel the instance's timers: reclaim-only, or every kind.
    fn cancel_kind(&mut self, target: &InstanceId, all: bool) {
        let ids: Vec<TimerId> = self
            .index
            .iter()
            .filter(|(_, (_, e))| e.targets(target) && (all || matches!(e, Entry::Reclaim { .. })))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.cancel(&id);
        }
    }

    async fn fire(&mut self, id: TimerId, entry: Entry) {
        // The row goes when the wake is spent: this table keeps no history.
        if let Some(row) = self.durable.remove(&id) {
            if let Err(error) = timer_entry::purge(&self.timers, &row) {
                eprintln!("aura: fired timer row not purged ({id:?}): {error}");
            }
        }
        let Some(realm) = std::sync::Weak::upgrade(&self.realm) else {
            return;
        };
        match entry {
            Entry::Deliver { target, tag } => {
                crate::Realm::deliver_timer(realm.clone(), target, tag).await;
            }
            Entry::Reclaim { target, kind } => match kind {
                ReclaimKind::Idle => {
                    crate::Realm::evict_instance(realm.clone(), &target).await;
                }
                ReclaimKind::Watchdog => {
                    crate::Realm::watchdog_expiry(realm.clone(), &target).await;
                }
            },
        }
    }
}

/// The realm's clock, milliseconds since the epoch — the same unit the table's
/// key uses (`due_ms`), so a row and an armed entry can be compared directly.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A stored instance key back to its variant: empty is the singleton (ADR-0042),
/// anything else is a named key. The row stores the RENDERED form on purpose —
/// a row must not depend on an in-process id.
fn instance_key_of(rendered: &str) -> aura_booth::InstanceKey {
    if rendered.is_empty() {
        aura_booth::InstanceKey::Singleton
    } else {
        aura_booth::InstanceKey::Named(rendered.to_string())
    }
}
