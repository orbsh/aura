//! Durable timer entries (ADR-0016 as amended 2026-11-09): the TABLE is the
//! schedule, the wheel is only the fast path.
//!
//! A durable timer is one row here — `(due_ms, id)` in the key, so the table's
//! own order IS the due order — and nothing about it lives in the in-memory
//! wheel until the realm arms the next due row. Boot rebuilds by reading the
//! table once (a `by_due` range read, not a decode of every row), and an entry
//! that came due while nothing ran is found by the same read bounded at `now`.
//! Firing and cancelling both `purge` the row: this table keeps no history.
//!
//! What is stored is the SCHEDULE, never the wheel: the wheel is a data
//! structure, and a restart rebuilds it from these rows.

use crate::mq::MqStore;
use okm_core::{Collection, DocumentEncode, KeyEncode};

/// The event-plane band's timer table (aura ADR-0040).
pub const TIMER_NS: u16 = 27;

/// `(due_ms, id)`: due first so a range read answers "what is owed by T"
/// without decoding the payload, and `id` keeps two entries at the same
/// millisecond apart.
#[derive(KeyEncode, Clone, PartialEq, Eq, Debug, Default)]
pub struct TimerEntryKey {
    pub due_ms: u64,
    pub id: u64,
}

/// Who is woken, and with what tag. The instance is stored RENDERED (booth type
/// + key) because a row must not depend on an in-process id; a singleton
/// renders as the empty string (ADR-0042), which is how it comes back.
#[derive(DocumentEncode, Clone, PartialEq, Eq, Debug)]
#[ok_ref(TimerEntryKey)]
#[ok_index(by_due { fields(due_ms) })]
#[ok_ns(27)]
pub struct TimerEntry {
    /// Mirror of the key's due segment — index fields must be PAYLOAD fields
    /// (the key is not one), so the `by_due` scan reads this.
    pub due_ms: u64,
    pub booth_type: String,
    pub instance_key: String,
    pub tag: String,
}

/// The table over one realm's store (ns 27 comes from the declaration).
pub type TimerTable = Collection<MqStore, TimerEntryKey, TimerEntry>;

/// Bind the table to a store handle.
pub fn table(store: MqStore) -> TimerTable {
    Collection::new(store)
}

/// Record one durable timer. The instance is handed in RENDERED (`booth type`,
/// `key`): a row must not depend on an in-process id, and the caller already
/// owns the rendering (a singleton renders as the empty string, ADR-0042).
pub fn put(
    table: &TimerTable,
    key: TimerEntryKey,
    booth_type: &str,
    instance_key: &str,
    tag: &str,
) -> okm_core::Result<()> {
    table.put(
        &key,
        &TimerEntry {
            due_ms: key.due_ms,
            booth_type: booth_type.to_string(),
            instance_key: instance_key.to_string(),
            tag: tag.to_string(),
        },
    )
}

/// Forget one entry: it fired, or it was cancelled. `false` = nothing there.
pub fn purge(table: &TimerTable, key: &TimerEntryKey) -> okm_core::Result<bool> {
    table.purge(key)
}

/// Everything owed at or before `now_ms`, oldest first. The boot path calls this
/// with `u64::MAX` (the whole table); the arming path calls it with the next
/// due instant.
pub fn due_by(
    table: &TimerTable,
    now_ms: u64,
) -> okm_core::Result<Vec<(TimerEntryKey, Option<TimerEntry>)>> {
    let hits = table.scan_range::<__OkmIndex_TimerEntry_by_due>(
        &[],
        Some(&now_ms.saturating_add(1).to_be_bytes()),
    )?;
    Ok(decoded(hits))
}

/// The whole table, oldest first — what a boot reads to rebuild the wheel.
pub fn all(table: &TimerTable) -> okm_core::Result<Vec<(TimerEntryKey, Option<TimerEntry>)>> {
    let hits = table.scan_range::<__OkmIndex_TimerEntry_by_due>(&[], None)?;
    Ok(decoded(hits))
}

/// An index scan can answer with a PARTIAL primary key (the probe did not reach
/// the key's tail); a timer entry is only real when the whole key was taken, so
/// the partials are dropped rather than guessed at.
fn decoded(
    hits: Vec<(okm_core::model::key::PrefixKey<TimerEntryKey>, Option<TimerEntry>)>,
) -> Vec<(TimerEntryKey, Option<TimerEntry>)> {
    hits.into_iter()
        .filter(|(key, _)| key.taken == TimerEntryKey::KEY_LEN)
        .map(|(key, document)| (key.decoded, document))
        .collect()
}