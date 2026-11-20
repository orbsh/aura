//! Type namespaces for a TEMPORARY realm (ADR-0050 §6).
//!
//! A persisted realm's type ns is a COLUMN: `meta::ns_of` reads the
//! `BoothName` row that registration wrote, so the number survives a restart
//! and two realms (and two processes) agree about which slice a type owns. A
//! temporary realm has neither rows nor a restart: its ns comes from a counter
//! that lives exactly as long as the realm does, and the map below is the whole
//! of that registry.
//!
//! **The band.** A persisted type's ns is `BOOTH_NS_BASE + id` — 101 upward —
//! and it is only ever used INSIDE that realm's key prefix. A temporary realm's
//! keys are not in that keyspace at all: its store is an in-memory engine of
//! its own (`MqStore::mem`, ADR-0050 §5/§6), so the two number spaces never
//! meet in storage and a shared number could not collide. The band is chosen
//! for the OTHER readers of an ns — a log line, a dump, a storage plan: a
//! temporary type allocates DOWN from `u16::MAX`, which is visibly not the
//! 101-and-up block a persisted type lives in, and the count is bounded at 256
//! so the counter cannot wrap out of its band. Exhaustion is a named error, not
//! a reuse.

use std::collections::HashMap;

/// The first ns a temporary realm hands out (the top of the band).
pub const NS_BAND_TOP: u16 = u16::MAX;

/// How many types one temporary realm may register. A job's declarations are a
/// graph's nodes, not a platform's catalogue; 256 keeps the band inside the
/// top page of the u16 space, where nothing persisted can be mistaken for it.
pub const NS_BAND_LEN: u16 = 256;

/// The in-memory type registry of one temporary realm: name → ns, issued once,
/// never reused, and forgotten when the realm is (ADR-0050 §5 — the realm does
/// not survive, so neither does its allocation).
#[derive(Debug, Default)]
pub struct TypeNs {
    issued: HashMap<String, u16>,
    count: u16,
}

impl TypeNs {
    pub fn new() -> Self {
        Self::default()
    }

    /// The ns for this type name: the one it already has, else the next free
    /// one. Stable across re-registration (a hot swap keeps the type's slice —
    /// the same rule the persisted registry follows).
    pub fn resolve(&mut self, name: &str) -> anyhow::Result<u16> {
        if let Some(ns) = self.issued.get(name) {
            return Ok(*ns);
        }
        if self.count >= NS_BAND_LEN {
            anyhow::bail!(
                "temporary realm: `{name}` is one type too many — the in-memory ns band holds \
                 {NS_BAND_LEN} types (0x{NS_BAND_TOP:04x} down to 0x{:04x})",
                NS_BAND_TOP - NS_BAND_LEN + 1
            );
        }
        let ns = NS_BAND_TOP - self.count;
        self.count += 1;
        self.issued.insert(name.to_string(), ns);
        Ok(ns)
    }

    /// The ns already issued to a type (`None` = never registered here). Peek,
    /// never allocate.
    pub fn of(&self, name: &str) -> Option<u16> {
        self.issued.get(name).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_counts_down_from_the_top_and_never_repeats() {
        let mut reg = TypeNs::new();
        assert_eq!(reg.resolve("a").unwrap(), NS_BAND_TOP);
        assert_eq!(reg.resolve("b").unwrap(), NS_BAND_TOP - 1);
        // Re-registration is stable: a hot swap keeps the type's slice.
        assert_eq!(reg.resolve("a").unwrap(), NS_BAND_TOP);
        assert_eq!(reg.of("b"), Some(NS_BAND_TOP - 1));
        assert_eq!(reg.of("ghost"), None);
        // Distinct for every name in the band.
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..NS_BAND_LEN - 2 {
            assert!(seen.insert(reg.resolve(&format!("t{i}")).unwrap()));
        }
        // The band is a ceiling, not a wrap.
        assert!(reg.resolve("one-too-many").is_err());
    }
}
