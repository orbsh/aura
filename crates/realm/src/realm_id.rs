//! Realm identity (ADR-0050 §1): an id is what lives in the key, a name is a
//! column in the `realms` table.
//!
//! Two shapes, one type. The variant is EXPLICIT, never inferred from the
//! width — a uuid's numeric value can fall inside `u32`'s range, and a
//! serializer that wrote the number rather than the tagged shape would erase
//! the distinction a resolver has to be able to read: "look in the memory
//! table, then in the persistent one" is decidable only if the tag says which
//! table to look in.
//!
//! Only two arms, and both are implemented. `Persisted` is allocated from the
//! `realms` table and prefixes its keys with `[u32 BE id]`; `Ephemeral` is a
//! uuidv7 minted in-process, backed by an in-memory store, and never written
//! anywhere (ADR-0050 §5/§6, ADR-0051). The distinction is the TAG, not the
//! width — see the type's own doc.

use serde::{Deserialize, Serialize};

/// A realm's identity.
///
/// The serde shape is ADJACENTLY tagged (`{"kind": ..., "id": ...}`), which is
/// why a `Persisted(1)` is not decodable as an `Ephemeral(1)`: the tag is a
/// byte in the encoding, not a convention about the number.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum RealmId {
    /// Allocated from the `realms` table: monotonic, never reused (ADR-0050
    /// §Consequences — a recycled id would hand a new realm an old realm's
    /// rows).
    Persisted(u32),
    /// A uuidv7 minted in-process; never reaches storage, so it has no
    /// prefix and no row. Its realm's store is an in-memory engine of its own
    /// (`MqStore::mem`), and it ceases to exist with the realm.
    Ephemeral(u128),
}

impl RealmId {
    /// Mint a temporary realm's identity: a version-7 uuid from the generator
    /// this workspace already carries (okm's `Uuid7` — no second uuid
    /// dependency). v7 rather than v4 because byte order is time order, so a
    /// dump or a log sorted by id is also sorted by age (ADR-0050 §1).
    pub fn ephemeral_now() -> Self {
        Self::Ephemeral(u128::from_be_bytes(*okm_core::Uuid7::now().as_bytes()))
    }

    /// The storage prefix of this id — `[u32 BE id]` for a persisted realm
    /// (ADR-0050 §2). `None` for the ephemeral kind: it writes nothing, so
    /// there is no prefix for it to be, and a caller that needs one is asking
    /// to bind a slice of the engine's keyspace to a realm that owns none
    /// (`MqStore::for_realm` says so by name).
    pub fn storage_prefix(self) -> Option<[u8; 4]> {
        match self {
            Self::Persisted(id) => Some(id.to_be_bytes()),
            Self::Ephemeral(_) => None,
        }
    }

    /// The allocated number, if this is the persisted kind.
    pub fn persisted(self) -> Option<u32> {
        match self {
            Self::Persisted(id) => Some(id),
            Self::Ephemeral(_) => None,
        }
    }
}

impl std::fmt::Display for RealmId {
    /// A name for logs and errors. The realm's human name lives in the
    /// `realms` table; an id is what a store row can be keyed by, so this
    /// renders the identity rather than guessing at a name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Persisted(id) => write!(f, "persisted:{id}"),
            Self::Ephemeral(u) => write!(f, "ephemeral:{u:032x}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_realm_id_round_trips_with_its_tag() {
        for id in [RealmId::Persisted(1), RealmId::Ephemeral(1)] {
            let json = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<RealmId>(&json).unwrap(), id);
        }
        // The tag is IN the encoding, and it is the discriminator: the same
        // number under the two tags is two different realms.
        assert_eq!(
            serde_json::to_value(RealmId::Persisted(1)).unwrap(),
            serde_json::json!({"kind": "persisted", "id": 1})
        );
        assert_eq!(
            serde_json::to_value(RealmId::Ephemeral(1)).unwrap(),
            serde_json::json!({"kind": "ephemeral", "id": 1})
        );
    }

    #[test]
    fn a_persisted_id_is_not_decodable_as_an_ephemeral_one() {
        // A persisted realm's encoding names its own kind, so a reader that
        // only understands the ephemeral kind cannot mistake it: the reverse
        // is the failure this test exists for (a serializer that wrote the
        // NUMBER would let `1` mean either).
        let persisted = serde_json::to_value(RealmId::Persisted(1)).unwrap();
        assert_eq!(
            serde_json::from_value::<RealmId>(persisted).unwrap(),
            RealmId::Persisted(1)
        );
        // A bare number is not a realm id at all: there is no untagged form
        // to fall back to.
        assert!(serde_json::from_str::<RealmId>("1").is_err());
        // Nor is a tag this type does not declare.
        assert!(serde_json::from_str::<RealmId>(r#"{"kind":"persisted_v2","id":1}"#).is_err());
    }

    #[test]
    fn the_storage_prefix_is_the_id_and_the_ephemeral_kind_has_none() {
        assert_eq!(RealmId::Persisted(1).storage_prefix(), Some([0, 0, 0, 1]));
        assert_eq!(RealmId::Persisted(258).storage_prefix(), Some([0, 0, 1, 2]));
        assert_eq!(RealmId::Ephemeral(1).storage_prefix(), None);
    }

    #[test]
    fn a_minted_ephemeral_id_is_a_fresh_v7() {
        let (a, b) = match (RealmId::ephemeral_now(), RealmId::ephemeral_now()) {
            (RealmId::Ephemeral(a), RealmId::Ephemeral(b)) => (a, b),
            _ => panic!("the mint is the ephemeral kind"),
        };
        assert_ne!(a, b, "two temporary realms never share an identity");
        let bytes = a.to_be_bytes();
        assert_eq!(bytes[6] >> 4, 7, "version nibble says uuidv7");
        assert_eq!(bytes[8] >> 6, 0b10, "RFC 9562 variant");
    }
}