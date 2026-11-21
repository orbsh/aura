//! The `lifecycle` block of `interface_schema`: the facts a TYPE declares about
//! itself, read by the host at registration.
//!
//! Two readers share this module, which is why it exists: `aura-engine` reads
//! the same block for `idle_ttl` / `max_exec` / `concurrency`, and `aura-realm`
//! reads `cron` when it arms a declared schedule. One duration grammar, one cron
//! grammar, one place a booth author can look up what a string means.
//!
//! Nothing here interprets a schedule at RUNTIME — that was ADR-0016 §3's rule.
//! `cron` answers one question only: given a declaration and the instant of
//! registration, when is the FIRST wake. Every later instant is the booth's own
//! computation on delivery (it re-arms), because the schedule's semantics belong
//! to the body that declared it.

use std::time::Duration;

/// One declared schedule entry: `{"tag": "beat", "every": "5m"}`.
///
/// `tag` is what the delivery carries (the same pointer an imperative
/// `ctx.timer.register` writes): the wake is told its tag and nothing else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronDecl {
    pub tag: String,
    pub every: Duration,
}

/// A duration as the declaration writes it: a number of seconds, or a string
/// with a unit suffix (`"300s"`, `"5m"`, `"2h"`). Anything else is not a
/// duration — there is no third spelling and no default.
pub fn duration(v: &serde_json::Value) -> Option<Duration> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().map(Duration::from_secs),
        serde_json::Value::String(s) => {
            match (s.chars().last()?, s[..s.len() - 1].parse::<u64>().ok()?) {
                ('s', n) => Some(Duration::from_secs(n)),
                ('m', n) => Some(Duration::from_secs(n * 60)),
                ('h', n) => Some(Duration::from_secs(n * 3600)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// `lifecycle.cron`, decoded strictly: an ARRAY of entries, each an object with
/// exactly the two names that mean something. A single object is refused rather
/// than accepted as a one-element array — a declaration that reads two ways is a
/// declaration whose meaning depends on who reads it.
///
/// The error text is the caller's to prefix with a type name; it names the
/// offending index so a booth author can find the entry.
pub fn cron(v: &serde_json::Value) -> Result<Vec<CronDecl>, String> {
    let entries = v
        .as_array()
        .ok_or_else(|| "lifecycle.cron: an array of {tag, every}".to_string())?;
    let mut out = Vec::with_capacity(entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let tag = entry
            .get("tag")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| format!("lifecycle.cron[{i}]: missing non-empty `tag`"))?;
        let every = entry
            .get("every")
            .and_then(duration)
            .ok_or_else(|| format!("lifecycle.cron[{i}]: missing `every` (e.g. \"5m\")"))?;
        if every.is_zero() {
            return Err(format!(
                "lifecycle.cron[{i}]: `every` must be longer than zero — a schedule that is \
                 always due is a loop, not a timer"
            ));
        }
        out.push(CronDecl {
            tag: tag.to_string(),
            every,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_duration_reads_seconds_minutes_and_hours() {
        assert_eq!(duration(&json!(30)), Some(Duration::from_secs(30)));
        assert_eq!(duration(&json!("30s")), Some(Duration::from_secs(30)));
        assert_eq!(duration(&json!("5m")), Some(Duration::from_secs(300)));
        assert_eq!(duration(&json!("2h")), Some(Duration::from_secs(7200)));
        // No third spelling: a bare number in a string, a bad suffix, and a
        // float are all "not a duration" rather than a guess.
        assert_eq!(duration(&json!("30")), None);
        assert_eq!(duration(&json!("5d")), None);
        assert_eq!(duration(&json!(1.5)), None);
    }

    #[test]
    fn a_declared_cron_is_an_array_of_tag_and_every() {
        let declared = json!([{ "tag": "beat", "every": "5m" }, { "tag": "tidy", "every": 60 }]);
        assert_eq!(
            cron(&declared).unwrap(),
            vec![
                CronDecl {
                    tag: "beat".into(),
                    every: Duration::from_secs(300)
                },
                CronDecl {
                    tag: "tidy".into(),
                    every: Duration::from_secs(60)
                },
            ]
        );
    }

    #[test]
    fn a_malformed_declaration_names_what_is_wrong() {
        // A bare object is not a one-element array: the reader must not invent
        // the shape the author did not write.
        assert!(cron(&json!({ "tag": "beat", "every": "5m" })).is_err());
        assert!(cron(&json!([])).unwrap().is_empty());
        assert!(cron(&json!([{ "every": "5m" }]))
            .unwrap_err()
            .contains("missing non-empty `tag`"));
        assert!(cron(&json!([{ "tag": "beat" }]))
            .unwrap_err()
            .contains("missing `every`"));
        assert!(cron(&json!([{ "tag": "beat", "every": "0s" }]))
            .unwrap_err()
            .contains("longer than zero"));
        assert!(cron(&json!([{ "tag": "a", "every": "1s" }, { "tag": "", "every": "1s" }]))
            .unwrap_err()
            .contains("cron[1]"));
    }
}