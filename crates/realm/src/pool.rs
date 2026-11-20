//! The venue's nodes: who is connected, what each one can do, and where it is.
//!
//! A node is not a realm's property. It dials the venue once and serves whoever asks;
//! a realm that held its own copy of that map would be a realm where a job's
//! `Body::Remote` op cannot find the node that owns the executor — because a job's
//! realm is minted an instant before its first op and has inherited nothing
//! (ADR-0051). So the pool is ONE object per engine, borrowed by every realm it
//! creates in the same way those realms borrow the code base URL (boundary (d)).

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

use effector_protocol::NodeClass;
use tokio::sync::mpsc::UnboundedSender;

use crate::ActuatorConn;

/// One connected node, as it described itself when it dialled.
#[derive(Clone)]
pub struct NodeRecord {
    pub alias: String,
    pub conn: ActuatorConn,
    /// The carrier list from the node's registration: which languages it can run.
    pub carriers: Vec<String>,
    /// What contains this node (`host` / `container`), from its registration.
    pub class: NodeClass,
    /// Which BOX this is, as the node reported it (effector ADR-0006): the hardware's serial
    /// where the platform offers one, else an id the node minted once and kept.
    ///
    /// The alias means "the name this connection answers to" and can be reclaimed by any
    /// process that dials with it; the machine is the physical thing a caller's history
    /// belongs to. Unique by construction, which is the ONLY reason it can be self-reported:
    /// a second node claiming this value is a contradiction, and reporting it (Admitted::
    /// same_machine) is the check. Labels get no such check — many machines are `site:home`
    /// — which is why they come from admission instead (effector ADR-0005 §2b, ADR-0006 §3).
    /// Empty means unidentified, and unidentified is not a shared identity: two nodes that
    /// could not say which box they are must never read as one box.
    pub machine: String,
    /// Where this node is (`site:home`, `u101:laptop`) — the ADMITTING party's word.
    ///
    /// Never the node's own: it cannot name its labels without being able to name
    /// someone else's (effector ADR-0005 §2b), so nothing about labels travels in a
    /// registration frame. Whoever admits the connection mints them from what it knows
    /// (a grant, a deployment file) and hands them to the pool here. A node admitted by
    /// something that knows no grant is unplaced: reachable by jobs that ask for no
    /// place, by none that does.
    pub labels: Vec<String>,
}

impl NodeRecord {
    pub fn has_labels(&self, wanted: &[String]) -> bool {
        wanted.iter().all(|l| self.labels.contains(l))
    }
}

/// What an admission displaced or recognised.
#[derive(Default)]
pub struct Admitted {
    /// The record this alias was holding, if any. Reported by the caller: an alias changing
    /// hands is allowed, but never silent (ADR-0015 §7).
    pub displaced: Option<NodeRecord>,
    /// Other aliases that reported the SAME machine. Either the same box reconnecting under
    /// a new name — which is why the machine is recorded at all — or a serial two connections
    /// both claim. The two readings cannot be told apart, and they do not need to be: a
    /// duplicate here is impossible by accident, which is exactly what makes the report worth
    /// making and what makes a self-declared machine safe at all.
    pub same_machine: Vec<String>,
}

/// The nodes this engine can reach.
#[derive(Default)]
pub struct NodePool {
    nodes: RwLock<HashMap<String, NodeRecord>>,
}

impl NodePool {
    /// A poisoned lock is recovered, not propagated: the only writer is the gateway's
    /// presence guard, and a panic there leaves a map whose worst fault is a stale
    /// alias — which is a fact about a node, not a reason to stop serving every realm.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, NodeRecord>> {
        self.nodes.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, NodeRecord>> {
        self.nodes.write().unwrap_or_else(|e| e.into_inner())
    }

    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Add or replace a node's registration, reporting what it displaced and what else
    /// claims the same box.
    pub fn insert(&self, record: NodeRecord) -> Admitted {
        let mut nodes = self.write();
        let same_machine = if record.machine.is_empty() {
            // Unidentified boxes are never each other: an empty serial is the absence of a
            // claim, not a claim that everyone is the same machine.
            Vec::new()
        } else {
            nodes
                .values()
                .filter(|n| n.alias != record.alias && n.machine == record.machine)
                .map(|n| n.alias.clone())
                .collect()
        };
        let displaced = nodes.insert(record.alias.clone(), record);
        Admitted {
            displaced,
            same_machine,
        }
    }

    /// The alias currently serving `machine`, if any. One box, one live connection: a second
    /// dial under another name is the same machine and a caller may want to know.
    /// Every alias running on `machine` — the id is an ADDRESS (ADR-0054), so a job that
    /// names one reaches all of its processes. Two nodes of one box are the same hardware and
    /// different doors; picking one here would be this function deciding which.
    ///
    /// An empty id addresses nothing: the nodes that cannot say who they are do not become one
    /// node, and "the unidentified ones" is not a place a job can ask for.
    pub fn aliases_of_machine(&self, machine: &str) -> Vec<String> {
        if machine.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<String> = self
            .read()
            .values()
            .filter(|n| n.machine == machine)
            .map(|n| n.alias.clone())
            .collect();
        out.sort();
        out
    }

    /// `alias=machine` for every node, sorted — what a refusal lists when it could not match a
    /// machine id: the caller needs the ids that ARE here, not the aliases it could not use.
    pub fn aliases_with_machine(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .read()
            .values()
            .map(|n| {
                format!(
                    "{}={}",
                    n.alias,
                    if n.machine.is_empty() {
                        "(unidentified)"
                    } else {
                        &n.machine
                    }
                )
            })
            .collect();
        out.sort();
        out
    }

    /// Remove the node behind `alias`, but only if `channel` is still the connection
    /// that holds the name. A reconnect that replaced the alias leaves the new
    /// channel alone — the same identity check the presence guard has always done.
    pub fn remove(&self, alias: &str, channel: &UnboundedSender<effector_protocol::Frame>) -> bool {
        let mut nodes = self.write();
        if nodes
            .get(alias)
            .is_some_and(|n| n.conn.sender.same_channel(channel))
        {
            nodes.remove(alias);
            return true;
        }
        false
    }

    pub fn get(&self, alias: &str) -> Option<NodeRecord> {
        self.read().get(alias).cloned()
    }

    pub fn contains(&self, alias: &str) -> bool {
        self.read().contains_key(alias)
    }

    /// Every alias, sorted: a listing that a caller can compare, not a hash order.
    pub fn aliases(&self) -> BTreeSet<String> {
        self.read().keys().cloned().collect()
    }

    /// The nodes carrying every one of `labels`. An empty request means every node —
    /// the caller asked for no place, which is not the same as asking for nowhere.
    pub fn matching(&self, labels: &[String]) -> BTreeSet<String> {
        let nodes = self.read();
        if labels.is_empty() {
            return nodes.keys().cloned().collect();
        }
        nodes
            .values()
            .filter(|n| n.has_labels(labels))
            .map(|n| n.alias.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Which nodes a realm may reach.
///
/// The venue's own realms see the whole pool. A job's realm sees the nodes its
/// `Placement` names, and nothing else: the placement is a FILTER, not a hint, so a
/// call that names a node outside it is refused in words rather than quietly served
/// by a machine the job did not ask for (ADR-0053).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Reach {
    /// Every node in the pool.
    #[default]
    All,
    /// Exactly these aliases (sorted, so the set compares and prints deterministically).
    Only(BTreeSet<String>),
}

impl Reach {
    pub fn allows(&self, alias: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(set) => set.contains(alias),
        }
    }

    /// The aliases this realm can reach, sorted. `None` for "all": a listing of every
    /// node is the caller's to take from the pool, and answering it here would be a
    /// snapshot pretending to be the rule.
    pub fn restrict_to(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::All => None,
            Self::Only(set) => Some(set),
        }
    }

    /// The set as the job's outcome records it: what the job could see.
    pub fn listing(&self, pool: &NodePool) -> Vec<String> {
        match self {
            Self::All => pool.aliases().into_iter().collect(),
            Self::Only(set) => set.iter().cloned().collect(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn record(alias: &str, labels: &[&str]) -> NodeRecord {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        NodeRecord {
            alias: alias.to_string(),
            conn: ActuatorConn {
                sender: tx,
                peer: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
            },
            carriers: vec!["steel".into()],
            class: NodeClass::Host,
            machine: format!("box-{alias}"),
            labels: labels.iter().map(|l| l.to_string()).collect(),
        }
    }

    #[test]
    fn labels_match_by_containment_and_an_empty_request_matches_everything() {
        let pool = NodePool::default();
        pool.insert(record("a", &["site:home", "u101:laptop"]));
        pool.insert(record("b", &["site:home"]));
        pool.insert(record("c", &[]));

        assert_eq!(pool.matching(&[]), ["a", "b", "c"].map(String::from).into());
        assert_eq!(
            pool.matching(&["site:home".into()]),
            ["a", "b"].map(String::from).into()
        );
        // Every requested label must be present — a subset match, not a union.
        assert_eq!(
            pool.matching(&["site:home".into(), "u101:laptop".into()]),
            ["a"].map(String::from).into()
        );
        assert!(pool.matching(&["site:office".into()]).is_empty());
    }

    #[test]
    fn an_alias_replacement_reports_the_record_it_displaced() {
        let pool = NodePool::default();
        assert!(pool.insert(record("a", &["site:home"])).displaced.is_none());
        let old = pool
            .insert(record("a", &["site:office"]))
            .displaced
            .expect("displaced");
        assert_eq!(old.labels, vec!["site:home".to_string()]);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.get("a").unwrap().labels, vec!["site:office".to_string()]);
    }

    #[test]
    fn one_box_under_two_names_is_reported_not_merged() {
        let pool = NodePool::default();
        assert!(pool.insert(record("a", &[])).same_machine.is_empty());
        // The same machine dials again under a new alias: the pool keeps both records and
        // says which other alias claims the box, because nothing here is entitled to decide
        // that the two connections are one node.
        let again = pool.insert(record("a-renamed", &[]));
        assert!(again.same_machine.is_empty(), "a-renamed is a different box in this fixture");
        let mut same = record("b", &[]);
        same.machine = "box-a".into();
        assert_eq!(pool.insert(same).same_machine, vec!["a".to_string()]);
        // Two names, one box: the id reaches BOTH doors (ADR-0054 — an id addresses the
        // hardware, not one process), and `same_machine` above is what says they are one box.
        assert_eq!(
            pool.aliases_of_machine("box-a"),
            vec!["a".to_string(), "b".to_string()]
        );
        // An unidentified box is not every other unidentified box.
        let mut one = record("x", &[]);
        one.machine = String::new();
        let mut two = record("y", &[]);
        two.machine = String::new();
        pool.insert(one);
        assert!(pool.insert(two).same_machine.is_empty());
        assert!(pool.aliases_of_machine("").is_empty(), "an empty id addresses nothing");
    }

    #[test]
    fn removal_is_identity_checked_so_a_reconnect_keeps_its_name() {
        let pool = NodePool::default();
        pool.insert(record("a", &[]));
        let stale = pool.get("a").unwrap().conn.sender.clone();
        pool.insert(record("a", &[])); // the reconnect
        assert!(
            !pool.remove("a", &stale),
            "the old connection must not delete the new one"
        );
        assert!(pool.contains("a"));
        let live = pool.get("a").unwrap().conn.sender.clone();
        assert!(pool.remove("a", &live));
        assert!(!pool.contains("a"));
    }

    #[test]
    fn reach_is_a_filter_and_says_what_it_could_see() {
        let pool = NodePool::default();
        pool.insert(record("a", &[]));
        pool.insert(record("b", &[]));
        assert!(Reach::All.allows("anything"));
        assert_eq!(Reach::All.listing(&pool), vec!["a".to_string(), "b".to_string()]);

        let only = Reach::Only(["a".to_string()].into());
        assert!(only.allows("a"));
        assert!(!only.allows("b"), "a placement the job did not ask for");
        assert_eq!(only.listing(&pool), vec!["a".to_string()]);
    }
}