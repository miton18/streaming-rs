//! Pure Scuttlebutt state machine: no I/O, no clock, no randomness.
//!
//! Time is passed explicitly (`now: Duration`, any monotonic-ish origin) and
//! randomness through arguments, so everything here is unit-testable.
//!
//! # Versions
//!
//! Each node owns its key/values. Every local mutation bumps the node's
//! `max_version` and stamps the mutated key with it, so a node's history is a
//! sequence of versions `1..=max_version`, each one either the current version
//! of a live key, the version of a tombstone (deleted key), or overwritten.
//! A replica of node `N` held elsewhere always knows a *prefix* of that
//! history: "every version `<= max_version` that still matters". Deltas are
//! built and applied so that this prefix invariant holds:
//!
//! * a delta for `N` carries every surviving update in `(from, to]` sorted by
//!   version (possibly truncated by the byte budget, `to` then being the last
//!   version included), and the receiver applies only versions above its own
//!   `max_version`, then raises it to `to`.
//!
//! # Tombstones and `last_gc_version`
//!
//! Deleted keys are kept as tombstones for `tombstone_grace`, then dropped;
//! the highest dropped version is remembered as `last_gc_version` (`L`).
//! Invariant: *a replica with `last_gc_version = L` reflects every deletion
//! with a version `<= L`*. A peer whose digest shows `max_version < L` and
//! `last_gc_version < L` may still hold keys whose tombstones were dropped, so
//! it gets a *reset*: a full snapshot of the live keys (no tombstones) that
//! replaces what it knows; the snapshot's `last_gc_version` is the max of the
//! sender's `L` and the versions of the tombstones it omitted (only those
//! covered by the snapshot when truncated), which preserves the invariant.
//! Unknown nodes and older generations get resets too.
//!
//! # Failure detection
//!
//! A timeout detector: a node is alive while its heartbeat progressed during
//! the last `dead_after`. A node learnt for the first time starts *not alive*
//! until a heartbeat increase is observed (its info may be stale: it may be a
//! long-dead node still circulating). A higher generation is treated as proof
//! of life (the process restarted). Dead nodes are forgotten `forget_after`
//! after their last progress; the `(generation, heartbeat)` they were forgotten
//! with is remembered for another `forget_after`, so that stale copies still
//! circulating cannot resurrect them, while a real heartbeat increase can.

use crate::{
    config::{GossipConfig, Limits},
    error::GossipError,
    wire::{self, Delta, Digest, DigestEntry, KeyUpdate, NodeDelta, key_update_size, node_header_size, varint_len},
};
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

/// A cluster member as seen by the local node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// Node id.
    pub node_id: String,
    /// Generation.
    pub generation: u64,
    /// Advertise address.
    pub addr: String,
    /// Liveness according to the local failure detector (always true for self).
    pub alive: bool,
    /// Live key/values.
    pub values: BTreeMap<String, String>,
}

/// Timings and limits used by the state machine.
#[derive(Debug, Clone)]
pub struct StateConfig {
    /// See [`GossipConfig::dead_after`].
    pub dead_after: Duration,
    /// See [`GossipConfig::forget_after`].
    pub forget_after: Duration,
    /// See [`GossipConfig::tombstone_grace`].
    pub tombstone_grace: Duration,
    /// State limits.
    pub limits: Limits,
}

impl From<&GossipConfig> for StateConfig {
    fn from(c: &GossipConfig) -> Self {
        Self {
            dead_after: c.dead_after,
            forget_after: c.forget_after,
            tombstone_grace: c.tombstone_grace,
            limits: c.limits.clone(),
        }
    }
}

/// Counters maintained by the state machine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateCounters {
    /// Deltas that had to be truncated by the byte budget.
    pub deltas_truncated: u64,
    /// Node resets sent.
    pub resets_sent: u64,
    /// Incoming node deltas rejected (limits / malformed).
    pub node_deltas_rejected: u64,
    /// Nodes replaced by a higher generation.
    pub generation_bumps: u64,
    /// Nodes seen alive again after having been marked dead.
    pub nodes_revived: u64,
    /// Alive -> dead transitions.
    pub nodes_marked_dead: u64,
    /// Nodes forgotten.
    pub nodes_forgotten: u64,
    /// Nodes learnt for the first time.
    pub nodes_discovered: u64,
    /// Tombstones garbage-collected.
    pub tombstones_gc: u64,
}

/// Result of [`ClusterState::compute_delta`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeltaInfo {
    /// Exact encoded size of the delta.
    pub bytes: usize,
    /// Something was left out because of the budget.
    pub truncated: bool,
    /// Number of resets included.
    pub resets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Versioned {
    value: Option<String>,
    version: u64,
    tombstoned_at: Option<Duration>,
}

#[derive(Debug, Clone)]
struct NodeState {
    generation: u64,
    addr: String,
    heartbeat: u64,
    kvs: BTreeMap<String, Versioned>,
    max_version: u64,
    last_gc_version: u64,
    last_progress: Option<Duration>,
    learned_at: Duration,
    alive: bool,
    was_dead: bool,
}

impl NodeState {
    fn live_values(&self) -> BTreeMap<String, String> {
        self.kvs
            .iter()
            .filter_map(|(k, v)| v.value.as_ref().map(|val| (k.clone(), val.clone())))
            .collect()
    }

    fn live_count(&self) -> usize {
        self.kvs.values().filter(|v| v.value.is_some()).count()
    }
}

fn live_count(kvs: &BTreeMap<String, Versioned>) -> usize {
    kvs.values().filter(|v| v.value.is_some()).count()
}

fn visible_eq(a: &BTreeMap<String, Versioned>, b: &BTreeMap<String, Versioned>) -> bool {
    let live = |m: &BTreeMap<String, Versioned>| {
        m.iter()
            .filter_map(|(k, v)| v.value.as_ref().map(|x| (k.clone(), x.clone())))
            .collect::<Vec<_>>()
    };
    live(a) == live(b)
}

#[derive(Debug, Clone)]
struct Forgotten {
    generation: u64,
    heartbeat: u64,
    at: Duration,
}

/// Read-only view of one node's replicated state (tests, debugging).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    /// Generation.
    pub generation: u64,
    /// Heartbeat.
    pub heartbeat: u64,
    /// Max version known.
    pub max_version: u64,
    /// Last GC version.
    pub last_gc_version: u64,
    /// Alive flag.
    pub alive: bool,
    /// Number of tombstones held.
    pub tombstones: usize,
    /// Live values.
    pub values: BTreeMap<String, String>,
}

/// The whole replicated cluster state held by one node.
#[derive(Debug, Clone)]
pub struct ClusterState {
    self_id: String,
    nodes: BTreeMap<String, NodeState>,
    forgotten: BTreeMap<String, Forgotten>,
    cfg: StateConfig,
    counters: StateCounters,
}

/// Small deterministic mixing function for tie-breaking.
fn mix(s: &str, salt: u64) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64 ^ salt;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h ^ (h >> 29)
}

impl ClusterState {
    /// New state containing only the local node.
    #[must_use]
    pub fn new(self_id: &str, generation: u64, addr: &str, cfg: StateConfig, now: Duration) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self_id.to_string(),
            NodeState {
                generation,
                addr: addr.to_string(),
                heartbeat: 0,
                kvs: BTreeMap::new(),
                max_version: 0,
                last_gc_version: 0,
                last_progress: Some(now),
                learned_at: now,
                alive: true,
                was_dead: false,
            },
        );
        Self {
            self_id: self_id.to_string(),
            nodes,
            forgotten: BTreeMap::new(),
            cfg,
            counters: StateCounters::default(),
        }
    }

    /// Local node id.
    #[must_use]
    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    /// Counters.
    #[must_use]
    pub fn counters(&self) -> &StateCounters {
        &self.counters
    }

    #[allow(clippy::expect_used)]
    fn me(&mut self) -> &mut NodeState {
        self.nodes.get_mut(&self.self_id).expect("self node is never removed")
    }

    /// Publish / overwrite a local key. Returns whether anything changed.
    ///
    /// # Errors
    /// Limit violations.
    pub fn set_local(&mut self, key: &str, value: &str, _now: Duration) -> Result<bool, GossipError> {
        let l = &self.cfg.limits;
        if key.is_empty() || key.len() > l.max_key_len {
            return Err(GossipError::KeyTooLong {
                len: key.len(),
                max: l.max_key_len,
            });
        }
        if value.len() > l.max_value_len {
            return Err(GossipError::ValueTooLong {
                len: value.len(),
                max: l.max_value_len,
            });
        }
        let max_keys = l.max_keys;
        let me = self.me();
        match me.kvs.get(key) {
            Some(v) if v.value.as_deref() == Some(value) => return Ok(false),
            Some(v) if v.value.is_some() => {}
            _ => {
                if me.live_count() >= max_keys {
                    return Err(GossipError::TooManyKeys { max: max_keys });
                }
            }
        }
        me.max_version += 1;
        let version = me.max_version;
        me.kvs.insert(
            key.to_string(),
            Versioned {
                value: Some(value.to_string()),
                version,
                tombstoned_at: None,
            },
        );
        Ok(true)
    }

    /// Delete a local key (leaves a tombstone). Returns whether it existed.
    pub fn remove_local(&mut self, key: &str, now: Duration) -> bool {
        let me = self.me();
        match me.kvs.get(key) {
            Some(v) if v.value.is_some() => {}
            _ => return false,
        }
        me.max_version += 1;
        let version = me.max_version;
        me.kvs.insert(
            key.to_string(),
            Versioned {
                value: None,
                version,
                tombstoned_at: Some(now),
            },
        );
        true
    }

    /// Increment the local heartbeat (once per round).
    pub fn beat(&mut self) {
        self.me().heartbeat += 1;
    }

    /// Local heartbeat.
    #[must_use]
    pub fn heartbeat(&self) -> u64 {
        self.nodes.get(&self.self_id).map_or(0, |n| n.heartbeat)
    }

    /// Digest of everything known.
    #[must_use]
    pub fn digest(&self) -> Digest {
        Digest {
            entries: self
                .nodes
                .iter()
                .map(|(id, n)| DigestEntry {
                    node_id: id.clone(),
                    generation: n.generation,
                    heartbeat: n.heartbeat,
                    max_version: n.max_version,
                    last_gc_version: n.last_gc_version,
                })
                .collect(),
        }
    }

    /// Check an incoming digest against the limits.
    ///
    /// # Errors
    /// Description of the violation.
    pub fn validate_digest(&self, d: &Digest) -> Result<(), String> {
        let l = &self.cfg.limits;
        if d.entries.len() > l.max_nodes {
            return Err(format!("digest has {} entries > {}", d.entries.len(), l.max_nodes));
        }
        for e in &d.entries {
            if e.node_id.is_empty() || e.node_id.len() > l.max_node_id_len {
                return Err("digest node id length out of bounds".into());
            }
        }
        Ok(())
    }

    /// Compute what a peer with digest `peer` is missing, within `budget`
    /// encoded bytes. `tie_break` randomises the order of equally stale nodes.
    ///
    /// Two passes: first one header per node that needs an update (so that
    /// heartbeats, which drive failure detection, are not starved by a large
    /// backlog), nodes sorted by decreasing staleness; then the key updates,
    /// lowest versions first, node by node in the same order.
    pub fn compute_delta(&mut self, peer: &Digest, budget: usize, tie_break: u64) -> (Delta, DeltaInfo) {
        let peer: HashMap<&str, &DigestEntry> = peer.entries.iter().map(|e| (e.node_id.as_str(), e)).collect();

        struct Candidate<'a> {
            header: NodeDelta,
            stale: Vec<(&'a String, &'a Versioned)>,
            full_to: u64,
            omitted_tombstones: Vec<u64>,
            order: (usize, u64),
        }
        let mut candidates: Vec<Candidate<'_>> = Vec::new();
        for (id, ns) in &self.nodes {
            let (reset, floor, peer_hb) = match peer.get(id.as_str()) {
                None => (true, 0, None),
                Some(d) if d.generation < ns.generation => (true, 0, None),
                Some(d) if d.generation > ns.generation => continue,
                Some(d) => {
                    if d.max_version < ns.last_gc_version && d.last_gc_version < ns.last_gc_version {
                        (true, 0, Some(d.heartbeat))
                    } else {
                        (false, d.max_version, Some(d.heartbeat))
                    }
                }
            };
            let mut stale: Vec<(&String, &Versioned)> = ns
                .kvs
                .iter()
                .filter(|(_, v)| v.version > floor && !(reset && v.value.is_none()))
                .collect();
            stale.sort_by_key(|(_, v)| v.version);
            let mut omitted_tombstones: Vec<u64> = if reset {
                ns.kvs
                    .values()
                    .filter(|v| v.value.is_none())
                    .map(|v| v.version)
                    .collect()
            } else {
                Vec::new()
            };
            omitted_tombstones.sort_unstable();
            let needs_hb = peer_hb.is_none_or(|hb| hb < ns.heartbeat);
            if !reset && stale.is_empty() && !needs_hb {
                continue;
            }
            candidates.push(Candidate {
                header: NodeDelta {
                    node_id: id.clone(),
                    generation: ns.generation,
                    addr: ns.addr.clone(),
                    heartbeat: ns.heartbeat,
                    reset,
                    from_version: floor,
                    to_version: if reset { 0 } else { floor },
                    last_gc_version: ns.last_gc_version,
                    updates: Vec::new(),
                },
                order: (stale.len(), mix(id, tie_break)),
                full_to: ns.max_version.max(floor),
                stale,
                omitted_tombstones,
            });
        }
        candidates.sort_by_key(|c| std::cmp::Reverse(c.order));

        let mut info = DeltaInfo::default();
        // Pass 1: headers. Sizes are computed with the final `to_version`
        // bound (`full_to`) so that the later update of `to_version` can only
        // shrink the header.
        let mut total = varint_len(0);
        let mut included: Vec<Candidate<'_>> = Vec::new();
        for mut c in candidates {
            c.header.to_version = c.full_to;
            c.header.last_gc_version = if c.header.reset {
                c.omitted_tombstones
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .max(c.header.last_gc_version)
            } else {
                // Only meaningful for resets: keep the header small.
                0
            };
            let hsize = node_header_size(&c.header, 0);
            let count_growth = varint_len(included.len() as u64 + 1) - varint_len(included.len() as u64);
            if total + hsize + count_growth > budget {
                info.truncated = true;
                continue;
            }
            total += hsize + count_growth;
            included.push(c);
        }
        // Pass 2: updates.
        let mut nodes = Vec::with_capacity(included.len());
        for mut c in included {
            let mut complete = true;
            for (k, v) in &c.stale {
                let n = c.header.updates.len() as u64;
                let grow = key_update_size(k, v.value.as_deref(), v.version) + varint_len(n + 1) - varint_len(n);
                if total + grow > budget {
                    complete = false;
                    break;
                }
                total += grow;
                c.header.updates.push(KeyUpdate {
                    key: (*k).clone(),
                    value: v.value.clone(),
                    version: v.version,
                });
            }
            if !complete {
                info.truncated = true;
                let to = c.header.updates.last().map_or(c.header.from_version, |u| u.version);
                c.header.to_version = to;
                if c.header.reset {
                    // Only the tombstones covered by the truncated snapshot
                    // are accounted as garbage-collected.
                    let own_gc = self.nodes.get(&c.header.node_id).map_or(0, |n| n.last_gc_version);
                    let covered = c.omitted_tombstones.iter().copied().filter(|&t| t <= to).max();
                    c.header.last_gc_version = covered.unwrap_or(0).max(own_gc);
                }
            }
            if c.header.reset {
                info.resets += 1;
            }
            nodes.push(c.header);
        }
        let delta = Delta { nodes };
        info.bytes = wire::delta_size(&delta);
        debug_assert!(info.bytes <= budget.max(1), "delta {} > budget {budget}", info.bytes);
        if info.truncated {
            self.counters.deltas_truncated += 1;
        }
        self.counters.resets_sent += info.resets as u64;
        (delta, info)
    }

    fn validate_node_delta(&self, nd: &NodeDelta) -> Result<(), String> {
        let l = &self.cfg.limits;
        if nd.node_id.is_empty() || nd.node_id.len() > l.max_node_id_len {
            return Err("node id length out of bounds".into());
        }
        if nd.addr.len() > l.max_addr_len {
            return Err("addr too long".into());
        }
        if nd.from_version > nd.to_version {
            return Err("from_version > to_version".into());
        }
        if nd.reset && nd.from_version != 0 {
            return Err("reset with from_version != 0".into());
        }
        let mut prev = nd.from_version;
        for u in &nd.updates {
            if u.key.is_empty() || u.key.len() > l.max_key_len {
                return Err("key length out of bounds".into());
            }
            if u.value.as_ref().is_some_and(|v| v.len() > l.max_value_len) {
                return Err("value too long".into());
            }
            if u.version <= prev || u.version > nd.to_version {
                return Err("update versions not increasing within (from, to]".into());
            }
            prev = u.version;
        }
        Ok(())
    }

    fn kvs_from_updates(updates: &[KeyUpdate], now: Duration) -> BTreeMap<String, Versioned> {
        updates
            .iter()
            .map(|u| {
                (
                    u.key.clone(),
                    Versioned {
                        value: u.value.clone(),
                        version: u.version,
                        tombstoned_at: u.value.is_none().then_some(now),
                    },
                )
            })
            .collect()
    }

    /// Apply a delta received from a peer. Returns whether the visible
    /// membership (values, liveness, nodes) changed.
    pub fn apply_delta(&mut self, delta: Delta, now: Duration) -> bool {
        let mut changed = false;
        for nd in delta.nodes {
            match self.apply_node_delta(nd, now) {
                Ok(c) => changed |= c,
                Err(e) => {
                    tracing::debug!(error = %e, "rejected node delta");
                    self.counters.node_deltas_rejected += 1;
                }
            }
        }
        changed
    }

    fn apply_node_delta(&mut self, nd: NodeDelta, now: Duration) -> Result<bool, String> {
        if nd.node_id == self.self_id {
            return Ok(false);
        }
        self.validate_node_delta(&nd)?;
        let max_keys = self.cfg.limits.max_keys;
        if let Some(f) = self.forgotten.get(&nd.node_id)
            && (nd.generation < f.generation || (nd.generation == f.generation && nd.heartbeat <= f.heartbeat))
        {
            return Ok(false);
        }
        let Some(ns) = self.nodes.get_mut(&nd.node_id) else {
            if !nd.reset {
                return Ok(false);
            }
            if self.nodes.len() >= self.cfg.limits.max_nodes {
                return Err("max_nodes reached".into());
            }
            let kvs = Self::kvs_from_updates(&nd.updates, now);
            if live_count(&kvs) > max_keys {
                return Err("too many keys".into());
            }
            self.forgotten.remove(&nd.node_id);
            self.counters.nodes_discovered += 1;
            self.nodes.insert(
                nd.node_id.clone(),
                NodeState {
                    generation: nd.generation,
                    addr: nd.addr,
                    heartbeat: nd.heartbeat,
                    kvs,
                    max_version: nd.to_version,
                    last_gc_version: nd.last_gc_version,
                    last_progress: None,
                    learned_at: now,
                    alive: false,
                    was_dead: false,
                },
            );
            return Ok(true);
        };
        if nd.generation < ns.generation {
            return Ok(false);
        }
        if nd.generation > ns.generation {
            if !nd.reset {
                return Ok(false);
            }
            let kvs = Self::kvs_from_updates(&nd.updates, now);
            if live_count(&kvs) > max_keys {
                return Err("too many keys".into());
            }
            let revived = !ns.alive && ns.was_dead;
            *ns = NodeState {
                generation: nd.generation,
                addr: nd.addr,
                heartbeat: nd.heartbeat,
                kvs,
                max_version: nd.to_version,
                last_gc_version: nd.last_gc_version,
                last_progress: Some(now),
                learned_at: now,
                alive: true,
                was_dead: false,
            };
            self.counters.generation_bumps += 1;
            if revived {
                self.counters.nodes_revived += 1;
            }
            return Ok(true);
        }
        // Same generation.
        let mut changed = false;
        if nd.reset {
            let kvs = Self::kvs_from_updates(&nd.updates, now);
            if live_count(&kvs) > max_keys {
                return Err("too many keys".into());
            }
            changed |= !visible_eq(&ns.kvs, &kvs);
            ns.kvs = kvs;
            ns.max_version = nd.to_version;
            ns.last_gc_version = nd.last_gc_version;
            ns.addr = nd.addr;
        } else if nd.from_version <= ns.max_version {
            let fresh: Vec<&KeyUpdate> = nd.updates.iter().filter(|u| u.version > ns.max_version).collect();
            if !fresh.is_empty() {
                let mut kvs = ns.kvs.clone();
                for u in fresh {
                    kvs.insert(
                        u.key.clone(),
                        Versioned {
                            value: u.value.clone(),
                            version: u.version,
                            tombstoned_at: u.value.is_none().then_some(now),
                        },
                    );
                }
                if live_count(&kvs) > max_keys {
                    return Err("too many keys".into());
                }
                changed |= !visible_eq(&ns.kvs, &kvs);
                ns.kvs = kvs;
            }
            ns.max_version = ns.max_version.max(nd.to_version);
        }
        if nd.heartbeat > ns.heartbeat {
            ns.heartbeat = nd.heartbeat;
            ns.last_progress = Some(now);
            if !ns.alive {
                ns.alive = true;
                changed = true;
                if ns.was_dead {
                    self.counters.nodes_revived += 1;
                }
            }
        }
        Ok(changed)
    }

    /// Run the failure detector, forget long-dead nodes and garbage-collect
    /// tombstones. Returns whether the visible membership changed.
    pub fn tick(&mut self, now: Duration) -> bool {
        let mut changed = false;
        let cfg = self.cfg.clone();
        let mut to_forget = Vec::new();
        for (id, ns) in &mut self.nodes {
            if *id == self.self_id {
                continue;
            }
            let fresh = ns.last_progress.is_some_and(|t| now.saturating_sub(t) < cfg.dead_after);
            if ns.alive && !fresh {
                ns.alive = false;
                ns.was_dead = true;
                self.counters.nodes_marked_dead += 1;
                changed = true;
            }
            let since = ns.last_progress.unwrap_or(ns.learned_at);
            if !ns.alive && now.saturating_sub(since) >= cfg.forget_after {
                to_forget.push(id.clone());
            }
        }
        for id in to_forget {
            if let Some(ns) = self.nodes.remove(&id) {
                self.forgotten.insert(
                    id,
                    Forgotten {
                        generation: ns.generation,
                        heartbeat: ns.heartbeat,
                        at: now,
                    },
                );
                self.counters.nodes_forgotten += 1;
                changed = true;
            }
        }
        self.forgotten
            .retain(|_, f| now.saturating_sub(f.at) < cfg.forget_after);
        for ns in self.nodes.values_mut() {
            let mut gc_max = None;
            ns.kvs.retain(|_, v| match v.tombstoned_at {
                Some(t) if now.saturating_sub(t) >= cfg.tombstone_grace => {
                    gc_max = Some(gc_max.unwrap_or(0).max(v.version));
                    self.counters.tombstones_gc += 1;
                    false
                }
                _ => true,
            });
            if let Some(g) = gc_max {
                ns.last_gc_version = ns.last_gc_version.max(g);
            }
        }
        changed
    }

    /// All known members (self included), sorted by node id.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.nodes
            .iter()
            .map(|(id, n)| Member {
                node_id: id.clone(),
                generation: n.generation,
                addr: n.addr.clone(),
                alive: *id == self.self_id || n.alive,
                values: n.live_values(),
            })
            .collect()
    }

    /// Advertise addresses of other nodes: `(alive, not alive)`.
    #[must_use]
    pub fn peer_addrs(&self) -> (Vec<String>, Vec<String>) {
        let mut live = Vec::new();
        let mut dead = Vec::new();
        for (id, n) in &self.nodes {
            if *id == self.self_id {
                continue;
            }
            if n.alive {
                live.push(n.addr.clone());
            } else {
                dead.push(n.addr.clone());
            }
        }
        (live, dead)
    }

    /// Inspect one node.
    #[must_use]
    pub fn node_view(&self, id: &str) -> Option<NodeView> {
        self.nodes.get(id).map(|n| NodeView {
            generation: n.generation,
            heartbeat: n.heartbeat,
            max_version: n.max_version,
            last_gc_version: n.last_gc_version,
            alive: *id == self.self_id || n.alive,
            tombstones: n.kvs.values().filter(|v| v.value.is_none()).count(),
            values: n.live_values(),
        })
    }

    /// Whether a forgotten record exists for `id`.
    #[must_use]
    pub fn is_forgotten(&self, id: &str) -> bool {
        self.forgotten.contains_key(id)
    }
}

/// Randomness source for [`select_targets`].
pub trait TargetRng {
    /// Uniform index in `0..n` (`n > 0`).
    fn index(&mut self, n: usize) -> usize;
    /// `true` with probability `p`.
    fn chance(&mut self, p: f64) -> bool;
}

/// Choose the peers to gossip with in one round:
/// * `fanout` distinct random live peers;
/// * a random seed when no live peer is known, else with `seed_probability`;
/// * a random non-alive node with probability `dead / (live + 1)`, so that
///   partitions heal and freshly learnt nodes are contacted directly.
///
/// `self_addr` and duplicates are never returned.
pub fn select_targets(
    live: &[String],
    dead: &[String],
    seeds: &[String],
    self_addr: &str,
    fanout: usize,
    seed_probability: f64,
    rng: &mut impl TargetRng,
) -> Vec<String> {
    let mut targets: Vec<String> = Vec::new();
    let mut pool: Vec<&String> = live.iter().filter(|a| *a != self_addr).collect();
    let live_count = pool.len();
    while targets.len() < fanout && !pool.is_empty() {
        let i = rng.index(pool.len());
        let a = pool.swap_remove(i);
        if !targets.contains(a) {
            targets.push(a.clone());
        }
    }
    if live_count == 0 || rng.chance(seed_probability) {
        let candidates: Vec<&String> = seeds
            .iter()
            .filter(|s| *s != self_addr && !targets.contains(s))
            .collect();
        if !candidates.is_empty() {
            targets.push(candidates[rng.index(candidates.len())].clone());
        }
    }
    let dead_candidates: Vec<&String> = dead
        .iter()
        .filter(|a| *a != self_addr && !targets.contains(a))
        .collect();
    if !dead_candidates.is_empty() {
        #[allow(clippy::cast_precision_loss)]
        let p = dead_candidates.len() as f64 / (live_count + 1) as f64;
        if rng.chance(p.min(1.0)) {
            targets.push(dead_candidates[rng.index(dead_candidates.len())].clone());
        }
    }
    targets
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
