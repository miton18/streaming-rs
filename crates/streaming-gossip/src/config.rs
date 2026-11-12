//! Configuration and limits.

use crate::{error::GossipError, wire};
use std::time::Duration;

/// Size limits enforced on the gossiped state.
///
/// They keep the state light so that a (re)joining node resynchronises in a
/// few rounds. Every node of a cluster must use the same limits: a node
/// rejects incoming node states that violate *its own* limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Maximum number of live keys a node may publish.
    pub max_keys: usize,
    /// Maximum key length in bytes (keys must also be non-empty).
    pub max_key_len: usize,
    /// Maximum value length in bytes.
    pub max_value_len: usize,
    /// Maximum node id length in bytes.
    pub max_node_id_len: usize,
    /// Maximum advertise address length in bytes.
    pub max_addr_len: usize,
    /// Maximum number of nodes accepted in a digest.
    pub max_nodes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_keys: 64,
            max_key_len: 256,
            max_value_len: 1024,
            max_node_id_len: 128,
            max_addr_len: 256,
            max_nodes: 1024,
        }
    }
}

impl Limits {
    /// Upper bound of the encoded size of one key update (key + value + version).
    #[must_use]
    pub fn max_update_bytes(&self) -> usize {
        // key (varint len + bytes) + Option tag + value (varint len + bytes) + version varint
        wire::varint_len(self.max_key_len as u64)
            + self.max_key_len
            + 1
            + wire::varint_len(self.max_value_len as u64)
            + self.max_value_len
            + wire::MAX_VARINT_U64
    }

    /// Upper bound of the encoded size of a node delta header (no updates).
    #[must_use]
    pub fn max_node_header_bytes(&self) -> usize {
        wire::varint_len(self.max_node_id_len as u64)
            + self.max_node_id_len
            + wire::varint_len(self.max_addr_len as u64)
            + self.max_addr_len
            + 5 * wire::MAX_VARINT_U64 // generation, heartbeat, from, to, last_gc
            + 1 // reset flag
            + wire::MAX_VARINT_USIZE // updates length
    }

    /// Upper bound of the encoded size of one node's full live state.
    #[must_use]
    pub fn max_node_state_bytes(&self) -> usize {
        self.max_node_header_bytes() + self.max_keys * self.max_update_bytes()
    }

    /// Upper bound of the encoded size of one digest entry.
    #[must_use]
    pub fn max_digest_entry_bytes(&self) -> usize {
        wire::varint_len(self.max_node_id_len as u64) + self.max_node_id_len + 4 * wire::MAX_VARINT_U64
    }

    /// Upper bound of the encoded size of a digest describing `nodes` nodes.
    #[must_use]
    pub fn max_digest_bytes(&self, nodes: usize) -> usize {
        wire::MAX_VARINT_USIZE + nodes * self.max_digest_entry_bytes()
    }
}

/// Gossip configuration.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// Cluster identifier: exchanges with a different cluster id are rejected.
    pub cluster_id: String,
    /// Unique node identifier (stable across restarts).
    pub node_id: String,
    /// Incarnation number; must increase on every process restart.
    pub generation: u64,
    /// Address to bind.
    pub listen_addr: String,
    /// Address peers connect to.
    pub advertise_addr: String,
    /// Advertise addresses of seed nodes.
    pub seeds: Vec<String>,
    /// Delay between two gossip rounds.
    pub gossip_interval: Duration,
    /// Number of random live peers contacted per round.
    pub gossip_fanout: usize,
    /// Probability of additionally contacting a seed in a round (always when no
    /// live peer is known).
    pub seed_gossip_probability: f64,
    /// A node whose heartbeat has not progressed for this long is dead.
    pub dead_after: Duration,
    /// A dead node is forgotten this long after its last heartbeat progress.
    pub forget_after: Duration,
    /// Tombstones of deleted keys are garbage-collected after this period.
    pub tombstone_grace: Duration,
    /// Timeout of a TCP connect.
    pub connect_timeout: Duration,
    /// Timeout of a whole SYN / SYN-ACK / ACK exchange (both sides).
    pub exchange_timeout: Duration,
    /// Maximum encoded size of a delta (SYN-ACK or ACK payload part).
    pub delta_budget_bytes: usize,
    /// Hard limit of a frame payload; larger frames are rejected.
    pub max_frame_bytes: usize,
    /// Maximum concurrently served inbound exchanges.
    pub max_inbound_exchanges: usize,
    /// State limits.
    pub limits: Limits,
}

impl GossipConfig {
    /// Configuration with defaults; `addr` is used both as listen and
    /// advertise address, and there are no seeds.
    #[must_use]
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        generation: u64,
        addr: impl Into<String>,
    ) -> Self {
        let addr = addr.into();
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            generation,
            listen_addr: addr.clone(),
            advertise_addr: addr,
            seeds: Vec::new(),
            gossip_interval: Duration::from_secs(1),
            gossip_fanout: 1,
            seed_gossip_probability: 0.1,
            dead_after: Duration::from_secs(10),
            forget_after: Duration::from_secs(60),
            tombstone_grace: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(2),
            exchange_timeout: Duration::from_secs(5),
            delta_budget_bytes: 64 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_inbound_exchanges: 64,
            limits: Limits::default(),
        }
    }

    /// Check internal consistency.
    ///
    /// # Errors
    /// [`GossipError::InvalidConfig`] describing the first problem found.
    pub fn validate(&self) -> Result<(), GossipError> {
        let bad = |m: String| Err(GossipError::InvalidConfig(m));
        let l = &self.limits;
        if self.node_id.is_empty() || self.node_id.len() > l.max_node_id_len {
            return bad(format!("node_id must be 1..={} bytes", l.max_node_id_len));
        }
        if self.advertise_addr.is_empty() || self.advertise_addr.len() > l.max_addr_len {
            return bad(format!("advertise_addr must be 1..={} bytes", l.max_addr_len));
        }
        if self.cluster_id.len() > l.max_node_id_len {
            return bad(format!("cluster_id longer than {} bytes", l.max_node_id_len));
        }
        if self.gossip_interval.is_zero() {
            return bad("gossip_interval must be > 0".into());
        }
        if self.dead_after <= self.gossip_interval {
            return bad("dead_after must be greater than gossip_interval".into());
        }
        if self.forget_after <= self.dead_after {
            return bad("forget_after must be greater than dead_after".into());
        }
        if !(0.0..=1.0).contains(&self.seed_gossip_probability) {
            return bad("seed_gossip_probability must be within 0..=1".into());
        }
        if l.max_keys == 0 || l.max_key_len == 0 || l.max_nodes == 0 {
            return bad("limits must be > 0".into());
        }
        let min_budget = wire::MAX_VARINT_USIZE + l.max_node_header_bytes() + l.max_update_bytes();
        if self.delta_budget_bytes < min_budget {
            return bad(format!(
                "delta_budget_bytes ({}) cannot hold one maximal key update ({min_budget} bytes)",
                self.delta_budget_bytes
            ));
        }
        let min_frame =
            wire::MESSAGE_OVERHEAD + l.max_digest_bytes(l.max_nodes) + self.delta_budget_bytes + self.cluster_id.len();
        if self.max_frame_bytes < min_frame {
            return bad(format!(
                "max_frame_bytes ({}) must hold a full digest of max_nodes plus a delta ({min_frame} bytes)",
                self.max_frame_bytes
            ));
        }
        if self.max_frame_bytes > wire::ABSOLUTE_MAX_FRAME {
            return bad(format!(
                "max_frame_bytes above the absolute limit {}",
                wire::ABSOLUTE_MAX_FRAME
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() { GossipConfig::new("c", "n", 1, "127.0.0.1:7000").validate().unwrap(); }

    #[test]
    fn inconsistent_configs_are_rejected() {
        let base = GossipConfig::new("c", "n", 1, "127.0.0.1:7000");
        let mut checks: Vec<(&str, GossipConfig)> = Vec::new();
        let mut c = base.clone();
        c.node_id = String::new();
        checks.push(("empty node id", c));
        let mut c = base.clone();
        c.delta_budget_bytes = 100;
        checks.push(("budget below one key", c));
        let mut c = base.clone();
        c.max_frame_bytes = c.delta_budget_bytes;
        checks.push(("frame below digest+delta", c));
        let mut c = base.clone();
        c.dead_after = c.gossip_interval;
        checks.push(("dead_after <= interval", c));
        let mut c = base.clone();
        c.forget_after = c.dead_after;
        checks.push(("forget_after <= dead_after", c));
        let mut c = base.clone();
        c.seed_gossip_probability = 1.5;
        checks.push(("probability", c));
        let mut c = base.clone();
        c.max_frame_bytes = wire::ABSOLUTE_MAX_FRAME + 1;
        checks.push(("absolute frame limit", c));
        for (what, c) in checks {
            assert!(
                matches!(c.validate(), Err(GossipError::InvalidConfig(_))),
                "{what} accepted"
            );
        }
    }

    #[test]
    fn default_limits_bounds() {
        let l = Limits::default();
        // 64 keys of 256 + 1024 bytes: one node's full state is ~83 KiB, so a
        // first sync of a maximal node takes 2 rounds with the 64 KiB budget.
        assert!(l.max_node_state_bytes() < 90 * 1024);
        assert!(l.max_digest_entry_bytes() <= 170);
    }

    #[test]
    fn size_bounds_are_tight() {
        let l = Limits::default();
        let key = "k".repeat(l.max_key_len);
        let value = "v".repeat(l.max_value_len);
        assert_eq!(
            wire::key_update_size(&key, Some(&value), u64::MAX),
            l.max_update_bytes()
        );
        let nd = wire::NodeDelta {
            node_id: "n".repeat(l.max_node_id_len),
            generation: u64::MAX,
            addr: "a".repeat(l.max_addr_len),
            heartbeat: u64::MAX,
            reset: true,
            from_version: u64::MAX,
            to_version: u64::MAX,
            last_gc_version: u64::MAX,
            updates: Vec::new(),
        };
        assert_eq!(wire::node_header_size(&nd, usize::MAX), l.max_node_header_bytes());
    }

    fn rejected(c: &GossipConfig) -> bool { matches!(c.validate(), Err(GossipError::InvalidConfig(_))) }

    #[test]
    fn identity_lengths_are_inclusive() {
        let base = GossipConfig::new("c", "n", 1, "127.0.0.1:7000");
        let l = base.limits.clone();
        let mut c = base.clone();
        c.node_id = "n".repeat(l.max_node_id_len);
        c.cluster_id = "c".repeat(l.max_node_id_len);
        c.advertise_addr = "a".repeat(l.max_addr_len);
        c.max_frame_bytes = wire::ABSOLUTE_MAX_FRAME;
        c.validate().unwrap();

        let mut long = c.clone();
        long.node_id.push('n');
        assert!(rejected(&long), "node id");
        let mut long = c.clone();
        long.cluster_id.push('c');
        assert!(rejected(&long), "cluster id");
        let mut long = c.clone();
        long.advertise_addr.push('a');
        assert!(rejected(&long), "address");
        let mut empty = c;
        empty.advertise_addr.clear();
        assert!(rejected(&empty), "empty address");
    }

    #[test]
    fn every_limit_must_be_positive() {
        let base = GossipConfig::new("c", "n", 1, "127.0.0.1:7000");
        for zero in [
            |l: &mut Limits| l.max_keys = 0,
            |l: &mut Limits| l.max_key_len = 0,
            |l: &mut Limits| l.max_nodes = 0,
        ] {
            let mut c = base.clone();
            zero(&mut c.limits);
            assert!(rejected(&c), "{:?}", c.limits);
        }
    }

    #[test]
    fn budget_and_frame_minimums_are_inclusive() {
        let mut c = GossipConfig::new("cluster", "n", 1, "127.0.0.1:7000");
        let l = c.limits.clone();
        // A length prefix, one maximal node header and one maximal update.
        c.delta_budget_bytes = 10 + l.max_node_header_bytes() + l.max_update_bytes();
        // Version byte, discriminant and cluster id length, a full digest of
        // max_nodes, the delta and the cluster id.
        let min_frame = 1 + 5 + 10 + l.max_digest_bytes(l.max_nodes) + c.delta_budget_bytes + "cluster".len();
        c.max_frame_bytes = min_frame;
        c.validate().unwrap();
        let mut small = c.clone();
        small.max_frame_bytes -= 1;
        assert!(rejected(&small), "frame");
        let mut small = c.clone();
        small.delta_budget_bytes -= 1;
        small.max_frame_bytes -= 1;
        assert!(rejected(&small), "budget");
    }
}
