#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation
)]

use super::*;
use crate::wire::{delta_size, digest_size};

const S: Duration = Duration::from_secs(1);

fn cfg() -> StateConfig {
    StateConfig {
        dead_after: 10 * S,
        forget_after: 60 * S,
        tombstone_grace: 30 * S,
        limits: Limits::default(),
    }
}

fn node(id: &str, generation: u64) -> ClusterState {
    ClusterState::new(id, generation, &format!("{id}:7000"), cfg(), Duration::ZERO)
}

/// `a` initiates a full SYN / SYN-ACK / ACK exchange with `b`.
fn sync(
    a: &mut ClusterState,
    b: &mut ClusterState,
    budget: usize,
    now: Duration,
) -> (DeltaInfo, DeltaInfo) {
    let da = a.digest();
    let db = b.digest();
    let (to_a, ia) = b.compute_delta(&da, budget, 7);
    assert!(ia.bytes <= budget);
    assert_eq!(ia.bytes, delta_size(&to_a));
    a.apply_delta(to_a, now);
    let (to_b, ib) = a.compute_delta(&db, budget, 11);
    assert!(ib.bytes <= budget);
    b.apply_delta(to_b, now);
    (ia, ib)
}

fn values_of(s: &ClusterState, id: &str) -> Option<BTreeMap<String, String>> {
    s.node_view(id).map(|v| v.values)
}

#[test]
fn local_limits_are_enforced() {
    let mut s = node("a", 1);
    let l = Limits::default();
    assert!(matches!(
        s.set_local("", "v", Duration::ZERO),
        Err(GossipError::KeyTooLong { .. })
    ));
    let long_key = "k".repeat(l.max_key_len + 1);
    assert!(matches!(
        s.set_local(&long_key, "v", Duration::ZERO),
        Err(GossipError::KeyTooLong { .. })
    ));
    assert!(
        s.set_local(&"k".repeat(l.max_key_len), "v", Duration::ZERO)
            .unwrap()
    );
    let long_val = "v".repeat(l.max_value_len + 1);
    assert!(matches!(
        s.set_local("x", &long_val, Duration::ZERO),
        Err(GossipError::ValueTooLong { .. })
    ));
    assert!(
        s.set_local("x", &"v".repeat(l.max_value_len), Duration::ZERO)
            .unwrap()
    );
    for i in 2..l.max_keys {
        s.set_local(&format!("key{i}"), "v", Duration::ZERO)
            .unwrap();
    }
    assert_eq!(s.node_view("a").unwrap().values.len(), l.max_keys);
    assert_eq!(
        s.set_local("one-too-many", "v", Duration::ZERO),
        Err(GossipError::TooManyKeys { max: l.max_keys })
    );
    // Overwriting an existing key at the cap is fine.
    assert!(s.set_local("x", "other", Duration::ZERO).unwrap());
    // Removing frees a slot, even though the tombstone is still held.
    assert!(s.remove_local("x", Duration::ZERO));
    assert!(s.set_local("one-too-many", "v", Duration::ZERO).unwrap());
}

#[test]
fn versions_are_monotonic_and_noops_do_not_bump() {
    let mut s = node("a", 1);
    assert!(s.set_local("k", "v1", Duration::ZERO).unwrap());
    assert_eq!(s.node_view("a").unwrap().max_version, 1);
    assert!(!s.set_local("k", "v1", Duration::ZERO).unwrap());
    assert_eq!(s.node_view("a").unwrap().max_version, 1);
    assert!(s.set_local("k", "v2", Duration::ZERO).unwrap());
    assert!(!s.remove_local("absent", Duration::ZERO));
    assert!(s.remove_local("k", Duration::ZERO));
    assert!(!s.remove_local("k", Duration::ZERO));
    let v = s.node_view("a").unwrap();
    assert_eq!(v.max_version, 3);
    assert_eq!(v.tombstones, 1);
    assert!(v.values.is_empty());
}

#[test]
fn two_nodes_sync_both_ways() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    a.set_local("addr", "10.0.0.1", Duration::ZERO).unwrap();
    b.set_local("load", "3", Duration::ZERO).unwrap();
    sync(&mut a, &mut b, 64 * 1024, S);
    assert_eq!(values_of(&b, "a"), values_of(&a, "a"));
    assert_eq!(values_of(&a, "b"), values_of(&b, "b"));
    // Nothing left to send.
    let (d, info) = b.compute_delta(&a.digest(), 64 * 1024, 0);
    assert!(d.nodes.is_empty(), "{d:?}");
    assert!(!info.truncated);
    // Newly learnt nodes are not alive until their heartbeat progresses.
    assert!(!a.node_view("b").unwrap().alive);
    b.beat();
    sync(&mut a, &mut b, 64 * 1024, 2 * S);
    assert!(a.node_view("b").unwrap().alive);
    assert!(b.node_view("a").unwrap().alive || a.heartbeat() == 0);
}

#[test]
fn delta_respects_budget_and_version_order_and_converges() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    for i in 0..60 {
        a.set_local(&format!("key-{i:02}"), &"x".repeat(200), Duration::ZERO)
            .unwrap();
    }
    // Overwrite some keys so that versions are not in key order.
    for i in (0..60).step_by(7) {
        a.set_local(&format!("key-{i:02}"), &"y".repeat(150), Duration::ZERO)
            .unwrap();
    }
    let budget = 2048;
    let mut rounds = 0;
    let mut last_max = 0;
    loop {
        let (delta, info) = a.compute_delta(&b.digest(), budget, rounds);
        assert!(info.bytes <= budget, "{} > {budget}", info.bytes);
        assert_eq!(info.bytes, delta_size(&delta));
        let nd = delta.nodes.iter().find(|n| n.node_id == "a");
        if let Some(nd) = nd {
            // Lowest versions first, contiguous with what b knows.
            assert!(nd.updates.windows(2).all(|w| w[0].version < w[1].version));
            assert_eq!(nd.from_version, last_max);
            if let Some(first) = nd.updates.first() {
                assert!(first.version > last_max);
            }
            last_max = nd.to_version;
        }
        let done = !info.truncated;
        b.apply_delta(delta, S);
        assert_eq!(b.node_view("a").unwrap().max_version, last_max);
        rounds += 1;
        if done {
            break;
        }
        assert!(rounds < 100);
    }
    assert!(
        rounds >= 6,
        "60 * ~210 bytes must need several 2 KiB rounds, got {rounds}"
    );
    assert_eq!(values_of(&b, "a"), values_of(&a, "a"));
    assert_eq!(a.counters().deltas_truncated, rounds - 1);
}

#[test]
fn heartbeats_are_not_starved_by_a_large_backlog() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    let mut c = node("c", 1);
    c.set_local("x", "1", Duration::ZERO).unwrap();
    // a learns about c, b learns about a and c.
    sync(&mut a, &mut c, 1 << 20, S);
    sync(&mut b, &mut a, 1 << 20, S);
    for i in 0..60 {
        a.set_local(&format!("k{i}"), &"z".repeat(500), Duration::ZERO)
            .unwrap();
    }
    c.beat();
    sync(&mut a, &mut c, 1 << 20, 2 * S);
    // a now has a huge own backlog for b plus a newer heartbeat of c.
    let (delta, info) = a.compute_delta(&b.digest(), 1500, 3);
    assert!(info.truncated);
    let c_delta = delta
        .nodes
        .iter()
        .find(|n| n.node_id == "c")
        .expect("c header present");
    assert_eq!(c_delta.heartbeat, 1);
    assert!(c_delta.updates.is_empty());
}

#[test]
fn higher_generation_replaces_everything() {
    let mut a1 = node("a", 1);
    let mut b = node("b", 1);
    a1.set_local("old", "1", Duration::ZERO).unwrap();
    a1.set_local("shared", "old", Duration::ZERO).unwrap();
    a1.beat();
    sync(&mut b, &mut a1, 1 << 16, S);
    a1.beat();
    sync(&mut b, &mut a1, 1 << 16, 2 * S);
    assert!(b.node_view("a").unwrap().alive);

    let mut a2 = node("a", 2);
    a2.set_local("shared", "new", Duration::ZERO).unwrap();
    sync(&mut b, &mut a2, 1 << 16, 3 * S);
    let v = b.node_view("a").unwrap();
    assert_eq!(v.generation, 2);
    assert!(v.alive, "a generation bump is proof of life");
    assert_eq!(
        v.values,
        BTreeMap::from([("shared".to_string(), "new".to_string())])
    );
    assert_eq!(b.counters().generation_bumps, 1);

    // A stale copy of generation 1 (e.g. relayed by a slow peer) is ignored.
    let (stale, _) = a1.compute_delta(&Digest::default(), 1 << 16, 0);
    b.apply_delta(stale, 4 * S);
    assert_eq!(b.node_view("a").unwrap().generation, 2);
    assert_eq!(values_of(&b, "a"), values_of(&a2, "a"));
    // And b never sends generation 1 data back to a2 (it does not know it any more).
    let (d, _) = b.compute_delta(&a2.digest(), 1 << 16, 0);
    assert!(
        d.nodes
            .iter()
            .all(|n| n.node_id != "a" || n.generation == 2)
    );
}

#[test]
fn lower_generation_in_peer_digest_triggers_reset() {
    let mut a2 = node("a", 2);
    a2.set_local("k", "v", Duration::ZERO).unwrap();
    let peer = Digest {
        entries: vec![DigestEntry {
            node_id: "a".into(),
            generation: 1,
            heartbeat: 100,
            max_version: 100,
            last_gc_version: 0,
        }],
    };
    let (d, info) = a2.compute_delta(&peer, 1 << 16, 0);
    assert_eq!(info.resets, 1);
    assert!(d.nodes[0].reset);
    assert_eq!(d.nodes[0].updates.len(), 1);
}

#[test]
fn deletions_propagate_and_tombstones_are_gced() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    a.set_local("k", "v", Duration::ZERO).unwrap();
    a.set_local("keep", "v", Duration::ZERO).unwrap();
    sync(&mut b, &mut a, 1 << 16, S);
    a.remove_local("k", S);
    sync(&mut b, &mut a, 1 << 16, 2 * S);
    assert_eq!(values_of(&b, "a").unwrap().len(), 1);
    assert_eq!(b.node_view("a").unwrap().tombstones, 1);
    // GC after the grace period, on both sides.
    a.tick(S + cfg().tombstone_grace - Duration::from_millis(1));
    assert_eq!(a.node_view("a").unwrap().tombstones, 1);
    a.tick(S + cfg().tombstone_grace);
    let va = a.node_view("a").unwrap();
    assert_eq!(va.tombstones, 0);
    assert_eq!(va.last_gc_version, 3);
    assert!(a.counters().tombstones_gc >= 1);
    b.tick(2 * S + cfg().tombstone_grace);
    assert_eq!(b.node_view("a").unwrap().tombstones, 0);
    // b is up to date: no reset even though a GC'd.
    let (d, info) = a.compute_delta(&b.digest(), 1 << 16, 0);
    assert_eq!(info.resets, 0, "{d:?}");
}

#[test]
fn peer_behind_gc_gets_reset_and_drops_deleted_key() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    a.set_local("k", "v", Duration::ZERO).unwrap();
    a.set_local("keep", "v", Duration::ZERO).unwrap();
    sync(&mut b, &mut a, 1 << 16, S);
    assert!(values_of(&b, "a").unwrap().contains_key("k"));
    // b misses the deletion, a GCs the tombstone.
    a.remove_local("k", S);
    a.set_local("later", "v", 2 * S).unwrap();
    a.tick(S + cfg().tombstone_grace);
    assert_eq!(a.node_view("a").unwrap().tombstones, 0);
    let (d, info) = a.compute_delta(&b.digest(), 1 << 16, 0);
    assert_eq!(info.resets, 1);
    b.apply_delta(d, 40 * S);
    let vb = b.node_view("a").unwrap();
    assert_eq!(vb.values, a.node_view("a").unwrap().values);
    assert!(!vb.values.contains_key("k"));
    assert_eq!(vb.last_gc_version, 3);
}

/// The resetting peer (r) must itself be able to propagate the deletion to a
/// third, stale peer (d) although r never saw the tombstone.
#[test]
fn reset_snapshot_records_omitted_tombstones() {
    let mut a = node("a", 1);
    let mut d = node("d", 1);
    let mut r = node("r", 1);
    a.set_local("k", "v", Duration::ZERO).unwrap(); // v1
    a.set_local("x", "1", Duration::ZERO).unwrap(); // v2
    sync(&mut d, &mut a, 1 << 16, S); // d has k
    a.remove_local("k", S); // tombstone v3 (not GC'd yet)
    a.set_local("y", "1", S).unwrap(); // v4
    sync(&mut r, &mut a, 1 << 16, 2 * S); // r learns a via reset: tombstone omitted
    let vr = r.node_view("a").unwrap();
    assert_eq!(vr.tombstones, 0);
    assert_eq!(vr.last_gc_version, 3);
    // d (max_version 2 < 3, last_gc 0) syncs with r only.
    sync(&mut d, &mut r, 1 << 16, 3 * S);
    let vd = d.node_view("a").unwrap();
    assert!(!vd.values.contains_key("k"), "{vd:?}");
    assert_eq!(vd.values, a.node_view("a").unwrap().values);
}

#[test]
fn truncated_reset_converges_without_reset_loops() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    for i in 0..40 {
        a.set_local(&format!("k{i:02}"), &"v".repeat(300), Duration::ZERO)
            .unwrap();
    }
    for i in 0..10 {
        a.remove_local(&format!("k{i:02}"), Duration::ZERO);
    }
    a.tick(cfg().tombstone_grace); // GC: last_gc = 50
    assert_eq!(a.node_view("a").unwrap().last_gc_version, 50);
    let mut rounds = 0;
    let mut resets = 0;
    loop {
        let (delta, info) = a.compute_delta(&b.digest(), 1500, 0);
        resets += info.resets;
        let truncated = info.truncated;
        b.apply_delta(delta, S);
        rounds += 1;
        if !truncated {
            break;
        }
        assert!(rounds < 50);
    }
    assert_eq!(resets, 1, "only the first delta is a reset");
    assert_eq!(values_of(&b, "a"), values_of(&a, "a"));
}

#[test]
fn failure_detector_transitions() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    sync(&mut a, &mut b, 1 << 16, Duration::ZERO);
    let alive = |s: &ClusterState| s.node_view("b").unwrap().alive;
    assert!(!alive(&a), "learnt but no progress yet");
    b.beat();
    sync(&mut a, &mut b, 1 << 16, S);
    assert!(alive(&a));
    assert!(!a.tick(S + cfg().dead_after - Duration::from_millis(1)));
    assert!(alive(&a));
    assert!(a.tick(S + cfg().dead_after));
    assert!(!alive(&a));
    assert_eq!(a.counters().nodes_marked_dead, 1);
    // The digest still carries the dead node so that others hear about it.
    assert_eq!(a.digest().entries.len(), 2);
    // Revive on heartbeat progress.
    b.beat();
    sync(&mut a, &mut b, 1 << 16, 20 * S);
    assert!(alive(&a));
    assert_eq!(a.counters().nodes_revived, 1);
    // Dead again then forgotten.
    a.tick(20 * S + cfg().dead_after);
    assert!(!alive(&a));
    assert!(a.tick(20 * S + cfg().forget_after));
    assert!(a.node_view("b").is_none());
    assert!(a.is_forgotten("b"));
    assert_eq!(a.counters().nodes_forgotten, 1);
    // A stale copy (same generation and heartbeat) does not resurrect it.
    let (stale, _) = b.compute_delta(&Digest::default(), 1 << 16, 0);
    a.apply_delta(stale.clone(), 81 * S);
    assert!(a.node_view("b").is_none());
    // A real heartbeat increase does.
    b.beat();
    let (fresh, _) = b.compute_delta(&Digest::default(), 1 << 16, 0);
    assert!(a.apply_delta(fresh, 82 * S));
    assert!(a.node_view("b").is_some());
    // The forgotten record expires.
    a.tick(80 * S + cfg().forget_after);
    assert!(!a.is_forgotten("b"));
}

#[test]
fn self_is_always_alive_and_never_overwritten() {
    let mut a = node("a", 5);
    a.set_local("k", "mine", Duration::ZERO).unwrap();
    a.tick(1000 * S);
    assert!(a.node_view("a").unwrap().alive);
    let mut impostor = node("a", 9);
    impostor.set_local("k", "theirs", Duration::ZERO).unwrap();
    let (d, _) = impostor.compute_delta(&Digest::default(), 1 << 16, 0);
    assert!(!a.apply_delta(d, S));
    let v = a.node_view("a").unwrap();
    assert_eq!(v.generation, 5);
    assert_eq!(v.values["k"], "mine");
}

#[test]
fn invalid_incoming_deltas_are_rejected() {
    let mut b = node("b", 1);
    let l = Limits::default();
    let mk = |updates: Vec<KeyUpdate>, to: u64| Delta {
        nodes: vec![NodeDelta {
            node_id: "a".into(),
            generation: 1,
            addr: "a:1".into(),
            heartbeat: 1,
            reset: true,
            from_version: 0,
            to_version: to,
            last_gc_version: 0,
            updates,
        }],
    };
    let up = |k: String, v: String, version| KeyUpdate {
        key: k,
        value: Some(v),
        version,
    };
    b.apply_delta(
        mk(vec![up("k".repeat(l.max_key_len + 1), "v".into(), 1)], 1),
        S,
    );
    b.apply_delta(
        mk(vec![up("k".into(), "v".repeat(l.max_value_len + 1), 1)], 1),
        S,
    );
    b.apply_delta(
        mk(
            vec![up("k".into(), "v".into(), 2), up("j".into(), "v".into(), 2)],
            2,
        ),
        S,
    );
    b.apply_delta(mk(vec![up("k".into(), "v".into(), 5)], 3), S);
    let too_many = (1..=(l.max_keys as u64 + 1))
        .map(|i| up(format!("k{i}"), "v".into(), i))
        .collect();
    b.apply_delta(mk(too_many, l.max_keys as u64 + 1), S);
    assert_eq!(b.counters().node_deltas_rejected, 5);
    assert!(b.node_view("a").is_none());
    // Unknown node without reset: ignored (cannot build a prefix).
    let mut d = mk(vec![up("k".into(), "v".into(), 4)], 4);
    d.nodes[0].reset = false;
    d.nodes[0].from_version = 3;
    assert!(!b.apply_delta(d, S));
    assert!(b.node_view("a").is_none());
    // Digest with too many nodes.
    let big = Digest {
        entries: (0..=l.max_nodes)
            .map(|i| DigestEntry {
                node_id: format!("n{i}"),
                generation: 0,
                heartbeat: 0,
                max_version: 0,
                last_gc_version: 0,
            })
            .collect(),
    };
    assert!(b.validate_digest(&big).is_err());
}

#[test]
fn gap_in_versions_is_not_applied() {
    let mut a = node("a", 1);
    let mut b = node("b", 1);
    a.set_local("k1", "v", Duration::ZERO).unwrap();
    sync(&mut b, &mut a, 1 << 16, S);
    a.set_local("k2", "v", Duration::ZERO).unwrap();
    a.set_local("k3", "v", Duration::ZERO).unwrap();
    // A delta computed for a peer that knows up to version 2 must not be
    // applied by b, which only knows version 1.
    let ahead = Digest {
        entries: vec![DigestEntry {
            node_id: "a".into(),
            generation: 1,
            heartbeat: 0,
            max_version: 2,
            last_gc_version: 0,
        }],
    };
    let (d, _) = a.compute_delta(&ahead, 1 << 16, 0);
    b.apply_delta(d, 2 * S);
    let v = b.node_view("a").unwrap();
    assert_eq!(v.max_version, 1);
    assert!(!v.values.contains_key("k3"));
}

#[test]
fn digest_and_full_state_sizes_are_bounded_by_limits() {
    let l = Limits::default();
    let mut a = ClusterState::new(
        &"n".repeat(l.max_node_id_len),
        u64::MAX,
        &"x".repeat(l.max_addr_len),
        cfg(),
        Duration::ZERO,
    );
    for i in 0..l.max_keys {
        let key = format!("{i:0width$}", width = l.max_key_len);
        a.set_local(&key, &"v".repeat(l.max_value_len), Duration::ZERO)
            .unwrap();
    }
    for _ in 0..1000 {
        a.beat();
    }
    let digest = a.digest();
    assert!(digest_size(&digest) <= l.max_digest_bytes(1));
    let (delta, info) = a.compute_delta(&Digest::default(), usize::MAX, 0);
    assert!(!info.truncated);
    assert!(delta_size(&delta) <= l.max_node_state_bytes() + wire::MAX_VARINT_USIZE);
}

struct SeqRng(u64);
impl SeqRng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}
impl TargetRng for SeqRng {
    fn index(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    #[allow(clippy::cast_precision_loss)]
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

#[test]
fn select_targets_properties() {
    let live: Vec<String> = (0..5).map(|i| format!("l{i}")).collect();
    let dead: Vec<String> = (0..3).map(|i| format!("d{i}")).collect();
    let seeds = vec!["s0".to_string(), "me".to_string(), "l0".to_string()];
    let mut rng = SeqRng(42);
    let (mut saw_seed, mut saw_dead) = (false, false);
    for _ in 0..1000 {
        let t = select_targets(&live, &dead, &seeds, "me", 2, 0.2, &mut rng);
        let mut dedup = t.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), t.len(), "duplicates in {t:?}");
        assert!(!t.contains(&"me".to_string()));
        // fanout live peers, plus possibly the seed "l0" which is also live.
        assert!((2..=3).contains(&t.iter().filter(|a| a.starts_with('l')).count()));
        saw_seed |= t.contains(&"s0".to_string());
        saw_dead |= t.iter().any(|a| a.starts_with('d'));
    }
    assert!(saw_seed && saw_dead);
    // No live peer: always a seed (never self).
    for _ in 0..100 {
        let t = select_targets(&[], &[], &seeds, "me", 1, 0.0, &mut rng);
        assert_eq!(t.len(), 1);
        assert_ne!(t[0], "me");
    }
    assert!(select_targets(&[], &[], &["me".to_string()], "me", 1, 1.0, &mut rng).is_empty());
}

/// Random operations + random pairwise exchanges with random (small) budgets
/// and random tombstone GC, then quiescent full exchanges: every replica must
/// equal its owner's state.
#[test]
fn randomized_convergence() {
    for seed in 1..=200u64 {
        let mut rng = SeqRng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let n = 2 + (rng.next() % 5) as usize;
        let mut cfg = cfg();
        cfg.tombstone_grace = Duration::from_secs(1 + rng.next() % 20);
        let mut nodes: Vec<ClusterState> = (0..n)
            .map(|i| {
                ClusterState::new(
                    &format!("n{i}"),
                    1,
                    &format!("n{i}"),
                    cfg.clone(),
                    Duration::ZERO,
                )
            })
            .collect();
        let mut now = Duration::ZERO;
        for _step in 0..400 {
            now += Duration::from_millis(100);
            let i = rng.index(n);
            match rng.next() % 10 {
                0..=3 => {
                    let k = format!("k{}", rng.next() % 12);
                    let v = format!("v{}", rng.next() % 1000);
                    nodes[i].set_local(&k, &v, now).unwrap();
                }
                4 => {
                    let k = format!("k{}", rng.next() % 12);
                    nodes[i].remove_local(&k, now);
                }
                5 => {
                    nodes[i].beat();
                    nodes[i].tick(now);
                }
                _ => {
                    let j = rng.index(n);
                    if i != j {
                        let budget = 60 + (rng.next() % 600) as usize;
                        let (lo, hi) = if i < j { (i, j) } else { (j, i) };
                        let (left, right) = nodes.split_at_mut(hi);
                        sync(&mut left[lo], &mut right[0], budget, now);
                    }
                }
            }
        }
        // Quiesce: exchanges with a generous budget, in all pairs, a few times.
        for _ in 0..4 {
            for i in 0..n {
                for j in (i + 1)..n {
                    let (left, right) = nodes.split_at_mut(j);
                    sync(&mut left[i], &mut right[0], 1 << 20, now);
                }
            }
        }
        for owner in 0..n {
            let id = format!("n{owner}");
            let truth = values_of(&nodes[owner], &id).unwrap();
            for (r, replica) in nodes.iter().enumerate() {
                assert_eq!(
                    values_of(replica, &id).as_ref(),
                    Some(&truth),
                    "seed {seed}: replica n{r} of {id} diverged"
                );
            }
        }
    }
}
