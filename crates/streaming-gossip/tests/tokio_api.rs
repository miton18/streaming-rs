//! The `Gossip` handle itself, on real localhost TCP: accessors, bind
//! errors, counters, cluster id mismatches and seed selection.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use moonpool_core::TokioProviders;
use std::time::Duration;
use streaming_gossip::{Gossip, GossipConfig, GossipError};
use tokio_util::sync::CancellationToken;

fn free_addr() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    format!("127.0.0.1:{port}")
}

fn config(cluster: &str, id: &str, addr: &str, seeds: &[&str]) -> GossipConfig {
    let mut c = GossipConfig::new(cluster, id, 7, addr);
    c.seeds = seeds.iter().map(ToString::to_string).collect();
    c.gossip_interval = Duration::from_millis(20);
    c.dead_after = Duration::from_secs(5);
    c.forget_after = Duration::from_secs(20);
    c
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accessors_counters_and_shutdown() {
    let (a_addr, b_addr) = (free_addr(), free_addr());
    let parent = CancellationToken::new();
    let a = Gossip::start(
        TokioProviders::new(),
        config("c", "a", &a_addr, &[&b_addr]),
        parent.child_token(),
    )
    .await
    .unwrap();
    let b = Gossip::start(
        TokioProviders::new(),
        config("c", "b", &b_addr, &[]),
        parent.child_token(),
    )
    .await
    .unwrap();
    assert_eq!(a.advertise_addr(), a_addr);
    assert_eq!(format!("{a:?}"), "Gossip { node_id: \"a\", generation: 7, .. }");
    assert!(a.node_view("a").is_some());
    assert!(a.node_view("nobody").is_none());
    until("a and b know each other", || {
        a.live_members().len() == 2 && b.live_members().len() == 2
    })
    .await;
    assert_eq!(a.node_view("b").unwrap().generation, 7);
    let s = a.stats();
    assert!(s.last_digest_bytes > 0 && s.max_digest_bytes >= s.last_digest_bytes);
    assert!(s.max_delta_bytes > 0 && s.max_delta_bytes >= s.last_delta_bytes);

    // The listen address is taken: a distinct error.
    let err = Gossip::start(
        TokioProviders::new(),
        config("c", "a2", &a_addr, &[]),
        parent.child_token(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, GossipError::AddrInUse { .. }), "{err}");
    let mut bad = config("c", "a3", "127.0.0.1:1", &[]);
    bad.listen_addr = "256.0.0.1:1".into();
    let err = Gossip::start(TokioProviders::new(), bad, parent.child_token())
        .await
        .unwrap_err();
    assert!(matches!(err, GossipError::Bind { .. }), "{err}");

    assert!(!a.shutdown_token().is_cancelled());
    a.shutdown();
    assert!(a.shutdown_token().is_cancelled());
    parent.cancel();
    assert!(b.shutdown_token().is_cancelled());
}

#[tokio::test(flavor = "current_thread")]
async fn the_initiator_counts_a_cluster_mismatch() {
    let (a_addr, b_addr) = (free_addr(), free_addr());
    let token = CancellationToken::new();
    let a = Gossip::start(
        TokioProviders::new(),
        config("one", "a", &a_addr, &[&b_addr]),
        token.child_token(),
    )
    .await
    .unwrap();
    // b has no seed and never initiates: only a's view of the refusal counts.
    let _b = Gossip::start(
        TokioProviders::new(),
        config("two", "b", &b_addr, &[]),
        token.child_token(),
    )
    .await
    .unwrap();
    until("a hears that b is in another cluster", || {
        a.stats().cluster_mismatches > 0
    })
    .await;
    assert_eq!(a.members().len(), 1);
    token.cancel();
}

/// With no fanout, a node only contacts its seed with
/// `seed_gossip_probability`, once it knows a live peer.
#[tokio::test(flavor = "current_thread")]
async fn seeds_are_contacted_with_the_configured_probability() {
    let (a_addr, b_addr) = (free_addr(), free_addr());
    let token = CancellationToken::new();
    let mut ca = config("c", "a", &a_addr, &[&b_addr]);
    ca.gossip_fanout = 0;
    ca.seed_gossip_probability = 0.5;
    let a = Gossip::start(TokioProviders::new(), ca, token.child_token())
        .await
        .unwrap();
    let b = Gossip::start(
        TokioProviders::new(),
        config("c", "b", &b_addr, &[]),
        token.child_token(),
    )
    .await
    .unwrap();
    until("a knows b alive", || a.live_members().len() == 2).await;
    let before = a.stats();
    until("forty rounds", || a.stats().rounds >= before.rounds + 40).await;
    let after = a.stats();
    let rounds = after.rounds - before.rounds;
    let initiated = after.exchanges_initiated - before.exchanges_initiated;
    assert!(
        initiated > 0 && initiated * 10 <= rounds * 8,
        "{initiated} exchanges in {rounds} rounds"
    );
    drop(b);
    token.cancel();
}
