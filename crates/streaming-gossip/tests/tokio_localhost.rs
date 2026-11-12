//! The same `Gossip` code with `TokioProviders` on real localhost TCP.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use moonpool_core::TokioProviders;
use std::{collections::BTreeMap, time::Duration};
use streaming_gossip::{Gossip, GossipConfig};
use tokio_util::sync::CancellationToken;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn config(i: usize, addr: &str, seed: &str) -> GossipConfig {
    let mut c = GossipConfig::new("tokio-test", format!("node-{i}"), 1, addr);
    c.seeds = vec![seed.to_string()];
    c.gossip_interval = Duration::from_millis(50);
    c.dead_after = Duration::from_secs(2);
    c.forget_after = Duration::from_secs(10);
    c
}

fn converged(nodes: &[Gossip<TokioProviders>], expect: &BTreeMap<String, BTreeMap<String, String>>) -> bool {
    nodes.iter().all(|g| {
        let live = g.live_members();
        live.len() == expect.len() && live.iter().all(|m| expect.get(&m.node_id) == Some(&m.values))
    })
}

#[tokio::test(flavor = "current_thread")]
async fn three_nodes_converge_on_localhost() {
    let addrs: Vec<String> = (0..3).map(|_| format!("127.0.0.1:{}", free_port())).collect();
    let shutdown = CancellationToken::new();
    let mut nodes = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let g = Gossip::start(
            TokioProviders::new(),
            config(i, addr, &addrs[0]),
            shutdown.child_token(),
        )
        .await
        .expect("start");
        g.set("addr", addr).unwrap();
        g.set("load", &i.to_string()).unwrap();
        nodes.push(g);
    }
    let mut changes = nodes[2].changes();

    let expect = |nodes: &[Gossip<TokioProviders>]| -> BTreeMap<String, BTreeMap<String, String>> {
        nodes
            .iter()
            .map(|g| {
                let me = g.members().into_iter().find(|m| m.node_id == g.node_id()).unwrap();
                (me.node_id, me.values)
            })
            .collect()
    };

    let wait = |nodes: Vec<Gossip<TokioProviders>>| async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let e = expect(&nodes);
            if converged(&nodes, &e) {
                return nodes;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no convergence: {:#?}",
                nodes
                    .iter()
                    .map(|g| (g.node_id().to_string(), g.members(), g.stats()))
                    .collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    let nodes = wait(nodes).await;
    assert!(changes.has_changed().unwrap());
    changes.mark_unchanged();

    // Change and delete values while running.
    nodes[1].set("load", "42").unwrap();
    nodes[0].remove("load");
    let nodes = wait(nodes).await;
    assert_eq!(
        nodes[2]
            .members()
            .iter()
            .find(|m| m.node_id == "node-1")
            .unwrap()
            .values["load"],
        "42"
    );
    assert!(
        !nodes[2]
            .members()
            .iter()
            .find(|m| m.node_id == "node-0")
            .unwrap()
            .values
            .contains_key("load")
    );
    assert!(changes.has_changed().unwrap());
    for g in &nodes {
        let s = g.stats();
        assert!(
            s.exchanges_completed > 0 && s.bytes_sent > 0 && s.bytes_received > 0,
            "{s:?}"
        );
    }

    // Stop node 2: the others mark it dead.
    nodes[2].shutdown();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let dead_everywhere = nodes[..2].iter().all(|g| {
            g.members()
                .iter()
                .find(|m| m.node_id == "node-2")
                .is_none_or(|m| !m.alive)
        });
        if dead_everywhere {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "node-2 never seen dead");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    shutdown.cancel();
}
