//! Convergence under simulated network chaos: N nodes (3, 5, 9), one seed,
//! values changed while gossip runs; every node must end up seeing all N
//! nodes alive with identical key/values.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use async_trait::async_trait;
use common::{
    NodeParams, Registry, builder_with, check_report, convergence_gap, fail, seeds, sleep,
};
use moonpool_sim::{
    Chaos, ChaosMode, RandomProvider, SimContext, SimulationResult, TimeProvider, Workload,
    assert_always, assert_sometimes,
};

struct ConvergenceWorkload {
    registry: Registry,
    n: usize,
    params: NodeParams,
    churn: Duration,
    settle_deadline: Duration,
}

fn check_sizes(registry: &Registry, params: &NodeParams, n: usize) {
    for (ip, g) in registry.live() {
        let s = g.stats();
        assert_always!(
            s.max_delta_bytes <= params.delta_budget_bytes as u64,
            "delta within budget",
            { "ip" => ip, "max_delta_bytes" => s.max_delta_bytes, "budget" => params.delta_budget_bytes }
        );
        assert_always!(
            s.max_digest_bytes <= params.limits.max_digest_bytes(n) as u64,
            "digest within limits",
            { "ip" => ip, "max_digest_bytes" => s.max_digest_bytes }
        );
    }
}

#[async_trait]
impl Workload for ConvergenceWorkload {
    fn name(&self) -> &str {
        "convergence"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // All processes boot at t=0.
        let boot_deadline = ctx.time().now() + Duration::from_secs(10);
        while self.registry.live().len() < self.n {
            if ctx.time().now() > boot_deadline {
                return Err(fail(format!(
                    "only {} nodes booted",
                    self.registry.live().len()
                )));
            }
            sleep(ctx, Duration::from_millis(50)).await?;
        }

        // Churn: mutate values on random nodes while gossip runs.
        let start = ctx.time().now();
        let mut cfg_version = 0u64;
        while ctx.time().now() - start < self.churn {
            sleep(
                ctx,
                Duration::from_millis(ctx.random().random_range(20..400)),
            )
            .await?;
            let live = self.registry.live();
            let (_, g) = &live[ctx.random().random_range(0..live.len())];
            match ctx.random().random_range(0..7u32) {
                0 | 1 => g
                    .set("load", &ctx.random().random_range(0..1000u32).to_string())
                    .unwrap(),
                2 => {
                    cfg_version += 1;
                    g.set("cfg-version", &cfg_version.to_string()).unwrap();
                }
                3 => g.set("tmp", "here").unwrap(),
                4 => g.remove("tmp"),
                5 => g
                    .set(
                        &format!("dyn-{}", ctx.random().random_range(0..6u32)),
                        &"d".repeat(40),
                    )
                    .unwrap(),
                _ => g.remove(&format!("dyn-{}", ctx.random().random_range(0..6u32))),
            }
            check_sizes(&self.registry, &self.params, self.n);
        }

        // Settle: no more changes; the cluster must converge.
        let settle_start = ctx.time().now();
        loop {
            let live = self.registry.live();
            let gap = convergence_gap(&live, self.n);
            let Some(gap) = gap else { break };
            if ctx.time().now() - settle_start > self.settle_deadline {
                assert_always!(false, "cluster converges after churn");
                let stats: Vec<_> = live.iter().map(|(ip, g)| (ip.clone(), g.stats())).collect();
                return Err(fail(format!(
                    "no convergence {:?} after churn: {gap}; stats {stats:#?}",
                    self.settle_deadline
                )));
            }
            sleep(ctx, Duration::from_millis(100)).await?;
        }
        assert_always!(true, "cluster converges after churn");
        let settle_time = ctx.time().now() - settle_start;
        tracing::info!(?settle_time, n = self.n, "converged");
        check_sizes(&self.registry, &self.params, self.n);

        let live = self.registry.live();
        let sum = |f: fn(&streaming_gossip::GossipStats) -> u64| {
            live.iter().map(|(_, g)| f(&g.stats())).sum::<u64>()
        };
        assert_sometimes!(
            sum(|s| s.deltas_truncated) > 0,
            "delta truncated by the byte budget"
        );
        assert_sometimes!(sum(|s| s.connect_failures) > 0, "connect failure handled");
        assert_sometimes!(sum(|s| s.tombstones_gc) > 0, "tombstone garbage-collected");
        assert_sometimes!(
            sum(|s| s.frames_rejected) > 0,
            "corrupted or malformed frame rejected"
        );
        Ok(())
    }
}

fn run(n: usize, network: Option<ChaosMode>, iterations: u64) {
    let registry = Registry::default();
    let params = NodeParams::default();
    let mut b = builder_with(&registry, n, params.clone(), seeds(iterations)).workload(
        ConvergenceWorkload {
            registry: registry.clone(),
            n,
            params,
            churn: Duration::from_secs(15),
            settle_deadline: Duration::from_secs(45),
        },
    );
    if let Some(mode) = network {
        b = b.enable_chaos([Chaos::Network(mode)]);
    }
    let report = b.run();
    check_report(&format!("convergence n={n} network={network:?}"), &report);
}

#[test]
fn convergence_3_nodes() {
    run(3, None, 100);
}

#[test]
fn convergence_5_nodes() {
    run(5, None, 100);
}

#[test]
fn convergence_9_nodes() {
    run(9, None, 100);
}

/// Heavier network chaos (partitions, clogging, random closes, pair latency).
#[test]
fn convergence_5_nodes_random_network_chaos() {
    run(5, Some(ChaosMode::Random), 100);
}
