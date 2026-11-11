//! A node configured with another cluster id (but the same seed) is refused:
//! the cluster converges without it and it learns nobody.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use async_trait::async_trait;
use common::{
    NodeParams, Registry, builder_with, check_report, convergence_gap, fail, seeds, sleep,
};
use moonpool_sim::{
    SimContext, SimulationResult, TimeProvider, Workload, assert_always, assert_sometimes,
};

const N: usize = 4;

struct IsolationWorkload {
    registry: Registry,
}

#[async_trait]
impl Workload for IsolationWorkload {
    fn name(&self) -> &str {
        "isolation"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The workload is not a process: its topology lists every process;
        // the foreign node is the numerically highest one.
        let foreign_ip = ctx
            .topology()
            .all_process_ips()
            .iter()
            .max_by_key(|ip| ip.parse::<std::net::IpAddr>().expect("ip"))
            .unwrap()
            .clone();
        let start = ctx.time().now();
        loop {
            sleep(ctx, Duration::from_millis(200)).await?;
            let all = self.registry.live();
            let (foreign, cluster): (Vec<_>, Vec<_>) =
                all.into_iter().partition(|(ip, _)| *ip == foreign_ip);
            for (ip, g) in &cluster {
                let knows_foreign = g
                    .members()
                    .iter()
                    .any(|m| m.node_id == common::node_id_for(&foreign_ip));
                assert_always!(!knows_foreign, "cluster member never learns the foreign node", { "ip" => ip });
            }
            for (_, g) in &foreign {
                assert_always!(g.members().len() == 1, "foreign node learns nobody");
            }
            let mismatches: u64 = cluster
                .iter()
                .chain(&foreign)
                .map(|(_, g)| g.stats().cluster_mismatches)
                .sum();
            if ctx.time().now() - start > Duration::from_secs(10)
                && convergence_gap(&cluster, N - 1).is_none()
            {
                assert_sometimes!(mismatches > 0, "cluster id mismatch refused");
                return Ok(());
            }
            if ctx.time().now() - start > Duration::from_secs(60) {
                return Err(fail(format!(
                    "no convergence: {:?}",
                    convergence_gap(&cluster, N - 1)
                )));
            }
        }
    }
}

#[test]
fn foreign_cluster_is_refused() {
    let registry = Registry::default();
    let params = NodeParams {
        foreign_last: true,
        ..NodeParams::default()
    };
    let report = builder_with(&registry, N, params, seeds(100))
        .workload(IsolationWorkload {
            registry: registry.clone(),
        })
        .run();
    check_report("foreign_cluster_is_refused", &report);
}
