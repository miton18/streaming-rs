//! Shared Moonpool simulation harness: a `Process` running the real
//! `Gossip<SimProviders>` and a registry through which workloads observe and
//! drive the live incarnations.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
use moonpool_sim::{
    Process, SimContext, SimProviders, SimTimeProvider, SimulationBuilder, SimulationError, SimulationReport,
    SimulationResult, TimeProvider,
};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use streaming_gossip::{Gossip, GossipConfig, GossipStats, Limits};

pub const PORT: u16 = 7000;
pub const CLUSTER: &str = "sim-cluster";
pub const FOREIGN_CLUSTER: &str = "other-cluster";

/// Parameters shared by every node of a test.
#[derive(Clone, Debug)]
pub struct NodeParams {
    pub cluster_id: String,
    pub gossip_interval: Duration,
    pub dead_after: Duration,
    pub forget_after: Duration,
    pub tombstone_grace: Duration,
    pub delta_budget_bytes: usize,
    pub limits: Limits,
    /// Number of `filler-*` keys each incarnation publishes at boot.
    pub filler_keys: usize,
    /// Value length of the filler keys.
    pub filler_len: usize,
    /// Tie the node's shutdown token to the process future (the required
    /// usage). `false` is only for the negative control test showing that
    /// without it the simulator leaves zombie tasks running.
    pub tie_to_process: bool,
    /// The last process uses another cluster id (isolation test).
    pub foreign_last: bool,
}

impl Default for NodeParams {
    fn default() -> Self {
        Self {
            cluster_id: CLUSTER.to_string(),
            gossip_interval: Duration::from_millis(200),
            dead_after: Duration::from_secs(2),
            forget_after: Duration::from_secs(15),
            tombstone_grace: Duration::from_secs(4),
            // Smallest budget the default limits allow is ~1.8 KiB; with the
            // filler keys below a full cluster state exceeds it, so initial
            // syncs and resyncs after reboots are truncated.
            delta_budget_bytes: 2048,
            limits: Limits::default(),
            filler_keys: 8,
            filler_len: 200,
            tie_to_process: true,
            foreign_last: false,
        }
    }
}

impl NodeParams {
    pub fn config(&self, node_id: &str, generation: u64, addr: &str, seed: &str) -> GossipConfig {
        let mut c = GossipConfig::new(self.cluster_id.clone(), node_id, generation, addr);
        c.seeds = vec![seed.to_string()];
        c.gossip_interval = self.gossip_interval;
        c.dead_after = self.dead_after;
        c.forget_after = self.forget_after;
        c.tombstone_grace = self.tombstone_grace;
        c.connect_timeout = Duration::from_secs(1);
        c.exchange_timeout = Duration::from_millis(1500);
        c.delta_budget_bytes = self.delta_budget_bytes;
        c.limits = self.limits.clone();
        c
    }
}

/// All process IPs (a process's topology omits its own), numerically sorted:
/// the first one is the seed.
pub fn all_process_ips_with_self(ctx: &SimContext) -> Vec<String> {
    let mut ips: Vec<std::net::IpAddr> = ctx
        .topology()
        .all_process_ips()
        .iter()
        .chain(std::iter::once(&ctx.my_ip().to_string()))
        .map(|s| s.parse().expect("ip"))
        .collect();
    ips.sort();
    ips.dedup();
    ips.into_iter().map(|ip| ip.to_string()).collect()
}

pub fn node_id_for(ip: &str) -> String { format!("node-{ip}") }

pub fn addr_for(ip: &str) -> String { format!("{ip}:{PORT}") }

pub struct LiveNode {
    pub gossip: Gossip<SimProviders>,
    pub booted_at: Duration,
}

pub struct Retired {
    pub ip: String,
    pub gossip: Gossip<SimProviders>,
    pub stats_at_retire: GossipStats,
    pub at: Duration,
}

#[derive(Default)]
pub struct RegistryInner {
    pub live: BTreeMap<String, LiveNode>,
    pub retired: Vec<Retired>,
    pub boots: usize,
}

/// Shared between the process factory and the workloads (single-threaded sim).
#[derive(Clone, Default)]
pub struct Registry(pub Arc<Mutex<RegistryInner>>);

impl Registry {
    pub fn clear(&self) { *self.0.lock() = RegistryInner::default(); }

    pub fn live(&self) -> Vec<(String, Gossip<SimProviders>)> {
        self.0
            .lock()
            .live
            .iter()
            .map(|(ip, n)| (ip.clone(), n.gossip.clone()))
            .collect()
    }
}

/// Removes the incarnation from the registry when the process future is
/// dropped (crash = abort) or returns (graceful shutdown), after cancelling
/// the node. This is where the "tie the node to its owning future" contract
/// is honoured.
struct Registration {
    registry: Registry,
    ip: String,
    gossip: Gossip<SimProviders>,
    time: SimTimeProvider,
    tie_to_process: bool,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if self.tie_to_process {
            self.gossip.shutdown();
        }
        let mut r = self.registry.0.lock();
        let same = r
            .live
            .get(&self.ip)
            .is_some_and(|n| n.gossip.generation() == self.gossip.generation());
        if same {
            r.live.remove(&self.ip);
        }
        r.retired.push(Retired {
            ip: self.ip.clone(),
            gossip: self.gossip.clone(),
            stats_at_retire: self.gossip.stats(),
            at: self.time.now(),
        });
    }
}

pub struct GossipProcess {
    pub registry: Registry,
    pub params: NodeParams,
}

#[async_trait]
impl Process for GossipProcess {
    fn name(&self) -> &str { "gossip" }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let ip = ctx.my_ip().to_string();
        let now = ctx.time().now();
        // Generation derived from simulated time: strictly increases across reboots.
        let generation = u64::try_from(now.as_millis()).unwrap_or(u64::MAX) + 1;
        let all = all_process_ips_with_self(ctx);
        let seed_ip = all[0].clone();
        let mut config = self
            .params
            .config(&node_id_for(&ip), generation, &addr_for(&ip), &addr_for(&seed_ip));
        if self.params.foreign_last && all.last() == Some(&ip) {
            config.cluster_id = FOREIGN_CLUSTER.to_string();
        }
        let tie = self.params.tie_to_process;
        let token = if tie {
            ctx.shutdown().child_token()
        } else {
            tokio_util::sync::CancellationToken::new()
        };
        // Aborting this future (crash) cancels the token: no zombie tasks.
        let _guard = tie.then(|| token.clone().drop_guard());
        let gossip = Gossip::start(ctx.providers().clone(), config, token)
            .await
            .map_err(|e| fail(format!("gossip start on {ip}: {e}")))?;
        gossip.set("addr", &addr_for(&ip)).map_err(err)?;
        gossip.set("incarnation", &generation.to_string()).map_err(err)?;
        gossip.set(&format!("only-{generation}"), "x").map_err(err)?;
        for i in 0..self.params.filler_keys {
            let v: String = std::iter::repeat_n(char::from(b'a' + (i % 26) as u8), self.params.filler_len).collect();
            gossip.set(&format!("filler-{i:03}"), &v).map_err(err)?;
        }
        {
            let mut r = self.registry.0.lock();
            r.boots += 1;
            r.live.insert(
                ip.clone(),
                LiveNode {
                    gossip: gossip.clone(),
                    booted_at: now,
                },
            );
        }
        let _registration = Registration {
            registry: self.registry.clone(),
            ip,
            gossip,
            time: ctx.time().clone(),
            tie_to_process: tie,
        };
        ctx.shutdown().cancelled().await;
        Ok(())
    }
}

pub fn err(e: impl std::fmt::Display) -> SimulationError { fail(e.to_string()) }

/// Build a workload error; printed immediately (the runner only logs it via
/// tracing) together with the seed.
pub fn fail(msg: String) -> SimulationError {
    let msg = format!("[seed={}] {msg}", moonpool_sim::current_sim_seed());
    eprintln!("WORKLOAD FAILURE {msg}");
    SimulationError::InvalidState(msg)
}

/// Ground truth: each live node's own values.
pub fn ground_truth(nodes: &[(String, Gossip<SimProviders>)]) -> BTreeMap<String, (u64, BTreeMap<String, String>)> {
    nodes
        .iter()
        .map(|(_, g)| {
            let me = g
                .members()
                .into_iter()
                .find(|m| m.node_id == g.node_id())
                .expect("self is a member");
            (me.node_id, (me.generation, me.values))
        })
        .collect()
}

/// `None` when every live node sees exactly the live nodes, all alive, with
/// the owners' generations and values; otherwise a description of the first
/// difference.
pub fn convergence_gap(nodes: &[(String, Gossip<SimProviders>)], expected: usize) -> Option<String> {
    if nodes.len() != expected {
        return Some(format!("{} live incarnations, expected {expected}", nodes.len()));
    }
    let truth = ground_truth(nodes);
    for (ip, g) in nodes {
        let members = g.members();
        if members.len() != truth.len() {
            return Some(format!(
                "{ip} knows {} nodes: {:?}",
                members.len(),
                members
                    .iter()
                    .map(|m| (&m.node_id, m.generation, m.alive))
                    .collect::<Vec<_>>()
            ));
        }
        for m in &members {
            let Some((generation, values)) = truth.get(&m.node_id) else {
                return Some(format!("{ip} knows unknown node {}", m.node_id));
            };
            if !m.alive {
                return Some(format!("{ip} sees {} dead", m.node_id));
            }
            if m.generation != *generation {
                return Some(format!(
                    "{ip} sees {} at generation {} instead of {generation}",
                    m.node_id, m.generation
                ));
            }
            if &m.values != values {
                return Some(format!(
                    "{ip} has different values for {}: {:?} vs {:?}",
                    m.node_id, m.values, values
                ));
            }
        }
    }
    None
}

/// Seeds for a test: `GOSSIP_SIM_SEED=<n>` runs a single seed,
/// `GOSSIP_SIM_ITERATIONS` / `GOSSIP_SIM_SEED_BASE` override the default
/// range `base..base+default_iterations`.
pub fn seeds(default_iterations: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("GOSSIP_SIM_SEED").ok().and_then(|v| v.parse().ok()) {
        return vec![s];
    }
    let n = std::env::var("GOSSIP_SIM_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default_iterations);
    let base = std::env::var("GOSSIP_SIM_SEED_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1u64);
    (base..base + n).collect()
}

pub fn builder_with(registry: &Registry, processes: usize, params: NodeParams, seeds: Vec<u64>) -> SimulationBuilder {
    let reg_factory = registry.clone();
    let reg_reset = registry.clone();
    let iterations = seeds.len();
    SimulationBuilder::new()
        .processes(processes, move || {
            Box::new(GossipProcess {
                registry: reg_factory.clone(),
                params: params.clone(),
            })
        })
        .before_iteration(move || reg_reset.clear())
        .set_iterations(iterations)
        .set_debug_seeds(seeds)
}

/// Fail the test (printing the failing seeds) unless every iteration passed
/// and every `assert_always!` / `assert_sometimes!` contract held.
pub fn check_report(name: &str, report: &SimulationReport) {
    report.eprint();
    eprintln!(
        "[{name}] iterations={} ok={} failed={} seeds={}..={}",
        report.iterations,
        report.successful_runs,
        report.failed_runs,
        report.seeds_used.first().copied().unwrap_or(0),
        report.seeds_used.last().copied().unwrap_or(0),
    );
    for (seed, m) in report.seeds_used.iter().zip(&report.individual_metrics) {
        if let Err(e) = m {
            eprintln!("[{name}] seed {seed} failed: {e}");
        }
    }
    assert!(
        report.failed_runs == 0 && report.seeds_failing.is_empty(),
        "[{name}] failing seeds: {:?} (rerun one with GOSSIP_SIM_SEED=<seed>)",
        report.seeds_failing
    );
    assert!(
        report.assertion_violations.is_empty(),
        "[{name}] assert_always violations: {:?}; failing seeds: {:?}",
        report.assertion_violations,
        report.seeds_failing
    );
    assert!(
        report.coverage_violations.is_empty(),
        "[{name}] assert_sometimes never satisfied: {:?}",
        report.coverage_violations
    );
    assert!(report.successful_runs == report.iterations && report.iterations > 0);
}

/// Sleep helper for workloads.
pub async fn sleep(ctx: &SimContext, d: Duration) -> SimulationResult<()> { ctx.time().sleep(d).await.map_err(err) }
