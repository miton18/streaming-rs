//! Crash / reboot under `Attrition`: crashed nodes are seen dead, rebooted
//! nodes (new generation, derived from simulated time) come back alive with
//! fresh state, and the tasks of a crashed incarnation never make progress
//! again.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use common::{NodeParams, Registry, builder_with, check_report, convergence_gap, fail, node_id_for, seeds, sleep};
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, RandomProvider, SimContext, SimulationResult, TimeProvider, Workload,
    assert_always, assert_sometimes,
};
use streaming_gossip::GossipStats;

/// Allowance for the last heartbeat of a crashed node to finish spreading
/// (a peer that receives it late restarts its `dead_after` timer). See the
/// crate docs: detection time = dead_after + dissemination delay.
const DISSEMINATION_SLACK: Duration = Duration::from_secs(8);

struct CrashWorkload {
    registry: Registry,
    n: usize,
    params: NodeParams,
    chaos: Duration,
    settle_deadline: Duration,
}

impl CrashWorkload {
    /// Checks run continuously during the whole test.
    fn check_invariants(&self, now: Duration) {
        let r = self.registry.0.lock();
        // 1. A retired (crashed / shut down) incarnation never makes progress.
        for old in &r.retired {
            let s = old.gossip.stats();
            assert_always!(
                s == old.stats_at_retire,
                "retired incarnation is frozen (no task progress after crash)",
                { "ip" => old.ip, "generation" => old.gossip.generation(), "frames_sent_now" => s.frames_sent, "frames_sent_at_retire" => old.stats_at_retire.frames_sent }
            );
            if now > old.at + Duration::from_secs(2) {
                assert_sometimes!(true, "retired incarnation observed frozen 2s after its crash");
            }
        }
        // 2. A node down for longer than dead_after + slack is not alive (with
        //    its old generation) in any live observer's view.
        let mut last_retired: BTreeMap<&str, (u64, Duration)> = BTreeMap::new();
        for old in &r.retired {
            last_retired.insert(old.ip.as_str(), (old.gossip.generation(), old.at));
        }
        for (ip, (generation, at)) in last_retired {
            if r.live.contains_key(ip) {
                continue;
            }
            if now < at + self.params.dead_after + DISSEMINATION_SLACK {
                continue;
            }
            let id = node_id_for(ip);
            for (obs_ip, obs) in &r.live {
                let view = obs.gossip.node_view(&id);
                let wrongly_alive = view.as_ref().is_some_and(|v| v.alive && v.generation == generation);
                assert_always!(
                    !wrongly_alive,
                    "node down for longer than dead_after + slack is not seen alive",
                    { "observer" => obs_ip, "down" => ip, "generation" => generation, "down_since_ms" => at.as_millis(), "now_ms" => now.as_millis() }
                );
                if view.is_some_and(|v| !v.alive) {
                    assert_sometimes!(true, "observer sees a crashed node dead");
                }
            }
        }
    }
}

#[async_trait]
impl Workload for CrashWorkload {
    fn name(&self) -> &str {
        "crash"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let start = ctx.time().now();
        while ctx.time().now() - start < self.chaos {
            sleep(ctx, Duration::from_millis(200)).await?;
            self.check_invariants(ctx.time().now());
            if ctx.random().random_bool(0.2) {
                let live = self.registry.live();
                if !live.is_empty() {
                    let (_, g) = &live[ctx.random().random_range(0..live.len())];
                    g.set("load", &ctx.random().random_range(0..100u32).to_string())
                        .unwrap();
                }
            }
        }
        // Chaos is over: every process restarts and the cluster converges.
        let settle_start = ctx.time().now();
        loop {
            let now = ctx.time().now();
            self.check_invariants(now);
            let live = self.registry.live();
            let Some(gap) = convergence_gap(&live, self.n) else {
                break;
            };
            if now - settle_start > self.settle_deadline {
                assert_always!(false, "cluster converges after attrition");
                let stats: Vec<_> = live.iter().map(|(ip, g)| (ip.clone(), g.stats())).collect();
                return Err(fail(format!("no convergence after attrition: {gap}; {stats:#?}")));
            }
            sleep(ctx, Duration::from_millis(100)).await?;
        }
        assert_always!(true, "cluster converges after attrition");

        // Old incarnations' values are gone everywhere.
        let live = self.registry.live();
        let r = self.registry.0.lock();
        for old in &r.retired {
            let old_key = format!("only-{}", old.gossip.generation());
            let old_id = node_id_for(&old.ip);
            for (obs_ip, obs) in &live {
                let leaked = obs
                    .members()
                    .iter()
                    .any(|m| m.node_id == old_id && m.values.contains_key(&old_key));
                assert_always!(!leaked, "old incarnation values are gone", { "observer" => obs_ip, "key" => old_key });
                if leaked {
                    return Err(fail(format!("{obs_ip} still has {old_key} of {}", old.ip)));
                }
            }
        }
        let crashed = !r.retired.is_empty();
        drop(r);
        let total = |f: fn(&GossipStats) -> u64| live.iter().map(|(_, g)| f(&g.stats())).sum::<u64>();
        assert_sometimes!(crashed, "a node crashed or was rebooted");
        assert_sometimes!(total(|s| s.generation_bumps) > 0, "generation bump observed");
        assert_sometimes!(total(|s| s.nodes_marked_dead) > 0, "node marked dead");
        assert_sometimes!(total(|s| s.nodes_revived) > 0, "dead node revived");
        assert_sometimes!(total(|s| s.nodes_forgotten) > 0, "dead node forgotten");
        assert_sometimes!(total(|s| s.connect_failures) > 0, "connect failure handled");
        assert_sometimes!(total(|s| s.deltas_truncated) > 0, "delta truncated by the byte budget");
        Ok(())
    }
}

fn attrition() -> Chaos {
    Chaos::Attrition {
        config: Attrition {
            max_dead: 1,
            prob_graceful: 0.2,
            prob_crash: 0.6,
            prob_wipe: 0.2,
            // Some outages shorter than dead_after, some longer than forget_after.
            recovery_delay_ms: Some(1000..20_000),
            grace_period_ms: Some(200..2000),
            scope: AttritionScope::PerProcess,
        },
        mode: ChaosMode::Random,
    }
}

#[test]
fn crash_reboot_with_attrition() {
    let n = 5;
    let registry = Registry::default();
    let params = NodeParams::default();
    let chaos = Duration::from_secs(60);
    let report = builder_with(&registry, n, params.clone(), seeds(100))
        .workload(CrashWorkload {
            registry: registry.clone(),
            n,
            params,
            chaos,
            settle_deadline: Duration::from_secs(60),
        })
        .enable_chaos([attrition()])
        .chaos_duration(chaos)
        .run();
    check_report("crash_reboot_with_attrition", &report);
}

/// Negative control: when the shutdown token is NOT tied to the process
/// future, the simulator leaves the crashed incarnation's tasks running
/// (zombies) and the frozen-stats check above would catch it.
struct ZombieControl {
    registry: Registry,
    chaos: Duration,
}

#[async_trait]
impl Workload for ZombieControl {
    fn name(&self) -> &str {
        "zombie-control"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let start = ctx.time().now();
        while ctx.time().now() - start < self.chaos {
            sleep(ctx, Duration::from_millis(500)).await?;
            let r = self.registry.0.lock();
            for old in &r.retired {
                if ctx.time().now() > old.at + Duration::from_secs(2) {
                    let moved = old.gossip.stats().rounds > old.stats_at_retire.rounds;
                    assert_always!(moved, "untied incarnation keeps running after its crash");
                }
            }
        }
        let crashed = !self.registry.0.lock().retired.is_empty();
        assert_sometimes!(crashed, "control: a node crashed");
        // Stop the zombies so the iteration can end cleanly.
        for old in &self.registry.0.lock().retired {
            old.gossip.shutdown();
        }
        for (_, g) in self.registry.live() {
            g.shutdown();
        }
        Ok(())
    }
}

#[test]
fn zombie_tasks_without_drop_guard_negative_control() {
    let registry = Registry::default();
    let params = NodeParams {
        tie_to_process: false,
        ..NodeParams::default()
    };
    let chaos = Duration::from_secs(30);
    let report = builder_with(&registry, 3, params, seeds(30))
        .workload(ZombieControl {
            registry: registry.clone(),
            chaos,
        })
        .enable_chaos([attrition()])
        .chaos_duration(chaos)
        .run();
    check_report("zombie_negative_control", &report);
}
