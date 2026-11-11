//! Exactly-once under chaos: the real worker code, rebooted at will, against
//! a KV, a source and sinks that fail in every way a remote system can.
//!
//! Run a specific seed with `SIM_SEEDS=42,43 cargo test -p streaming-sim`,
//! more iterations with `SIM_ITERATIONS=1000`.

use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, SimulationBuilder, SimulationReport,
};
use parking_lot::Mutex;
use streaming_sim::{Auditor, Driver, EnvSlot, Operator, Scenario, SimEnv, WorkerProcess};

fn iterations(default: usize) -> usize {
    std::env::var("SIM_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn run(
    scenario: Scenario,
    workers: usize,
    attrition: bool,
    default_iterations: usize,
) -> SimulationReport {
    let slot: EnvSlot = Arc::new(Mutex::new(None));
    let mut builder = SimulationBuilder::new()
        .before_iteration({
            let slot = slot.clone();
            let scenario = scenario.clone();
            move || *slot.lock() = Some(SimEnv::new(&scenario))
        })
        .processes(workers, {
            let slot = slot.clone();
            let scenario = scenario.clone();
            move || Box::new(WorkerProcess::new(slot.clone(), scenario.clone()))
        })
        .workload(Driver::new(slot.clone(), scenario.clone()))
        .workload(Operator::new(slot.clone(), scenario.clone()))
        .workload(Auditor::new(slot.clone()))
        .run_time_budget(Duration::from_secs(3_600));
    if attrition {
        builder = builder
            .enable_chaos([Chaos::Attrition {
                config: Attrition {
                    max_dead: workers.saturating_sub(1).max(1),
                    prob_graceful: 0.3,
                    prob_crash: 0.7,
                    prob_wipe: 0.0,
                    recovery_delay_ms: Some(300..4_000),
                    grace_period_ms: Some(100..1_500),
                    scope: AttritionScope::PerProcess,
                },
                mode: ChaosMode::Random,
            }])
            .chaos_duration(scenario.produce_for + Duration::from_secs(20));
    }
    let seeds: Vec<u64> = std::env::var("SIM_SEEDS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|seed| seed.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();
    builder = if seeds.is_empty() {
        builder.set_iterations(iterations(default_iterations))
    } else {
        builder.set_iterations(seeds.len()).set_debug_seeds(seeds)
    };
    let report = builder.run();
    if report.failed_runs > 0
        || !report.assertion_violations.is_empty()
        || std::env::var("SIM_REPORT").is_ok()
    {
        report.eprint();
    }
    report
}

fn assert_clean(report: &SimulationReport) {
    assert_eq!(
        report.failed_runs, 0,
        "failing seeds: {:?} (replay with SIM_SEEDS=<seed>)",
        report.seeds_failing
    );
    assert!(
        report.assertion_violations.is_empty(),
        "assertion violations: {:?}",
        report.assertion_violations
    );
}

#[test]
fn quiet_cluster_counts_exactly() {
    let report = run(Scenario::default(), 1, false, 5);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_reboots() {
    let report = run(Scenario::default(), 3, true, 25);
    assert_clean(&report);
}

#[test]
fn exactly_once_when_every_worker_runs_every_slice() {
    let scenario = Scenario {
        duel: true,
        events: 800,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 15);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_gossip_membership() {
    let scenario = Scenario {
        gossip: true,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 15);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_windows_wider_than_an_epoch() {
    // More keys per window than records per epoch: a closed window is emitted
    // over several epochs, and may be resumed halfway after a crash.
    let scenario = Scenario {
        keys: 150,
        events: 3_000,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_allowed_lateness() {
    // Events up to 1.8 s old with a watermark only 0.6 s behind: many arrive
    // after their window ended, inside the allowed lateness.
    let scenario = Scenario {
        allowed_lateness: Duration::from_millis(1_500),
        max_event_age: Duration::from_millis(1_800),
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_sliding_windows() {
    let scenario = Scenario {
        window: Duration::from_secs(4),
        slide: Duration::from_secs(2),
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_under_operator_actions() {
    let scenario = Scenario {
        operator: true,
        produce_for: Duration::from_secs(40),
        events: 2_000,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_on_a_larger_cluster() {
    let scenario = Scenario {
        partitions: 6,
        events: 3_000,
        keys: 20,
        ..Scenario::default()
    };
    let report = run(scenario, 5, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_a_slow_store() {
    // Every KV command takes up to 150 ms: commits lag far behind reads.
    let scenario = Scenario {
        kv_latency_ms: 150,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_eager_idle_watermarks() {
    // Sparse traffic and a short idle timeout: watermarks follow the wall
    // clock between bursts, over and over.
    let scenario = Scenario {
        events: 600,
        produce_for: Duration::from_secs(60),
        idle_timeout: Duration::from_millis(300),
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_in_a_hostile_mix() {
    // Everything at once: workers fighting over every slice, an operator
    // interfering, windows wider than an epoch, three times the faults.
    let scenario = Scenario {
        duel: true,
        operator: true,
        keys: 60,
        events: 1_200,
        produce_for: Duration::from_secs(30),
        fault_scale: 3.0,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}

#[test]
fn exactly_once_with_a_silent_partition() {
    // One partition never receives anything: its watermark only ever moves
    // with the wall clock, and windows must still close.
    let scenario = Scenario {
        partitions: 4,
        silent_partitions: 1,
        ..Scenario::default()
    };
    let report = run(scenario, 3, true, 10);
    assert_clean(&report);
}
