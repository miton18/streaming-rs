//! How severe is the simulation? Plant known bugs in the engine, one at a
//! time, and require the scenarios to notice each of them.
//!
//! `cargo test -p streaming-sim --features mutants --test mutants -- --nocapture`
//! (`SIM_MUTANT=<name>` plants a single one)

use moonpool_sim::{Attrition, AttritionScope, Chaos, ChaosMode, SimulationBuilder, SimulationReport};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use streaming_runtime::mutation::{Mutant, plant};
use streaming_sim::{Auditor, Deployer, Driver, EnvSlot, Operator, Scenario, SimEnv, WorkerProcess};

fn run(scenario: &Scenario, iterations: usize) -> SimulationReport {
    let slot: EnvSlot = Arc::new(Mutex::new(None));
    SimulationBuilder::new()
        .before_iteration({
            let slot = slot.clone();
            let scenario = scenario.clone();
            move || *slot.lock() = Some(SimEnv::new(&scenario))
        })
        .processes(3, {
            let slot = slot.clone();
            let scenario = scenario.clone();
            move || Box::new(WorkerProcess::new(slot.clone(), scenario.clone()))
        })
        .workload(Driver::new(slot.clone(), scenario.clone()))
        .workload(Operator::new(slot.clone(), scenario.clone()))
        .workload(Deployer::new(slot.clone(), scenario.clone()))
        .workload(Auditor::new(slot.clone()))
        .run_time_budget(Duration::from_secs(3_600))
        .enable_chaos([Chaos::Attrition {
            config: Attrition {
                max_dead: 2,
                prob_graceful: 0.3,
                prob_crash: 0.7,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(300..4_000),
                grace_period_ms: Some(100..1_500),
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Random,
        }])
        .chaos_duration(scenario.produce_for + Duration::from_secs(20))
        .set_iterations(iterations)
        .run()
}

#[test]
fn every_planted_bug_is_noticed() {
    let seeds: usize = std::env::var("SIM_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12);
    // Hostile settings, each the natural enemy of some precaution: workers
    // fighting over every slice; the same with three times the faults and an
    // operator; long stalls, after which a worker resumes far behind; a store
    // applying commands long after their sender gave up; a source whose
    // partitions come and go, with and without a fight; and an application
    // redeployed while it runs, two binaries side by side, with and without
    // a placement to keep them apart.
    let scenarios = [
        Scenario {
            duel: true,
            ..Scenario::default()
        },
        Scenario {
            duel: true,
            operator: true,
            fault_scale: 3.0,
            keys: 60,
            ..Scenario::default()
        },
        Scenario {
            duel: true,
            stall_ms: 6_000,
            gc_grace: Duration::from_secs(24),
            ..Scenario::default()
        },
        Scenario {
            late_ms: 800,
            gc_grace: Duration::from_secs(5),
            fault_scale: 3.0,
            slide: Duration::from_millis(500),
            ..Scenario::default()
        },
        Scenario {
            repartition: true,
            ..Scenario::default()
        },
        Scenario {
            repartition: true,
            duel: true,
            slide: Duration::from_millis(500),
            ..Scenario::default()
        },
        Scenario {
            deploy: true,
            ..Scenario::default()
        },
        Scenario {
            deploy: true,
            duel: true,
            ..Scenario::default()
        },
    ];
    // `SIM_MUTANT=<name>` plants that one only.
    let only = std::env::var("SIM_MUTANT").ok();
    let mut survivors = Vec::new();
    for mutant in Mutant::ALL {
        if only.as_ref().is_some_and(|only| *only != format!("{mutant:?}")) {
            continue;
        }
        plant(Some(mutant));
        let mut noticed = 0;
        let mut runs = 0;
        for scenario in &scenarios {
            let report = run(scenario, seeds);
            runs += seeds;
            noticed += report.failed_runs.max(report.seeds_failing.len());
            if noticed == 0 && !report.assertion_violations.is_empty() {
                noticed = 1;
            }
        }
        plant(None);
        eprintln!("[MUTANT] {mutant:?}: noticed in {noticed} of {runs} runs");
        if noticed == 0 {
            survivors.push(mutant);
        }
    }
    assert!(
        survivors.is_empty(),
        "planted bugs the simulation did not notice: {survivors:?}"
    );
}
