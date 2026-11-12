//! Deterministic simulation of `Warp10Sink<SimProviders>` against `FakeWarp10`.
//!
//! The fake runs as a crashable `Process` (its state lives outside, in an
//! `Arc`, and survives reboots). One workload writes a known set of points in
//! batches through the sink with a retry-until-ok loop, under the simulator's
//! default network chaos, process attrition, and faults injected into the fake
//! (503s, connection dropped before or after the points were stored).
//!
//! Invariant checked at the end of every seed: the fake holds exactly the
//! expected points with the expected values, whatever the number of retries.
//!
//! The simulator's default chaos includes wire bit flips. TCP checksums would
//! catch those in production; HTTP and the GTS format have no integrity check,
//! so a flipped digit in a request body is a different, valid point. A
//! `FaultWatch` invariant records whether a flip happened in the seed and the
//! exact-equality check is only enforced on seeds without any (counts are
//! printed by the test).

use async_trait::async_trait;
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, Invariant, Process, RandomProvider, SIM_FAULT_EVENT_NAME, SimContext,
    SimulationBuilder, SimulationError, SimulationResult, TimeProvider, TraceQuery, Workload, assert_always,
    assert_sometimes,
};
use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use streaming_core::{Emit, EmitId, NodeId, Origin, Position, SinkConnector, SliceId, datum};
use streaming_warp10::{
    Gts, GtsValue, Warp10Config, Warp10Sink,
    fake::{FakeWarp10, Fault, PointKey},
};

const TOKEN: &str = "sim-write-token";
const BATCHES: usize = 12;
const POINTS_PER_BATCH: usize = 8;
const CHAOS_FOR: Duration = Duration::from_secs(8);
/// A batch that does not make it after this many attempts is a bug.
const MAX_ATTEMPTS: u32 = 400;

#[derive(Clone, Default)]
struct World {
    fake: Option<FakeWarp10>,
    bit_flips: Arc<AtomicU64>,
    saw_crash: Arc<AtomicBool>,
    strict_seeds: Arc<AtomicU64>,
    relaxed_seeds: Arc<AtomicU64>,
    total_attempts: Arc<AtomicU64>,
    total_errors: Arc<AtomicU64>,
    total_batches: Arc<AtomicU64>,
}

impl World {
    fn new() -> Self {
        Self {
            fake: Some(FakeWarp10::new(TOKEN)),
            ..Self::default()
        }
    }
    fn fake(&self) -> &FakeWarp10 {
        self.fake.as_ref().unwrap()
    }
    fn reset(&self) {
        self.fake().reset();
        self.bit_flips.store(0, Ordering::SeqCst);
        self.saw_crash.store(false, Ordering::SeqCst);
    }
}

// -- the system under test: the fake ingress, as a crashable process -------

struct FakeProcess {
    fake: FakeWarp10,
}

#[async_trait]
impl Process for FakeProcess {
    fn name(&self) -> &'static str {
        "warp10"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        loop {
            match self
                .fake
                .serve(ctx.providers().clone(), ctx.my_ip(), ctx.shutdown().clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::debug!("fake warp10 serve error: {e}");
                    let _ = ctx.time().sleep(Duration::from_millis(50)).await;
                    if ctx.shutdown().is_cancelled() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

// -- fault bookkeeping ------------------------------------------------------

struct FaultWatch {
    cursor: Cell<usize>,
    world: World,
}

impl Invariant for FaultWatch {
    fn name(&self) -> &'static str {
        "fault_watch"
    }

    fn observe(&self, q: &dyn TraceQuery, _sim_time_ms: u64) {
        for e in q.since(SIM_FAULT_EVENT_NAME, &self.cursor) {
            match e.str("kind") {
                Some("bit_flip") => {
                    self.world.bit_flips.fetch_add(1, Ordering::SeqCst);
                }
                Some("process_force_kill" | "process_graceful_shutdown" | "process_restart") => {
                    self.world.saw_crash.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }

    fn reset(&mut self) {
        self.cursor.set(0);
    }
}

// -- the workload -----------------------------------------------------------

/// The value of point `i` of batch `b`: all four types, with awkward text.
fn value_of(b: usize, i: usize, generation: u32) -> GtsValue {
    match (b + i) % 4 {
        0 => GtsValue::Long((b * 1000 + i) as i64 * if generation == 0 { 1 } else { -1 }),
        1 => GtsValue::Double((b * 100 + i) as f64 / 4.0 + f64::from(generation)),
        2 => GtsValue::Bool((b + i + generation as usize).is_multiple_of(3)),
        _ => GtsValue::Text(format!("b{b}/i{i} g{generation} + é 'q'")),
    }
}

/// Point `i` of batch `b`. The last batch rewrites the first batch's points
/// (generation 1), to exercise overwrite semantics under retries.
fn point(b: usize, i: usize) -> Gts {
    let (src, generation) = if b == BATCHES - 1 { (0, 1) } else { (b, 0) };
    Gts::new("sim.sensor")
        .label("sensor", format!("s{}", i % 3))
        .label("site", "paris nord")
        .at(1_700_000_000_000 + (src * POINTS_PER_BATCH + i) as i64)
        .value(value_of(src, i, generation))
}

fn expected() -> BTreeMap<PointKey, GtsValue> {
    let mut out = BTreeMap::new();
    for b in 0..BATCHES {
        for i in 0..POINTS_PER_BATCH {
            let g = point(b, i);
            out.insert(
                PointKey {
                    class: g.class.clone(),
                    labels: g.labels.clone(),
                    ts: g.ts_ms * 1000,
                },
                g.value,
            );
        }
    }
    out
}

fn emit(n: u64, gts: Gts) -> Emit {
    let origin = Origin::source(Position(0, n, 0));
    let slice = SliceId::Source {
        node: NodeId::new("n").unwrap(),
        partition: 0,
    };
    Emit {
        id: EmitId::new(&slice, &origin),
        origin,
        ts: 0,
        key: None,
        value: datum(gts),
    }
}

#[derive(Default)]
struct Stats {
    attempts: u64,
    errors: u64,
    timeouts: u64,
    status_errors: u64,
}

struct Writer {
    world: World,
    stats: Stats,
}

#[async_trait]
impl Workload for Writer {
    fn name(&self) -> &'static str {
        "writer"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        self.stats = Stats::default();
        let server = ctx.topology().all_process_ips()[0].clone();
        let mut config = Warp10Config::new("http://warp10.sim/api/v0/update", TOKEN)
            .map_err(|e| SimulationError::InvalidState(e.to_string()))?;
        config.connect_address = Some(server);
        config.timeout = Duration::from_millis(1500);
        config.max_batch_bytes = 220; // a few points per request: batches get split
        let sink = Warp10Sink::new(ctx.providers().clone(), config)
            .map_err(|e| SimulationError::InvalidState(e.to_string()))?;
        let fake = self.world.fake().clone();

        for b in 0..BATCHES {
            let batch: Vec<Emit> = (0..POINTS_PER_BATCH)
                .map(|i| emit((b * POINTS_PER_BATCH + i) as u64, point(b, i)))
                .collect();
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                self.stats.attempts += 1;
                if attempt > MAX_ATTEMPTS {
                    return Err(SimulationError::InvalidState(format!(
                        "batch {b} still failing after {MAX_ATTEMPTS} attempts"
                    )));
                }
                // Faults injected on purpose, on top of the network chaos.
                if ctx.random().random_bool(0.35) {
                    fake.push_fault(match ctx.random().random_range(0..3u32) {
                        0 => Fault::Status(503),
                        1 => Fault::DropAfterCommit,
                        _ => Fault::DropBeforeCommit,
                    });
                }
                let outcome = moonpool_sim::select! {
                    biased;
                    r = sink.write(&batch) => r,
                    () = ctx.shutdown().cancelled() => return Ok(()),
                };
                match outcome {
                    Ok(()) => break,
                    Err(e) => {
                        self.stats.errors += 1;
                        if e.0.contains("timed out") {
                            self.stats.timeouts += 1;
                        }
                        if e.0.contains("HTTP 503") {
                            self.stats.status_errors += 1;
                        }
                        assert!(!e.0.contains(TOKEN), "token leaked in `{e}`");
                        let backoff = Duration::from_millis(20 * u64::from(attempt.min(10)));
                        let _ = ctx.time().sleep(backoff).await;
                    }
                }
            }
            let _ = ctx.time().sleep(Duration::from_millis(100)).await;
        }
        // Let straggling requests of abandoned attempts land before the check.
        let _ = ctx.time().sleep(Duration::from_secs(1)).await;
        Ok(())
    }

    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let fake = self.world.fake();
        let fstats = fake.stats();
        let world = &self.world;
        world.total_attempts.fetch_add(self.stats.attempts, Ordering::SeqCst);
        world.total_errors.fetch_add(self.stats.errors, Ordering::SeqCst);
        world.total_batches.fetch_add(BATCHES as u64, Ordering::SeqCst);

        assert_sometimes!(self.stats.errors > 0, "a write failed (outcome unknown)");
        assert_sometimes!(self.stats.attempts > BATCHES as u64, "a batch had to be retried");
        assert_sometimes!(self.stats.status_errors > 0, "the sink saw an HTTP 503");
        assert_sometimes!(self.stats.timeouts > 0, "the sink timed out on a request");
        assert_sometimes!(
            fstats.drops_after_commit > 0,
            "a connection was dropped after the points were stored"
        );
        assert_sometimes!(
            fstats.drops_before_commit > 0,
            "a connection was dropped before the points were stored"
        );
        assert_sometimes!(
            fstats.points_written > (BATCHES * POINTS_PER_BATCH) as u64,
            "points were written more than once"
        );
        assert_sometimes!(
            world.saw_crash.load(Ordering::SeqCst),
            "the ingress process was rebooted"
        );
        let flips = world.bit_flips.load(Ordering::SeqCst);
        assert_sometimes!(flips > 0, "the simulator flipped bits on the wire");
        assert_sometimes!(flips == 0, "a seed without wire corruption (strict check ran)");

        if flips > 0 {
            world.relaxed_seeds.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        world.strict_seeds.fetch_add(1, Ordering::SeqCst);

        let held = fake.points();
        let want = expected();
        let identical = held == want;
        assert_always!(
            identical,
            "the ingress holds exactly the expected points with the expected values",
            { "held" => held.len(), "expected" => want.len(), "seed_client" => ctx.client_id() }
        );
        if !identical {
            let missing: Vec<_> = want.iter().filter(|(k, v)| held.get(*k) != Some(*v)).take(3).collect();
            let extra: Vec<_> = held.keys().filter(|k| !want.contains_key(*k)).take(3).collect();
            return Err(SimulationError::InvalidState(format!(
                "stored points differ: held {} expected {}; wrong or missing e.g. {missing:?}; extra e.g. {extra:?}",
                held.len(),
                want.len()
            )));
        }
        Ok(())
    }
}

fn run_sim(seeds: Vec<u64>) -> (moonpool_sim::SimulationReport, World) {
    let world = World::new();
    let (w_reset, w_proc, w_inv, w_writer) = (world.clone(), world.clone(), world.clone(), world.clone());
    let report = SimulationBuilder::new()
        .processes(1, move || {
            Box::new(FakeProcess {
                fake: w_proc.fake().clone(),
            })
        })
        .workload(Writer {
            world: w_writer,
            stats: Stats::default(),
        })
        .invariant(FaultWatch {
            cursor: Cell::new(0),
            world: w_inv,
        })
        .enable_chaos([Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 0.4,
                prob_crash: 0.6,
                prob_wipe: 0.0,
                recovery_delay_ms: Some(50..800),
                grace_period_ms: Some(10..200),
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Random,
        }])
        .chaos_duration(CHAOS_FOR)
        .before_iteration(move || w_reset.reset())
        .set_iterations(seeds.len())
        .set_debug_seeds(seeds)
        .run();
    (report, world)
}

fn assert_report(report: &moonpool_sim::SimulationReport, world: &World, check_coverage: bool) {
    eprintln!(
        "iterations={} ok={} failed={} failing_seeds={:?}\nstrict seeds={} relaxed (bit flips) seeds={}\nattempts={} errors={} batches={}\nassertion_violations={:?}\ncoverage_violations={:?}",
        report.iterations,
        report.successful_runs,
        report.failed_runs,
        report.seeds_failing,
        world.strict_seeds.load(Ordering::SeqCst),
        world.relaxed_seeds.load(Ordering::SeqCst),
        world.total_attempts.load(Ordering::SeqCst),
        world.total_errors.load(Ordering::SeqCst),
        world.total_batches.load(Ordering::SeqCst),
        report.assertion_violations,
        report.coverage_violations,
    );
    for d in &report.assertion_details {
        eprintln!(
            "  [{:?}] pass={} fail={} :: {}",
            d.kind, d.pass_count, d.fail_count, d.msg
        );
    }
    assert_eq!(report.failed_runs, 0, "failing seeds: {:?}", report.seeds_failing);
    assert!(
        report.assertion_violations.is_empty(),
        "{:?}",
        report.assertion_violations
    );
    assert!(
        !check_coverage || report.coverage_violations.is_empty(),
        "{:?}",
        report.coverage_violations
    );
}

#[test]
fn warp10_sink_under_chaos_many_seeds() {
    // 100 seeds by default; `WARP10_SIM_SEEDS=1000` runs more.
    let count: u64 = std::env::var("WARP10_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let (report, world) = run_sim((1..=count).collect());
    assert_report(&report, &world, true);
    assert!(world.strict_seeds.load(Ordering::SeqCst) > 0, "no strict seed ran");
}

/// A fixed seed, quick to replay when debugging.
#[test]
fn warp10_sink_single_seed_replay() {
    let (report, world) = run_sim(vec![42]);
    assert_report(&report, &world, false);
}
