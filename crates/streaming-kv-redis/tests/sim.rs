//! Deterministic simulation of `RedisKv<SimProviders>` against `FakeRedis`.
//!
//! The fake server runs as a crashable `Process` (its data lives outside and
//! survives reboots); several client workloads race on `HSETNX` and `INCR`
//! under the simulator's default network chaos plus process attrition.
//!
//! Invariants (checked with the simulator's assertion macros):
//! - for every raced (key, field) at most one client ever receives `Ok(true)`;
//! - the stored value belongs to the client that received `Ok(true)`, or, when
//!   nobody did, to a client that got an error (outcome unknown);
//! - a field reported as existing (`Ok(false)`) really exists;
//! - `INCR` never returns the same value twice, and the stored counter lies
//!   between the number of successes and the number of attempts;
//! - `hgetall` (paged over `HSCAN`) never reports a wrong value and never
//!   misses a field that was confirmed before the scan began.
//!
//! The simulator's default chaos includes wire bit flips. TCP would catch
//! those in production; RESP has no checksum, so a flipped reply digit
//! (`:0` -> `:1`) is indistinguishable from a real answer. A `FaultWatch`
//! invariant therefore records whether a bit flip happened in the seed, and
//! the checks that corruption can legitimately fool are only enforced on
//! seeds without any (reported in the test output).

use async_trait::async_trait;
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, Invariant, Process, RandomProvider, SIM_FAULT_EVENT_NAME, SimContext,
    SimulationBuilder, SimulationResult, TimeProvider, TraceQuery, Workload, assert_always, assert_sometimes,
};
use parking_lot::Mutex;
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use streaming_core::{Kv, KvError};
use streaming_kv_redis::{FakeRedis, RedisConfig, RedisKv};

const CLIENTS: usize = 4;
const FIELDS_PER_ROUND: usize = 12;
const RUN_UNTIL: Duration = Duration::from_secs(15);
const CHAOS_FOR: Duration = Duration::from_secs(9);

type Slot = (String, String);

#[derive(Default)]
struct Ledger {
    /// `Ok(true)` receivers per (key, field).
    winners: HashMap<Slot, Vec<usize>>,
    /// Clients that got an error for a (key, field): outcome unknown.
    unknown: HashMap<Slot, HashSet<usize>>,
    /// (key, field) pairs some client saw as existing (`Ok(true)` or `Ok(false)`).
    confirmed: HashSet<Slot>,
    /// Slots for which some client got `Ok(false)`.
    reported_existing: HashSet<Slot>,
    /// Every `INCR` value returned, and who got it.
    incr_values: HashMap<i64, usize>,
    incr_attempts: u64,
    incr_successes: u64,
}

#[derive(Clone)]
struct World {
    fake: Arc<Mutex<FakeRedis>>,
    ledger: Arc<Mutex<Ledger>>,
    bit_flips: Arc<AtomicU64>,
    strict_seeds: Arc<AtomicU64>,
    relaxed_seeds: Arc<AtomicU64>,
    saw_crash: Arc<AtomicBool>,
}

impl World {
    fn new() -> Self {
        Self {
            fake: Arc::new(Mutex::new(FakeRedis::new())),
            ledger: Arc::new(Mutex::new(Ledger::default())),
            bit_flips: Arc::new(AtomicU64::new(0)),
            strict_seeds: Arc::new(AtomicU64::new(0)),
            relaxed_seeds: Arc::new(AtomicU64::new(0)),
            saw_crash: Arc::new(AtomicBool::new(false)),
        }
    }

    fn reset(&self) {
        *self.fake.lock() = FakeRedis::new();
        *self.ledger.lock() = Ledger::default();
        self.bit_flips.store(0, Ordering::SeqCst);
        self.saw_crash.store(false, Ordering::SeqCst);
    }

    fn fake(&self) -> FakeRedis { self.fake.lock().clone() }
}

// -- server ---------------------------------------------------------------

struct FakeRedisProcess {
    world: World,
}

#[async_trait]
impl Process for FakeRedisProcess {
    fn name(&self) -> &str { "fakeredis" }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // A fresh `serve` on every boot; the data is the shared Arc.
        let fake = self.world.fake();
        fake.serve(ctx.providers().clone(), ctx.my_ip(), ctx.shutdown().clone())
            .await?;
        Ok(())
    }
}

// -- fault bookkeeping ----------------------------------------------------

struct FaultWatch {
    cursor: Cell<usize>,
    world: World,
}

impl Invariant for FaultWatch {
    fn name(&self) -> &'static str { "fault_watch" }

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

    fn reset(&mut self) { self.cursor.set(0); }
}

// -- clients --------------------------------------------------------------

struct Client {
    id: usize,
    world: World,
    /// Per-seed statistics for the sometimes-assertions of `check`.
    stats: Stats,
}

#[derive(Default)]
struct Stats {
    errors: u64,
    timeouts: u64,
    trues: u64,
    falses: u64,
    multi_page_scans: u64,
}

fn encode_value(client: usize, slot: &Slot, attempt: u32, pad: usize) -> Vec<u8> {
    let mut v = format!("c{client}|{}|{}|{attempt}|", slot.0, slot.1).into_bytes();
    v.extend(std::iter::repeat_n(b'.', pad));
    v
}

fn owner_of(value: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(value).ok()?;
    text.strip_prefix('c')?.split('|').next()?.parse().ok()
}

fn is_timeout(e: &KvError) -> bool { matches!(e, KvError::Unavailable(m) if m.contains("timed out")) }

impl Client {
    fn note_error(&mut self, e: &KvError) {
        self.stats.errors += 1;
        if is_timeout(e) {
            self.stats.timeouts += 1;
        }
    }

    async fn round(&mut self, ctx: &SimContext, kv: &RedisKv<moonpool_sim::SimProviders>, round: usize) {
        let key = format!("race:{round}");
        let mut order: Vec<usize> = (0..FIELDS_PER_ROUND).collect();
        // Deterministic per-seed shuffle through the provider.
        for i in (1..order.len()).rev() {
            let j = ctx.random().random_range(0..i + 1);
            order.swap(i, j);
        }
        for field in order {
            let slot: Slot = (key.clone(), format!("f{field}"));
            let pad = if ctx.random().random_bool(0.05) {
                ctx.random().random_range(20_000..60_000)
            } else {
                ctx.random().random_range(0..2_000)
            };
            self.race(ctx, kv, &slot, pad).await;
            let pause = ctx.random().random_range(1..40);
            let _ = ctx.time().sleep(Duration::from_millis(pause)).await;
        }

        // INCR: unique values.
        for _ in 0..2 {
            self.world.ledger.lock().incr_attempts += 1;
            match kv.incr("counter").await {
                Ok(v) => {
                    let mut ledger = self.world.ledger.lock();
                    ledger.incr_successes += 1;
                    let prev = ledger.incr_values.insert(v, self.id);
                    if self.world.bit_flips.load(Ordering::SeqCst) == 0 {
                        assert_always!(prev.is_none(), "INCR never returns the same value twice", { "value" => v });
                    }
                }
                Err(e) => self.note_error(&e),
            }
        }

        // hgetall over HSCAN pages (scan_count 4 -> several pages per round).
        let before: HashSet<String> = {
            let ledger = self.world.ledger.lock();
            ledger
                .confirmed
                .iter()
                .filter(|(k, _)| *k == key)
                .map(|(_, f)| f.clone())
                .collect()
        };
        let scans_before = self.world.fake().command_count("HSCAN");
        match kv.hgetall(&key).await {
            Ok(all) => {
                if self.world.fake().command_count("HSCAN") - scans_before >= 2 {
                    self.stats.multi_page_scans += 1;
                }
                if self.world.bit_flips.load(Ordering::SeqCst) == 0 {
                    let got: HashSet<&str> = all.iter().map(|(f, _)| f.as_str()).collect();
                    for f in &before {
                        assert_always!(
                            got.contains(f.as_str()),
                            "hgetall returns every field confirmed before it began",
                            { "key" => key, "field" => f }
                        );
                    }
                    let fake = self.world.fake();
                    for (f, v) in &all {
                        assert_always!(
                            fake.peek_field(&key, f).as_ref() == Some(v),
                            "hgetall returns the stored value of each field",
                            { "key" => key, "field" => f }
                        );
                    }
                }
            }
            Err(e) => self.note_error(&e),
        }
    }

    /// One raced `HSETNX`, retried blindly on error like a naive caller would.
    async fn race(&mut self, ctx: &SimContext, kv: &RedisKv<moonpool_sim::SimProviders>, slot: &Slot, pad: usize) {
        for attempt in 0..3u32 {
            let value = encode_value(self.id, slot, attempt, pad);
            match kv.hsetnx(&slot.0, &slot.1, value).await {
                Ok(created) => {
                    let mut ledger = self.world.ledger.lock();
                    ledger.confirmed.insert(slot.clone());
                    if created {
                        self.stats.trues += 1;
                        let winners = ledger.winners.entry(slot.clone()).or_default();
                        winners.push(self.id);
                        if self.world.bit_flips.load(Ordering::SeqCst) == 0 {
                            assert_always!(
                                winners.len() == 1,
                                "at most one client receives Ok(true) per (key, field)",
                                { "slot" => format!("{slot:?}"), "winners" => format!("{winners:?}") }
                            );
                        }
                    } else {
                        self.stats.falses += 1;
                        ledger.reported_existing.insert(slot.clone());
                    }
                    return;
                }
                Err(e) => {
                    self.note_error(&e);
                    self.world
                        .ledger
                        .lock()
                        .unknown
                        .entry(slot.clone())
                        .or_default()
                        .insert(self.id);
                    let _ = ctx.time().sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }
}

#[async_trait]
impl Workload for Client {
    fn name(&self) -> &str {
        match self.id {
            0 => "client-0",
            1 => "client-1",
            2 => "client-2",
            _ => "client-n",
        }
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let server = ctx.topology().all_process_ips()[0].clone();
        let kv = RedisKv::new(
            ctx.providers().clone(),
            RedisConfig {
                addr: server,
                connections: 2,
                command_timeout: Duration::from_millis(1500),
                connect_timeout: Duration::from_millis(1500),
                scan_count: 4,
                max_fields_per_command: 5,
                ..RedisConfig::default()
            },
        );
        let mut round = 0;
        while ctx.time().now() < RUN_UNTIL {
            self.round(ctx, &kv, round).await;
            round += 1;
        }
        // The ledger now holds everything; publish stats for `check`.
        let mut ledger = self.world.ledger.lock();
        let _ = &mut *ledger;
        Ok(())
    }

    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Sometimes-assertions are per client: prove the failure paths ran.
        assert_sometimes!(self.stats.errors > 0, "client saw an error (outcome unknown)");
        assert_sometimes!(self.stats.timeouts > 0, "client saw a command timeout");
        assert_sometimes!(self.stats.trues > 0, "hsetnx returned true");
        assert_sometimes!(self.stats.falses > 0, "hsetnx returned false");
        assert_sometimes!(self.stats.multi_page_scans > 0, "hgetall paged over several HSCANs");

        if ctx.client_id() != 0 {
            return Ok(());
        }
        let world = &self.world;
        let fake = world.fake();
        let conns = fake.connections_accepted();
        assert_sometimes!(
            conns > (CLIENTS * 2) as u64,
            "clients reconnected after losing connections"
        );
        assert_sometimes!(
            world.saw_crash.load(Ordering::SeqCst),
            "the server process was rebooted"
        );
        let flips = world.bit_flips.load(Ordering::SeqCst);
        assert_sometimes!(flips > 0, "the simulator flipped bits on the wire");
        assert_sometimes!(flips == 0, "a seed without wire corruption (strict checks ran)");
        if flips > 0 {
            world.relaxed_seeds.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        world.strict_seeds.fetch_add(1, Ordering::SeqCst);

        let ledger = world.ledger.lock();
        let all_slots: HashSet<&Slot> = ledger
            .winners
            .keys()
            .chain(ledger.unknown.keys())
            .chain(ledger.reported_existing.iter())
            .collect();
        for slot in all_slots {
            let stored = fake.peek_field(&slot.0, &slot.1);
            if ledger.reported_existing.contains(slot) {
                assert_always!(stored.is_some(), "a field reported as existing exists", { "slot" => format!("{slot:?}") });
            }
            let owner = stored.as_deref().and_then(owner_of);
            match ledger.winners.get(slot) {
                Some(winners) => {
                    assert_always!(
                        owner == winners.first().copied(),
                        "the stored value belongs to the client that received Ok(true)",
                        { "slot" => format!("{slot:?}"), "owner" => format!("{owner:?}"), "winners" => format!("{winners:?}") }
                    );
                }
                None => {
                    // Nobody was told "created": if the field exists, the
                    // creator must be a client whose call errored.
                    if let Some(o) = owner {
                        let unknown = ledger.unknown.get(slot);
                        assert_always!(
                            unknown.is_some_and(|u| u.contains(&o)),
                            "without an Ok(true), the stored value belongs to a client that got an error",
                            { "slot" => format!("{slot:?}"), "owner" => o }
                        );
                    }
                }
            }
        }

        // INCR: stored counter bounded by successes and attempts.
        let stored = fake
            .peek_string("counter")
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let max_returned = ledger.incr_values.keys().copied().max().unwrap_or(0);
        assert_always!(stored >= max_returned, "counter is at least every returned INCR value", { "stored" => stored, "max" => max_returned });
        assert_always!(
            stored >= ledger.incr_successes as i64,
            "counter is at least the number of successful INCRs",
            { "stored" => stored, "successes" => ledger.incr_successes }
        );
        assert_always!(
            stored <= ledger.incr_attempts as i64,
            "counter is at most the number of INCR attempts",
            { "stored" => stored, "attempts" => ledger.incr_attempts }
        );
        Ok(())
    }
}

fn run_sim(seeds: Vec<u64>) -> (moonpool_sim::SimulationReport, World) {
    let world = World::new();
    let (w_reset, w_proc, w_cli, w_inv) = (world.clone(), world.clone(), world.clone(), world.clone());
    let report = SimulationBuilder::new()
        .processes(1, move || Box::new(FakeRedisProcess { world: w_proc.clone() }))
        .workloads(moonpool_sim::WorkloadCount::Fixed(CLIENTS), move |id| {
            Box::new(Client {
                id,
                world: w_cli.clone(),
                stats: Stats::default(),
            })
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
        "iterations={} ok={} failed={} failing_seeds={:?}\nstrict seeds={} relaxed (bit flips) seeds={}\nassertion_violations={:?}\ncoverage_violations={:?}",
        report.iterations,
        report.successful_runs,
        report.failed_runs,
        report.seeds_failing,
        world.strict_seeds.load(Ordering::SeqCst),
        world.relaxed_seeds.load(Ordering::SeqCst),
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
fn redis_client_under_chaos_many_seeds() {
    // 100 seeds by default; `KVREDIS_SIM_SEEDS=1000 cargo test --release ...`
    // runs more.
    let count: u64 = std::env::var("KVREDIS_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let seeds: Vec<u64> = (1..=count).collect();
    let (report, world) = run_sim(seeds);
    assert_report(&report, &world, true);
    assert!(world.strict_seeds.load(Ordering::SeqCst) > 0, "no strict seed ran");
}

/// A fixed seed, quick to replay when debugging.
#[test]
fn redis_client_single_seed_replay() {
    let (report, world) = run_sim(vec![42]);
    // One seed cannot cover every `assert_sometimes!`; only the always-checks apply.
    assert_report(&report, &world, false);
}
