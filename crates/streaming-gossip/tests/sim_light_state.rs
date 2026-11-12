//! Light state: with tight limits and a small delta budget, digests and
//! deltas stay within what the limits imply, limit violations are refused,
//! and a (re)joining node resynchronises the whole cluster state within a
//! bounded number of rounds / bytes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_possible_truncation)]

mod common;

use async_trait::async_trait;
use common::{NodeParams, Registry, builder_with, check_report, convergence_gap, fail, ground_truth, seeds, sleep};
use moonpool_sim::{
    Attrition, AttritionScope, Chaos, ChaosMode, SimContext, SimulationResult, TimeProvider, Workload, assert_always,
    assert_sometimes,
};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use streaming_gossip::{GossipError, Limits, wire};

const N: usize = 5;

fn limits() -> Limits {
    Limits {
        max_keys: 16,
        max_key_len: 32,
        max_value_len: 128,
        max_node_id_len: 32,
        max_addr_len: 32,
        max_nodes: 64,
    }
}

fn params() -> NodeParams {
    NodeParams {
        delta_budget_bytes: 512,
        limits: limits(),
        // 3 keys are always published (addr, incarnation, only-<gen>): fill up to the cap.
        filler_keys: 13,
        filler_len: 128,
        ..NodeParams::default()
    }
}

/// Upper bound of the bytes of state the cluster minus one node holds.
fn others_state_bound() -> usize {
    (N - 1) * limits().max_node_state_bytes()
}

/// Number of deltas needed at least to ship that state.
fn min_deltas() -> usize {
    others_state_bound().div_ceil(params().delta_budget_bytes - wire::MAX_VARINT_USIZE)
}

/// Bound asserted on the number of the rejoining node's own rounds until it
/// holds the whole cluster state. Each round initiates one exchange (one
/// delta received); inbound exchanges bring more. The factor absorbs
/// connect failures / hangs (1 s timeout = 5 rounds) and exchanges with peers
/// that are themselves still syncing.
fn max_rounds_to_sync() -> u64 {
    (2 * min_deltas() + 25) as u64
}

static MAX_ROUNDS_SEEN: AtomicU64 = AtomicU64::new(0);
static MAX_BYTES_SEEN: AtomicU64 = AtomicU64::new(0);
static RESYNCS: AtomicU64 = AtomicU64::new(0);

struct LightWorkload {
    registry: Registry,
    chaos: Duration,
}

struct Tracked {
    generation: u64,
    booted_at: Duration,
    synced: bool,
}

impl LightWorkload {
    fn check_sizes(&self) {
        let p = params();
        for (ip, g) in self.registry.live() {
            let s = g.stats();
            assert_always!(
                s.max_delta_bytes <= p.delta_budget_bytes as u64,
                "light: delta within budget",
                { "ip" => ip, "max_delta_bytes" => s.max_delta_bytes }
            );
            // Exact bound for these ids: per entry id + 4 varints.
            let id_len = g.node_id().len();
            let digest_bound = wire::MAX_VARINT_USIZE + N * (1 + id_len + 4 * wire::MAX_VARINT_U64);
            assert_always!(
                s.max_digest_bytes <= digest_bound as u64 && digest_bound <= p.limits.max_digest_bytes(N),
                "light: digest within limits",
                { "ip" => ip, "max_digest_bytes" => s.max_digest_bytes, "bound" => digest_bound }
            );
            if s.max_delta_bytes > 0 {
                assert_sometimes!(
                    s.max_delta_bytes > (p.delta_budget_bytes as u64) / 2,
                    "light: deltas fill the budget"
                );
            }
        }
    }

    /// Track rebooted incarnations until they hold the whole cluster state.
    fn check_resync(&self, tracked: &mut BTreeMap<String, Tracked>) -> SimulationResult<()> {
        let live = self.registry.live();
        let booted: BTreeMap<String, Duration> = self
            .registry
            .0
            .lock()
            .live
            .iter()
            .map(|(ip, n)| (ip.clone(), n.booted_at))
            .collect();
        for (ip, g) in &live {
            let entry = tracked.entry(ip.clone()).or_insert(Tracked {
                generation: g.generation(),
                booted_at: booted[ip],
                synced: false,
            });
            if entry.generation != g.generation() {
                *entry = Tracked {
                    generation: g.generation(),
                    booted_at: booted[ip],
                    synced: false,
                };
            }
            if entry.synced || entry.booted_at.is_zero() {
                // Initial boot of the whole cluster is not a rejoin.
                continue;
            }
            let truth = ground_truth(&live);
            let view: BTreeMap<_, _> = g
                .members()
                .into_iter()
                .map(|m| (m.node_id, (m.generation, m.values)))
                .collect();
            let has_all = truth.iter().all(|(id, t)| view.get(id) == Some(t));
            let s = g.stats();
            if has_all {
                entry.synced = true;
                RESYNCS.fetch_add(1, Ordering::Relaxed);
                MAX_ROUNDS_SEEN.fetch_max(s.rounds, Ordering::Relaxed);
                MAX_BYTES_SEEN.fetch_max(s.bytes_received, Ordering::Relaxed);
                assert_sometimes!(true, "light: rejoined node resynchronised");
                assert_sometimes!(
                    s.frames_received as usize >= min_deltas(),
                    "light: rejoin needed several deltas"
                );
                // Bytes: the state itself (at most twice: concurrent exchanges
                // may deliver the same versions) + per frame overhead (header,
                // tags, the peer's digest).
                let digest_bound = wire::MAX_VARINT_USIZE + N * (1 + g.node_id().len() + 4 * wire::MAX_VARINT_U64);
                let per_frame = (wire::FRAME_HEADER_LEN + wire::MESSAGE_OVERHEAD + 32 + digest_bound) as u64;
                let byte_bound = 2 * others_state_bound() as u64 + s.frames_received * per_frame;
                assert_always!(
                    s.bytes_received <= byte_bound,
                    "light: resync bytes within 2x state + per-frame overhead",
                    { "ip" => ip, "bytes" => s.bytes_received, "bound" => byte_bound }
                );
            }
            assert_always!(
                entry.synced || s.rounds <= max_rounds_to_sync(),
                "light: rejoining node resyncs within the round bound",
                { "ip" => ip, "rounds" => s.rounds, "bound" => max_rounds_to_sync() }
            );
            if !entry.synced && s.rounds > max_rounds_to_sync() {
                return Err(fail(format!(
                    "{ip} not resynced after {} rounds (bound {}): {s:?}",
                    s.rounds,
                    max_rounds_to_sync()
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Workload for LightWorkload {
    fn name(&self) -> &str {
        "light-state"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let mut tracked = BTreeMap::new();
        let start = ctx.time().now();
        let mut limit_checked = false;
        while ctx.time().now() - start < self.chaos + Duration::from_secs(60) {
            sleep(ctx, Duration::from_millis(100)).await?;
            self.check_sizes();
            self.check_resync(&mut tracked)?;
            let live = self.registry.live();
            if !limit_checked && let Some((_, g)) = live.first() {
                let l = limits();
                let too_long = g.set("x", &"v".repeat(l.max_value_len + 1));
                let too_many = g.set("one-more-key", "v");
                let key_too_long = g.set(&"k".repeat(l.max_key_len + 1), "v");
                let ok = matches!(too_long, Err(GossipError::ValueTooLong { .. }))
                    && matches!(too_many, Err(GossipError::TooManyKeys { .. }))
                    && matches!(key_too_long, Err(GossipError::KeyTooLong { .. }));
                assert_always!(ok, "light: limit violations are refused");
                limit_checked = true;
            }
            if ctx.time().now() - start > self.chaos && convergence_gap(&live, N).is_none() {
                assert_always!(true, "light: cluster converges");
                return Ok(());
            }
        }
        let live = self.registry.live();
        assert_always!(false, "light: cluster converges");
        Err(fail(format!("no convergence: {:?}", convergence_gap(&live, N))))
    }
}

#[test]
fn light_state_and_fast_resync() {
    let registry = Registry::default();
    let chaos = Duration::from_secs(40);
    let report = builder_with(&registry, N, params(), seeds(100))
        .workload(LightWorkload {
            registry: registry.clone(),
            chaos,
        })
        .enable_chaos([Chaos::Attrition {
            config: Attrition {
                max_dead: 1,
                prob_graceful: 0.0,
                prob_crash: 0.7,
                prob_wipe: 0.3,
                recovery_delay_ms: Some(1000..8000),
                grace_period_ms: None,
                scope: AttritionScope::PerProcess,
            },
            mode: ChaosMode::Random,
        }])
        .chaos_duration(chaos)
        .run();
    eprintln!(
        "[light] limits imply: node state <= {} B, others' state <= {} B, >= {} deltas of {} B; \
         round bound {}; observed over {} resyncs: max rounds {} max bytes received {}",
        limits().max_node_state_bytes(),
        others_state_bound(),
        min_deltas(),
        params().delta_budget_bytes,
        max_rounds_to_sync(),
        RESYNCS.load(Ordering::Relaxed),
        MAX_ROUNDS_SEEN.load(Ordering::Relaxed),
        MAX_BYTES_SEEN.load(Ordering::Relaxed),
    );
    check_report("light_state_and_fast_resync", &report);
}
