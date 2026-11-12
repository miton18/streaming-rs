#!/usr/bin/env bash
# Seed sweep: run the exactly-once simulation on as many fresh seeds as fit in
# a wall-clock budget, every scenario in turn, on every core.
#
#   scripts/sim-sweep.sh                      # 15 minutes, one job per core
#   SWEEP_BUDGET_SECS=60 scripts/sim-sweep.sh # a quick look
#
# Each job runs one test of `crates/streaming-sim/tests/exactly_once.rs` for
# SWEEP_ITERATIONS seeds (SWEEP_CONFIGS draws for the random configurations).
# Unpinned runs draw new seeds every time, so every job explores new ground.
# No job starts once the budget is spent; the ones running finish.
#
# Everything lands in SWEEP_OUT (default `target/sim-sweep`):
#   runs.tsv       one line per job: test, iterations, exit code, seconds
#   failures/      the full log of every failing job
#   summary.md     totals, and a replay command per failing seed
# The script exits 1 when any job failed.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUDGET=${SWEEP_BUDGET_SECS:-900}
JOBS=${SWEEP_JOBS:-$(nproc)}
ITERATIONS=${SWEEP_ITERATIONS:-100}
CONFIGS=${SWEEP_CONFIGS:-10}
# A job lasts seconds; one that has not ended after five minutes holds a
# seed that does not end.
JOB_TIMEOUT=${SWEEP_JOB_TIMEOUT_SECS:-300}
OUT=${SWEEP_OUT:-$ROOT/target/sim-sweep}
PROFILE=${SWEEP_PROFILE:-release}
REPLAYS=${SWEEP_REPLAYS:-3}     # replay commands listed per failing log
KEPT_LOGS=${SWEEP_KEPT_LOGS:-50} # failing logs kept

rm -rf "$OUT"
mkdir -p "$OUT/runs" "$OUT/failures"
cd "$ROOT"

echo "Building the simulation ($PROFILE profile)..."
BIN=$(cargo test --profile "$PROFILE" -p streaming-sim --test exactly_once --no-run \
    --message-format=json 2>/dev/null |
    grep -o '"executable":"[^"]*"' | sed 's/"executable":"\(.*\)"/\1/' | tail -n 1)
if [[ -z $BIN || ! -x $BIN ]]; then
    # Say why, with cargo's own words.
    cargo test --profile "$PROFILE" -p streaming-sim --test exactly_once --no-run
    echo "error: cannot find the simulation test binary" >&2
    exit 2
fi
mapfile -t TESTS < <("$BIN" --list --format=terse | sed -n 's/: test$//p')
if ((${#TESTS[@]} == 0)); then
    echo "error: the simulation lists no test" >&2
    exit 2
fi

START=$(date +%s)
DEADLINE=$((START + BUDGET))
echo "Sweeping ${#TESTS[@]} scenarios on $JOBS jobs for ${BUDGET}s."

# A shared counter hands out jobs, so the scenarios are taken in turn whatever
# each one lasts.
echo 0 >"$OUT/.counter"
next_job() {
    local n
    {
        flock 9
        n=$(<"$OUT/.counter")
        echo $((n + 1)) >"$OUT/.counter"
    } 9>"$OUT/.lock"
    echo "$n"
}

worker() {
    while (($(date +%s) < DEADLINE)); do
        local n test log began rc iterations seeds
        n=$(next_job)
        test=${TESTS[n % ${#TESTS[@]}]}
        iterations=$ITERATIONS seeds=$ITERATIONS
        if [[ $test == exactly_once_in_random_configurations ]]; then
            # Three seeds per draw, as in the ordinary test.
            iterations=3 seeds=$((CONFIGS * 3))
        fi
        log=$OUT/runs/$(printf '%05d' "$n")-$test.log
        began=$(date +%s)
        rc=0
        SIM_TRACE_SEEDS=1 SIM_ITERATIONS=$iterations SIM_CONFIGS=$CONFIGS timeout "$JOB_TIMEOUT" \
            "$BIN" --exact "$test" --test-threads=1 --nocapture >"$log" 2>&1 || rc=$?
        printf '%s\t%s\t%s\t%s\n' "$test" "$seeds" "$rc" "$(($(date +%s) - began))" \
            >>"$OUT/runs.tsv"
        if ((rc == 124)); then
            echo "killed after ${JOB_TIMEOUT}s" >>"$log"
        fi
        if ((rc != 0)); then
            echo "FAILED: $test (job $n, exit $rc)"
            # Enough logs to diagnose, not enough to fill the disk when
            # everything fails.
            if (($(ls "$OUT/failures" | wc -l) < KEPT_LOGS)); then
                cp "$log" "$OUT/failures/"
            fi
        fi
        rm -f "$log"
    done
}

touch "$OUT/runs.tsv"
for _ in $(seq "$JOBS"); do
    worker &
done
wait

# The summary is best effort: a grep that finds nothing is not an error.
set +e
ELAPSED=$(($(date +%s) - START))
FAILED_JOBS=$(awk -F'\t' '$3 != 0' "$OUT/runs.tsv" | wc -l)

{
    echo "## Simulation seed sweep"
    echo
    echo "Commit \`$(git rev-parse --short HEAD 2>/dev/null || echo unknown)\`," \
        "${ELAPSED}s on $JOBS jobs, profile \`$PROFILE\`."
    echo
    echo "| Scenario | Jobs | Seeds | Failed jobs |"
    echo "|---|---:|---:|---:|"
    awk -F'\t' '{ jobs[$1]++; seeds[$1] += $2; if ($3 != 0) failed[$1]++ }
        END { for (t in jobs) printf "| `%s` | %d | %d | %d |\n", t, jobs[t], seeds[t], failed[t] }' \
        "$OUT/runs.tsv" | sort
    awk -F'\t' '{ jobs++; seeds += $2 } END { printf "| **Total** | %d | %d | %s |\n", jobs, seeds, "'"$FAILED_JOBS"'" }' \
        "$OUT/runs.tsv"
    echo
    if ((FAILED_JOBS == 0)); then
        echo "Every seed passed."
    else
        echo "### Failures"
        echo
        echo "Replay them locally (the logs of the first $KEPT_LOGS failing jobs are in the" \
            "\`failures\` directory of the artifact):"
        echo
        echo '```sh'
        for log in $(ls "$OUT"/failures/*.log | head -n 20); do
            test=$(basename "$log" .log)
            test=${test#*-}
            if grep -q '^killed after' "$log"; then
                # The last seed that started is the one that never ended.
                seed=$(grep -o '^\[SEED\] [0-9]*' "$log" | tail -n 1 | sed 's/.* //')
                config=$(grep -o 'SIM_CONFIG_SEED=[0-9]* starts' "$log" | tail -n 1 | sed 's/ starts$//; s/$/ /')
                echo "# $test: seed $seed did not end within ${JOB_TIMEOUT}s"
                echo "${config}SIM_SEEDS=$seed cargo test --profile $PROFILE -p streaming-sim --test exactly_once $test -- --exact --nocapture"
                continue
            fi
            if [[ $test == exactly_once_in_random_configurations ]]; then
                # failing configurations (SIM_CONFIG_SEED, seeds): [(c, [s, ...]), ...]
                grep -o '([0-9]*, \[[0-9, ]*\])' "$log" | head -n "$REPLAYS" | while read -r pair; do
                    config=$(sed 's/(\([0-9]*\),.*/\1/' <<<"$pair")
                    for seed in $(sed 's/.*\[\(.*\)\].*/\1/; s/,/ /g' <<<"$pair"); do
                        echo "SIM_CONFIG_SEED=$config SIM_SEEDS=$seed cargo test --profile $PROFILE -p streaming-sim --test exactly_once $test -- --exact --nocapture"
                    done
                done
                grep -o 'SIM_CONFIG_SEED=[0-9]* starts' "$log" | tail -n 1 |
                    sed 's/ starts$//; s/^/# last draw started: /'
            else
                {
                    grep -o 'failing seeds: \[[0-9, ]*\]' "$log" |
                        sed 's/.*\[\(.*\)\]/\1/; s/,/ /g' | tr ' ' '\n'
                    grep -o '\[VIOLATION\] seed=[0-9]*' "$log" | sed 's/.*=//'
                } | grep . | sort -u | head -n "$REPLAYS" | while read -r seed; do
                    echo "SIM_SEEDS=$seed cargo test --profile $PROFILE -p streaming-sim --test exactly_once $test -- --exact --nocapture"
                done
            fi
        done
        echo '```'
        echo
        echo "First violations:"
        echo
        echo '```'
        grep -h '\[VIOLATION\]' "$OUT"/failures/*.log | sort -u | head -n 20 || true
        echo '```'
    fi
} >"$OUT/summary.md"

cat "$OUT/summary.md"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
    cat "$OUT/summary.md" >>"$GITHUB_STEP_SUMMARY"
fi
rm -f "$OUT/.counter" "$OUT/.lock"
if ((FAILED_JOBS > 0)); then
    echo "error: $FAILED_JOBS simulation jobs failed, see $OUT/summary.md" >&2
    exit 1
fi
