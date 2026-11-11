#!/usr/bin/env bash
# End-to-end check of the stock `streamd` on real infrastructure:
#
#   Pulsar (3 partitions) -> count per key in 5 s windows -> Pulsar + API rows
#
# with the state in a real Redis, two workers found through gossip, and the
# worker running the pipeline killed with SIGKILL halfway through. Passes when
# the counts per key equal what was produced.
#
# Needs: redis-server, a Pulsar distribution (PULSAR_HOME, with Java), curl,
# python3. Example:
#
#   PULSAR_HOME=~/apache-pulsar-4.2.5 scripts/e2e-pulsar-redis.sh
set -euo pipefail

: "${PULSAR_HOME:?set PULSAR_HOME to an Apache Pulsar distribution}"
REDIS_PORT=${REDIS_PORT:-6390}
API=${API:-127.0.0.1:8090}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
TOPIC=persistent://public/default/events-$$
OUT=persistent://public/default/counts-$$
PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  redis-cli -p "$REDIS_PORT" shutdown nosave >/dev/null 2>&1 || true
  [ -n "${STARTED_PULSAR:-}" ] && pkill -f "pulsar standalone" 2>/dev/null || true
  echo "logs: $WORK"
}
trap cleanup EXIT

echo "== building streamd with the Pulsar connector"
(cd "$ROOT" && cargo build -p streamd --features pulsar)
STREAMD=${CARGO_TARGET_DIR:-$ROOT/target}/debug/streamd

echo "== starting Redis on port $REDIS_PORT"
redis-server --port "$REDIS_PORT" --save "" --appendonly no >"$WORK/redis.log" 2>&1 &
PIDS+=($!)

if ! curl -sf -o /dev/null http://127.0.0.1:8080/admin/v2/clusters; then
  echo "== starting Pulsar standalone"
  (cd "$PULSAR_HOME" && PULSAR_MEM="-Xms512m -Xmx1g -XX:MaxDirectMemorySize=512m" \
     bin/pulsar standalone --no-functions-worker --no-stream-storage >"$WORK/pulsar.log" 2>&1 &)
  STARTED_PULSAR=1
  for _ in $(seq 1 90); do
    curl -sf -o /dev/null http://127.0.0.1:8080/admin/v2/clusters && break
    sleep 1
  done
fi
curl -sf -X PUT "http://127.0.0.1:8080/admin/v2/${TOPIC/:\/\//\/}/partitions" \
     -H 'Content-Type: application/json' -d 3

start_worker() { # id, gossip port, extra arguments...
  local id=$1 port=$2
  shift 2
  "$STREAMD" worker --id "$id" --kv "redis://127.0.0.1:$REDIS_PORT" \
     --gossip "127.0.0.1:$port" "$@" >>"$WORK/$id.log" 2>&1 &
  echo $!
}
echo "== starting two workers"
W1=$(start_worker worker-1 7946 --listen "$API")
PIDS+=("$W1")
sleep 1
PIDS+=("$(start_worker worker-2 7947 --seeds 127.0.0.1:7946)")
sleep 6

cat >"$WORK/graph.json" <<JSON
{
  "tenant": "acme", "graph": "count-per-key",
  "settings": {"max_out_of_orderness": "1s", "idle_timeout": "3s",
               "linger": "200ms", "flush_interval": "500ms"},
  "nodes": [
    {"id": "events", "op": "std/pulsar-source@1",
     "config": {"service_url": "pulsar://127.0.0.1:6650", "topic": "$TOPIC",
                "subscription": "count-per-key"}},
    {"id": "by-key", "op": "std/pulsar-key@1"},
    {"id": "window", "op": "std/tumbling@1", "config": {"size": "5s"}},
    {"id": "count", "op": "std/count@1"},
    {"id": "to-json", "op": "std/count-to-json@1"},
    {"id": "results", "op": "std/kv-sink@1", "config": {"retain": 1000}},
    {"id": "to-pulsar", "op": "std/count-to-pulsar@1"},
    {"id": "out", "op": "std/pulsar-sink@1",
     "config": {"service_url": "pulsar://127.0.0.1:6650", "topic": "$OUT"}}
  ],
  "edges": [["events", "by-key"], ["by-key", "window"], ["window", "count"],
            ["count", "to-json"], ["to-json", "results"],
            ["count", "to-pulsar"], ["to-pulsar", "out"]]
}
JSON
echo "== submitting the graph"
curl -sf -X PUT "http://$API/api/v1/tenants/acme/graphs/count-per-key" --data @"$WORK/graph.json"
echo
sleep 8

produce() { # key, count
  (cd "$PULSAR_HOME" && bin/pulsar-client produce "$TOPIC" -k "$1" -m event -n "$2" >/dev/null 2>&1)
}
echo "== producing, killing worker-1 halfway"
produce alpha 120
produce beta 75
produce gamma 40
sleep 2
kill -9 "$W1"
produce alpha 30
produce beta 20
sleep 25
# The API was served by the worker that was killed: bring it back to read the result.
PIDS+=("$(start_worker worker-1 7946 --listen "$API" --seeds 127.0.0.1:7947)")
sleep 12

echo "== checking the counts"
curl -sf "http://$API/api/v1/tenants/acme/graphs/count-per-key/outputs/results?limit=2000" |
python3 -c '
import collections, json, sys
totals = collections.Counter()
for row in json.load(sys.stdin)["rows"]:
    totals[row["value"]["key"]] += row["value"]["count"]
expected = {"alpha": 150, "beta": 95, "gamma": 40}
print("counted:", dict(totals))
sys.exit(0 if dict(totals) == expected else "MISMATCH: expected %r" % expected)
'
echo "OK: every message was counted exactly once across the worker kill"
