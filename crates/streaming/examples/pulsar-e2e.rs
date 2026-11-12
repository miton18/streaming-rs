//! The application `scripts/e2e-pulsar-redis.sh` runs on real Pulsar and
//! Redis: a Pulsar topic counted per key over 5-second windows, the counts
//! kept for the API and written to another topic.
//!
//! The topics come from the environment: `E2E_PULSAR_URL`
//! (`pulsar://127.0.0.1:6650` by default), `E2E_TOPIC` and `E2E_OUT`.

use std::time::Duration;
use streaming::{
    App,
    core::{
        GraphBuilder, GraphSettings,
        stdops::{Count, Tumbling},
    },
    runtime::builtin::KvSink,
    stock::{CountToJson, CountToPulsar, PulsarKey, PulsarSink, PulsarSource},
};

fn env(name: &str, default: Option<&str>) -> String {
    std::env::var(name)
        .ok()
        .or_else(|| default.map(str::to_owned))
        .unwrap_or_else(|| panic!("set {name}"))
}

fn count_per_key() -> GraphBuilder {
    let service_url = env("E2E_PULSAR_URL", Some("pulsar://127.0.0.1:6650"));
    let mut graph = GraphBuilder::new("acme", "count-per-key").settings(GraphSettings {
        max_out_of_orderness: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(3),
        linger: Duration::from_millis(200),
        flush_interval: Duration::from_millis(500),
        ..GraphSettings::default()
    });
    let events = graph.node(
        "events",
        PulsarSource {
            service_url: service_url.clone(),
            topic: env("E2E_TOPIC", None),
            subscription: "count-per-key".to_owned(),
        },
    );
    let by_key = graph.then(events, "by-key", PulsarKey);
    let window = graph.then(by_key, "window", Tumbling::of(Duration::from_secs(5)));
    let count = graph.then(window, "count", Count);
    let to_json = graph.then(count, "to-json", CountToJson);
    graph.then(to_json, "results", KvSink { retain: 1000 });
    let to_pulsar = graph.then(count, "to-pulsar", CountToPulsar);
    graph.then(
        to_pulsar,
        "out",
        PulsarSink {
            service_url,
            topic: env("E2E_OUT", None),
        },
    );
    graph
}

fn main() -> std::process::ExitCode { App::new().tenant("acme", "Acme").graph(count_per_key()).main() }
