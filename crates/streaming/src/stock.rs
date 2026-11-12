//! Operators every application has, beyond the connectors.
//!
//! They are the glue of a common pipeline (read, key, window, count, write).
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `std/count-to-json@1` | map | `Windowed<u64>` -> JSON | |
//! | `std/count-to-gts@1` | map | `Windowed<u64>` -> `Gts` | `{"class": "...", "label": "key"}` |
//! | `std/pulsar-key@1` | key_by | `PulsarMessage` | (feature `pulsar`) |
//! | `std/count-to-pulsar@1` | map | `Windowed<u64>` -> `PulsarOutput` | (feature `pulsar`) |

use moonpool_core::Providers;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use streaming_core::{Operator, Registry, Windowed};
#[cfg(feature = "pulsar")]
use streaming_pulsar::{PulsarMessage, PulsarOutput};
use streaming_warp10::Gts;

fn default_label() -> String {
    "key".to_owned()
}

/// `std/count-to-json@1`: turns a windowed count into a JSON object
/// `{key, count, windowStart, windowEnd}`.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct CountToJson;

impl Operator for CountToJson {
    const OP: &'static str = "std/count-to-json@1";
}

/// `std/count-to-gts@1`: turns a windowed count into a Warp 10 point stamped
/// at the window start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountToGts {
    /// Class of the produced series.
    pub class: String,
    /// Name of the label carrying the grouping key.
    #[serde(default = "default_label")]
    pub label: String,
}

impl CountToGts {
    /// Points of class `class`, the key in label `key`.
    #[must_use]
    pub fn new(class: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            label: default_label(),
        }
    }

    /// Carry the key in label `label` instead.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
}

impl Operator for CountToGts {
    const OP: &'static str = "std/count-to-gts@1";
}

/// `std/pulsar-source@1`: reads a Pulsar topic, one slice per partition.
/// Only the main settings: the connector takes more (authentication, event
/// time, deduplication), see `streaming-pulsar`.
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PulsarSource {
    /// `pulsar://host:port` or `pulsar+ssl://host:port`.
    pub service_url: String,
    /// Topic, partitioned or not.
    pub topic: String,
    /// Subscription the cluster reads through.
    pub subscription: String,
}

#[cfg(feature = "pulsar")]
impl Operator for PulsarSource {
    const OP: &'static str = "std/pulsar-source@1";
}

/// `std/pulsar-sink@1`: writes to a Pulsar topic (at least once).
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PulsarSink {
    /// `pulsar://host:port` or `pulsar+ssl://host:port`.
    pub service_url: String,
    /// Topic.
    pub topic: String,
}

#[cfg(feature = "pulsar")]
impl Operator for PulsarSink {
    const OP: &'static str = "std/pulsar-sink@1";
}

/// `std/pulsar-key@1`: groups Pulsar messages by their message key.
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct PulsarKey;

#[cfg(feature = "pulsar")]
impl Operator for PulsarKey {
    const OP: &'static str = "std/pulsar-key@1";
}

/// `std/count-to-pulsar@1`: turns a windowed count into a keyed Pulsar
/// message with a JSON payload.
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct CountToPulsar;

#[cfg(feature = "pulsar")]
impl Operator for CountToPulsar {
    const OP: &'static str = "std/count-to-pulsar@1";
}

fn count_as_json(count: &Windowed<u64>) -> Value {
    json!({
        "key": count.key.display_lossy(),
        "count": count.value,
        "windowStart": count.window.start,
        "windowEnd": count.window.end,
    })
}

/// Register the glue operators.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    registry.register_map(
        CountToJson::OP,
        "Turns a windowed count into a JSON object {key, count, windowStart, windowEnd}.",
        |count: &Windowed<u64>| Some(count_as_json(count)),
    );
    registry.register_map_with(
        CountToGts::OP,
        "Turns a windowed count into a Warp 10 point stamped at the window start.",
        |config: CountToGts| {
            move |count: &Windowed<u64>, out: &mut Vec<Gts>| {
                out.push(
                    Gts::new(config.class.clone())
                        .label(config.label.clone(), count.key.display_lossy())
                        .at(count.window.start)
                        .value(count.value as i64),
                );
            }
        },
    );
    #[cfg(feature = "pulsar")]
    {
        registry.register_key(
            PulsarKey::OP,
            "Groups Pulsar messages by their message key (empty when absent).",
            |message: &PulsarMessage| message.key.clone().unwrap_or_default(),
        );
        registry.register_map(
            CountToPulsar::OP,
            "Turns a windowed count into a keyed Pulsar message with a JSON payload.",
            |count: &Windowed<u64>| {
                Some(PulsarOutput {
                    key: Some(count.key.display_lossy()),
                    payload: count_as_json(count).to_string().into_bytes(),
                    properties: Vec::new(),
                })
            },
        );
        streaming_pulsar::register(registry);
    }
    registry.with_params(
        CountToGts::OP,
        [
            streaming_core::ParamSpec::new("class", "text", "Class of the series.").required(),
            streaming_core::ParamSpec::new("label", "text", "Label that carries the key.").default("key"),
        ],
    );
}
