//! Operators shipped with the stock `streamd` binary, beyond the connectors.
//!
//! They are the glue that lets a common pipeline (read, key, window, count,
//! write) be described in JSON without compiling a custom worker.
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `std/count-to-json@1` | map | `Windowed<u64>` -> JSON | |
//! | `std/count-to-gts@1` | map | `Windowed<u64>` -> `Gts` | `{"class": "...", "label": "key"}` |
//! | `std/pulsar-key@1` | key_by | `PulsarMessage` | (feature `pulsar`) |
//! | `std/count-to-pulsar@1` | map | `Windowed<u64>` -> `PulsarOutput` | (feature `pulsar`) |

use moonpool_core::Providers;
use serde::Deserialize;
use serde_json::{Value, json};
use streaming_core::{Registry, Windowed};
#[cfg(feature = "pulsar")]
use streaming_pulsar::{PulsarMessage, PulsarOutput};
use streaming_warp10::Gts;

fn default_label() -> String {
    "key".to_owned()
}

#[derive(Debug, Deserialize)]
struct CountToGts {
    /// Class of the produced series.
    class: String,
    /// Name of the label carrying the grouping key.
    #[serde(default = "default_label")]
    label: String,
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
        "std/count-to-json@1",
        "Turns a windowed count into a JSON object {key, count, windowStart, windowEnd}.",
        |count: &Windowed<u64>| Some(count_as_json(count)),
    );
    registry.register_map_with(
        "std/count-to-gts@1",
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
            "std/pulsar-key@1",
            "Groups Pulsar messages by their message key (empty when absent).",
            |message: &PulsarMessage| message.key.clone().unwrap_or_default(),
        );
        registry.register_map(
            "std/count-to-pulsar@1",
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
        "std/count-to-gts@1",
        [
            streaming_core::ParamSpec::new("class", "text", "Class of the series.").required(),
            streaming_core::ParamSpec::new("label", "text", "Label that carries the key.").default("key"),
        ],
    );
}
