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
//!
//! With the `pulsar` feature, [`PulsarSource`] and [`PulsarSink`] of
//! `streaming-pulsar` are found here too.

use moonpool_core::Providers;
use serde_json::{Value, json};
use streaming_core::{Operator, Pipe, Registry, Transformer, Windowed};
#[cfg(feature = "pulsar")]
use streaming_pulsar::{PulsarMessage, PulsarOutput};
#[cfg(feature = "pulsar")]
pub use streaming_pulsar::{PulsarSink, PulsarSource};
use streaming_warp10::Gts;

/// Turns a windowed count into a JSON object {key, count, windowStart,
/// windowEnd}.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/count-to-json@1")]
pub struct CountToJson;

impl Transformer<Windowed<u64>> for CountToJson {
    type Out = Value;

    fn transform(&self, input: Pipe<Windowed<u64>>) -> Pipe<Value> { input.map(count_as_json) }
}

/// Turns a windowed count into a Warp 10 point stamped at the window start.
#[derive(Debug, Clone, PartialEq, Eq, Operator)]
#[operator("std/count-to-gts@1")]
pub struct CountToGts {
    /// Class of the series.
    #[param]
    pub class: String,
    /// Label that carries the key.
    #[param(default = "key")]
    pub label: String,
}

impl CountToGts {
    /// Points of class `class`, the key in label `key`.
    #[must_use]
    pub fn new(class: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            label: "key".to_owned(),
        }
    }

    /// Carry the key in label `label` instead.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
}

impl Transformer<Windowed<u64>> for CountToGts {
    type Out = Gts;

    fn transform(&self, input: Pipe<Windowed<u64>>) -> Pipe<Gts> {
        let (class, label) = (self.class.clone(), self.label.clone());
        input.map(move |count| {
            Gts::new(class.clone())
                .label(label.clone(), count.key.display_lossy())
                .at(count.window.start)
                .value(count.value as i64)
        })
    }
}

/// Groups Pulsar messages by their message key (empty when absent).
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/pulsar-key@1")]
pub struct PulsarKey;

#[cfg(feature = "pulsar")]
impl Transformer<PulsarMessage> for PulsarKey {
    type Out = streaming_core::Keyed<PulsarMessage>;

    fn transform(&self, input: Pipe<PulsarMessage>) -> Pipe<Self::Out> {
        input.key_by(|message| message.key.clone().unwrap_or_default())
    }
}

/// Turns a windowed count into a keyed Pulsar message with a JSON payload.
#[cfg(feature = "pulsar")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/count-to-pulsar@1")]
pub struct CountToPulsar;

#[cfg(feature = "pulsar")]
impl Transformer<Windowed<u64>> for CountToPulsar {
    type Out = PulsarOutput;

    fn transform(&self, input: Pipe<Windowed<u64>>) -> Pipe<PulsarOutput> {
        input.map(|count| PulsarOutput {
            key: Some(count.key.display_lossy()),
            payload: count_as_json(count).to_string().into_bytes(),
            properties: Vec::new(),
        })
    }
}

fn count_as_json(count: &Windowed<u64>) -> Value {
    json!({
        "key": count.key.display_lossy(),
        "count": count.value,
        "windowStart": count.window.start,
        "windowEnd": count.window.end,
    })
}

/// Register the glue operators, and the Pulsar connectors with the `pulsar`
/// feature.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    let known = "a stock operator is one kind of operator";
    registry.transformer(&CountToJson).expect(known);
    registry.transformer(&CountToGts::new("class")).expect(known);
    #[cfg(feature = "pulsar")]
    {
        registry.transformer(&PulsarKey).expect(known);
        registry.transformer(&CountToPulsar).expect(known);
        streaming_pulsar::register(registry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use streaming_core::{Key, OpFactory, OpRef, Window, datum};

    fn apply(op: &str, config: Value) -> Vec<streaming_core::Datum> {
        let mut registry = Registry::<TokioProviders>::standard();
        register(&mut registry);
        let Some((_, OpFactory::Map(factory))) = registry.get(&OpRef::new(op).unwrap()) else {
            panic!("{op} is not a map");
        };
        let map = factory(&config).unwrap();
        let mut out = Vec::new();
        let count = Windowed {
            window: Window::new(1_000, 2_000),
            key: Key::encode(&"alice".to_owned()).unwrap(),
            value: 7_u64,
        };
        map.apply(&datum(count), &mut out).unwrap();
        out
    }

    #[test]
    fn a_count_becomes_a_json_row() {
        let out = apply(CountToJson::OP, Value::Null);
        assert_eq!(
            out[0].downcast_ref::<Value>().unwrap(),
            &json!({"key": "alice", "count": 7, "windowStart": 1_000, "windowEnd": 2_000})
        );
    }

    #[test]
    fn a_count_becomes_a_point_labelled_with_its_key() {
        for (config, label) in [
            (json!({"class": "events.count"}), "key"),
            (json!({"class": "events.count", "label": "user"}), "user"),
        ] {
            let out = apply(CountToGts::OP, config);
            let point = out[0].downcast_ref::<Gts>().unwrap();
            let expected = Gts::new("events.count").label(label, "alice").at(1_000).value(7);
            assert_eq!(point, &expected);
        }
        assert_eq!(CountToGts::new("c").label, "key");
        let read: CountToGts = serde_json::from_value(json!({"class": "c"})).unwrap();
        assert_eq!(read, CountToGts::new("c"));
        let params = CountToGts::params();
        assert!(params[0].required && !params[1].required);
        assert_eq!(params[1].default, Some(json!("key")));
    }
}
