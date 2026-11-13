//! The glue between a common pipeline and the Pulsar connectors:
//! `std/pulsar-key@1` and `std/count-to-pulsar@1`.

use crate::{PulsarMessage, PulsarOutput};
use serde_json::json;
use streaming_core::{Keyed, Operator, Pipe, Transformer, Windowed};

/// Groups Pulsar messages by their message key (empty when absent).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/pulsar-key@1")]
pub struct PulsarKey;

impl Transformer<PulsarMessage> for PulsarKey {
    type Out = Keyed<PulsarMessage>;

    fn transform(&self, input: Pipe<PulsarMessage>) -> Pipe<Self::Out> {
        input.key_by(|message| message.key.clone().unwrap_or_default())
    }
}

/// Turns a windowed count into a keyed Pulsar message whose payload is the
/// JSON object {key, count, windowStart, windowEnd}.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/count-to-pulsar@1")]
pub struct CountToPulsar;

impl Transformer<Windowed<u64>> for CountToPulsar {
    type Out = PulsarOutput;

    fn transform(&self, input: Pipe<Windowed<u64>>) -> Pipe<PulsarOutput> {
        input.map(|count| {
            let key = count.key.display_lossy();
            let payload = json!({
                "key": key,
                "count": count.value,
                "windowStart": count.window.start,
                "windowEnd": count.window.end,
            });
            PulsarOutput {
                key: Some(key),
                payload: payload.to_string().into_bytes(),
                properties: Vec::new(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use serde_json::Value;
    use streaming_core::{Key, OpFactory, OpRef, Registry, Window, datum};

    #[test]
    fn a_count_becomes_a_keyed_message_with_a_json_payload() {
        let mut registry = Registry::<TokioProviders>::standard();
        crate::register(&mut registry);
        let Some((_, OpFactory::Map(factory))) = registry.get(&OpRef::new(CountToPulsar::OP).unwrap()) else {
            panic!("{} is not a map", CountToPulsar::OP);
        };
        let map = factory(&Value::Null).unwrap();
        let count = Windowed {
            window: Window::new(1_000, 2_000),
            key: Key::encode(&"alice".to_owned()).unwrap(),
            value: 7_u64,
        };
        let mut out = Vec::new();
        map.apply(&datum(count), &mut out).unwrap();
        let message = out[0].downcast_ref::<PulsarOutput>().unwrap();
        assert_eq!(message.key.as_deref(), Some("alice"));
        let payload: Value = serde_json::from_slice(&message.payload).unwrap();
        assert_eq!(
            payload,
            json!({"key": "alice", "count": 7, "windowStart": 1_000, "windowEnd": 2_000})
        );
    }
}
