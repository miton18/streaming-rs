//! Operators every application has, beyond the connectors.
//!
//! They are the glue of a common pipeline (read, key, window, count, write).
//!
//! | reference | kind | types | configuration |
//! |---|---|---|---|
//! | `std/count-to-json@1` | map | `Windowed<u64>` -> JSON | |
//!
//! The glue that belongs to a connector lives in its crate, next to it, and
//! is registered with it: `std/count-to-gts@1` in `streaming::warp10` (feature
//! `warp10`), `std/pulsar-key@1` and `std/count-to-pulsar@1` in
//! `streaming::pulsar` (feature `pulsar`).

use moonpool_core::Providers;
use serde_json::{Value, json};
use streaming_core::{Operator, Pipe, Registry, Transformer, Windowed};

/// Turns a windowed count into a JSON object {key, count, windowStart,
/// windowEnd}.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/count-to-json@1")]
pub struct CountToJson;

impl Transformer<Windowed<u64>> for CountToJson {
    type Out = Value;

    fn transform(&self, input: Pipe<Windowed<u64>>) -> Pipe<Value> { input.map(count_as_json) }
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
    registry
        .transformer(&CountToJson)
        .expect("a stock operator is one kind of operator");
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
}
