//! `std/count-to-gts@1`: the glue between a windowed count and this sink.

use crate::Gts;
use streaming_core::{Operator, Pipe, Transformer, Windowed};

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

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use serde_json::json;
    use streaming_core::{Key, OpFactory, OpRef, Registry, Window, datum};

    #[test]
    fn a_count_becomes_a_point_labelled_with_its_key() {
        let mut registry = Registry::<TokioProviders>::standard();
        crate::register(&mut registry);
        let Some((_, OpFactory::Map(factory))) = registry.get(&OpRef::new(CountToGts::OP).unwrap()) else {
            panic!("{} is not a map", CountToGts::OP);
        };
        for (config, label) in [
            (json!({"class": "events.count"}), "key"),
            (json!({"class": "events.count", "label": "user"}), "user"),
        ] {
            let map = factory(&config).unwrap();
            let count = Windowed {
                window: Window::new(1_000, 2_000),
                key: Key::encode(&"alice".to_owned()).unwrap(),
                value: 7_u64,
            };
            let mut out = Vec::new();
            map.apply(&datum(count), &mut out).unwrap();
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
