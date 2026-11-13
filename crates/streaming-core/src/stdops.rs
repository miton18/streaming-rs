//! Type-agnostic operators shipped with the framework.
//!
//! | reference | kind | configuration |
//! |---|---|---|
//! | `std/tumbling@1` | window | `{"size": "5m", "allowedLateness": "0s"}` |
//! | `std/sliding@1` | window | `{"size": "5m", "slide": "1m", "allowedLateness": "0s"}` |
//! | `std/count@1` | aggregate | none; emits `Windowed<u64>` |
//! | `std/identity@1` | map | none |
//! | `std/union@1` | map | none; any number of inputs of one type |
//! | `std/flatten@1` | map | none; a `Vec<T>` given back one element at a time |
//! | `std/branch@1` | map | `{"branch": 0}`, fixed by the code; after a split, registered by the builder for its type only |
//!
//! [`json_decoder!`](crate::json_decoder) declares a decoder to a type of
//! the application's.

use crate::{
    Operator,
    data::Windowed,
    ops::{OpError, WindowAssigner},
    pipe::{Keyed, Opaque, Pipe, Routed, Transformer, Windows},
    registry::Registry,
    time::{Timestamp, Window},
};
use moonpool_core::Providers;
use std::{any::Any, time::Duration};

/// Fixed-size, non-overlapping event-time windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Operator)]
#[operator("std/tumbling@1")]
pub struct Tumbling {
    /// Length of each window.
    #[param(min = "1ms")]
    pub size: Duration,
    /// How long after its end a window still accepts records.
    #[param(default = "0s")]
    pub allowed_lateness: Duration,
}

impl Tumbling {
    /// Tumbling windows of the given size, without allowed lateness.
    #[must_use]
    pub fn of(size: Duration) -> Self {
        Self {
            size,
            allowed_lateness: Duration::ZERO,
        }
    }
}

impl WindowAssigner for Tumbling {
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>) {
        let size = self.size.as_millis() as i64;
        let start = ts.div_euclid(size) * size;
        out.push(Window::new(start, start + size));
    }

    fn allowed_lateness(&self) -> Duration { self.allowed_lateness }
}

impl<T: Any + Send + Sync> Transformer<Keyed<T>> for Tumbling {
    type Out = Windows<T>;

    fn transform(&self, input: Pipe<Keyed<T>>) -> Pipe<Windows<T>> { input.window_with(*self) }
}

/// Fixed-size event-time windows starting every `slide`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Operator)]
#[operator("std/sliding@1", check = slide_within_size)]
pub struct Sliding {
    /// Length of each window.
    #[param(min = "1ms")]
    pub size: Duration,
    /// A window starts every `slide`.
    #[param(min = "1ms")]
    pub slide: Duration,
    /// How long after its end a window still accepts records.
    #[param(default = "0s")]
    pub allowed_lateness: Duration,
}

fn slide_within_size(sliding: &Sliding) -> Result<(), OpError> {
    if sliding.slide > sliding.size {
        return Err(OpError::Config("`slide` must not exceed `size`".into()));
    }
    Ok(())
}

impl Sliding {
    /// Windows of length `size` starting every `slide`, without allowed
    /// lateness.
    #[must_use]
    pub fn of(size: Duration, slide: Duration) -> Self {
        Self {
            size,
            slide,
            allowed_lateness: Duration::ZERO,
        }
    }
}

impl WindowAssigner for Sliding {
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>) {
        let size = self.size.as_millis() as i64;
        let slide = self.slide.as_millis() as i64;
        let mut start = ts.div_euclid(slide) * slide;
        while start > ts - size {
            out.push(Window::new(start, start + size));
            start -= slide;
        }
    }

    fn allowed_lateness(&self) -> Duration { self.allowed_lateness }
}

impl<T: Any + Send + Sync> Transformer<Keyed<T>> for Sliding {
    type Out = Windows<T>;

    fn transform(&self, input: Pipe<Keyed<T>>) -> Pipe<Windows<T>> { input.window_with(*self) }
}

/// Counts the elements of each key in each window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/count@1")]
pub struct Count;

impl<T: Any + Send + Sync> Transformer<Windows<T>> for Count {
    type Out = Windowed<u64>;

    fn transform(&self, input: Pipe<Windows<T>>) -> Pipe<Windowed<u64>> {
        input.aggregate(
            || 0_u64,                       // the state of one key in one window
            |count, _element| *count += 1,  // an element comes
            |count, other| *count += other, // two partitions each held a part of the window
        )
    }
}

/// Forwards every element unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/identity@1")]
pub struct Identity;

impl<T: Any + Send + Sync> Transformer<T> for Identity {
    type Out = T;

    fn transform(&self, input: Pipe<T>) -> Pipe<T> { input }
}

/// Brings together what its inputs carry, all of one type, into one flow.
/// [`GraphBuilder::union`](crate::GraphBuilder::union) connects them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/union@1")]
pub struct Union;

impl<T: Any + Send + Sync> Transformer<T> for Union {
    type Out = T;

    fn transform(&self, input: Pipe<T>) -> Pipe<T> { input }
}

/// Gives back the elements of a `Vec<T>` one at a time, in order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/flatten@1")]
pub struct Flatten;

impl<T: Any + Clone + Send + Sync> Transformer<Vec<T>> for Flatten {
    type Out = T;

    fn transform(&self, input: Pipe<Vec<T>>) -> Pipe<T> {
        input.flat_map(|elements, out| out.extend(elements.iter().cloned()))
    }
}

/// Keeps what a split routed to one of its branches.
/// [`GraphBuilder::split`](crate::GraphBuilder::split) adds one per branch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Operator)]
#[operator("std/branch@1")]
pub struct Branch {
    /// The index of the branch, in the order the branches were named.
    pub branch: usize,
}

impl<T: Any + Send + Sync> Transformer<Routed<T>> for Branch {
    type Out = T;

    fn transform(&self, input: Pipe<Routed<T>>) -> Pipe<T> { input.branch(self.branch) }
}

/// Declare an operator decoding JSON, bytes or text, into a type of the
/// application's, dropping what does not decode. The reference is the
/// application's: two decoders to two types are two operators.
///
/// ```ignore
/// json_decoder! {
///     /// Decodes a JSON event.
///     pub struct Decode("demo/decode@1") -> Event;
/// }
/// let decoded = graph.filter_map(events, "decode", Decode);
/// ```
#[macro_export]
macro_rules! json_decoder {
    ($(#[$meta:meta])* $vis:vis struct $name:ident($op:literal) -> $out:ty;) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, $crate::Operator)]
        #[operator($op)]
        $vis struct $name;

        impl $crate::StatelessFilterMap<::std::vec::Vec<u8>> for $name {
            type Out = $out;

            fn apply(&self, bytes: &::std::vec::Vec<u8>) -> ::std::option::Option<$out> {
                $crate::__private_json::from_slice(bytes).ok()
            }
        }

        impl $crate::StatelessFilterMap<::std::string::String> for $name {
            type Out = $out;

            fn apply(&self, text: &::std::string::String) -> ::std::option::Option<$out> {
                $crate::__private_json::from_str(text).ok()
            }
        }
    };
}

/// Register the `std/*` operators listed in the module documentation, each
/// for any type of element.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    let second = Duration::from_secs(1);
    let registered = [
        registry.transformer::<Keyed<Opaque>, _>(&Tumbling::of(second)).err(),
        registry
            .transformer::<Keyed<Opaque>, _>(&Sliding::of(second, second))
            .err(),
        registry.transformer::<Windows<Opaque>, _>(&Count).err(),
        registry.transformer::<Opaque, _>(&Identity).err(),
        registry.transformer::<Opaque, _>(&Union).err(),
    ];
    debug_assert!(
        registered.iter().all(Option::is_none),
        "a stock operator is not one step"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tumbling_aligns_on_epoch() {
        let assigner = Tumbling::of(Duration::from_secs(300));
        let mut out = Vec::new();
        assigner.assign(301_000, &mut out);
        assigner.assign(-1, &mut out);
        assert_eq!(out, vec![Window::new(300_000, 600_000), Window::new(-300_000, 0)]);
    }

    #[test]
    fn sliding_assigns_every_overlapping_window() {
        let assigner = Sliding {
            size: Duration::from_secs(10),
            slide: Duration::from_secs(5),
            allowed_lateness: Duration::ZERO,
        };
        let mut out = Vec::new();
        assigner.assign(12_000, &mut out);
        assert_eq!(out, vec![Window::new(10_000, 20_000), Window::new(5_000, 15_000)]);
        assert!(out.iter().all(|w| w.contains(12_000)));

        // On a window boundary, the window ending there is not included.
        out.clear();
        assigner.assign(10_000, &mut out);
        assert_eq!(out, vec![Window::new(10_000, 20_000), Window::new(5_000, 15_000)]);
    }

    #[test]
    fn sliding_windows_before_the_epoch() {
        let assigner = Sliding {
            size: Duration::from_secs(10),
            slide: Duration::from_secs(5),
            allowed_lateness: Duration::ZERO,
        };
        let mut out = Vec::new();
        assigner.assign(-3_000, &mut out);
        assert_eq!(out, vec![Window::new(-5_000, 5_000), Window::new(-10_000, 0)]);
    }

    #[test]
    fn allowed_lateness_is_configured_or_zero() {
        let registry = crate::registry::Registry::<moonpool_core::TokioProviders>::standard();
        let build = |op: &str, config: serde_json::Value| match registry.get(&crate::ids::OpRef::new(op).unwrap()) {
            Some((_, crate::registry::OpFactory::Window(factory))) => factory(&config).unwrap(),
            _ => panic!("{op} is not a window"),
        };
        let tumbling = build("std/tumbling@1", serde_json::json!({"size": "5m"}));
        assert_eq!(tumbling.allowed_lateness(), Duration::ZERO);
        let tumbling = build(
            "std/tumbling@1",
            serde_json::json!({"size": "5m", "allowedLateness": "2s"}),
        );
        assert_eq!(tumbling.allowed_lateness(), Duration::from_secs(2));
        let sliding = build("std/sliding@1", serde_json::json!({"size": "5m", "slide": "1m"}));
        assert_eq!(sliding.allowed_lateness(), Duration::ZERO);
        let sliding = build(
            "std/sliding@1",
            serde_json::json!({"size": "5m", "slide": "1m", "allowedLateness": "3s"}),
        );
        assert_eq!(sliding.allowed_lateness(), Duration::from_secs(3));

        // An assigner that does not say accepts nothing late.
        struct Plain;
        impl WindowAssigner for Plain {
            fn assign(&self, _: Timestamp, _: &mut Vec<Window>) {}
        }
        assert_eq!(Plain.allowed_lateness(), Duration::ZERO);
    }

    #[test]
    fn window_configs_are_validated() {
        let registry = crate::registry::Registry::<moonpool_core::TokioProviders>::standard();
        let check = |op: &str, config: serde_json::Value| match registry.get(&crate::ids::OpRef::new(op).unwrap()) {
            Some((_, crate::registry::OpFactory::Window(factory))) => factory(&config).is_ok(),
            _ => panic!("{op} is not a window"),
        };
        let sliding = |size: &str, slide: &str| serde_json::json!({"size": size, "slide": slide});
        assert!(check("std/sliding@1", sliding("10s", "5s")));
        assert!(check("std/sliding@1", sliding("10s", "10s")));
        assert!(!check("std/sliding@1", sliding("10s", "11s")));
        assert!(!check("std/sliding@1", sliding("10s", "0ms")));
        assert!(!check("std/tumbling@1", serde_json::json!({"size": "0ms"})));
    }

    #[test]
    fn count_merges_and_round_trips() {
        let registry = crate::registry::Registry::<moonpool_core::TokioProviders>::standard();
        let op = crate::ids::OpRef::new("std/count@1").unwrap();
        let Some((descriptor, crate::registry::OpFactory::Aggregate(factory))) = registry.get(&op) else {
            panic!("std/count@1 is not an aggregate");
        };
        // Registered for any type of element, it counts whatever comes.
        assert_eq!(descriptor.input, crate::TypeTag::Any);
        assert_eq!(descriptor.output, crate::TypeTag::of::<Windowed<u64>>());
        assert_eq!(
            descriptor.description,
            "Counts the elements of each key in each window."
        );
        let count = factory(&serde_json::Value::Null).unwrap();
        let mut a = count.init();
        let mut b = count.init();
        for _ in 0..3 {
            count.add(&mut a, &crate::datum("x")).unwrap();
        }
        count.add(&mut b, &crate::datum(7_u8)).unwrap();
        count.merge(&mut a, &b).unwrap();
        let bytes = count.encode(&a).unwrap();
        assert_eq!(bytes, postcard::to_allocvec(&4_u64).unwrap());
        let restored = count.decode(&bytes).unwrap();
        let key = crate::Key::encode(&"k".to_owned()).unwrap();
        let out = count.finish(&restored, Window::new(0, 10), &key).unwrap();
        assert_eq!(out.downcast_ref::<Windowed<u64>>().unwrap().value, 4);
    }

    #[test]
    fn the_stock_operators_describe_themselves() {
        let catalog = crate::registry::Registry::<moonpool_core::TokioProviders>::standard().catalog();
        let get = |op: &str| catalog.get(&crate::ids::OpRef::new(op).unwrap()).unwrap().clone();
        let tumbling = get("std/tumbling@1");
        assert_eq!(tumbling.kind, crate::OpKind::Window);
        assert_eq!(tumbling.description, "Fixed-size, non-overlapping event-time windows.");
        let names: Vec<(&str, bool)> = tumbling.params.iter().map(|p| (p.name.as_str(), p.required)).collect();
        assert_eq!(names, [("size", true), ("allowedLateness", false)]);
        assert_eq!(tumbling.params[1].default, Some(serde_json::json!("0s")));
        let sliding = get("std/sliding@1");
        let names: Vec<&str> = sliding.params.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["size", "slide", "allowedLateness"]);
        let identity = get("std/identity@1");
        assert_eq!(
            (identity.kind, identity.input, identity.output),
            (crate::OpKind::Map, crate::TypeTag::Any, crate::TypeTag::Any)
        );
        assert!(identity.signatures.is_empty());
    }
}
