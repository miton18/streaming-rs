//! Type-agnostic operators shipped with the framework.
//!
//! | reference | kind | configuration |
//! |---|---|---|
//! | `std/tumbling@1` | window | `{"size": "5m", "allowedLateness": "0s"}` |
//! | `std/sliding@1` | window | `{"size": "5m", "slide": "1m", "allowedLateness": "0s"}` |
//! | `std/count@1` | aggregate | none; emits `Windowed<u64>` |
//! | `std/identity@1` | map | none |

use crate::{
    builder::Operator,
    data::{Datum, Key, Windowed, datum},
    ops::{DynAcc, DynAggregator, DynMap, OpError, WindowAssigner},
    registry::{OpDescriptor, OpFactory, OpKind, Registry, TypeTag},
    time::{Timestamp, Window, duration_str},
};
use moonpool_core::Providers;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

fn zero() -> Duration {
    Duration::ZERO
}

/// Fixed-size, non-overlapping windows aligned on the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tumbling {
    /// Window length.
    #[serde(with = "duration_str")]
    pub size: Duration,
    /// How long after its end a window still accepts records.
    #[serde(with = "duration_str", default = "zero", skip_serializing_if = "Duration::is_zero")]
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

impl Operator for Tumbling {
    const OP: &'static str = "std/tumbling@1";
}

impl WindowAssigner for Tumbling {
    fn assign(&self, ts: Timestamp, out: &mut Vec<Window>) {
        let size = self.size.as_millis() as i64;
        let start = ts.div_euclid(size) * size;
        out.push(Window::new(start, start + size));
    }

    fn allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }
}

/// Fixed-size windows starting every `slide`, aligned on the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sliding {
    /// Window length.
    #[serde(with = "duration_str")]
    pub size: Duration,
    /// Distance between two window starts.
    #[serde(with = "duration_str")]
    pub slide: Duration,
    /// How long after its end a window still accepts records.
    #[serde(with = "duration_str", default = "zero", skip_serializing_if = "Duration::is_zero")]
    pub allowed_lateness: Duration,
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

impl Operator for Sliding {
    const OP: &'static str = "std/sliding@1";
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

    fn allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }
}

fn check_positive(what: &str, duration: Duration) -> Result<(), OpError> {
    if duration.as_millis() == 0 {
        return Err(OpError::Config(format!("`{what}` must be at least 1ms")));
    }
    Ok(())
}

/// Counts elements of any type. Emits `Windowed<u64>`.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Count;

impl Operator for Count {
    const OP: &'static str = "std/count@1";
}

impl Count {
    fn get(acc: &DynAcc) -> Result<u64, OpError> {
        acc.downcast_ref::<u64>()
            .copied()
            .ok_or(OpError::Type { expected: "u64" })
    }
}

impl DynAggregator for Count {
    fn init(&self) -> DynAcc {
        Box::new(0_u64)
    }

    fn add(&self, acc: &mut DynAcc, _item: &Datum) -> Result<(), OpError> {
        let count = acc.downcast_mut::<u64>().ok_or(OpError::Type { expected: "u64" })?;
        *count += 1;
        Ok(())
    }

    fn merge(&self, into: &mut DynAcc, other: &DynAcc) -> Result<(), OpError> {
        let other = Self::get(other)?;
        let count = into.downcast_mut::<u64>().ok_or(OpError::Type { expected: "u64" })?;
        *count += other;
        Ok(())
    }

    fn finish(&self, acc: &DynAcc, window: Window, key: &Key) -> Result<Datum, OpError> {
        Ok(datum(Windowed {
            window,
            key: key.clone(),
            value: Self::get(acc)?,
        }))
    }

    fn encode(&self, acc: &DynAcc) -> Result<Vec<u8>, OpError> {
        postcard::to_allocvec(&Self::get(acc)?).map_err(|e| OpError::Failed(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<DynAcc, OpError> {
        postcard::from_bytes::<u64>(bytes)
            .map(|count| Box::new(count) as DynAcc)
            .map_err(|e| OpError::Failed(format!("cannot decode count: {e}")))
    }
}

/// Forwards every element unchanged.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Identity;

impl Operator for Identity {
    const OP: &'static str = "std/identity@1";
}

impl DynMap for Identity {
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        out.push(input.clone());
        Ok(())
    }
}

/// Register the `std/*` operators listed in the module documentation.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    registry.register_window(
        Tumbling::OP,
        "Fixed-size, non-overlapping event-time windows.",
        |config: Tumbling| {
            check_positive("size", config.size)?;
            Ok(config)
        },
    );
    registry.register_window(
        Sliding::OP,
        "Fixed-size event-time windows starting every `slide`.",
        |config: Sliding| {
            check_positive("size", config.size)?;
            check_positive("slide", config.slide)?;
            if config.slide > config.size {
                return Err(OpError::Config("`slide` must not exceed `size`".into()));
            }
            Ok(config)
        },
    );
    registry.register_dyn_aggregator(
        Count::OP,
        "Counts the elements of each key in each window.",
        TypeTag::Any,
        TypeTag::of::<Windowed<u64>>(),
        |_config: &Value| Ok(Arc::new(Count) as Arc<dyn DynAggregator>),
    );
    registry.register(
        OpDescriptor {
            op: crate::ids::OpRef::new(Identity::OP).unwrap_or_else(|e| panic!("{e}")),
            kind: OpKind::Map,
            input: TypeTag::Any,
            output: TypeTag::Any,
            description: "Forwards every element unchanged.".to_owned(),
            params: Vec::new(),
        },
        OpFactory::Map(Arc::new(|_config: &Value| Ok(Arc::new(Identity) as Arc<dyn DynMap>))),
    );
    let lateness = || {
        crate::ParamSpec::new(
            "allowedLateness",
            "duration",
            "How long after its end a window still accepts records.",
        )
        .default("0s")
    };
    registry.with_params(
        Tumbling::OP,
        [
            crate::ParamSpec::new("size", "duration", "Length of each window.").required(),
            lateness(),
        ],
    );
    registry.with_params(
        Sliding::OP,
        [
            crate::ParamSpec::new("size", "duration", "Length of each window.").required(),
            crate::ParamSpec::new("slide", "duration", "A window starts every `slide`.").required(),
            lateness(),
        ],
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
        let count = Count;
        let mut a = count.init();
        let mut b = count.init();
        for _ in 0..3 {
            count.add(&mut a, &datum("x")).unwrap();
        }
        count.add(&mut b, &datum(7_u8)).unwrap();
        count.merge(&mut a, &b).unwrap();
        let restored = count.decode(&count.encode(&a).unwrap()).unwrap();
        let key = Key::encode(&"k".to_owned()).unwrap();
        let out = count.finish(&restored, Window::new(0, 10), &key).unwrap();
        assert_eq!(out.downcast_ref::<Windowed<u64>>().unwrap().value, 4);
    }
}
