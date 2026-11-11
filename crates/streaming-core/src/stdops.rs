//! Type-agnostic operators shipped with the framework.
//!
//! | reference | kind | configuration |
//! |---|---|---|
//! | `std/tumbling@1` | window | `{"size": "5m", "allowed_lateness": "0s"}` |
//! | `std/sliding@1` | window | `{"size": "5m", "slide": "1m", "allowed_lateness": "0s"}` |
//! | `std/count@1` | aggregate | none; emits `Windowed<u64>` |
//! | `std/identity@1` | map | none |

use std::sync::Arc;
use std::time::Duration;

use moonpool_core::Providers;
use serde::Deserialize;
use serde_json::Value;

use crate::data::{Datum, Key, Windowed, datum};
use crate::ops::{DynAcc, DynAggregator, DynMap, OpError, WindowAssigner};
use crate::registry::{OpDescriptor, OpFactory, OpKind, Registry, TypeTag};
use crate::time::{Timestamp, Window, duration_str};

fn zero() -> Duration {
    Duration::ZERO
}

/// Fixed-size, non-overlapping windows aligned on the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct Tumbling {
    /// Window length.
    #[serde(with = "duration_str")]
    pub size: Duration,
    /// How long after its end a window still accepts records.
    #[serde(with = "duration_str", default = "zero")]
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

    fn allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }
}

/// Fixed-size windows starting every `slide`, aligned on the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct Sliding {
    /// Window length.
    #[serde(with = "duration_str")]
    pub size: Duration,
    /// Distance between two window starts.
    #[serde(with = "duration_str")]
    pub slide: Duration,
    /// How long after its end a window still accepts records.
    #[serde(with = "duration_str", default = "zero")]
    pub allowed_lateness: Duration,
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
#[derive(Debug, Clone, Copy, Default)]
pub struct Count;

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
        let count = acc
            .downcast_mut::<u64>()
            .ok_or(OpError::Type { expected: "u64" })?;
        *count += 1;
        Ok(())
    }

    fn merge(&self, into: &mut DynAcc, other: &DynAcc) -> Result<(), OpError> {
        let other = Self::get(other)?;
        let count = into
            .downcast_mut::<u64>()
            .ok_or(OpError::Type { expected: "u64" })?;
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

struct Identity;

impl DynMap for Identity {
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        out.push(input.clone());
        Ok(())
    }
}

/// Register the `std/*` operators listed in the module documentation.
pub fn register<P: Providers>(registry: &mut Registry<P>) {
    registry.register_window(
        "std/tumbling@1",
        "Fixed-size, non-overlapping event-time windows.",
        |config: Tumbling| {
            check_positive("size", config.size)?;
            Ok(config)
        },
    );
    registry.register_window(
        "std/sliding@1",
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
        "std/count@1",
        "Counts the elements of each key in each window.",
        TypeTag::Any,
        TypeTag::of::<Windowed<u64>>(),
        |_config: &Value| Ok(Arc::new(Count) as Arc<dyn DynAggregator>),
    );
    registry.register(
        OpDescriptor {
            op: crate::ids::OpRef::new("std/identity@1").unwrap_or_else(|e| panic!("{e}")),
            kind: OpKind::Map,
            input: TypeTag::Any,
            output: TypeTag::Any,
            description: "Forwards every element unchanged.".to_owned(),
        },
        OpFactory::Map(Arc::new(|_config: &Value| {
            Ok(Arc::new(Identity) as Arc<dyn DynMap>)
        })),
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
        assert_eq!(
            out,
            vec![Window::new(300_000, 600_000), Window::new(-300_000, 0)]
        );
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
        assert_eq!(
            out,
            vec![Window::new(10_000, 20_000), Window::new(5_000, 15_000)]
        );
        assert!(out.iter().all(|w| w.contains(12_000)));
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
