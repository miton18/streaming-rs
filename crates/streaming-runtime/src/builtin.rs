//! Connectors shipped with the runtime.
//!
//! | reference | kind | input | configuration |
//! |---|---|---|---|
//! | `std/kv-sink@1` | sink | `serde_json::Value` | `{"retain": 500}` |

use async_trait::async_trait;
use moonpool_core::Providers;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};
use streaming_core::{
    DynKv, Emit, GraphKey, OpError, Operator, Registry, SinkConnector, SinkError, Timestamp, TypeTag,
    control::OutputRow, keys, ops::downcast,
};

fn default_retain() -> usize {
    500
}

/// `std/kv-sink@1`: keeps the most recent elements of a node as JSON rows
/// readable through the HTTP API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KvSink {
    /// Number of most recent rows kept (at least one).
    #[serde(default = "default_retain")]
    pub retain: usize,
}

impl Default for KvSink {
    fn default() -> Self {
        Self {
            retain: default_retain(),
        }
    }
}

impl Operator for KvSink {
    const OP: &'static str = "std/kv-sink@1";
}

/// Writes elements as JSON rows into a KV hash, where the HTTP API reads them.
///
/// Each row lives under its emit id, so writing an element again replaces it:
/// the sink is idempotent by construction. Only the most recent rows are kept;
/// this sink is meant for inspecting results, not for archiving them.
struct KvSinkConnector {
    kv: DynKv,
    key: String,
    retain: usize,
    known: Mutex<Option<BTreeSet<(Timestamp, String)>>>,
}

#[async_trait]
impl SinkConnector for KvSinkConnector {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        let err = |error: &dyn std::fmt::Display| SinkError(error.to_string());
        if self.known.lock().is_none() {
            let existing: Vec<(String, OutputRow)> = self.kv.hgetall_json(&self.key).await.map_err(|e| err(&e))?;
            self.known
                .lock()
                .get_or_insert_with(|| existing.into_iter().map(|(id, row)| (row.ts, id)).collect());
        }
        let mut entries = Vec::with_capacity(batch.len());
        for emit in batch {
            let value = downcast::<Value>(&emit.value).map_err(|e| err(&e))?;
            let row = OutputRow {
                id: emit.id.to_string(),
                ts: emit.ts,
                key: emit.key.as_ref().map(streaming_core::Key::display_lossy),
                value: value.clone(),
            };
            entries.push((row.id.clone(), serde_json::to_vec(&row).map_err(|e| err(&e))?));
        }
        self.kv.hset(&self.key, entries).await.map_err(|e| err(&e))?;

        let expired: Vec<String> = {
            let mut guard = self.known.lock();
            let known = guard.get_or_insert_with(BTreeSet::new);
            known.extend(batch.iter().map(|emit| (emit.ts, emit.id.to_string())));
            let mut expired = Vec::new();
            while known.len() > self.retain {
                match known.pop_first() {
                    Some((_, id)) => expired.push(id),
                    None => break,
                }
            }
            expired
        };
        if !expired.is_empty() {
            self.kv.hdel(&self.key, expired).await.map_err(|e| err(&e))?;
        }
        Ok(())
    }
}

/// Register the runtime's connectors. `kv` is where `std/kv-sink@1` writes.
pub fn register<P: Providers>(registry: &mut Registry<P>, kv: DynKv) {
    registry.register_sink(
        KvSink::OP,
        "Keeps the most recent elements as JSON rows readable through the API.",
        TypeTag::of::<Value>(),
        move |context, config| {
            let config: KvSink = if config.is_null() {
                KvSink::default()
            } else {
                serde_json::from_value(config.clone()).map_err(|e| OpError::Config(e.to_string()))?
            };
            let graph = GraphKey::new(context.tenant.clone(), context.graph.clone());
            Ok(Arc::new(KvSinkConnector {
                kv: kv.clone(),
                key: keys::graph_output(&graph, &context.node),
                retain: config.retain.max(1),
                known: Mutex::new(None),
            }) as Arc<dyn SinkConnector>)
        },
    );
    registry.with_params(
        KvSink::OP,
        [
            streaming_core::ParamSpec::new("retain", "integer", "How many of the most recent rows are kept.")
                .default(500),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use streaming_core::{
        EmitId, GraphId, Kv, MemKv, NodeId, OpContext, OpRef, Origin, Position, SliceId, TenantId,
        clock::{NoSecrets, SystemClock},
        datum,
        registry::OpFactory,
    };

    fn sink(kv: &Arc<MemKv>, config: Value) -> Arc<dyn SinkConnector> {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry, kv.clone());
        let op = OpRef::new("std/kv-sink@1").unwrap();
        let Some((_, OpFactory::Sink(factory))) = registry.get(&op) else {
            panic!("std/kv-sink@1 is not registered as a sink");
        };
        let context = OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(NoSecrets),
            tenant: TenantId::new("acme").unwrap(),
            graph: GraphId::new("g").unwrap(),
            node: NodeId::new("out").unwrap(),
        };
        factory(&context, &config).unwrap()
    }

    fn emits(range: std::ops::Range<u64>) -> Vec<Emit> {
        let slice = SliceId::Source {
            node: NodeId::new("n").unwrap(),
            partition: 0,
        };
        range
            .map(|n| {
                let origin = Origin::source(Position(0, n, 0));
                Emit {
                    id: EmitId::new(&slice, &origin),
                    origin,
                    ts: n as Timestamp,
                    key: None,
                    value: datum(Value::from(n)),
                }
            })
            .collect()
    }

    const ROWS: &str = "t:acme:g:g:out:out";

    async fn rows(kv: &dyn Kv) -> Vec<i64> {
        let rows: Vec<(String, OutputRow)> = kv.hgetall_json(ROWS).await.unwrap();
        let mut ts: Vec<i64> = rows.into_iter().map(|(_, row)| row.ts).collect();
        ts.sort_unstable();
        ts
    }

    #[tokio::test]
    async fn the_most_recent_rows_are_kept() {
        let graph = GraphKey::new(TenantId::new("acme").unwrap(), GraphId::new("g").unwrap());
        assert_eq!(keys::graph_output(&graph, &NodeId::new("out").unwrap()), ROWS);
        let kv = Arc::new(MemKv::new());
        let out = sink(&kv, serde_json::json!({"retain": 3}));
        out.write(&emits(0..3)).await.unwrap();
        assert_eq!(rows(&*kv).await, [0, 1, 2]);
        out.write(&emits(3..4)).await.unwrap();
        assert_eq!(rows(&*kv).await, [1, 2, 3]);
        // Writing an element again replaces its row.
        out.write(&emits(3..4)).await.unwrap();
        assert_eq!(rows(&*kv).await, [1, 2, 3]);
        // A new instance starts from the rows already there.
        let again = sink(&kv, serde_json::json!({"retain": 3}));
        again.write(&emits(4..5)).await.unwrap();
        assert_eq!(rows(&*kv).await, [2, 3, 4]);
        let kv: &dyn Kv = &*kv;
        let row: OutputRow = kv
            .hget_json(ROWS, &emits(4..5)[0].id.to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.value, Value::from(4));
    }

    #[tokio::test]
    async fn five_hundred_rows_by_default() {
        for config in [Value::Null, serde_json::json!({})] {
            let kv = Arc::new(MemKv::new());
            let out = sink(&kv, config);
            out.write(&emits(0..501)).await.unwrap();
            let kept = rows(&*kv).await;
            assert_eq!((kept.len(), kept[0]), (500, 1));
        }
    }
}
