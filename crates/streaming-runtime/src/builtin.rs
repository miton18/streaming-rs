//! Connectors shipped with the runtime.
//!
//! | reference | kind | input | configuration |
//! |---|---|---|---|
//! | `std/kv-sink@1` | sink | `serde_json::Value` | `{"retain": 500}` |

use moonpool_core::Providers;
use serde_json::Value;
use std::collections::BTreeSet;
use streaming_core::{
    Batch, Context, DynKv, GraphKey, OpError, Operator, Registry, Sink, SinkPipe, Timestamp, control::OutputRow, keys,
};

/// Keeps the most recent elements as JSON rows readable through the API.
///
/// Each row lives under its emit id, so writing an element again replaces it:
/// the sink is idempotent by construction. Only the most recent rows are kept;
/// this sink is meant for inspecting results, not for archiving them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Operator)]
#[operator("std/kv-sink@1")]
pub struct KvSink {
    /// How many of the most recent rows are kept.
    #[param(default = 500, min = 1)]
    pub retain: usize,
}

impl Default for KvSink {
    fn default() -> Self { Self { retain: 500 } }
}

impl KvSink {
    /// Write a batch and forget the rows beyond `retain`. `known` is what the
    /// hash holds, read on the first write.
    async fn write(
        &self,
        kv: &DynKv,
        key: &str,
        known: &mut Option<BTreeSet<(Timestamp, String)>>,
        batch: &Batch<Value>,
    ) -> Result<(), OpError> {
        let failed = |error: &dyn std::fmt::Display| OpError::Failed(error.to_string());
        let known = match known {
            Some(known) => known,
            None => {
                let existing: Vec<(String, OutputRow)> = kv.hgetall_json(key).await.map_err(|e| failed(&e))?;
                known.insert(existing.into_iter().map(|(id, row)| (row.ts, id)).collect())
            }
        };
        let mut entries = Vec::with_capacity(batch.len());
        for element in batch.iter() {
            let row = OutputRow {
                id: element.id.to_string(),
                ts: element.ts,
                key: element.key.map(streaming_core::Key::display_lossy),
                value: element.value.clone(),
            };
            entries.push((row.id.clone(), serde_json::to_vec(&row).map_err(|e| failed(&e))?));
        }
        kv.hset(key, entries).await.map_err(|e| failed(&e))?;

        known.extend(batch.iter().map(|element| (element.ts, element.id.to_string())));
        let mut expired = Vec::new();
        while known.len() > self.retain.max(1) {
            match known.pop_first() {
                Some((_, id)) => expired.push(id),
                None => break,
            }
        }
        if !expired.is_empty() {
            kv.hdel(key, expired).await.map_err(|e| failed(&e))?;
        }
        Ok(())
    }
}

impl Sink<Value> for KvSink {
    async fn start<P: Providers>(&self, ctx: &Context<P>, mut input: SinkPipe<Value>) -> Result<(), OpError> {
        let graph = GraphKey::new(ctx.tenant.clone(), ctx.graph.clone());
        let key = keys::graph_output(&graph, &ctx.node);
        let mut known = None;
        while let Some(batch) = input.next().await {
            // A failure ends the sink: the engine starts it again with the
            // same batch, and it reads again what the hash holds.
            self.write(&ctx.kv, &key, &mut known, &batch).await?;
            batch.done();
        }
        Ok(())
    }
}

/// Register the runtime's connectors.
pub fn register<P: Providers>(registry: &mut Registry<P>) { registry.sink::<Value, KvSink>(); }

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;
    use std::sync::Arc;
    use streaming_core::{
        Emit, EmitId, GraphId, Kv, MemKv, NodeId, OpContext, OpRef, Origin, Position, SinkConnector, SliceId, TenantId,
        clock::{NoSecrets, SystemClock},
        datum,
        registry::OpFactory,
    };

    fn sink(kv: &Arc<MemKv>, config: Value) -> Arc<dyn SinkConnector> {
        let mut registry = Registry::<TokioProviders>::empty();
        register(&mut registry);
        let op = OpRef::new("std/kv-sink@1").unwrap();
        let Some((_, OpFactory::Sink(factory))) = registry.get(&op) else {
            panic!("std/kv-sink@1 is not registered as a sink");
        };
        let context = OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(SystemClock),
            secrets: Arc::new(NoSecrets),
            kv: kv.clone(),
            shared: Arc::default(),
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
