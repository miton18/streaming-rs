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
