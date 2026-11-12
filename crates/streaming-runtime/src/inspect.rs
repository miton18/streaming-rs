//! Read-only views of what a slice has durably committed, for tooling and
//! tests. Nothing here is needed by a worker.

use crate::state::{self, NO_WATERMARK, StateError};
use streaming_core::{Kv, KvError, Position, SliceKey, Timestamp};

/// The last commit of a slice, as found in its log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceHead {
    /// Last committed epoch.
    pub epoch: u64,
    /// Source position reached by that epoch.
    pub position: Option<Position>,
    /// Watermark of the slice at the end of that epoch.
    pub watermark_ms: Option<Timestamp>,
    /// Token of the slice instance that wrote the commit.
    pub owner: String,
    /// Epochs at or below this one are final.
    pub sealed: u64,
    /// Number of commit fields the log currently holds above the seal.
    pub log_fields: usize,
}

/// Read the last commit of a slice. `None` when it never committed.
pub async fn slice_head(kv: &dyn Kv, slice: &SliceKey) -> Result<Option<SliceHead>, KvError> {
    let log = state::read_log(kv, slice).await.map_err(|error| match error {
        StateError::Kv(error) => error,
        StateError::Corrupt { slice, reason } => KvError::Malformed { key: slice, reason },
    })?;
    Ok(log.head().map(|head| SliceHead {
        epoch: head.epoch,
        position: head.position,
        watermark_ms: (head.watermark != NO_WATERMARK).then_some(head.watermark),
        owner: head.owner.clone(),
        sealed: log.floor,
        log_fields: log.live.len(),
    }))
}

/// Whether an emitter slice whose head is at `position` has emitted `window`
/// entirely.
#[must_use]
pub fn covers(position: Option<Position>, window: &streaming_core::Window) -> bool {
    state::emitted(window, position)
}

/// What is wrong, if anything, with the commit record `value` just written to
/// the log of `slice` under `field`. For harnesses that watch every write.
///
/// A slice never commits a change to a window that the emitter of its
/// aggregate has already emitted: the emitter waits for the watermark of
/// every slice, and a slice drops what arrives behind its watermark.
pub async fn late_commit(kv: &dyn Kv, slice: &SliceKey, field: &str, value: &[u8]) -> Option<String> {
    let epoch = streaming_core::keys::parse_commit_field(field)?;
    let record: state::CommitRecord = postcard::from_bytes(value).ok()?;
    // A write at or below the seal is void: its writer is about to remove it.
    let seal = state::read_seal(kv, slice).await.ok()?;
    if epoch <= seal.floor || record.plan < seal.plan {
        return None;
    }
    for node in &record.changes {
        let emitter = SliceKey {
            tenant: slice.tenant.clone(),
            graph: slice.graph.clone(),
            slice: streaming_core::SliceId::Emitter {
                node: node.node.clone(),
            },
        };
        let upto = state::read_log(kv, &emitter)
            .await
            .ok()?
            .head()
            .and_then(|head| head.position);
        if let Some(changes) = node
            .windows
            .iter()
            .find(|changes| !changes.entries.is_empty() && state::emitted(&changes.window, upto))
        {
            return Some(format!(
                "slice {slice} committed epoch {epoch} with a change to window {} of `{}`, which was already emitted",
                changes.window, node.node
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metrics::ts_key,
        state::{CommitRecord, NodeChanges, StateEntry, WindowChanges},
    };
    use futures::executor::block_on;
    use streaming_core::{Key, MemKv, NodeId, Window};

    fn record(epoch: u64, plan: u64, position: Option<Position>, window: Option<Window>) -> CommitRecord {
        CommitRecord {
            epoch,
            owner: "o".into(),
            position,
            watermark: 0,
            plan,
            changes: window
                .map(|window| NodeChanges {
                    node: NodeId::new("count").unwrap(),
                    windows: vec![WindowChanges {
                        window,
                        entries: vec![StateEntry {
                            key: Key::encode(&"k".to_owned()).unwrap(),
                            acc: vec![1],
                            prev_epoch: 0,
                        }],
                    }],
                })
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn commits_into_emitted_windows_are_reported() {
        block_on(async {
            let kv = MemKv::new();
            let source: SliceKey = "acme/g/src.events.0".parse().unwrap();
            let emitter: SliceKey = "acme/g/agg.count".parse().unwrap();
            let first = Window::new(0, 10_000);
            let second = Window::new(10_000, 20_000);
            // The emitter is done with the first window, half way through the second.
            let done = Position(ts_key(10_000), ts_key(0), u64::MAX);
            let half = Position(ts_key(20_000), ts_key(10_000), 5);
            assert!(covers(Some(done), &first));
            assert!(!covers(Some(half), &second));
            assert!(!covers(None, &first));
            state::try_commit(&kv, &emitter, &record(1, 0, Some(done), None))
                .await
                .unwrap();
            let check = |record: CommitRecord| {
                let kv = &kv;
                let source = &source;
                async move {
                    let bytes = postcard::to_allocvec(&record).unwrap();
                    late_commit(kv, source, &format!("c:{}", record.epoch), &bytes).await
                }
            };
            let late = check(record(5, 0, None, Some(first))).await;
            assert!(late.is_some_and(|m| m.contains("already emitted")));
            assert_eq!(check(record(5, 0, None, Some(second))).await, None);
            let mut empty = record(5, 0, None, Some(first));
            empty.changes[0].windows[0].entries.clear();
            assert_eq!(check(empty).await, None);
            // Void writes: at or below the seal, or under an older plan.
            state::compact(&kv, &source, 5, &[], 0).await.unwrap();
            assert_eq!(check(record(5, 0, None, Some(first))).await, None);
            assert!(check(record(6, 0, None, Some(first))).await.is_some());
            state::close_log(&kv, &source, 2).await.unwrap();
            assert_eq!(check(record(7, 1, None, Some(first))).await, None);
            assert!(check(record(7, 3, None, Some(first))).await.is_some());
            // Not a commit field, or not a record.
            assert_eq!(late_commit(&kv, &source, "f:7", &[]).await, None);
            assert_eq!(late_commit(&kv, &source, "c:7", &[0xff]).await, None);
        });
    }
}
