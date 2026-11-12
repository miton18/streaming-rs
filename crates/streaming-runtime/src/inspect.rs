//! Read-only views of what a slice has durably committed, for tooling and
//! tests. Nothing here is needed by a worker.

use streaming_core::{Kv, KvError, Position, SliceKey, Timestamp};

use crate::state::{self, NO_WATERMARK, StateError};

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
