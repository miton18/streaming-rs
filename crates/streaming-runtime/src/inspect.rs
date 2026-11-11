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
    let log = state::read_log(kv, slice)
        .await
        .map_err(|error| match error {
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
