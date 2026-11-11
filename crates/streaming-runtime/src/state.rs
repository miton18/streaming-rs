//! Durable state of a slice: the commit log and the window accumulators.
//!
//! # The commit log
//!
//! Each slice owns one hash, its log. Committing epoch `e` means creating the
//! field `c:<e>` with `HSETNX`. The field holds, in one value, everything the
//! epoch changed: the source position reached, the watermark, and the new
//! accumulators of every key touched. `HSETNX` succeeds for exactly one
//! writer, so two workers running the same slice cannot both commit epoch
//! `e`: the loser is *fenced*, drops what it holds in memory and starts over
//! from the log. This single-field compare-and-set is the only atomicity the
//! framework needs from the store.
//!
//! # Materialization
//!
//! Accumulators are then copied out of the commit record into one hash per
//! window, under versioned fields `<key>@<epoch>`. Versioned fields make the
//! copy safe to repeat and safe against a fenced worker finishing late: a
//! reader keeps, for each key, the field with the highest epoch. Once a record
//! is materialized, older records are trimmed from the log; whoever opens a
//! log replays every record still present, in order.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use streaming_core::{Key, Kv, KvError, NodeId, Position, SliceKey, Timestamp, Window, keys};

/// Watermark of a slice that has not seen any event time yet.
pub(crate) const NO_WATERMARK: Timestamp = i64::MIN;

/// New accumulator of one key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StateEntry {
    pub key: Key,
    /// Encoded accumulator, replacing any earlier one.
    pub acc: Vec<u8>,
    /// Epoch of the version this one replaces, `0` if none.
    pub prev_epoch: u64,
}

/// Changes of one epoch to one window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WindowChanges {
    pub window: Window,
    pub entries: Vec<StateEntry>,
}

/// Changes of one epoch to one aggregate node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NodeChanges {
    pub node: NodeId,
    pub windows: Vec<WindowChanges>,
}

/// Everything one epoch of a slice changed. Stored as a single KV field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommitRecord {
    pub epoch: u64,
    /// Identifies the slice instance that wrote the record, so that an instance
    /// can recognise its own commit after a reply was lost.
    pub owner: String,
    /// Last source position included in the epoch.
    pub position: Option<Position>,
    /// Watermark of the slice at the end of the epoch.
    pub watermark: Timestamp,
    pub changes: Vec<NodeChanges>,
}

/// A state failure.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum StateError {
    #[error(transparent)]
    Kv(#[from] KvError),
    #[error("commit log of `{slice}` is corrupt: {reason}")]
    Corrupt { slice: String, reason: String },
}

/// The commit records currently in a log, oldest first.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogView {
    pub records: Vec<CommitRecord>,
}

impl LogView {
    /// The most recent commit, if the slice ever committed.
    pub(crate) fn head(&self) -> Option<&CommitRecord> {
        self.records.last()
    }
}

/// Read the commit log of a slice.
pub(crate) async fn read_log(kv: &dyn Kv, slice: &SliceKey) -> Result<LogView, StateError> {
    let mut records = Vec::new();
    for (field, value) in kv.hgetall(&keys::slice_log(slice)).await? {
        if keys::parse_commit_field(&field).is_none() {
            continue;
        }
        let record: CommitRecord =
            postcard::from_bytes(&value).map_err(|e| StateError::Corrupt {
                slice: slice.to_string(),
                reason: format!("field `{field}`: {e}"),
            })?;
        records.push(record);
    }
    records.sort_by_key(|record| record.epoch);
    Ok(LogView { records })
}

/// Result of a commit attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    /// The epoch is ours.
    Committed,
    /// Another instance of the slice committed this epoch, or went past it.
    Fenced,
}

/// Try to commit `record`. Safe to call again with the same record after an
/// error: a record that did get written is recognised by its owner.
pub(crate) async fn try_commit(
    kv: &dyn Kv,
    slice: &SliceKey,
    record: &CommitRecord,
) -> Result<CommitOutcome, StateError> {
    let log = keys::slice_log(slice);
    let field = keys::commit_field(record.epoch);
    let bytes = postcard::to_allocvec(record).map_err(|e| StateError::Corrupt {
        slice: slice.to_string(),
        reason: format!("cannot encode epoch {}: {e}", record.epoch),
    })?;
    if kv.hsetnx(&log, &field, bytes).await? {
        return Ok(CommitOutcome::Committed);
    }
    match kv.hget(&log, &field).await? {
        Some(existing) => match postcard::from_bytes::<CommitRecord>(&existing) {
            Ok(existing) if existing.owner == record.owner => Ok(CommitOutcome::Committed),
            _ => Ok(CommitOutcome::Fenced),
        },
        // Already trimmed: someone committed a later epoch.
        None => Ok(CommitOutcome::Fenced),
    }
}

/// Copy the accumulators of a commit record into the window hashes. Idempotent.
pub(crate) async fn materialize(
    kv: &dyn Kv,
    slice: &SliceKey,
    record: &CommitRecord,
) -> Result<(), KvError> {
    for node in &record.changes {
        for changes in &node.windows {
            if changes.entries.is_empty() {
                continue;
            }
            // Index first: a window hash is never unknown to the index.
            kv.hset(
                &keys::slice_window_index(slice, &node.node),
                vec![(changes.window.to_string(), b"1".to_vec())],
            )
            .await?;
            let window_key = keys::slice_window(slice, &node.node, &changes.window);
            kv.hset(
                &window_key,
                changes
                    .entries
                    .iter()
                    .map(|entry| {
                        (
                            keys::state_field(&entry.key, record.epoch),
                            entry.acc.clone(),
                        )
                    })
                    .collect(),
            )
            .await?;
            let superseded: Vec<String> = changes
                .entries
                .iter()
                .filter(|entry| entry.prev_epoch != 0 && entry.prev_epoch != record.epoch)
                .map(|entry| keys::state_field(&entry.key, entry.prev_epoch))
                .collect();
            if !superseded.is_empty() {
                kv.hdel(&window_key, superseded).await?;
            }
        }
    }
    Ok(())
}

/// Remove materialized commit records from the log.
pub(crate) async fn trim(kv: &dyn Kv, slice: &SliceKey, epochs: &[u64]) -> Result<(), KvError> {
    if epochs.is_empty() {
        return Ok(());
    }
    kv.hdel(
        &keys::slice_log(slice),
        epochs
            .iter()
            .map(|epoch| keys::commit_field(*epoch))
            .collect(),
    )
    .await
}

/// Current accumulators of a window: for each key, its encoded accumulator and
/// the epoch that wrote it.
pub(crate) type WindowState = BTreeMap<Key, (Vec<u8>, u64)>;

fn overlay(state: &mut WindowState, records: &[CommitRecord], node: &NodeId, window: &Window) {
    for record in records {
        for changes in record
            .changes
            .iter()
            .filter(|changes| &changes.node == node)
        {
            for window_changes in changes.windows.iter().filter(|w| &w.window == window) {
                for entry in &window_changes.entries {
                    let newer = state
                        .get(&entry.key)
                        .is_none_or(|(_, epoch)| *epoch <= record.epoch);
                    if newer {
                        state.insert(entry.key.clone(), (entry.acc.clone(), record.epoch));
                    }
                }
            }
        }
    }
}

/// Read the accumulators of one window of one slice.
///
/// `pending` are commit records that may not be materialized yet (the records
/// found in the log); they are overlaid on what the window hash holds.
pub(crate) async fn read_window(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    window: &Window,
    pending: &[CommitRecord],
) -> Result<WindowState, KvError> {
    let mut state = WindowState::new();
    for (field, value) in kv.hgetall(&keys::slice_window(slice, node, window)).await? {
        let Some((key, epoch)) = keys::parse_state_field(&field) else {
            continue;
        };
        if state.get(&key).is_none_or(|(_, known)| *known < epoch) {
            state.insert(key, (value, epoch));
        }
    }
    overlay(&mut state, pending, node, window);
    Ok(state)
}

/// Windows a slice holds accumulators for, for one aggregate node.
pub(crate) async fn list_windows(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    pending: &[CommitRecord],
) -> Result<BTreeSet<Window>, KvError> {
    let mut windows = BTreeSet::new();
    for (field, _) in kv.hgetall(&keys::slice_window_index(slice, node)).await? {
        if let Ok(window) = field.parse::<Window>() {
            windows.insert(window);
        }
    }
    for record in pending {
        for changes in record
            .changes
            .iter()
            .filter(|changes| &changes.node == node)
        {
            windows.extend(changes.windows.iter().map(|w| w.window));
        }
    }
    Ok(windows)
}

/// Delete the accumulators of a window once it has been emitted. Idempotent.
pub(crate) async fn drop_window(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    window: &Window,
) -> Result<(), KvError> {
    kv.del(vec![keys::slice_window(slice, node, window)])
        .await?;
    kv.hdel(
        &keys::slice_window_index(slice, node),
        vec![window.to_string()],
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use streaming_core::MemKv;

    fn slice() -> SliceKey {
        "acme/g/src.events.0".parse().unwrap()
    }

    fn node() -> NodeId {
        NodeId::new("count").unwrap()
    }

    fn key(name: &str) -> Key {
        Key::encode(&name.to_owned()).unwrap()
    }

    fn record(epoch: u64, owner: &str, entries: &[(&str, u64, u64)]) -> CommitRecord {
        CommitRecord {
            epoch,
            owner: owner.to_owned(),
            position: Some(Position(0, epoch * 10, 0)),
            watermark: epoch as i64 * 1000,
            changes: vec![NodeChanges {
                node: node(),
                windows: vec![WindowChanges {
                    window: Window::new(0, 10_000),
                    entries: entries
                        .iter()
                        .map(|(name, value, prev)| StateEntry {
                            key: key(name),
                            acc: value.to_le_bytes().to_vec(),
                            prev_epoch: *prev,
                        })
                        .collect(),
                }],
            }],
        }
    }

    fn value(state: &WindowState, name: &str) -> u64 {
        u64::from_le_bytes(state[&key(name)].0.clone().try_into().unwrap())
    }

    #[test]
    fn only_one_instance_commits_an_epoch() {
        block_on(async {
            let kv = MemKv::new();
            let ours = record(1, "worker-a#1", &[("k", 1, 0)]);
            let theirs = record(1, "worker-b#9", &[("k", 5, 0)]);
            assert_eq!(
                try_commit(&kv, &slice(), &ours).await.unwrap(),
                CommitOutcome::Committed
            );
            assert_eq!(
                try_commit(&kv, &slice(), &theirs).await.unwrap(),
                CommitOutcome::Fenced
            );
            // A retry after a lost reply recognises its own record.
            assert_eq!(
                try_commit(&kv, &slice(), &ours).await.unwrap(),
                CommitOutcome::Committed
            );
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!(log.head(), Some(&ours));
        });
    }

    #[test]
    fn materialization_is_idempotent_and_versioned() {
        block_on(async {
            let kv = MemKv::new();
            let window = Window::new(0, 10_000);
            let first = record(1, "o", &[("a", 1, 0), ("b", 1, 0)]);
            let second = record(2, "o", &[("a", 3, 1)]);
            materialize(&kv, &slice(), &first).await.unwrap();
            materialize(&kv, &slice(), &second).await.unwrap();
            // A fenced instance finishing late replays the older record.
            materialize(&kv, &slice(), &first).await.unwrap();
            let state = read_window(&kv, &slice(), &node(), &window, &[])
                .await
                .unwrap();
            assert_eq!((value(&state, "a"), value(&state, "b")), (3, 1));
            assert_eq!(
                list_windows(&kv, &slice(), &node(), &[]).await.unwrap(),
                BTreeSet::from([window])
            );
            drop_window(&kv, &slice(), &node(), &window).await.unwrap();
            assert!(
                read_window(&kv, &slice(), &node(), &window, &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                list_windows(&kv, &slice(), &node(), &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn pending_records_are_overlaid() {
        block_on(async {
            let kv = MemKv::new();
            let window = Window::new(0, 10_000);
            let first = record(1, "o", &[("a", 1, 0)]);
            let second = record(2, "o", &[("a", 2, 1), ("c", 7, 0)]);
            materialize(&kv, &slice(), &first).await.unwrap();
            // `second` is committed but not materialized yet.
            let pending = [first.clone(), second.clone()];
            let state = read_window(&kv, &slice(), &node(), &window, &pending)
                .await
                .unwrap();
            assert_eq!((value(&state, "a"), value(&state, "c")), (2, 7));
            // Overlaying an older record over a newer materialized one changes nothing.
            materialize(&kv, &slice(), &second).await.unwrap();
            let state = read_window(&kv, &slice(), &node(), &window, &[first])
                .await
                .unwrap();
            assert_eq!(value(&state, "a"), 2);
        });
    }

    #[test]
    fn trim_keeps_the_head() {
        block_on(async {
            let kv = MemKv::new();
            for epoch in 1..=3 {
                try_commit(&kv, &slice(), &record(epoch, "o", &[]))
                    .await
                    .unwrap();
            }
            trim(&kv, &slice(), &[1, 2]).await.unwrap();
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!(log.records.len(), 1);
            assert_eq!(log.head().unwrap().epoch, 3);
        });
    }
}
