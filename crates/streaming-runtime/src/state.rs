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
//! # Keeping the log small without weakening it
//!
//! `HSETNX` only arbitrates while the field exists: deleting the field of an
//! old epoch would let a worker that fell far behind create it again and
//! believe it committed. Two rules prevent that:
//!
//! - a superseded record is *emptied*, not deleted: the field stays as a
//!   tombstone and keeps refusing late writers;
//! - tombstones are only deleted below a *seal*. Sealing epoch `f` (a
//!   write-once field `f:<f>` in a second hash; the seal is the highest such
//!   field, so it can only grow) declares every epoch up to `f` final. A
//!   committer reads the seal *after* its `HSETNX` succeeded: an epoch at or
//!   below the seal was already final, the write is void and the writer is
//!   fenced. Since a field is sealed before it is deleted, a write that
//!   revives a deleted field always finds the seal above it.
//!
//! Readers apply the same rule: whatever sits at or below the seal is ignored.
//!
//! # Materialization
//!
//! Accumulators are copied out of the commit record into one hash per window,
//! under versioned fields `<key>@<epoch>`. Versioned fields make the copy safe
//! to repeat and safe against a fenced worker finishing late: a reader keeps,
//! for each key, the field with the highest epoch.
//!
//! Only the instance that owns the slice copies records out: its own, and
//! those it inherited when it took the slice over. Until a record is copied
//! out it keeps its payload in the log, where every reader finds it; whoever
//! empties a record has copied it out entirely.
//!
//! # Forgetting a window
//!
//! The store offers no atomicity across keys, and a write issued by an
//! instance that has since lost the slice may still land. Three rules make
//! sure such a write never leaves a window hash that nobody will delete:
//!
//! - a window is entered in the index of its slice *before* anything is
//!   written to its hash;
//! - a writer only writes while the record it copies still holds its payload
//!   in the log, as of a read at most a quarter of `gc_grace` old;
//! - once a window is emitted its hash is deleted at once, but its index entry
//!   stays until no record in the log has mentioned the window for `gc_grace`.
//!   The hash is then deleted once more, and only then the entry. The instant
//!   the countdown started is kept in the entry itself, so that it carries on
//!   across instances of the emitter.
//!
//! A store command takes effect within its timeout or not at all: with a
//! `gc_grace` above three timeouts plus the clock difference between workers,
//! every write to a window lands before the last delete.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use streaming_core::{Key, Kv, KvError, NodeId, Position, SliceKey, Timestamp, Window, keys};

use crate::mutation::{Mutant, planted};

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

/// What a commit log holds, as of one read.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogView {
    /// Commit records above the seal that still hold their payload, oldest
    /// first. The last one is the head of the slice.
    pub records: Vec<CommitRecord>,
    /// The seal: every epoch at or below it is final.
    pub floor: u64,
    /// Epochs above the seal present in the log, records and tombstones alike.
    pub live: Vec<u64>,
    /// Epochs at or below the seal still present in the log: leftovers.
    pub sealed: Vec<u64>,
}

impl LogView {
    /// The most recent commit, if the slice ever committed.
    pub(crate) fn head(&self) -> Option<&CommitRecord> {
        self.records.last()
    }
}

/// The seal of a slice log: every epoch at or below it is final.
pub(crate) async fn read_floor(kv: &dyn Kv, slice: &SliceKey) -> Result<u64, KvError> {
    Ok(kv
        .hgetall(&keys::slice_seal(slice))
        .await?
        .iter()
        .filter_map(|(field, _)| keys::parse_seal_field(field))
        .max()
        .unwrap_or(0))
}

/// Read the commit log of a slice.
pub(crate) async fn read_log(kv: &dyn Kv, slice: &SliceKey) -> Result<LogView, StateError> {
    // The log is read before the seal, so that anything revived below the
    // seal while we read is seen as such. If the slice moved so fast that the
    // whole snapshot ended up below the seal, read again.
    for _ in 0..4 {
        let fields = kv.hgetall(&keys::slice_log(slice)).await?;
        let floor = read_floor(kv, slice).await?;
        let mut view = LogView {
            floor,
            ..LogView::default()
        };
        let mut any = false;
        for (field, value) in fields {
            let Some(epoch) = keys::parse_commit_field(&field) else {
                continue;
            };
            any = true;
            if epoch <= floor {
                view.sealed.push(epoch);
                continue;
            }
            view.live.push(epoch);
            if value.is_empty() {
                continue;
            }
            let record: CommitRecord =
                postcard::from_bytes(&value).map_err(|e| StateError::Corrupt {
                    slice: slice.to_string(),
                    reason: format!("field `{field}`: {e}"),
                })?;
            view.records.push(record);
        }
        view.records.sort_by_key(|record| record.epoch);
        view.live.sort_unstable();
        if view.records.is_empty() && any {
            continue;
        }
        return Ok(view);
    }
    Err(StateError::Kv(KvError::Unavailable(format!(
        "commit log of `{slice}` kept moving while being read"
    ))))
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
    if !kv.hsetnx(&log, &field, bytes).await? {
        // The field exists: ours from an attempt whose reply was lost, or
        // somebody else's (possibly already emptied or deleted).
        let ours = match kv.hget(&log, &field).await? {
            Some(existing) if !existing.is_empty() => {
                postcard::from_bytes::<CommitRecord>(&existing)
                    .is_ok_and(|existing| existing.owner == record.owner)
            }
            _ => false,
        };
        if !ours && !planted(Mutant::CommitWithoutFencing) {
            return Ok(CommitOutcome::Fenced);
        }
    }
    // The field is ours. It only counts if the epoch was not already final:
    // below the seal, the field we created is a deleted one coming back.
    if !planted(Mutant::CommitBelowTheSeal) && record.epoch <= read_floor(kv, slice).await? {
        let _ = kv.hdel(&log, vec![field]).await;
        return Ok(CommitOutcome::Fenced);
    }
    Ok(CommitOutcome::Committed)
}

/// Copy the changes of one epoch to one window into the hash of that window.
/// Idempotent.
///
/// The index entry goes first: whatever a writer leaves behind when it stops
/// half-way is listed, so it can be found and deleted.
pub(crate) async fn materialize_window(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    changes: &WindowChanges,
    epoch: u64,
) -> Result<(), KvError> {
    if changes.entries.is_empty() {
        return Ok(());
    }
    kv.hset(
        &keys::slice_window_index(slice, node),
        vec![(changes.window.to_string(), b"1".to_vec())],
    )
    .await?;
    let window_key = keys::slice_window(slice, node, &changes.window);
    kv.hset(
        &window_key,
        changes
            .entries
            .iter()
            .map(|entry| (keys::state_field(&entry.key, epoch), entry.acc.clone()))
            .collect(),
    )
    .await?;
    let superseded: Vec<String> = changes
        .entries
        .iter()
        .filter(|entry| entry.prev_epoch != 0 && entry.prev_epoch != epoch)
        .map(|entry| keys::state_field(&entry.key, entry.prev_epoch))
        .collect();
    if !superseded.is_empty() {
        kv.hdel(&window_key, superseded).await?;
    }
    Ok(())
}

/// Copy every change of a commit record into the window hashes.
#[cfg(test)]
pub(crate) async fn materialize(
    kv: &dyn Kv,
    slice: &SliceKey,
    record: &CommitRecord,
) -> Result<(), KvError> {
    for node in &record.changes {
        for changes in &node.windows {
            materialize_window(kv, slice, &node.node, changes, record.epoch).await?;
        }
    }
    Ok(())
}

/// Empty records that are superseded and copied out. Their fields stay and
/// keep refusing late writers.
pub(crate) async fn tombstone(
    kv: &dyn Kv,
    slice: &SliceKey,
    epochs: &[u64],
) -> Result<(), KvError> {
    if epochs.is_empty() {
        return Ok(());
    }
    kv.hset(
        &keys::slice_log(slice),
        epochs
            .iter()
            .map(|epoch| (keys::commit_field(*epoch), Vec::new()))
            .collect(),
    )
    .await
}

/// Seal every epoch up to `floor`, then delete what the log holds below it.
///
/// `epochs` are the log fields to delete (all at or below `floor`) and
/// `previous_floor` the seal this one replaces, if known. The order matters:
/// a field must be sealed before it is deleted.
pub(crate) async fn compact(
    kv: &dyn Kv,
    slice: &SliceKey,
    floor: u64,
    epochs: &[u64],
    previous_floor: u64,
) -> Result<(), KvError> {
    let seal = keys::slice_seal(slice);
    kv.hset(&seal, vec![(keys::seal_field(floor), b"1".to_vec())])
        .await?;
    let fields: Vec<String> = epochs
        .iter()
        .filter(|epoch| **epoch <= floor)
        .map(|epoch| keys::commit_field(*epoch))
        .collect();
    if !fields.is_empty() {
        kv.hdel(&keys::slice_log(slice), fields).await?;
    }
    if previous_floor != 0 && previous_floor < floor {
        // Older seals are redundant: the highest one is the seal.
        let older: Vec<String> = kv
            .hgetall(&seal)
            .await?
            .into_iter()
            .filter(|(field, _)| keys::parse_seal_field(field).is_some_and(|f| f < floor))
            .map(|(field, _)| field)
            .collect();
        if !older.is_empty() {
            kv.hdel(&seal, older).await?;
        }
    }
    Ok(())
}

/// Delete log fields found at or below the seal. They are final or void.
pub(crate) async fn sweep(kv: &dyn Kv, slice: &SliceKey, epochs: &[u64]) -> Result<(), KvError> {
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

/// Prefix of the value of an index entry once an emitter has seen that no
/// commit record mentions the window any more, followed by that instant.
const QUIET: &str = "q:";

/// Windows the index of a slice lists for one aggregate node. Each comes with
/// the wall-clock instant from which no commit record mentioned it, once an
/// emitter has recorded one (see [`mark_quiet`]).
pub(crate) async fn indexed_windows(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
) -> Result<BTreeMap<Window, Option<Timestamp>>, KvError> {
    Ok(kv
        .hgetall(&keys::slice_window_index(slice, node))
        .await?
        .iter()
        .filter_map(|(field, value)| {
            let window = field.parse::<Window>().ok()?;
            let since = std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.strip_prefix(QUIET))
                .and_then(|instant| instant.parse().ok());
            Some((window, since))
        })
        .collect())
}

/// Record in the index that, as of `since`, no commit record mentions these
/// emitted windows any more: the countdown to [`unindex_windows`] starts, and
/// carries on whichever instance of the emitter comes next. A writer entering
/// a window in the index again erases the instant, which restarts it.
pub(crate) async fn mark_quiet(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    windows: &[Window],
    since: Timestamp,
) -> Result<(), KvError> {
    if windows.is_empty() {
        return Ok(());
    }
    kv.hset(
        &keys::slice_window_index(slice, node),
        windows
            .iter()
            .map(|window| (window.to_string(), format!("{QUIET}{since}").into_bytes()))
            .collect(),
    )
    .await
}

/// Windows changed by commit records that still hold their payload.
pub(crate) fn pending_windows(records: &[CommitRecord], node: &NodeId) -> BTreeSet<Window> {
    records
        .iter()
        .flat_map(|record| &record.changes)
        .filter(|changes| &changes.node == node)
        .flat_map(|changes| changes.windows.iter().map(|w| w.window))
        .collect()
}

/// Windows a slice holds accumulators for, for one aggregate node: those
/// indexed and those only present in `pending` commit records.
pub(crate) async fn list_windows(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    pending: &[CommitRecord],
) -> Result<BTreeSet<Window>, KvError> {
    let mut windows: BTreeSet<Window> = indexed_windows(kv, slice, node)
        .await?
        .into_keys()
        .collect();
    windows.extend(pending_windows(pending, node));
    Ok(windows)
}

/// Largest number of keys one delete may carry.
const DELETE_BATCH: usize = 100;

/// Delete the accumulators of windows. Their index entries stay.
pub(crate) async fn delete_windows(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    windows: &[Window],
) -> Result<(), KvError> {
    for batch in windows.chunks(DELETE_BATCH) {
        kv.del(
            batch
                .iter()
                .map(|window| keys::slice_window(slice, node, window))
                .collect(),
        )
        .await?;
    }
    Ok(())
}

/// Remove windows from the index of a slice. Only once nothing can write to
/// their hashes any more, and after a last [`delete_windows`].
pub(crate) async fn unindex_windows(
    kv: &dyn Kv,
    slice: &SliceKey,
    node: &NodeId,
    windows: &[Window],
) -> Result<(), KvError> {
    if windows.is_empty() {
        return Ok(());
    }
    kv.hdel(
        &keys::slice_window_index(slice, node),
        windows.iter().map(ToString::to_string).collect(),
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
            delete_windows(&kv, &slice(), &node(), &[window])
                .await
                .unwrap();
            assert!(
                read_window(&kv, &slice(), &node(), &window, &[])
                    .await
                    .unwrap()
                    .is_empty()
            );
            // The entry outlives the state: it is what a late write is found by.
            let indexed = || async { indexed_windows(&kv, &slice(), &node()).await.unwrap() };
            assert_eq!(indexed().await, BTreeMap::from([(window, None)]));
            // The countdown to forgetting the window is kept in the entry...
            mark_quiet(&kv, &slice(), &node(), &[window], 1_234)
                .await
                .unwrap();
            assert_eq!(indexed().await, BTreeMap::from([(window, Some(1_234))]));
            // ... and a writer entering the window again restarts it.
            materialize(&kv, &slice(), &first).await.unwrap();
            assert_eq!(indexed().await, BTreeMap::from([(window, None)]));
            delete_windows(&kv, &slice(), &node(), &[window])
                .await
                .unwrap();
            unindex_windows(&kv, &slice(), &node(), &[window])
                .await
                .unwrap();
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
    fn superseded_records_are_emptied_and_still_refuse_writers() {
        block_on(async {
            let kv = MemKv::new();
            for epoch in 1..=3 {
                try_commit(&kv, &slice(), &record(epoch, "o", &[]))
                    .await
                    .unwrap();
            }
            tombstone(&kv, &slice(), &[1, 2]).await.unwrap();
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!(log.records.len(), 1);
            assert_eq!(log.head().unwrap().epoch, 3);
            assert_eq!(log.live, [1, 2, 3]);
            // A worker that fell behind cannot commit an epoch already taken.
            let late = record(2, "late", &[("k", 9, 0)]);
            assert_eq!(
                try_commit(&kv, &slice(), &late).await.unwrap(),
                CommitOutcome::Fenced
            );
        });
    }

    #[test]
    fn a_deleted_epoch_cannot_be_revived() {
        block_on(async {
            let kv = MemKv::new();
            for epoch in 1..=5 {
                try_commit(&kv, &slice(), &record(epoch, "o", &[]))
                    .await
                    .unwrap();
            }
            compact(&kv, &slice(), 4, &[1, 2, 3, 4], 0).await.unwrap();
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!((log.floor, log.live.clone()), (4, vec![5]));

            // The field of epoch 2 is gone, so HSETNX alone would accept this.
            let late = record(2, "late", &[("k", 9, 0)]);
            assert_eq!(
                try_commit(&kv, &slice(), &late).await.unwrap(),
                CommitOutcome::Fenced
            );
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!(log.head().unwrap().epoch, 5);
            assert!(log.sealed.is_empty(), "the revived field was removed");

            // A revived field left behind by a writer that died is ignored.
            kv.hsetnx(
                &keys::slice_log(&slice()),
                &keys::commit_field(3),
                postcard::to_allocvec(&record(3, "ghost", &[("k", 1, 0)])).unwrap(),
            )
            .await
            .unwrap();
            let log = read_log(&kv, &slice()).await.unwrap();
            assert_eq!(log.records.iter().map(|r| r.epoch).collect::<Vec<_>>(), [5]);
            assert_eq!(log.sealed, [3]);
            sweep(&kv, &slice(), &log.sealed).await.unwrap();

            // The seal only grows, even if an older one is written late.
            compact(&kv, &slice(), 2, &[], 0).await.unwrap();
            assert_eq!(read_floor(&kv, &slice()).await.unwrap(), 4);
            // The next epoch still commits normally.
            assert_eq!(
                try_commit(&kv, &slice(), &record(6, "o", &[]))
                    .await
                    .unwrap(),
                CommitOutcome::Committed
            );
        });
    }
}
