//! An in-process partitioned log, usable as a replayable source.
//!
//! It plays the role of a broker for the standalone mode, for tests and for
//! simulations: producers append, readers poll by position, acknowledged
//! records are released.

use async_trait::async_trait;
use moonpool_core::{Providers, TimeProvider};
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use streaming_core::{Datum, PartitionReader, Position, SourceConnector, SourceError, SourceRecord, Timestamp};
use tokio::sync::Notify;

#[derive(Default)]
struct Partition {
    /// Offset of the first retained record.
    base: u64,
    records: VecDeque<(Timestamp, Datum)>,
}

impl Partition {
    fn end(&self) -> u64 {
        self.base + self.records.len() as u64
    }
}

/// A partitioned, append-only, in-memory log.
///
/// Positions are `Position(0, offset + 1, 0)`, offsets starting at zero.
pub struct MemLog {
    partitions: Mutex<Vec<Arc<Mutex<Partition>>>>,
    appended: Notify,
    release_acked: bool,
}

impl MemLog {
    /// A log with `partitions` partitions that keeps every record.
    #[must_use]
    pub fn new(partitions: u32) -> Arc<Self> {
        Self::with_retention(partitions, false)
    }

    /// A log that drops records once they are acknowledged, when
    /// `release_acked` is set.
    #[must_use]
    pub fn with_retention(partitions: u32, release_acked: bool) -> Arc<Self> {
        Arc::new(Self {
            partitions: Mutex::new((0..partitions.max(1)).map(|_| Arc::default()).collect()),
            appended: Notify::new(),
            release_acked,
        })
    }

    /// Number of partitions.
    #[must_use]
    pub fn partitions(&self) -> u32 {
        self.partitions.lock().len() as u32
    }

    /// Add a partition at the end. Returns its index.
    pub fn add_partition(&self) -> u32 {
        let mut partitions = self.partitions.lock();
        partitions.push(Arc::default());
        partitions.len() as u32 - 1
    }

    /// Remove the last partition, with whatever it still holds.
    pub fn remove_partition(&self) {
        let mut partitions = self.partitions.lock();
        if partitions.len() > 1 {
            partitions.pop();
        }
        drop(partitions);
        self.appended.notify_waiters();
    }

    fn partition(&self, partition: u32) -> Option<Arc<Mutex<Partition>>> {
        self.partitions.lock().get(partition as usize).cloned()
    }

    /// Append a record and wake waiting readers. Returns its position.
    pub fn append(&self, partition: u32, event_time: Timestamp, value: Datum) -> Position {
        let Some(partition) = self.partition(partition) else {
            return Position::default();
        };
        let position = {
            let mut partition = partition.lock();
            partition.records.push_back((event_time, value));
            Position(0, partition.end(), 0)
        };
        self.appended.notify_waiters();
        position
    }

    /// Position of the last record of a partition, if it ever had one.
    #[must_use]
    pub fn end(&self, partition: u32) -> Option<Position> {
        let end = self.partition(partition)?.lock().end();
        (end > 0).then_some(Position(0, end, 0))
    }

    /// Number of records currently retained in a partition.
    #[must_use]
    pub fn retained(&self, partition: u32) -> usize {
        self.partition(partition)
            .map_or(0, |partition| partition.lock().records.len())
    }

    /// Up to `max` records of `partition` whose offset is at least `from`.
    /// Records already released are skipped.
    #[must_use]
    pub fn read(&self, partition: u32, from: u64, max: usize) -> Vec<SourceRecord> {
        let Some(partition) = self.partition(partition) else {
            return Vec::new();
        };
        let partition = partition.lock();
        let start = from.max(partition.base);
        let skip = (start - partition.base) as usize;
        partition
            .records
            .iter()
            .enumerate()
            .skip(skip)
            .take(max)
            .map(|(index, (event_time, value))| SourceRecord {
                position: Position(0, partition.base + index as u64 + 1, 0),
                event_time: *event_time,
                value: value.clone(),
            })
            .collect()
    }

    /// Everything up to `upto` included has been durably processed.
    pub fn ack(&self, partition: u32, upto: Position) {
        if !self.release_acked {
            return;
        }
        let Some(partition) = self.partition(partition) else {
            return;
        };
        let mut partition = partition.lock();
        while partition.base < upto.1 && partition.records.pop_front().is_some() {
            partition.base += 1;
        }
    }

    /// Resolves at the next append.
    pub async fn appended(&self) {
        self.appended.notified().await;
    }

    /// A source connector reading this log.
    pub fn source<P: Providers>(self: &Arc<Self>, providers: P) -> Arc<dyn SourceConnector> {
        Arc::new(MemLogSource {
            log: self.clone(),
            providers,
        })
    }
}

struct MemLogSource<P: Providers> {
    log: Arc<MemLog>,
    providers: P,
}

#[async_trait]
impl<P: Providers> SourceConnector for MemLogSource<P> {
    async fn partitions(&self) -> Result<u32, SourceError> {
        Ok(self.log.partitions())
    }

    async fn open(&self, partition: u32, after: Option<Position>) -> Result<Box<dyn PartitionReader>, SourceError> {
        Ok(Box::new(MemLogReader {
            log: self.log.clone(),
            providers: self.providers.clone(),
            partition,
            next: after.map_or(0, |position| position.1),
        }))
    }
}

struct MemLogReader<P: Providers> {
    log: Arc<MemLog>,
    providers: P,
    partition: u32,
    next: u64,
}

#[async_trait]
impl<P: Providers> PartitionReader for MemLogReader<P> {
    async fn poll(&mut self, max: usize, max_wait: Duration) -> Result<Vec<SourceRecord>, SourceError> {
        let wait = self.log.appended();
        let mut records = self.log.read(self.partition, self.next, max);
        if records.is_empty() {
            // Registered before reading, so an append in between is not missed.
            let _ = self.providers.time().timeout(max_wait, wait).await;
            records = self.log.read(self.partition, self.next, max);
        }
        if let Some(last) = records.last() {
            self.next = last.position.1;
        }
        Ok(records)
    }

    async fn ack(&mut self, upto: Position) -> Result<(), SourceError> {
        self.log.ack(self.partition, upto);
        Ok(())
    }

    async fn end_position(&mut self) -> Result<Option<Position>, SourceError> {
        Ok(self.log.end(self.partition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streaming_core::datum;

    #[test]
    fn read_ack_and_release() {
        let log = MemLog::with_retention(2, true);
        for i in 0..5_u64 {
            assert_eq!(log.append(0, i as i64, datum(i)), Position(0, i + 1, 0));
        }
        assert_eq!(log.end(0), Some(Position(0, 5, 0)));
        assert_eq!(log.end(1), None);
        let first = log.read(0, 0, 3);
        assert_eq!(first.iter().map(|r| r.position.1).collect::<Vec<_>>(), [1, 2, 3]);
        log.ack(0, Position(0, 2, 0));
        assert_eq!(log.retained(0), 3);
        // A reader behind the released prefix resumes at the first retained record.
        let rest = log.read(0, 0, 10);
        assert_eq!(rest.iter().map(|r| r.position.1).collect::<Vec<_>>(), [3, 4, 5]);
    }
}
