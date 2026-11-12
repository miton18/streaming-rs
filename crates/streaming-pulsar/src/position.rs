//! Mapping between Pulsar message ids and framework positions.
//!
//! A Pulsar message id is `(ledger id, entry id, batch index)` inside one
//! topic partition; the batch index is `-1` for a message that was not
//! batched. The framework position is
//!
//! `Position(ledger_id, entry_id, batch_index + 1)`
//!
//! so a non-batched message gets `.2 == 0` and the members of a batch get
//! `1..=n`. Lexicographic order of positions is then the order in which the
//! broker stores (and a single consumer receives) messages of one partition:
//! ledgers are created with increasing ids, entries are appended with
//! increasing ids inside a ledger, and batch members are unpacked in index
//! order. The partition is not part of the position: positions are only
//! compared inside one partition.

use magnetar::MessageId;
use streaming_core::Position;

/// Framework position of a delivered message.
#[must_use]
pub fn position_of(id: &MessageId) -> Position {
    let batch = u64::try_from(id.batch_index).map_or(0, |index| index + 1);
    Position(id.ledger_id, id.entry_id, batch)
}

/// Position of the last stored message, from a `GetLastMessageId` answer.
/// The broker answers with a negative entry (or ledger) id for a partition
/// that never stored a message.
#[must_use]
pub fn end_position_of(id: &MessageId) -> Option<Position> {
    let empty = (id.ledger_id as i64) < 0 || (id.entry_id as i64) < 0;
    (!empty).then(|| position_of(id))
}

/// Name of partition `partition` of a partitioned topic.
#[must_use]
pub fn partition_topic(topic: &str, partition: u32) -> String {
    format!("{topic}-partition-{partition}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ledger: u64, entry: u64, batch_index: i32, batch_size: i32) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition: 2,
            batch_index,
            batch_size,
        }
    }

    #[test]
    fn non_batched_and_batched_positions() {
        assert_eq!(position_of(&id(7, 3, -1, -1)), Position(7, 3, 0));
        assert_eq!(position_of(&id(7, 3, 0, 4)), Position(7, 3, 1));
        assert_eq!(position_of(&id(7, 3, 3, 4)), Position(7, 3, 4));
    }

    #[test]
    fn order_matches_delivery_order() {
        // A partition as the broker stores it: a plain entry, a batch of three,
        // another plain entry, then a ledger rollover.
        let delivered = [
            id(7, 0, -1, -1),
            id(7, 1, 0, 3),
            id(7, 1, 1, 3),
            id(7, 1, 2, 3),
            id(7, 2, -1, -1),
            id(9, 0, 0, 2),
            id(9, 0, 1, 2),
            id(9, 1, -1, -1),
        ];
        let positions: Vec<Position> = delivered.iter().map(position_of).collect();
        assert!(
            positions.windows(2).all(|w| w[0] < w[1]),
            "positions must strictly increase: {positions:?}"
        );
    }

    #[test]
    fn end_positions() {
        // An empty partition: entry -1 on the wire, u64::MAX here.
        assert_eq!(end_position_of(&id(12, u64::MAX, -1, -1)), None);
        assert_eq!(end_position_of(&MessageId::EARLIEST), None);
        assert_eq!(end_position_of(&id(12, 4, -1, -1)), Some(Position(12, 4, 0)));
        // The last member of a batch is at or after every member already read.
        let end = end_position_of(&id(12, 5, 2, 3)).unwrap();
        assert_eq!(end, position_of(&id(12, 5, 2, 3)));
        // A broker that does not report the batch index gives (12, 5, 0),
        // which every member of that entry reaches.
        let coarse = end_position_of(&id(12, 5, -1, -1)).unwrap();
        assert!(position_of(&id(12, 5, 0, 3)) >= coarse);
        // Ledger and entry 0 are real positions, not the empty marker.
        assert_eq!(end_position_of(&id(0, 0, -1, -1)), Some(Position(0, 0, 0)));
        assert_eq!(end_position_of(&id(u64::MAX, 4, -1, -1)), None);
    }

    #[test]
    fn partition_names() {
        assert_eq!(
            partition_topic("persistent://public/default/orders", 3),
            "persistent://public/default/orders-partition-3"
        );
    }
}
