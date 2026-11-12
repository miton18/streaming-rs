//! Pulsar sequence ids derived from the origin of the elements a sink writes.
//!
//! Broker-side deduplication (`brokerDeduplicationEnabled`, or
//! `pulsar-admin namespaces set-deduplication --enable`) keeps, per topic
//! partition and per producer name, the highest sequence id it persisted, and
//! silently drops any later message whose sequence id is not greater. If the
//! sequence id of an element is a pure function of its identity and grows
//! along the stream, then re-emitting elements after a failure re-sends them
//! with the sequence ids they had the first time, and the broker drops the
//! ones it already holds: effectively-once towards Pulsar.
//!
//! # The derivation
//!
//! `seq = pack(origin.position.0, origin.position.1, origin.position.2, index)`
//! where `index` is the rank of the element among the *consecutive* elements
//! of the stream sharing the same origin position (0 for the first one), and
//! `pack` concatenates the four numbers on a fixed number of bits each
//! ([`SequenceLayout`]), most significant first. Packing is lexicographic, so
//! `seq` grows with `(position, index)` as long as every component fits its
//! width; a component that does not fit is a write error, never a silently
//! wrapped number. The total is at most 63 bits (Pulsar sequence ids are
//! signed 64-bit integers).
//!
//! # When it is sound
//!
//! All of the following must hold; the first three are guaranteed by the
//! runtime, the others are the graph author's responsibility:
//!
//! 1. One producer name per input slice: elements of one slice reach one sink
//!    instance, and the sink appends the slice id to the configured producer
//!    name. (Two slices interleave their positions; they must not share a
//!    deduplication state.)
//! 2. Inside one slice, `origin.position` never decreases from one element to
//!    the next.
//! 3. Re-emission after a failure replays whole origin positions: a replay
//!    never starts in the middle of the elements derived from one source
//!    record.
//! 4. The elements derived from one origin position reach the sink in the
//!    same order on every run, so that `index` is stable. This holds for a
//!    single input path made of deterministic operators (`map`, `filter`,
//!    `flat_map` emitting its outputs in a deterministic order, aggregates —
//!    which emit one element per position). It does **not** hold when the sink
//!    has several input paths (or the graph forks and joins again) whose
//!    elements for the same position may interleave differently on a replay:
//!    the broker would then drop an element it never stored (loss) and keep a
//!    second copy of another (duplicate).
//! 5. The positions fit the chosen layout, for the lifetime of the producer
//!    name. Pulsar positions are `(ledger id, entry id, batch index + 1)`:
//!    ledger ids are cluster-wide BookKeeper ids that only grow, entry ids are
//!    bounded by `managedLedgerMaxEntriesPerLedger` (50 000 by default), batch
//!    indexes by the upstream producers' `batchingMaxMessages`. A log with
//!    plain offsets uses `(0, offset, 0)`; an aggregate emitter
//!    `(window end ms, window start ms, key index)` and needs most bits on the
//!    first component.
//! 6. The source is never rewound to positions *before* what was already
//!    written under the same producer name for anything other than a replay:
//!    reprocessing a topic from scratch, or changing the graph so that
//!    positions mean something else, requires a new producer name, otherwise
//!    the broker drops everything up to the old high-water mark.
//! 7. The destination's partition count does not change while elements are
//!    being replayed (routing must be the same on every run, see the sink).
//!
//! 8. Deduplication is enabled for the destination namespace (or broker).
//!    Otherwise the broker ignores sequence ids and the sink is at-least-once
//!    with no error: nothing in the protocol tells the client.
//!
//! When any of these is in doubt, leave `broker_dedup` off: the sink is then
//! at-least-once, every message carrying the `streaming-emit-id` property a
//! consumer can deduplicate on.
//!
//! # How the ids reach the broker
//!
//! Magnetar 1.8 does not let a send carry its own sequence id: the producer
//! state machine numbers sends consecutively from the producer's
//! `initial_sequence_id` (and from 0 when none is given, which under a stable
//! producer name on a deduplicating namespace makes the broker silently drop
//! everything up to its high-water mark — the sink always sets it). The sink
//! therefore tracks the id each partition producer will assign next and,
//! when an element needs another one, waits for the receipts of what is in
//! flight on that producer, closes it and reopens it (same name) with
//! `initial_sequence_id` set to the element's id. The broker's high-water
//! mark for the name, returned when the producer attaches, lets the sink skip
//! elements the broker already holds instead of reopening for each of them.

use streaming_core::Position;

/// Bit layout of a derived sequence id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceLayout {
    bits: [u8; 4],
}

/// Running state of the derivation: the last position seen and the index the
/// next element with that position gets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SequenceState {
    last: Option<Position>,
    next_index: u64,
}

impl SequenceLayout {
    /// A layout giving `position_bits[i]` bits to component `i` of the
    /// position and `index_bits` bits to the index. At most 63 bits in total.
    pub fn new(position_bits: [u8; 3], index_bits: u8) -> Result<Self, String> {
        let bits = [position_bits[0], position_bits[1], position_bits[2], index_bits];
        let total: u32 = bits.iter().map(|b| u32::from(*b)).sum();
        if total > 63 {
            return Err(format!(
                "brokerDedup layout uses {total} bits, Pulsar sequence ids have 63"
            ));
        }
        if total == 0 {
            return Err("brokerDedup layout must use at least one bit".into());
        }
        Ok(Self { bits })
    }

    /// Bits per component: the three position components, then the index.
    #[must_use]
    pub fn bits(&self) -> [u8; 4] { self.bits }

    /// Pack four components; fails if one does not fit its width.
    pub fn pack(&self, components: [u64; 4]) -> Result<u64, String> {
        const NAMES: [&str; 4] = ["position.0", "position.1", "position.2", "index"];
        let mut seq: u64 = 0;
        for (i, (&value, &bits)) in components.iter().zip(&self.bits).enumerate() {
            if bits < 64 && value >> bits != 0 {
                return Err(format!(
                    "brokerDedup: {} = {value} does not fit in {bits} bits (layout {:?}); \
                     widen the layout under a new producer name",
                    NAMES[i], self.bits
                ));
            }
            seq = (seq << bits) | value;
        }
        Ok(seq)
    }

    /// Sequence id of the next element of the stream, whose origin position
    /// is `position`. Advances `state`.
    ///
    /// A position lower than the previous one means the stream restarted
    /// (re-emission): the index restarts at zero, exactly as a fresh sink
    /// would do, and the broker drops what it already holds.
    pub fn next(&self, state: &mut SequenceState, position: Position) -> Result<u64, String> {
        let index = match state.last {
            Some(last) if last == position => state.next_index,
            _ => 0,
        };
        let seq = self.pack([position.0, position.1, position.2, index])?;
        state.last = Some(position);
        state.next_index = index + 1;
        Ok(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_bounds() {
        assert!(SequenceLayout::new([24, 24, 7], 8).is_ok());
        assert!(SequenceLayout::new([32, 24, 7], 8).is_err());
        assert!(SequenceLayout::new([0, 0, 0], 0).is_err());
        assert!(SequenceLayout::new([0, 63, 0], 0).is_ok());
    }

    #[test]
    fn packing_is_lexicographic_and_checked() {
        let layout = SequenceLayout::new([20, 20, 10], 8).unwrap();
        let a = layout.pack([1, 2, 3, 4]).unwrap();
        assert_eq!(a, (1 << 38) | (2 << 18) | (3 << 8) | 4);
        // Order follows (position, index).
        let samples = [[1, 2, 3, 4], [1, 2, 3, 5], [1, 2, 4, 0], [1, 3, 0, 0], [2, 0, 0, 0]];
        let seqs: Vec<u64> = samples.iter().map(|c| layout.pack(*c).unwrap()).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
        // Overflow of any component is an error, not a wrap.
        assert!(layout.pack([1 << 20, 0, 0, 0]).is_err());
        assert!(layout.pack([0, 0, 1 << 10, 0]).is_err());
        assert!(layout.pack([0, 0, 0, 256]).is_err());
        // A zero-width component must be zero.
        let offsets = SequenceLayout::new([0, 55, 0], 8).unwrap();
        assert_eq!(offsets.pack([0, 7, 0, 1]).unwrap(), (7 << 8) | 1);
        assert!(offsets.pack([1, 7, 0, 0]).is_err());
        assert!(offsets.pack([0, 7, 1, 0]).is_err());
        // The maximum stays a non-negative i64.
        let max = SequenceLayout::new([21, 21, 21], 0)
            .unwrap()
            .pack([(1 << 21) - 1, (1 << 21) - 1, (1 << 21) - 1, 0])
            .unwrap();
        assert_eq!(max, (1 << 63) - 1);
    }

    #[test]
    fn indexes_elements_sharing_a_position() {
        let layout = SequenceLayout::new([24, 24, 7], 8).unwrap();
        let mut state = SequenceState::default();
        let p1 = Position(5, 9, 0);
        let p2 = Position(5, 10, 1);
        let first: Vec<u64> = [p1, p1, p1, p2, p2]
            .iter()
            .map(|p| layout.next(&mut state, *p).unwrap())
            .collect();
        assert!(first.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(first[0] & 0xff, 0);
        assert_eq!(first[2] & 0xff, 2);
        assert_eq!(first[3] & 0xff, 0);
        // Re-emission from p1 with a fresh state, or after a rewind of the
        // same state, yields exactly the same ids.
        let mut fresh = SequenceState::default();
        let replay: Vec<u64> = [p1, p1, p1, p2, p2]
            .iter()
            .map(|p| layout.next(&mut fresh, *p).unwrap())
            .collect();
        assert_eq!(first, replay);
        let rewound: Vec<u64> = [p1, p1, p1, p2, p2]
            .iter()
            .map(|p| layout.next(&mut state, *p).unwrap())
            .collect();
        assert_eq!(first, rewound);
    }

    #[test]
    fn index_overflow_is_reported() {
        let layout = SequenceLayout::new([24, 24, 7], 1).unwrap();
        let mut state = SequenceState::default();
        let p = Position(1, 1, 0);
        assert!(layout.next(&mut state, p).is_ok());
        assert!(layout.next(&mut state, p).is_ok());
        assert!(layout.next(&mut state, p).is_err());
    }
}
