//! Bugs planted on purpose, to check that the simulation notices them.
//!
//! A simulation that passes says little unless it is known to fail when the
//! engine is wrong. Each [`Mutant`] removes one precaution of the protocol;
//! `streaming-sim` plants them one at a time and requires its scenarios to
//! notice. They are only compiled in with the `mutants` feature: a production
//! build cannot carry one.

/// One precaution of the engine, removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Mutant {
    /// A commit that lost the race for its epoch is taken as won.
    CommitWithoutFencing = 1,
    /// A committer does not look at the seal after writing its record.
    CommitBelowTheSeal,
    /// The source is acknowledged as soon as an epoch is read, before its
    /// commit.
    AckBeforeCommit,
    /// The emitter does not check that it still owns its slice after reading
    /// the state of a window.
    EmitWithoutOwnership,
    /// The index entry of an emitted window is removed at once.
    ForgetAtOnce,
    /// A restarting slice forgets its watermark.
    RestartWithoutWatermark,
    /// A restarting slice ignores the commits that are not copied out yet.
    RestoreWithoutPending,
    /// An instance taking a slice over empties the records it inherits
    /// without copying them out.
    EmptyWithoutCopy,
}

impl Mutant {
    /// Every mutant.
    pub const ALL: [Self; 8] = [
        Self::CommitWithoutFencing,
        Self::CommitBelowTheSeal,
        Self::AckBeforeCommit,
        Self::EmitWithoutOwnership,
        Self::ForgetAtOnce,
        Self::RestartWithoutWatermark,
        Self::RestoreWithoutPending,
        Self::EmptyWithoutCopy,
    ];
}

#[cfg(feature = "mutants")]
static PLANTED: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Plant `mutant` in this process, or remove the one planted.
#[cfg(feature = "mutants")]
pub fn plant(mutant: Option<Mutant>) {
    PLANTED.store(
        mutant.map_or(0, |mutant| mutant as u8),
        std::sync::atomic::Ordering::SeqCst,
    );
}

/// Whether `mutant` is planted. Always `false` without the `mutants` feature.
#[inline]
pub(crate) fn planted(mutant: Mutant) -> bool {
    #[cfg(feature = "mutants")]
    {
        PLANTED.load(std::sync::atomic::Ordering::Relaxed) == mutant as u8
    }
    #[cfg(not(feature = "mutants"))]
    {
        let _ = mutant;
        false
    }
}
