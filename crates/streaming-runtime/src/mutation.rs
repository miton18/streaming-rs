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
    /// A slice entering a running pipeline starts without the watermark of
    /// the slices already there.
    JoinWithoutWatermark,
    /// The emitter forgets the slices whose partition is gone.
    EmitWithoutRetired,
    /// A graph being deleted is purged without closing its logs first, nor
    /// waiting for the commands in flight to land.
    DeleteWithoutClosing,
    /// A worker runs the graphs that the store describes otherwise than its
    /// code declares them, as the binary a deployment is replacing would.
    RunAnotherDeclaration,
    /// The controller takes the word of the index entry for a graph being
    /// deleted, and ignores the mark of the deletion.
    DeleteByTheIndexEntry,
    /// A worker says which graphs it steps aside from by their index entry,
    /// without looking at the description it would refuse to run.
    AsideByTheIndexEntry,
    /// A worker takes the word of the index entry for a graph being
    /// deleted, and runs it again when the entry is written back.
    RunWhatIsBeingDeleted,
    /// The log of a retired slice is closed without taking the epoch after
    /// the commits found emitted.
    ///
    /// Not in [`Mutant::ALL`] yet: the race it opens (an instance left behind
    /// committing between the drain check and the closing) shows in about
    /// one seed in a thousand of the repartition scenarios, one in eighty
    /// with six-second store stalls, too rarely for the bench. Plant it by
    /// hand until a scenario makes it reliable.
    CloseWithoutClaim,
}

impl Mutant {
    /// Every mutant.
    pub const ALL: [Self; 15] = [
        Self::CommitWithoutFencing,
        Self::CommitBelowTheSeal,
        Self::AckBeforeCommit,
        Self::EmitWithoutOwnership,
        Self::ForgetAtOnce,
        Self::RestartWithoutWatermark,
        Self::RestoreWithoutPending,
        Self::EmptyWithoutCopy,
        Self::JoinWithoutWatermark,
        Self::EmitWithoutRetired,
        Self::DeleteWithoutClosing,
        Self::RunAnotherDeclaration,
        Self::DeleteByTheIndexEntry,
        Self::AsideByTheIndexEntry,
        Self::RunWhatIsBeingDeleted,
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
