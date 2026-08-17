/// Physical source order used when exact mutation sequences tie.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum VersionSource {
    /// Exact per-entry ordering is unavailable; table id orders generations.
    Legacy(u64),
    /// Table id deterministically breaks ties between exact sequences.
    Versioned(u64),
    /// Memory beats persisted copies; newer memory generations break ties.
    Memory(u64),
}

/// The single arbitration key shared by point reads and streaming scans.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VersionOrder {
    sequence: u64,
    source: VersionSource,
}

impl VersionOrder {
    pub(crate) fn memory(sequence: u64, generation_rank: u64) -> Self {
        Self {
            sequence,
            source: VersionSource::Memory(generation_rank),
        }
    }

    pub(crate) fn sstable(sequence: Option<u64>, table_id: u64) -> Self {
        sequence.map_or(
            Self {
                sequence: 0,
                source: VersionSource::Legacy(table_id),
            },
            |sequence| Self {
                sequence,
                source: VersionSource::Versioned(table_id),
            },
        )
    }
}
