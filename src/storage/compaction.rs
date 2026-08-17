//! # Compaction
//!
//! Background compaction merges small SSTables into larger ones,
//! removing deleted/overwritten data and bounding file count.
//!
//! ## Strategy: Size-Tiered with Aggressive L0
//!
//! For SecOps workloads (bursty writes during incidents):
//! - L0: Flush target, compact when 4+ files accumulate
//! - L1-L6: 10x size multiplier per level
//! - Streaming merge to handle large files without RAM pressure

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::Bytes;
#[cfg(test)]
use parking_lot::Condvar;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock as AsyncRwLock};
use tracing::{debug, info, warn};

use crate::core::error::{Error, Result};
use crate::core::CompactionResult as PublicCompactionResult;

use super::directory_lock::DirectoryLock;
use super::engine::{Result as EngineResult, SstableStatistics, StorageError};
use super::manifest::{sync_directory, Manifest, SSTableManifestEntry};
use super::memtable::MemTableManager;
use super::sstable::{
    OutputAppendDecision, SSTableConfig, SSTableEntry, SSTableInfo, SSTableReader, SSTableWriter,
};
use super::version::VersionOrder;
use super::InProgressGuard;

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Max SSTables at L0 before triggering compaction
    pub l0_compaction_trigger: usize,
    /// Max levels (typically 7)
    pub max_levels: u32,
    /// Size multiplier between levels (typically 10)
    pub level_size_multiplier: u64,
    /// Target file size for L1+ (64MB default)
    pub target_file_size: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            max_levels: 7,
            level_size_multiplier: 10,
            target_file_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

/// Compaction job descriptor
#[derive(Debug, Clone)]
pub struct CompactionJob {
    pub input_sstables: Vec<SSTableManifestEntry>,
    pub output_level: u32,
    /// Caller-visible output path used by the deprecated single-output seam.
    pub output_path: PathBuf,
}

/// Result of the deprecated single-output compaction execution seam.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub input_ids: Vec<u64>,
    pub output_sstable: Option<SSTableManifestEntry>,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub entries_merged: u64,
    pub entries_dropped: u64,
    /// Tombstone versions among [`Self::entries_dropped`]. Winning tombstones
    /// remain current versions and are not counted as reclaimed.
    pub tombstones_dropped: u64,
    /// Keys that survived compaction (for vector index filtering)
    pub live_keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(super) struct CompactionSelection {
    pub(super) input_sstables: Vec<SSTableManifestEntry>,
    pub(super) output_level: u32,
}

#[derive(Debug, Clone)]
pub(super) struct CompactionExecution {
    pub(super) input_ids: Vec<u64>,
    pub(super) output_sstables: Vec<SSTableManifestEntry>,
    pub(super) bytes_read: u64,
    pub(super) bytes_written: u64,
    entries_merged: u64,
    pub(super) entries_dropped: u64,
    pub(super) tombstones_dropped: u64,
    live_keys: Vec<Vec<u8>>,
}

struct CompactionExecutionOptions {
    target_file_size: u64,
    first_output: Option<CompactionOutputIdentity>,
    require_output: bool,
    tombstone_reclamation_frontier: TombstoneReclamationFrontier,
}

#[derive(Clone, Copy)]
struct TombstoneReclamationFrontier(Option<u64>);

impl TombstoneReclamationFrontier {
    const RETAIN_ALL: Self = Self(None);

    const fn captured(first_sequence_outside_inputs: u64) -> Self {
        Self(Some(first_sequence_outside_inputs))
    }

    fn can_reclaim(self, tombstone_sequence: u64) -> bool {
        self.0.is_some_and(|frontier| tombstone_sequence < frontier)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SstableIdentity {
    id: u64,
    path: PathBuf,
}

impl From<&SSTableManifestEntry> for SstableIdentity {
    fn from(table: &SSTableManifestEntry) -> Self {
        Self {
            id: table.id,
            path: table.path.clone(),
        }
    }
}

#[derive(Default)]
struct TombstoneSequenceCache {
    minimum_sequences: HashMap<SstableIdentity, Option<u64>>,
    #[cfg(test)]
    scan_attempts: u64,
}

impl TombstoneSequenceCache {
    fn reclaimable_table_ids(
        &mut self,
        live_identities: &HashSet<SstableIdentity>,
        candidates: &[SSTableManifestEntry],
        frontier: TombstoneReclamationFrontier,
    ) -> EngineResult<HashSet<u64>> {
        self.minimum_sequences
            .retain(|identity, _| live_identities.contains(identity));
        let mut reclaimable = HashSet::new();
        for table in candidates {
            let identity = SstableIdentity::from(table);
            let minimum = match self.minimum_sequences.get(&identity).copied() {
                Some(minimum) => minimum,
                None => {
                    #[cfg(test)]
                    {
                        self.scan_attempts = self.scan_attempts.saturating_add(1);
                    }
                    let minimum = minimum_sstable_tombstone_sequence(&table.path)?;
                    self.minimum_sequences.insert(identity, minimum);
                    minimum
                }
            };
            if minimum.is_some_and(|sequence| frontier.can_reclaim(sequence)) {
                reclaimable.insert(table.id);
                break;
            }
        }
        Ok(reclaimable)
    }
}

struct CompactionOutputIdentity {
    id: u64,
    path: PathBuf,
    creation: CompactionOutputCreation,
}

enum CompactionOutputCreation {
    ClaimUnique,
    CallerOwned,
}

struct PendingCompactionOutput {
    id: u64,
    writer: SSTableWriter,
}

/// Selects and executes the SSTable merge portion of a coordinated compaction.
pub struct Compactor {
    config: CompactionConfig,
    sstable_config: SSTableConfig,
    data_dir: PathBuf,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    exact_projection_counter: Arc<AtomicU64>,
}

impl Compactor {
    pub fn new(
        config: CompactionConfig,
        sstable_config: SSTableConfig,
        data_dir: PathBuf,
        next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            config,
            sstable_config,
            data_dir,
            next_sstable_id,
            #[cfg(test)]
            exact_projection_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    fn exact_projection_count(&self) -> u64 {
        self.exact_projection_counter.load(Ordering::Relaxed)
    }

    /// Check if compaction is needed and return a legacy single-output job.
    ///
    /// New code should use [`crate::Db::compact`]. This adapter preserves the
    /// pre-0.5 low-level API and allocates its historical caller-visible path;
    /// coordinator selection remains side-effect free.
    #[deprecated(note = "use Db::compact for coordinated multi-output compaction")]
    pub fn pick_compaction(&self, sstables: &[SSTableManifestEntry]) -> Option<CompactionJob> {
        self.pick_compaction_selection(sstables)
            .map(|selection| CompactionJob {
                input_sstables: selection.input_sstables,
                output_level: selection.output_level,
                output_path: self.new_sstable_identity().1,
            })
    }

    fn pick_compaction_selection(
        &self,
        sstables: &[SSTableManifestEntry],
    ) -> Option<CompactionSelection> {
        let scope = sstables.iter().map(|table| table.id).collect();
        self.pick_compaction_in_scope(sstables, &scope)
    }

    /// Select work whose source pressure belongs to `scope`, expanding the
    /// chosen job through every currently live overlapping table for safety.
    fn pick_compaction_in_scope(
        &self,
        sstables: &[SSTableManifestEntry],
        scope: &HashSet<u64>,
    ) -> Option<CompactionSelection> {
        if self.config.max_levels < 2 {
            return None;
        }

        // Priority 1: L0 compaction (keeps write path fast)
        let mut l0_tables: Vec<_> = sstables
            .iter()
            .filter(|s| s.level == 0 && scope.contains(&s.id))
            .cloned()
            .collect();
        l0_tables.sort_by_key(|table| table.id);

        if !l0_tables.is_empty() && l0_tables.len() >= self.config.l0_compaction_trigger {
            let (input_sstables, output_level) =
                complete_overlap_closure(sstables, l0_tables, 1, self.config.max_levels);
            return Some(CompactionSelection {
                input_sstables,
                output_level,
            });
        }

        // Priority 2: Level compaction (when level exceeds size budget)
        let highest_output_level = self.config.max_levels - 1;
        for level in 1..highest_output_level {
            let level_tables: Vec<_> = sstables
                .iter()
                .filter(|s| s.level == level && scope.contains(&s.id))
                .cloned()
                .collect();
            let level_size: u64 = level_tables.iter().map(|s| s.size).sum();
            let max_size = self.max_level_size(level);

            if level_size > max_size && level_tables.len() >= 2 {
                // Pick oldest tables at this level
                let mut sorted = level_tables;
                sorted.sort_by_key(|s| (s.creation_time, s.id));
                let to_compact: Vec<_> = sorted.into_iter().take(4).collect();

                let (input_sstables, output_level) = complete_overlap_closure(
                    sstables,
                    to_compact,
                    level + 1,
                    self.config.max_levels,
                );
                return Some(CompactionSelection {
                    input_sstables,
                    output_level,
                });
            }
        }

        None
    }

    /// Select one component whose seed has an exactly identified reclaimable
    /// tombstone. The rewrite is therefore useful without size pressure, even
    /// when unrelated entries in the same table are newer than the frontier.
    fn pick_tombstone_reclamation_in_scope(
        &self,
        sstables: &[SSTableManifestEntry],
        scope: &HashSet<u64>,
        reclaimable_table_ids: &HashSet<u64>,
    ) -> Option<CompactionSelection> {
        if self.config.max_levels == 0 {
            return None;
        }
        let seed = sstables
            .iter()
            .filter(|table| scope.contains(&table.id) && reclaimable_table_ids.contains(&table.id))
            .min_by_key(|table| (table.level, table.creation_time, table.id))?
            .clone();
        let requested_output_level = seed.level.saturating_add(1).min(self.config.max_levels - 1);
        let (input_sstables, output_level) = complete_overlap_closure(
            sstables,
            vec![seed],
            requested_output_level,
            self.config.max_levels,
        );
        Some(CompactionSelection {
            input_sstables,
            output_level,
        })
    }

    /// Execute one legacy, caller-named, single-output compaction.
    ///
    /// This preserves the pre-0.5 low-level API. It does not coordinate with
    /// [`crate::Db::compact`] or publish a manifest, so new code should use the
    /// database method instead.
    #[deprecated(note = "use Db::compact for coordinated multi-output compaction")]
    pub fn execute(&self, job: CompactionJob) -> Result<CompactionResult> {
        let output_id = sstable_id_from_path(&job.output_path);
        let selection = CompactionSelection {
            input_sstables: job.input_sstables,
            output_level: job.output_level,
        };
        let mut output_attempts = CompactionOutputAttempts::new(Arc::new(Mutex::new(Vec::new())));
        let execution = self
            .execute_selection(
                selection,
                &mut output_attempts,
                CompactionExecutionOptions {
                    target_file_size: u64::MAX,
                    first_output: Some(CompactionOutputIdentity {
                        id: output_id,
                        path: job.output_path,
                        creation: CompactionOutputCreation::CallerOwned,
                    }),
                    require_output: true,
                    tombstone_reclamation_frontier: TombstoneReclamationFrontier::RETAIN_ALL,
                },
            )
            .map_err(|error| Error::Compaction {
                reason: error.to_string(),
            })?;
        output_attempts.mark_manifest_committed();
        debug_assert!(execution.output_sstables.len() <= 1);
        Ok(CompactionResult {
            input_ids: execution.input_ids,
            output_sstable: execution.output_sstables.into_iter().next(),
            bytes_read: execution.bytes_read,
            bytes_written: execution.bytes_written,
            entries_merged: execution.entries_merged,
            entries_dropped: execution.entries_dropped,
            tombstones_dropped: execution.tombstones_dropped,
            live_keys: execution.live_keys,
        })
    }

    /// Execute a coordinated compaction selection using streaming merge.
    fn execute_selection(
        &self,
        job: CompactionSelection,
        output_attempts: &mut CompactionOutputAttempts,
        mut options: CompactionExecutionOptions,
    ) -> EngineResult<CompactionExecution> {
        info!(
            "Starting compaction: {} inputs -> L{}",
            job.input_sstables.len(),
            job.output_level
        );

        let mut bytes_read = 0u64;
        let mut entries_merged = 0u64;
        let mut entries_dropped = 0u64;
        let mut tombstones_dropped = 0u64;

        // Open all input SSTables.
        let readers: Vec<SSTableReader> = job
            .input_sstables
            .iter()
            .map(|entry| {
                bytes_read += entry.size;
                SSTableReader::open(&entry.path)
            })
            .collect::<Result<Vec<_>>>()
            .map_err(compaction_error)?;

        // Streaming k-way merge
        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        let mut iterators: Vec<_> = readers.iter().map(|r| r.iter()).collect();

        // Initialize heap with first entry from each iterator
        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some(result) = iter.next_versioned() {
                let (key, entry) = result.map_err(compaction_error)?;
                heap.push(Reverse(MergeEntry::new(
                    key,
                    entry,
                    &job.input_sstables[idx],
                    idx,
                )));
            }
        }

        let mut last_key: Option<Bytes> = None;
        let mut live_keys: Vec<Vec<u8>> = Vec::new();
        let mut pending_output: Option<PendingCompactionOutput> = None;
        let mut output_sstables = Vec::new();

        while let Some(Reverse(entry)) = heap.pop() {
            // Deduplicate: keep only the newest version of each key
            let is_duplicate = last_key.as_ref().map(|k| k == &entry.key).unwrap_or(false);

            if is_duplicate {
                entries_dropped += 1;
                if entry.value.is_none() {
                    tombstones_dropped += 1;
                }
            } else {
                last_key = Some(entry.key.clone());
                let reclaim_tombstone = entry.value.is_none()
                    && options
                        .tombstone_reclamation_frontier
                        .can_reclaim(entry.sequence);
                if reclaim_tombstone {
                    entries_dropped += 1;
                    tombstones_dropped += 1;
                } else {
                    let append_decision = if let Some(output) = &pending_output {
                        if output.writer.is_empty() {
                            OutputAppendDecision::Append
                        } else {
                            output
                                .writer
                                .decide_target_size(
                                    &entry.key,
                                    entry.value.as_deref(),
                                    entry.sequence,
                                    options.target_file_size,
                                )
                                .map_err(compaction_error)?
                        }
                    } else {
                        OutputAppendDecision::Append
                    };
                    if append_decision == OutputAppendDecision::SplitBefore {
                        let completed = pending_output
                            .take()
                            .expect("a split is requested only for a nonempty output");
                        output_sstables.push(self.finish_output(job.output_level, completed)?);
                    }
                    if pending_output.is_none() {
                        pending_output =
                            Some(self.start_output(output_attempts, &mut options.first_output)?);
                    }
                    pending_output
                        .as_mut()
                        .expect("an output writer was just created")
                        .writer
                        .add_versioned(&entry.key, entry.value.as_deref(), entry.sequence)
                        .map_err(compaction_error)?;
                    if append_decision == OutputAppendDecision::AppendAndSeal {
                        let completed = pending_output
                            .take()
                            .expect("an appended output is present to seal");
                        output_sstables.push(self.finish_output(job.output_level, completed)?);
                    }
                    if entry.value.is_some() {
                        live_keys.push(entry.key.to_vec());
                    }
                    entries_merged += 1;
                }
            }

            // Advance the iterator that provided this entry
            if let Some(result) = iterators[entry.source].next_versioned() {
                let (key, next_entry) = result.map_err(compaction_error)?;
                heap.push(Reverse(MergeEntry::new(
                    key,
                    next_entry,
                    &job.input_sstables[entry.source],
                    entry.source,
                )));
            }
        }

        if let Some(completed) = pending_output {
            output_sstables.push(self.finish_output(job.output_level, completed)?);
        }
        if output_sstables.is_empty() && options.require_output {
            let empty = self.start_output(output_attempts, &mut options.first_output)?;
            output_sstables.push(self.finish_output(job.output_level, empty)?);
        }
        let bytes_written = output_sstables
            .iter()
            .fold(0_u64, |total, output| total.saturating_add(output.size));
        if options.require_output {
            debug_assert_eq!(output_sstables.len(), 1);
        } else {
            validate_compaction_outputs(&output_sstables)?;
        }

        let input_ids: Vec<u64> = job.input_sstables.iter().map(|s| s.id).collect();

        info!(
            "Compaction complete: {} entries merged, {} dropped, {:.2}MB -> {:.2}MB",
            entries_merged,
            entries_dropped,
            bytes_read as f64 / 1024.0 / 1024.0,
            bytes_written as f64 / 1024.0 / 1024.0
        );

        Ok(CompactionExecution {
            input_ids,
            output_sstables,
            bytes_read,
            bytes_written,
            entries_merged,
            entries_dropped,
            tombstones_dropped,
            live_keys,
        })
    }

    fn start_output(
        &self,
        output_attempts: &mut CompactionOutputAttempts,
        first_output: &mut Option<CompactionOutputIdentity>,
    ) -> EngineResult<PendingCompactionOutput> {
        let CompactionOutputIdentity { id, path, creation } = first_output
            .take()
            .unwrap_or_else(|| self.new_sstable_identity().into());
        if matches!(creation, CompactionOutputCreation::ClaimUnique) {
            output_attempts.claim(path.clone())?;
        }
        #[allow(unused_mut)]
        let mut writer =
            SSTableWriter::new(&path, self.sstable_config.clone()).map_err(compaction_error)?;
        if matches!(creation, CompactionOutputCreation::CallerOwned) {
            output_attempts.track(path);
        }
        #[cfg(test)]
        writer.set_exact_projection_counter(Arc::clone(&self.exact_projection_counter));
        Ok(PendingCompactionOutput { id, writer })
    }

    fn finish_output(
        &self,
        output_level: u32,
        pending: PendingCompactionOutput,
    ) -> EngineResult<SSTableManifestEntry> {
        let output_info = pending.writer.finish().map_err(compaction_error)?;

        #[cfg(test)]
        super::failpoints::check(
            &self.data_dir,
            super::failpoints::PersistenceBoundary::CompactionOutputPublication,
        )?;

        Ok(SSTableManifestEntry {
            id: pending.id,
            level: output_level,
            path: output_info.path,
            size: output_info.file_size,
            entry_count: output_info.entry_count,
            tombstone_count: output_info.tombstone_count,
            min_key: output_info.min_key,
            max_key: output_info.max_key,
            min_sequence: output_info.min_sequence,
            max_sequence: output_info.max_sequence,
            creation_time: output_info.creation_time,
        })
    }

    /// Delete old SSTable files after successful compaction
    pub fn cleanup_inputs(&self, paths: &[PathBuf]) -> Result<()> {
        for path in paths {
            if remove_sstable_if_present(path).map_err(|e| Error::Io {
                message: format!("Failed to delete compacted SSTable: {:?}", path),
                source: e,
            })? {
                debug!("Deleted compacted SSTable: {:?}", path);
            }
        }
        Ok(())
    }

    fn max_level_size(&self, level: u32) -> u64 {
        // L1 = target_file_size * 10, L2 = L1 * 10, etc.
        self.config.target_file_size * self.config.level_size_multiplier.pow(level)
    }

    fn new_sstable_identity(&self) -> (u64, PathBuf) {
        let id = self
            .next_sstable_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let path = self
            .data_dir
            .join("sstables")
            .join(format!("{}_{}.sst", id, timestamp));
        (id, path)
    }
}

impl From<(u64, PathBuf)> for CompactionOutputIdentity {
    fn from((id, path): (u64, PathBuf)) -> Self {
        Self {
            id,
            path,
            creation: CompactionOutputCreation::ClaimUnique,
        }
    }
}

fn compaction_error(error: Error) -> StorageError {
    StorageError::Compaction(error.to_string())
}

fn minimum_sstable_tombstone_sequence(path: &std::path::Path) -> EngineResult<Option<u64>> {
    let reader = SSTableReader::open(path).map_err(compaction_error)?;
    let mut minimum: Option<u64> = None;
    let mut entries = reader.iter();
    while let Some(entry) = entries.next_versioned() {
        let (_, entry) = entry.map_err(compaction_error)?;
        if entry.value.into_option().is_none() {
            let sequence = entry.sequence.unwrap_or(0);
            minimum = Some(minimum.map_or(sequence, |current| current.min(sequence)));
        }
    }
    Ok(minimum)
}

/// The manifest checkpoint is the next sequence recovery may replay, not the
/// last sequence already covered. Keep this named to make the inclusive WAL
/// iterator boundary explicit and avoid an overflowing `checkpoint + 1`.
const fn first_wal_replayable_sequence(checkpoint: u64) -> u64 {
    checkpoint
}

fn sstable_id_from_path(path: &std::path::Path) -> u64 {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.split('_').next())
        .and_then(|id| id.parse().ok())
        .unwrap_or(0)
}

/// Remove one SSTable using the shared cleanup policy.
///
/// A missing path is already clean. Other failures remain visible to callers
/// so coordinated cleanup can defer and retry them.
pub(super) fn remove_sstable_if_present(path: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn validate_compaction_outputs(outputs: &[SSTableManifestEntry]) -> EngineResult<()> {
    let mut ids = HashSet::with_capacity(outputs.len());
    let mut paths = HashSet::with_capacity(outputs.len());
    for output in outputs {
        if output.entry_count == 0 {
            return Err(StorageError::Compaction(
                "compaction produced an empty SSTable".to_string(),
            ));
        }
        if !ids.insert(output.id) || !paths.insert(&output.path) {
            return Err(StorageError::Compaction(
                "compaction produced duplicate output identities".to_string(),
            ));
        }
    }
    if outputs
        .windows(2)
        .any(|adjacent| adjacent[0].max_key >= adjacent[1].min_key)
    {
        return Err(StorageError::Compaction(
            "compaction outputs are not strictly ordered and nonoverlapping".to_string(),
        ));
    }
    Ok(())
}

/// Expand a source seed to the complete connected overlap component.
///
/// Source/target closure is required for leveled compaction. We deliberately
/// extend the fixed point across every level: legacy SSTables have no exact
/// per-entry sequence, so leaving an overlapping legacy table at a deeper
/// level could make the newly allocated output id look newer than its actual
/// source. Once this closure finishes, no unselected persisted table overlaps
/// the output range. A flush racing the job can only add current-format entries
/// with engine-wide sequences and is preserved separately at publication.
fn complete_overlap_closure(
    all: &[SSTableManifestEntry],
    seed: Vec<SSTableManifestEntry>,
    requested_output_level: u32,
    max_levels: u32,
) -> (Vec<SSTableManifestEntry>, u32) {
    let mut selected: HashSet<u64> = seed.iter().map(|table| table.id).collect();
    let mut min_key = seed
        .iter()
        .map(|table| table.min_key.as_slice())
        .min()
        .unwrap_or_default()
        .to_vec();
    let mut max_key = seed
        .iter()
        .map(|table| table.max_key.as_slice())
        .max()
        .unwrap_or_default()
        .to_vec();

    loop {
        let mut expanded = false;
        for table in all {
            if selected.contains(&table.id)
                || table.max_key.as_slice() < min_key.as_slice()
                || table.min_key.as_slice() > max_key.as_slice()
            {
                continue;
            }
            selected.insert(table.id);
            if table.min_key < min_key {
                min_key.clone_from(&table.min_key);
            }
            if table.max_key > max_key {
                max_key.clone_from(&table.max_key);
            }
            expanded = true;
        }
        if !expanded {
            break;
        }
    }

    let mut inputs = all
        .iter()
        .filter(|table| selected.contains(&table.id))
        .cloned()
        .collect::<Vec<_>>();
    inputs.sort_by_key(|table| (table.level, table.id));
    let highest_selected_level = inputs
        .iter()
        .map(|table| table.level)
        .max()
        .unwrap_or(requested_output_level);
    let output_level = requested_output_level
        .max(highest_selected_level)
        .min(max_levels.saturating_sub(1));
    (inputs, output_level)
}

/// Owns the complete compaction lifecycle shared by manual and background
/// requests. One fair lock is the input claim: queued requests reselect only
/// after the preceding publication has completed, so duplicate claims cannot
/// exist. Callers cancelled while waiting leave Tokio's queue normally. Once a
/// caller acquires the claim, ownership transfers to a job whose lifetime is
/// independent of the waiter, so cancellation cannot interrupt durable/live
/// publication.
pub(super) struct CompactionCoordinator {
    ownership: Arc<AsyncMutex<()>>,
    accepting_requests: AtomicBool,
    compactor: Compactor,
    data_dir: PathBuf,
    directory_lock: Weak<DirectoryLock>,
    sstables: Arc<AsyncRwLock<Vec<SSTableInfo>>>,
    manifest: Arc<AsyncMutex<Manifest>>,
    tombstone_reclamation_sources: TombstoneReclamationSources,
    tombstone_sequence_cache: Arc<Mutex<TombstoneSequenceCache>>,
    sstable_stats: Arc<SstableStatistics>,
    compactions_in_progress: Arc<AtomicU64>,
    deferred_cleanup: Arc<Mutex<Vec<PathBuf>>>,
    #[cfg(test)]
    before_manifest_gate: Mutex<Option<Arc<CompactionTestGate>>>,
    #[cfg(test)]
    during_manifest_gate: Mutex<Option<Arc<CompactionTestGate>>>,
    #[cfg(test)]
    after_manifest_gate: Mutex<Option<Arc<CompactionTestGate>>>,
}

/// Live layers which can publish or replay a version after compaction captures
/// its persisted inputs.
pub(super) struct TombstoneReclamationSources {
    memtable_manager: Arc<MemTableManager>,
    mutation_barrier: Arc<AsyncRwLock<()>>,
    unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
    wal_enabled: bool,
}

impl TombstoneReclamationSources {
    pub(super) fn new(
        memtable_manager: Arc<MemTableManager>,
        mutation_barrier: Arc<AsyncRwLock<()>>,
        unapplied_wal_sequences: Arc<Mutex<BTreeSet<u64>>>,
        wal_enabled: bool,
    ) -> Self {
        Self {
            memtable_manager,
            mutation_barrier,
            unapplied_wal_sequences,
            wal_enabled,
        }
    }

    async fn capture_frontier(
        &self,
        manifest: &AsyncMutex<Manifest>,
    ) -> EngineResult<TombstoneReclamationFrontier> {
        // Once this lock is released, every newly allocated mutation has a
        // sequence at or above the captured next sequence.
        let _mutations = self.mutation_barrier.write().await;
        if !self.wal_enabled {
            self.memtable_manager
                .flush_thread_local()
                .map_err(StorageError::MemTable)?;
        }

        let mut frontier = self.memtable_manager.current_sequence();
        if let Some(sequence) = self.memtable_manager.minimum_live_sequence() {
            frontier = frontier.min(sequence);
        }
        if let Some(sequence) = self.unapplied_wal_sequences.lock().first().copied() {
            frontier = frontier.min(sequence);
        }
        if self.wal_enabled {
            let checkpoint = manifest.lock().await.wal_checkpoint;
            frontier = frontier.min(first_wal_replayable_sequence(checkpoint));
        }
        Ok(TombstoneReclamationFrontier::captured(frontier))
    }
}

struct CapturedCompactionSelection {
    selection: CompactionSelection,
    tombstone_reclamation_frontier: TombstoneReclamationFrontier,
}

/// A manifest-installed generation waiting for the short asynchronous
/// live-reader/statistics publication step.
struct PreparedCompactionPublication {
    input_sstables: Vec<SSTableManifestEntry>,
    input_ids: HashSet<u64>,
    input_paths: Vec<PathBuf>,
    result: CompactionExecution,
    inputs_are_safe_to_unlink: bool,
    publication_error: Option<StorageError>,
}

#[derive(Clone, Copy)]
enum CompactionRequestKind {
    ManualDrain,
    BackgroundJob,
}

impl CompactionCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        config: CompactionConfig,
        sstable_config: SSTableConfig,
        data_dir: PathBuf,
        next_sstable_id: Arc<AtomicU64>,
        directory_lock: Weak<DirectoryLock>,
        sstables: Arc<AsyncRwLock<Vec<SSTableInfo>>>,
        manifest: Arc<AsyncMutex<Manifest>>,
        tombstone_reclamation_sources: TombstoneReclamationSources,
        sstable_stats: Arc<SstableStatistics>,
        compactions_in_progress: Arc<AtomicU64>,
        deferred_cleanup: Vec<PathBuf>,
    ) -> Self {
        Self {
            ownership: Arc::new(AsyncMutex::new(())),
            accepting_requests: AtomicBool::new(true),
            compactor: Compactor::new(config, sstable_config, data_dir.clone(), next_sstable_id),
            data_dir,
            directory_lock,
            sstables,
            manifest,
            tombstone_reclamation_sources,
            tombstone_sequence_cache: Arc::new(Mutex::new(TombstoneSequenceCache::default())),
            sstable_stats,
            compactions_in_progress,
            deferred_cleanup: Arc::new(Mutex::new(deferred_cleanup)),
            #[cfg(test)]
            before_manifest_gate: Mutex::new(None),
            #[cfg(test)]
            during_manifest_gate: Mutex::new(None),
            #[cfg(test)]
            after_manifest_gate: Mutex::new(None),
        }
    }

    /// Queue a compaction request and reselect against the state published by
    /// every earlier request. During graceful shutdown new requests are stable
    /// no-ops; a request already owning the claim runs to completion.
    pub(super) async fn request_manual(self: &Arc<Self>) -> EngineResult<PublicCompactionResult> {
        self.request(CompactionRequestKind::ManualDrain).await
    }

    pub(super) async fn request_background(
        self: &Arc<Self>,
    ) -> EngineResult<PublicCompactionResult> {
        self.request(CompactionRequestKind::BackgroundJob).await
    }

    async fn request(
        self: &Arc<Self>,
        kind: CompactionRequestKind,
    ) -> EngineResult<PublicCompactionResult> {
        let started = Instant::now();
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Ok(self.current_work_result(started).await);
        }

        let claim = Arc::clone(&self.ownership).lock_owned().await;
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Ok(self.current_work_result(started).await);
        }
        let Some(directory_lock) = self.directory_lock.upgrade() else {
            return Ok(self.current_work_result(started).await);
        };

        // Ownership and directory lifetime transfer before the caller reaches
        // another await point. Cancelling the waiter therefore detaches, but
        // never aborts, the accepted job. Shutdown drains `ownership`.
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            coordinator
                .run_owned(claim, directory_lock, kind, started)
                .await
        })
        .await
        .map_err(|error| {
            StorageError::Other(format!("coordinated compaction task failed: {error}"))
        })?
    }

    async fn current_work_result(&self, started: Instant) -> PublicCompactionResult {
        let snapshot = self.live_manifest_entries().await;
        PublicCompactionResult {
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            // Admission is already closed or directory ownership is gone. Do
            // not start an exact tombstone scan that can race active cleanup;
            // this path remains the documented stable metadata-only no-op.
            work_remaining: self
                .compactor
                .pick_compaction_selection(&snapshot)
                .is_some(),
            ..PublicCompactionResult::default()
        }
    }

    async fn run_owned(
        self: Arc<Self>,
        _claim: OwnedMutexGuard<()>,
        _directory_lock: Arc<DirectoryLock>,
        kind: CompactionRequestKind,
        started: Instant,
    ) -> EngineResult<PublicCompactionResult> {
        let _in_progress = InProgressGuard::new(Arc::clone(&self.compactions_in_progress));
        let initial = self.live_manifest_entries().await;
        let mut scope = initial.iter().map(|table| table.id).collect::<HashSet<_>>();
        let mut aggregate = PublicCompactionResult::default();
        self.prepare_claimed(None).await?;

        loop {
            let snapshot = self.live_manifest_entries().await;
            let ordinary_job = match kind {
                CompactionRequestKind::ManualDrain => {
                    self.compactor.pick_compaction_in_scope(&snapshot, &scope)
                }
                CompactionRequestKind::BackgroundJob => {
                    self.compactor.pick_compaction_selection(&snapshot)
                }
            };
            let (job, reclamation_only) = if let Some(job) = ordinary_job {
                (job, false)
            } else {
                let Some(job) = self
                    .select_tombstone_reclamation_in_scope(&snapshot, &scope)
                    .await?
                else {
                    break;
                };
                (job, true)
            };
            if !job
                .input_sstables
                .iter()
                .any(|input| input.level < job.output_level)
                && !reclamation_only
            {
                return Err(StorageError::Compaction(
                    "selected compaction job cannot advance any input".to_string(),
                ));
            }

            let prepared = self
                .prepare_claimed(Some(job))
                .await?
                .expect("a selected compaction job always prepares a publication");
            let result = self.publish_prepared(prepared).await?;
            for input_id in &result.input_ids {
                scope.remove(input_id);
            }
            if !reclamation_only {
                scope.extend(result.output_sstables.iter().map(|output| output.id));
            }
            aggregate.input_files = aggregate
                .input_files
                .saturating_add(u64::try_from(result.input_ids.len()).unwrap_or(u64::MAX));
            aggregate.output_files = aggregate
                .output_files
                .saturating_add(u64::try_from(result.output_sstables.len()).unwrap_or(u64::MAX));
            aggregate.bytes_read = aggregate.bytes_read.saturating_add(result.bytes_read);
            aggregate.bytes_written = aggregate.bytes_written.saturating_add(result.bytes_written);

            if matches!(kind, CompactionRequestKind::BackgroundJob) {
                break;
            }
        }

        aggregate.bytes_reclaimed = aggregate.bytes_read.saturating_sub(aggregate.bytes_written);
        aggregate.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let final_snapshot = self.live_manifest_entries().await;
        aggregate.work_remaining = self.has_global_selectable_work(&final_snapshot).await?;
        Ok(aggregate)
    }

    async fn has_global_selectable_work(
        &self,
        snapshot: &[SSTableManifestEntry],
    ) -> EngineResult<bool> {
        if self.compactor.pick_compaction_selection(snapshot).is_some() {
            return Ok(true);
        }
        let scope = snapshot
            .iter()
            .map(|table| table.id)
            .collect::<HashSet<_>>();
        Ok(self
            .select_tombstone_reclamation_in_scope(snapshot, &scope)
            .await?
            .is_some())
    }

    /// Cheaply reject an idle database before taking the mutation barrier.
    /// Once a tombstone exists, capture one proof frontier and reselect from a
    /// refreshed live list so selection and `work_remaining` share the same
    /// exact eligibility rule.
    async fn select_tombstone_reclamation_in_scope(
        &self,
        snapshot: &[SSTableManifestEntry],
        scope: &HashSet<u64>,
    ) -> EngineResult<Option<CompactionSelection>> {
        let has_tombstone_candidate = snapshot
            .iter()
            .any(|table| scope.contains(&table.id) && table.tombstone_count > 0);
        if !has_tombstone_candidate {
            return Ok(None);
        }
        let frontier = self
            .tombstone_reclamation_sources
            .capture_frontier(&self.manifest)
            .await?;
        let latest = self.live_manifest_entries().await;
        let reclaimable_table_ids = self
            .reclaimable_tombstone_table_ids(&latest, scope, frontier)
            .await?;
        Ok(self.compactor.pick_tombstone_reclamation_in_scope(
            &latest,
            scope,
            &reclaimable_table_ids,
        ))
    }

    /// Resolve immutable table metadata away from the async runtime. Cache
    /// keys include both id and path so a reused id cannot inherit a proof.
    /// The coordinator cache lock serializes scans across rejected concurrent
    /// callers; failed scans propagate without installing a cache entry.
    async fn reclaimable_tombstone_table_ids(
        &self,
        latest: &[SSTableManifestEntry],
        scope: &HashSet<u64>,
        frontier: TombstoneReclamationFrontier,
    ) -> EngineResult<HashSet<u64>> {
        let live_identities = latest
            .iter()
            .map(SstableIdentity::from)
            .collect::<HashSet<_>>();
        let mut candidates = latest
            .iter()
            .filter(|table| scope.contains(&table.id) && table.tombstone_count > 0)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|table| (table.level, table.creation_time, table.id));
        let cache = Arc::clone(&self.tombstone_sequence_cache);

        tokio::task::spawn_blocking(move || {
            let mut cache = cache.lock();
            cache.reclaimable_table_ids(&live_identities, &candidates, frontier)
        })
        .await
        .map_err(|error| {
            StorageError::Other(format!("tombstone metadata scan task failed: {error}"))
        })?
    }

    /// Permanently stop admitting requests. Waiting on the returned drain
    /// proves no previously claimed job can outlive shutdown/close. Repeated
    /// shutdown calls are idempotent.
    pub(super) fn pause_requests(&self) -> CompactionAdmissionPause<'_> {
        self.accepting_requests.store(false, Ordering::Release);
        CompactionAdmissionPause { coordinator: self }
    }

    async fn live_manifest_entries(&self) -> Vec<SSTableManifestEntry> {
        self.sstables
            .read()
            .await
            .iter()
            .map(|sstable| SSTableManifestEntry {
                id: sstable.id,
                level: sstable.level,
                path: sstable.path.clone(),
                size: sstable.file_size,
                entry_count: sstable.entry_count,
                tombstone_count: sstable.tombstone_count,
                min_key: sstable.min_key.clone(),
                max_key: sstable.max_key.clone(),
                min_sequence: sstable.min_sequence,
                max_sequence: sstable.max_sequence,
                creation_time: sstable.creation_time,
            })
            .collect()
    }

    /// Run cleanup plus the selected job's merge and durable manifest install
    /// away from async runtime workers. The owned coordinator claim remains in
    /// `run_owned` while this blocking task is awaited.
    async fn prepare_claimed(
        self: &Arc<Self>,
        job: Option<CompactionSelection>,
    ) -> EngineResult<Option<PreparedCompactionPublication>> {
        let job = match job {
            Some(job) => Some(self.capture_reclamation_safety(job).await?),
            None => None,
        };
        let coordinator = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            coordinator.retry_deferred_cleanup();
            job.map(|job| coordinator.execute_claimed_blocking(job))
                .transpose()
        })
        .await
        .map_err(|error| StorageError::Other(format!("compaction blocking task failed: {error}")))?
    }

    /// Capture the oldest sequence which can still arrive from outside this
    /// job, then refresh its persisted overlap closure. Memory is captured
    /// before SSTables because flush publishes the SSTable before releasing its
    /// immutable generation; that ordering covers either side of the handoff.
    async fn capture_reclamation_safety(
        &self,
        job: CompactionSelection,
    ) -> EngineResult<CapturedCompactionSelection> {
        let frontier = self
            .tombstone_reclamation_sources
            .capture_frontier(&self.manifest)
            .await?;

        let latest = self.live_manifest_entries().await;
        let selected_ids = job
            .input_sstables
            .iter()
            .map(|table| table.id)
            .collect::<HashSet<_>>();
        let seed = latest
            .iter()
            .filter(|table| selected_ids.contains(&table.id))
            .cloned()
            .collect::<Vec<_>>();
        if seed.len() != selected_ids.len() {
            // Explicit low-level test jobs can name absent inputs. They retain
            // tombstones and continue to the normal read/publication error.
            return Ok(CapturedCompactionSelection {
                selection: job,
                tombstone_reclamation_frontier: TombstoneReclamationFrontier::RETAIN_ALL,
            });
        }
        let (input_sstables, output_level) = complete_overlap_closure(
            &latest,
            seed,
            job.output_level,
            self.compactor.config.max_levels,
        );
        Ok(CapturedCompactionSelection {
            selection: CompactionSelection {
                input_sstables,
                output_level,
            },
            tombstone_reclamation_frontier: frontier,
        })
    }

    fn execute_claimed_blocking(
        &self,
        captured: CapturedCompactionSelection,
    ) -> EngineResult<PreparedCompactionPublication> {
        let CapturedCompactionSelection {
            selection: job,
            tombstone_reclamation_frontier,
        } = captured;
        let input_sstables = job.input_sstables.clone();
        let input_ids: HashSet<u64> = input_sstables.iter().map(|table| table.id).collect();
        let input_paths = input_sstables
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        let selected_input_bytes = input_sstables
            .iter()
            .fold(0_u64, |total, table| total.saturating_add(table.size));
        let mut output_attempts = CompactionOutputAttempts::new(Arc::clone(&self.deferred_cleanup));

        let execution = self.compactor.execute_selection(
            job,
            &mut output_attempts,
            CompactionExecutionOptions {
                target_file_size: self.compactor.config.target_file_size,
                first_output: None,
                require_output: false,
                tombstone_reclamation_frontier,
            },
        );
        self.sstable_stats
            .record_compaction_attempt(selected_input_bytes, output_attempts.produced_bytes());
        let result = execution?;

        if let Some(output) = result.output_sstables.first() {
            let output_directory = output.path.parent().ok_or_else(|| {
                StorageError::Compaction(format!(
                    "compaction output has no parent directory: {}",
                    output.path.display()
                ))
            })?;
            sync_directory(output_directory).map_err(|error| {
                StorageError::Compaction(format!(
                    "failed to durably publish compaction output directory {}: {error}",
                    output_directory.display()
                ))
            })?;
        }

        #[cfg(test)]
        if let Some(gate) = self.before_manifest_gate.lock().take() {
            gate.reach_and_wait();
        }

        let mut publication_error = None;
        let mut inputs_are_safe_to_unlink = true;
        {
            let mut live_manifest = self.manifest.blocking_lock();
            #[cfg(test)]
            if let Some(gate) = self.during_manifest_gate.lock().take() {
                gate.reach_and_wait();
            }
            if !input_ids
                .iter()
                .all(|id| live_manifest.sstables.iter().any(|table| table.id == *id))
            {
                return Err(StorageError::Compaction(
                    "claimed compaction inputs are no longer present in the manifest".to_string(),
                ));
            }

            let mut candidate = live_manifest.clone();
            candidate
                .sstables
                .retain(|table| !input_ids.contains(&table.id));
            candidate
                .sstables
                .extend(result.output_sstables.iter().cloned());

            #[cfg(test)]
            super::failpoints::check(
                &self.data_dir,
                super::failpoints::PersistenceBoundary::ManifestInstallation,
            )?;

            if let Err(save_error) = candidate.save(&self.data_dir) {
                let installed = Manifest::load_or_create(&self.data_dir)
                    .is_ok_and(|durable| manifests_have_same_identity(&durable, &candidate));
                if !installed {
                    return Err(StorageError::Manifest(save_error.to_string()));
                }

                // Rename may have succeeded before the directory sync failed.
                // Keep both generations if a second sync cannot prove the new
                // name durable; either the old or new manifest is then safe
                // after a crash, while this process continues with the loaded
                // candidate coherently.
                if let Err(sync_error) = sync_directory(&self.data_dir) {
                    inputs_are_safe_to_unlink = false;
                    publication_error = Some(StorageError::Manifest(format!(
                        "manifest installed but directory sync remained uncertain after {save_error}: {sync_error}"
                    )));
                }
            }
            #[cfg(test)]
            super::failpoints::crash_if_armed(
                &self.data_dir,
                super::failpoints::PersistenceBoundary::CompactionManifestPublication,
            );
            *live_manifest = candidate;
        }

        // A process crash reopens from the already durable manifest. Mark the
        // output committed before returning to the asynchronous live-list
        // publication step so the output guard cannot remove it.
        output_attempts.mark_manifest_committed();
        #[cfg(test)]
        if let Some(gate) = self.after_manifest_gate.lock().take() {
            gate.reach_and_wait();
        }

        Ok(PreparedCompactionPublication {
            input_sstables,
            input_ids,
            input_paths,
            result,
            inputs_are_safe_to_unlink,
            publication_error,
        })
    }

    async fn publish_prepared(
        self: &Arc<Self>,
        prepared: PreparedCompactionPublication,
    ) -> EngineResult<CompactionExecution> {
        let PreparedCompactionPublication {
            input_sstables,
            input_ids,
            input_paths,
            result,
            inputs_are_safe_to_unlink,
            publication_error,
        } = prepared;

        // The owned coordinator task cannot be cancelled with its caller while
        // it waits for this short live-list publication lock.
        {
            let mut live = self.sstables.write().await;
            debug_assert!(input_ids
                .iter()
                .all(|id| live.iter().any(|table| table.id == *id)));
            self.sstable_stats
                .publish_compaction(&mut live, &input_sstables, &result);
        }

        if inputs_are_safe_to_unlink {
            let coordinator = Arc::clone(self);
            tokio::task::spawn_blocking(move || coordinator.cleanup_or_defer(input_paths))
                .await
                .map_err(|error| {
                    StorageError::Other(format!("compaction cleanup task failed: {error}"))
                })?;
        }
        publication_error.map_or(Ok(result), Err)
    }

    fn retry_deferred_cleanup(&self) {
        let pending = std::mem::take(&mut *self.deferred_cleanup.lock());
        self.cleanup_or_defer(pending);
    }

    fn cleanup_or_defer(&self, paths: Vec<PathBuf>) {
        let mut deferred = Vec::new();
        for path in paths {
            match remove_sstable_if_present(&path) {
                Ok(true) => debug!("Deleted obsolete SSTable: {:?}", path),
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        "Deferring obsolete SSTable cleanup for {:?}: {}",
                        path, error
                    );
                    deferred.push(path);
                }
            }
        }
        self.deferred_cleanup.lock().extend(deferred);
    }

    #[cfg(test)]
    pub(super) async fn execute_job_for_test(
        self: &Arc<Self>,
        job: CompactionSelection,
    ) -> EngineResult<()> {
        let _claim = self.ownership.lock().await;
        let _in_progress = InProgressGuard::new(Arc::clone(&self.compactions_in_progress));
        let prepared = self
            .prepare_claimed(Some(job))
            .await?
            .expect("an explicit compaction job always prepares a publication");
        self.publish_prepared(prepared).await.map(|_| ())
    }

    #[cfg(test)]
    pub(super) fn accepting_requests_for_test(&self) -> bool {
        self.accepting_requests.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn tombstone_scan_attempts_for_test(&self) -> u64 {
        self.tombstone_sequence_cache.lock().scan_attempts
    }

    #[cfg(test)]
    pub(super) async fn hold_ownership_for_test(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.ownership.lock().await
    }

    #[cfg(test)]
    pub(super) fn gate_before_manifest_for_test(&self) -> Arc<CompactionTestGate> {
        let gate = Arc::new(CompactionTestGate::new());
        *self.before_manifest_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    pub(super) fn gate_during_manifest_for_test(&self) -> Arc<CompactionTestGate> {
        let gate = Arc::new(CompactionTestGate::new());
        *self.during_manifest_gate.lock() = Some(Arc::clone(&gate));
        gate
    }

    #[cfg(test)]
    pub(super) fn gate_after_manifest_for_test(&self) -> Arc<CompactionTestGate> {
        let gate = Arc::new(CompactionTestGate::new());
        *self.after_manifest_gate.lock() = Some(Arc::clone(&gate));
        gate
    }
}

fn manifests_have_same_identity(left: &Manifest, right: &Manifest) -> bool {
    if left.wal_checkpoint != right.wal_checkpoint || left.sstables.len() != right.sstables.len() {
        return false;
    }
    let left_ids = left
        .sstables
        .iter()
        .map(|table| table.id)
        .collect::<HashSet<_>>();
    right
        .sstables
        .iter()
        .all(|table| left_ids.contains(&table.id))
}

#[cfg(test)]
pub(super) struct CompactionTestGate {
    reached: AtomicBool,
    released: Mutex<bool>,
    release: Condvar,
}

#[cfg(test)]
impl CompactionTestGate {
    fn new() -> Self {
        Self {
            reached: AtomicBool::new(false),
            released: Mutex::new(false),
            release: Condvar::new(),
        }
    }

    fn reach_and_wait(&self) {
        self.reached.store(true, Ordering::Release);
        let mut released = self.released.lock();
        while !*released {
            self.release.wait(&mut released);
        }
    }

    pub(super) fn reached(&self) -> bool {
        self.reached.load(Ordering::Acquire)
    }

    pub(super) fn release(&self) {
        *self.released.lock() = true;
        self.release.notify_all();
    }
}

pub(super) struct CompactionAdmissionPause<'a> {
    coordinator: &'a CompactionCoordinator,
}

impl CompactionAdmissionPause<'_> {
    pub(super) async fn wait_until_idle(&self) {
        let _idle = self.coordinator.ownership.lock().await;
    }
}

/// Owns every output in one unpublished manifest candidate. Failure paths
/// remove all partial and finished outputs after their bytes have been
/// accounted; unlink failures are retried by the coordinator.
struct CompactionOutputAttempts {
    paths: Vec<PathBuf>,
    manifest_committed: bool,
    deferred_cleanup: Arc<Mutex<Vec<PathBuf>>>,
}

impl CompactionOutputAttempts {
    fn new(deferred_cleanup: Arc<Mutex<Vec<PathBuf>>>) -> Self {
        Self {
            paths: Vec::new(),
            manifest_committed: false,
            deferred_cleanup,
        }
    }

    fn claim(&mut self, path: PathBuf) -> EngineResult<()> {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                StorageError::Compaction(format!(
                    "failed to claim compaction output {}: {error}",
                    path.display()
                ))
            })?;
        self.paths.push(path);
        Ok(())
    }

    fn track(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn produced_bytes(&self) -> u64 {
        self.paths.iter().fold(0_u64, |total, path| {
            total.saturating_add(std::fs::metadata(path).map_or(0, |metadata| metadata.len()))
        })
    }

    fn mark_manifest_committed(&mut self) {
        self.manifest_committed = true;
    }
}

impl Drop for CompactionOutputAttempts {
    fn drop(&mut self) {
        if self.manifest_committed {
            return;
        }
        for path in &self.paths {
            match remove_sstable_if_present(path) {
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        "Deferring failed compaction output cleanup for {:?}: {}",
                        path, error
                    );
                    self.deferred_cleanup.lock().push(path.clone());
                }
            }
        }
    }
}

/// Entry for k-way merge heap
struct MergeEntry {
    key: Bytes,
    value: Option<Bytes>,
    sequence: u64,
    order: VersionOrder,
    source: usize,
}

impl MergeEntry {
    fn new(key: Bytes, entry: SSTableEntry, table: &SSTableManifestEntry, source: usize) -> Self {
        Self {
            key,
            value: entry.value.into_option(),
            sequence: entry.sequence.unwrap_or(0),
            order: VersionOrder::sstable(entry.sequence, table.id),
            source,
        }
    }
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.order == other.order
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // The reverse heap pops keys ascending and then applies the exact same
        // persistent sequence/legacy-source arbitration used by reads. Input
        // vector position never participates in winner selection.
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => other.order.cmp(&self.order),
            other_ord => other_ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::sstable::CompressionType;
    use super::*;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;

    type OwnedVersionedEntry = (Vec<u8>, Option<Vec<u8>>, u64);

    fn write_versioned_table(
        path: &std::path::Path,
        id: u64,
        sequence: u64,
        value: Option<&[u8]>,
    ) -> SSTableManifestEntry {
        let config = SSTableConfig {
            compression: CompressionType::None,
            ..SSTableConfig::default()
        };
        let mut writer = SSTableWriter::new(path, config).unwrap();
        writer
            .add_versioned(b"ordered:key", value, sequence)
            .unwrap();
        let info = writer.finish().unwrap();
        SSTableManifestEntry {
            id,
            level: 0,
            path: path.to_path_buf(),
            size: info.file_size,
            entry_count: info.entry_count,
            tombstone_count: info.tombstone_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: info.min_sequence,
            max_sequence: info.max_sequence,
            creation_time: id,
        }
    }

    fn metadata_table(
        id: u64,
        level: u32,
        min_key: &[u8],
        max_key: &[u8],
        creation_time: u64,
    ) -> SSTableManifestEntry {
        SSTableManifestEntry {
            id,
            level,
            path: PathBuf::from(format!("/tmp/{id}.sst")),
            size: 1,
            entry_count: 1,
            tombstone_count: 0,
            min_key: min_key.to_vec(),
            max_key: max_key.to_vec(),
            min_sequence: 0,
            max_sequence: 0,
            creation_time,
        }
    }

    fn write_entries_table(
        path: &std::path::Path,
        id: u64,
        config: SSTableConfig,
        entries: &[OwnedVersionedEntry],
    ) -> SSTableManifestEntry {
        let mut writer = SSTableWriter::new(path, config).unwrap();
        for (key, value, sequence) in entries {
            writer
                .add_versioned(key, value.as_deref(), *sequence)
                .unwrap();
        }
        let info = writer.finish().unwrap();
        SSTableManifestEntry {
            id,
            level: 0,
            path: info.path,
            size: info.file_size,
            entry_count: info.entry_count,
            tombstone_count: info.tombstone_count,
            min_key: info.min_key,
            max_key: info.max_key,
            min_sequence: info.min_sequence,
            max_sequence: info.max_sequence,
            creation_time: id,
        }
    }

    #[test]
    fn test_compaction_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.l0_compaction_trigger, 4);
        assert_eq!(config.max_levels, 7);
    }

    #[test]
    fn wal_checkpoint_is_the_inclusive_first_replayable_sequence() {
        assert_eq!(first_wal_replayable_sequence(0), 0);
        assert_eq!(first_wal_replayable_sequence(27), 27);
        assert_eq!(first_wal_replayable_sequence(u64::MAX), u64::MAX);
        let frontier = TombstoneReclamationFrontier::captured(u64::MAX);
        assert!(frontier.can_reclaim(u64::MAX - 1));
        assert!(!frontier.can_reclaim(u64::MAX));
    }

    #[test]
    fn test_pick_compaction_l0() {
        let config = CompactionConfig::default();
        let compactor = Compactor::new(
            config,
            SSTableConfig::default(),
            PathBuf::from("/tmp"),
            Arc::new(std::sync::atomic::AtomicU64::new(100)),
        );

        // Create 4 L0 SSTables
        let sstables: Vec<SSTableManifestEntry> = (0..4)
            .map(|i| SSTableManifestEntry {
                id: i,
                level: 0,
                path: PathBuf::from(format!("/tmp/{}.sst", i)),
                size: 1024,
                entry_count: 100,
                tombstone_count: 0,
                min_key: vec![],
                max_key: vec![],
                min_sequence: 0,
                max_sequence: 100,
                creation_time: i,
            })
            .collect();

        let job = compactor.pick_compaction_selection(&sstables);
        assert!(job.is_some());
        assert_eq!(job.unwrap().output_level, 1);
    }

    #[test]
    fn selection_closes_transitive_overlaps_across_source_target_and_deeper_levels() {
        let directory = TempDir::new().unwrap();
        std::fs::create_dir_all(directory.path().join("sstables")).unwrap();
        let compactor = Compactor::new(
            CompactionConfig {
                l0_compaction_trigger: 10,
                max_levels: 4,
                level_size_multiplier: 1,
                target_file_size: 1,
            },
            SSTableConfig::default(),
            directory.path().to_path_buf(),
            Arc::new(AtomicU64::new(100)),
        );
        let tables = vec![
            metadata_table(1, 1, b"a", b"b", 1),
            metadata_table(2, 1, b"a", b"b", 2),
            metadata_table(3, 1, b"a", b"b", 3),
            metadata_table(4, 1, b"a", b"b", 4),
            metadata_table(5, 2, b"b", b"f", 5),
            metadata_table(6, 1, b"f", b"h", 6),
            metadata_table(7, 3, b"h", b"z", 7),
            metadata_table(8, 2, b"zz", b"zzz", 8),
        ];

        let job = compactor.pick_compaction_selection(&tables).unwrap();
        assert_eq!(job.output_level, 3);
        assert_eq!(
            job.input_sstables
                .iter()
                .map(|table| table.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 6, 5, 7]
        );
    }

    #[test]
    fn top_configured_level_is_never_selected_as_a_source() {
        let directory = TempDir::new().unwrap();
        std::fs::create_dir_all(directory.path().join("sstables")).unwrap();
        let compactor = Compactor::new(
            CompactionConfig {
                l0_compaction_trigger: 10,
                max_levels: 3,
                level_size_multiplier: 1,
                target_file_size: 1,
            },
            SSTableConfig::default(),
            directory.path().to_path_buf(),
            Arc::new(AtomicU64::new(100)),
        );
        let top_level_only = vec![
            metadata_table(1, 2, b"a", b"m", 1),
            metadata_table(2, 2, b"n", b"z", 2),
        ];

        assert!(compactor
            .pick_compaction_selection(&top_level_only)
            .is_none());
    }

    #[test]
    fn top_level_reclamation_can_select_a_mixed_sequence_table() {
        let directory = TempDir::new().unwrap();
        std::fs::create_dir_all(directory.path().join("sstables")).unwrap();
        let compactor = Compactor::new(
            CompactionConfig {
                max_levels: 2,
                ..CompactionConfig::default()
            },
            SSTableConfig::default(),
            directory.path().to_path_buf(),
            Arc::new(AtomicU64::new(100)),
        );
        let path = directory.path().join("sstables/mixed.sst");
        let mut table = write_entries_table(
            &path,
            1,
            SSTableConfig::default(),
            &[
                (b"a:tombstone".to_vec(), None, 10),
                (b"z:live".to_vec(), Some(b"newer".to_vec()), 20),
            ],
        );
        table.level = 1;
        assert_eq!(minimum_sstable_tombstone_sequence(&path).unwrap(), Some(10));
        let tables = vec![table];
        let scope = HashSet::from([1]);

        assert!(compactor
            .pick_tombstone_reclamation_in_scope(&tables, &scope, &HashSet::new())
            .is_none());
        let selected = compactor
            .pick_tombstone_reclamation_in_scope(&tables, &scope, &HashSet::from([1]))
            .unwrap();
        assert_eq!(selected.output_level, 1);
        assert_eq!(selected.input_sstables[0].id, 1);
    }

    #[test]
    fn tombstone_sequence_cache_keys_by_id_and_path_and_retries_scan_errors() {
        let directory = TempDir::new().unwrap();
        let first = write_entries_table(
            &directory.path().join("first.sst"),
            1,
            SSTableConfig::default(),
            &[(b"key".to_vec(), None, 10)],
        );
        let second = write_entries_table(
            &directory.path().join("second.sst"),
            1,
            SSTableConfig::default(),
            &[(b"key".to_vec(), None, 20)],
        );
        let frontier = TombstoneReclamationFrontier::captured(15);
        let mut cache = TombstoneSequenceCache::default();

        let first_live = HashSet::from([SstableIdentity::from(&first)]);
        assert_eq!(
            cache
                .reclaimable_table_ids(&first_live, std::slice::from_ref(&first), frontier)
                .unwrap(),
            HashSet::from([1])
        );
        cache
            .reclaimable_table_ids(&first_live, std::slice::from_ref(&first), frontier)
            .unwrap();
        assert_eq!(cache.scan_attempts, 1);

        // Reusing an id at another immutable path cannot inherit the old
        // table's sequence proof.
        let second_live = HashSet::from([SstableIdentity::from(&second)]);
        assert!(cache
            .reclaimable_table_ids(&second_live, std::slice::from_ref(&second), frontier)
            .unwrap()
            .is_empty());
        assert_eq!(cache.scan_attempts, 2);
        assert!(!cache
            .minimum_sequences
            .contains_key(&SstableIdentity::from(&first)));

        let missing_path = directory.path().join("missing.sst");
        let mut missing = metadata_table(2, 1, b"a", b"z", 2);
        missing.path.clone_from(&missing_path);
        missing.tombstone_count = 1;
        let missing_identity = SstableIdentity::from(&missing);
        let missing_live = HashSet::from([missing_identity.clone()]);
        assert!(cache
            .reclaimable_table_ids(&missing_live, std::slice::from_ref(&missing), frontier,)
            .is_err());
        assert!(!cache.minimum_sequences.contains_key(&missing_identity));

        let repaired = write_entries_table(
            &missing_path,
            2,
            SSTableConfig::default(),
            &[(b"key".to_vec(), None, 5)],
        );
        assert_eq!(
            cache
                .reclaimable_table_ids(
                    &HashSet::from([SstableIdentity::from(&repaired)]),
                    std::slice::from_ref(&repaired),
                    frontier,
                )
                .unwrap(),
            HashSet::from([2])
        );
    }

    #[test]
    fn concurrent_tombstone_cache_callers_scan_an_identity_once() {
        let directory = TempDir::new().unwrap();
        let table = write_entries_table(
            &directory.path().join("shared.sst"),
            1,
            SSTableConfig::default(),
            &[(b"key".to_vec(), None, 10)],
        );
        let live = HashSet::from([SstableIdentity::from(&table)]);
        let cache = Arc::new(Mutex::new(TombstoneSequenceCache::default()));
        let mut callers = Vec::new();
        for _ in 0..2 {
            let cache = Arc::clone(&cache);
            let live = live.clone();
            let table = table.clone();
            callers.push(std::thread::spawn(move || {
                cache
                    .lock()
                    .reclaimable_table_ids(
                        &live,
                        &[table],
                        TombstoneReclamationFrontier::captured(11),
                    )
                    .unwrap()
            }));
        }
        for caller in callers {
            assert_eq!(caller.join().unwrap(), HashSet::from([1]));
        }
        assert_eq!(cache.lock().scan_attempts, 1);
    }

    #[test]
    fn compaction_selects_sequence_not_input_position() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let newest_tombstone = write_versioned_table(&sstable_directory.join("1.sst"), 1, 20, None);
        let stale_value =
            write_versioned_table(&sstable_directory.join("2.sst"), 2, 10, Some(b"stale"));

        let compactor = Compactor::new(
            CompactionConfig::default(),
            SSTableConfig {
                compression: CompressionType::None,
                ..SSTableConfig::default()
            },
            directory.path().to_path_buf(),
            Arc::new(AtomicU64::new(3)),
        );
        let mut attempts = CompactionOutputAttempts::new(Arc::new(Mutex::new(Vec::new())));
        let result = compactor
            .execute_selection(
                CompactionSelection {
                    input_sstables: vec![newest_tombstone, stale_value],
                    output_level: 1,
                },
                &mut attempts,
                CompactionExecutionOptions {
                    target_file_size: compactor.config.target_file_size,
                    first_output: None,
                    require_output: false,
                    tombstone_reclamation_frontier: TombstoneReclamationFrontier::RETAIN_ALL,
                },
            )
            .unwrap();
        attempts.mark_manifest_committed();

        let output = result.output_sstables.into_iter().next().unwrap();
        assert_eq!((output.min_sequence, output.max_sequence), (20, 20));
        let reader = SSTableReader::open(output.path).unwrap();
        let entry = reader.get_entry(b"ordered:key").unwrap().unwrap();
        assert_eq!(entry.sequence, Some(20));
        assert!(entry.value.into_option().is_none());
    }

    #[test]
    fn winning_tombstone_requires_a_strict_reclamation_frontier() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let tombstone = write_versioned_table(&sstable_directory.join("1.sst"), 1, 20, None);
        let stale = write_versioned_table(&sstable_directory.join("2.sst"), 2, 10, Some(b"stale"));

        for (case, frontier, tombstones_expected) in [
            (0_u64, TombstoneReclamationFrontier::RETAIN_ALL, 1_u64),
            (1, TombstoneReclamationFrontier::captured(20), 1),
            (2, TombstoneReclamationFrontier::captured(21), 0),
        ] {
            let compactor = Compactor::new(
                CompactionConfig::default(),
                SSTableConfig {
                    compression: CompressionType::None,
                    ..SSTableConfig::default()
                },
                directory.path().to_path_buf(),
                Arc::new(AtomicU64::new(10 + case * 10)),
            );
            let mut attempts = CompactionOutputAttempts::new(Arc::new(Mutex::new(Vec::new())));
            let result = compactor
                .execute_selection(
                    CompactionSelection {
                        input_sstables: vec![tombstone.clone(), stale.clone()],
                        output_level: 1,
                    },
                    &mut attempts,
                    CompactionExecutionOptions {
                        target_file_size: compactor.config.target_file_size,
                        first_output: None,
                        require_output: false,
                        tombstone_reclamation_frontier: frontier,
                    },
                )
                .unwrap();
            attempts.mark_manifest_committed();

            assert_eq!(result.tombstones_dropped, 1 - tombstones_expected);
            assert_eq!(
                result
                    .output_sstables
                    .iter()
                    .map(|table| table.tombstone_count)
                    .sum::<u64>(),
                tombstones_expected
            );
            assert_eq!(result.entries_dropped, 2 - tombstones_expected);
            if tombstones_expected == 0 {
                assert!(result.output_sstables.is_empty());
            }
        }
    }

    #[test]
    fn compressed_split_projection_work_is_bounded_by_output_count() {
        const KEY_COUNT: u64 = 400;
        const TARGET_SIZE: u64 = 24 * 1024;

        for compression in [
            CompressionType::None,
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let directory = TempDir::new().unwrap();
            let sstable_directory = directory.path().join("sstables");
            std::fs::create_dir_all(&sstable_directory).unwrap();
            let config = SSTableConfig {
                block_size: 2 * 1024,
                compression,
                ..SSTableConfig::default()
            };
            let older = (0..KEY_COUNT)
                .map(|index| {
                    (
                        format!("key-{index:06}").into_bytes(),
                        Some(vec![b'a'; 512]),
                        1,
                    )
                })
                .collect::<Vec<_>>();
            let newer = (0..KEY_COUNT)
                .map(|index| {
                    let mut value = vec![b'a'; 512];
                    value[0] = b'b';
                    (format!("key-{index:06}").into_bytes(), Some(value), 2)
                })
                .collect::<Vec<_>>();
            let inputs = vec![
                write_entries_table(
                    &sstable_directory.join("older.sst"),
                    1,
                    config.clone(),
                    &older,
                ),
                write_entries_table(
                    &sstable_directory.join("newer.sst"),
                    2,
                    config.clone(),
                    &newer,
                ),
            ];
            let compactor = Compactor::new(
                CompactionConfig {
                    target_file_size: TARGET_SIZE,
                    ..CompactionConfig::default()
                },
                config,
                directory.path().to_path_buf(),
                Arc::new(AtomicU64::new(10)),
            );
            let mut attempts = CompactionOutputAttempts::new(Arc::new(Mutex::new(Vec::new())));
            let result = compactor
                .execute_selection(
                    CompactionSelection {
                        input_sstables: inputs,
                        output_level: 1,
                    },
                    &mut attempts,
                    CompactionExecutionOptions {
                        target_file_size: TARGET_SIZE,
                        first_output: None,
                        require_output: false,
                        tombstone_reclamation_frontier: TombstoneReclamationFrontier::RETAIN_ALL,
                    },
                )
                .unwrap();
            attempts.mark_manifest_committed();

            assert!(result.output_sstables.len() > 1, "{compression:?}");
            assert!(result
                .output_sstables
                .iter()
                .all(|table| table.size <= TARGET_SIZE));
            assert!(result
                .output_sstables
                .windows(2)
                .all(|pair| pair[0].max_key < pair[1].min_key));
            let exact_projections = compactor.exact_projection_count();
            assert!(
                exact_projections <= result.output_sstables.len() as u64,
                "{compression:?}: {exact_projections} exact projections for {} outputs",
                result.output_sstables.len()
            );
            assert!(exact_projections < KEY_COUNT / 4, "{compression:?}");

            let mut observed = Vec::new();
            for output in &result.output_sstables {
                let reader = SSTableReader::open(&output.path).unwrap();
                let mut iterator = reader.iter();
                while let Some(entry) = iterator.next_versioned() {
                    let (key, entry) = entry.unwrap();
                    observed.push((key.to_vec(), entry));
                }
            }
            assert_eq!(observed.len(), KEY_COUNT as usize);
            for (index, (key, entry)) in observed.into_iter().enumerate() {
                assert_eq!(key, format!("key-{index:06}").as_bytes());
                assert_eq!(entry.sequence, Some(2));
                let value = entry.value.into_option().unwrap();
                assert_eq!(value.len(), 512);
                assert_eq!(value[0], b'b');
                assert!(value[1..].iter().all(|byte| *byte == b'a'));
            }
        }
    }

    #[test]
    fn split_size_is_soft_exact_and_preserves_binary_and_tombstone_entries() {
        let directory = TempDir::new().unwrap();
        let sstable_directory = directory.path().join("sstables");
        std::fs::create_dir_all(&sstable_directory).unwrap();
        let config = SSTableConfig {
            block_size: 64,
            compression: CompressionType::None,
            ..SSTableConfig::default()
        };
        let entries = vec![
            (vec![0, 0xff], Some(vec![0x11; 37]), 1),
            (vec![1, 0], Some(vec![0x22; 37]), 2),
            (vec![2, 0xff], None, 3),
            (vec![3, 0], Some(vec![0x44; 37]), 4),
        ];
        let input = write_entries_table(
            &sstable_directory.join("input.sst"),
            1,
            config.clone(),
            &entries,
        );
        let exact_two_entry_size = write_entries_table(
            &sstable_directory.join("calibration.sst"),
            2,
            config.clone(),
            &entries[..2],
        )
        .size;

        let execute = |target_file_size: u64, next_id: u64| {
            let compactor = Compactor::new(
                CompactionConfig {
                    target_file_size,
                    ..CompactionConfig::default()
                },
                config.clone(),
                directory.path().to_path_buf(),
                Arc::new(AtomicU64::new(next_id)),
            );
            let mut attempts = CompactionOutputAttempts::new(Arc::new(Mutex::new(Vec::new())));
            let result = compactor
                .execute_selection(
                    CompactionSelection {
                        input_sstables: vec![input.clone()],
                        output_level: 1,
                    },
                    &mut attempts,
                    CompactionExecutionOptions {
                        target_file_size,
                        first_output: None,
                        require_output: false,
                        tombstone_reclamation_frontier: TombstoneReclamationFrontier::RETAIN_ALL,
                    },
                )
                .unwrap();
            attempts.mark_manifest_committed();
            result
        };

        let exact = execute(exact_two_entry_size, 10);
        assert!(exact.output_sstables.len() >= 2);
        assert_eq!(exact.output_sstables[0].entry_count, 2);
        assert_eq!(
            exact
                .output_sstables
                .iter()
                .map(|table| table.entry_count)
                .sum::<u64>(),
            4
        );
        assert_eq!(
            exact
                .output_sstables
                .iter()
                .map(|table| table.tombstone_count)
                .sum::<u64>(),
            1
        );
        for adjacent in exact.output_sstables.windows(2) {
            assert!(adjacent[0].max_key < adjacent[1].min_key);
        }
        for (key, expected, sequence) in &entries {
            let table = exact
                .output_sstables
                .iter()
                .find(|table| {
                    table.min_key.as_slice() <= key.as_slice()
                        && key.as_slice() <= table.max_key.as_slice()
                })
                .unwrap();
            let entry = SSTableReader::open(&table.path)
                .unwrap()
                .get_entry(key)
                .unwrap()
                .unwrap();
            assert_eq!(entry.sequence, Some(*sequence));
            assert_eq!(entry.value.into_option().as_deref(), expected.as_deref());
        }

        let one_byte_over = execute(exact_two_entry_size - 1, 20);
        assert_eq!(one_byte_over.output_sstables[0].entry_count, 1);

        let single_entry_size = write_entries_table(
            &sstable_directory.join("single-calibration.sst"),
            3,
            config.clone(),
            &entries[..1],
        )
        .size;
        let oversized = execute(single_entry_size - 1, 30);
        assert!(oversized.output_sstables[0].size > single_entry_size - 1);
        assert!(oversized
            .output_sstables
            .iter()
            .all(|table| table.entry_count > 0));
    }
}
