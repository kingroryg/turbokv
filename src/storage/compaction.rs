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
use std::collections::{BinaryHeap, HashSet};
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
use super::sstable::{SSTableConfig, SSTableEntry, SSTableInfo, SSTableReader, SSTableWriter};
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
    pub output_path: PathBuf,
}

/// Result of a compaction run
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

/// Selects and executes the SSTable merge portion of a coordinated compaction.
pub struct Compactor {
    config: CompactionConfig,
    sstable_config: SSTableConfig,
    data_dir: PathBuf,
    next_sstable_id: Arc<std::sync::atomic::AtomicU64>,
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
        }
    }

    /// Check if compaction is needed and return a job if so
    pub fn pick_compaction(&self, sstables: &[SSTableManifestEntry]) -> Option<CompactionJob> {
        if self.config.max_levels < 2 {
            return None;
        }

        // Priority 1: L0 compaction (keeps write path fast)
        let mut l0_tables: Vec<_> = sstables.iter().filter(|s| s.level == 0).cloned().collect();
        l0_tables.sort_by_key(|table| table.id);

        if !l0_tables.is_empty() && l0_tables.len() >= self.config.l0_compaction_trigger {
            let (input_sstables, output_level) =
                complete_overlap_closure(sstables, l0_tables, 1, self.config.max_levels);
            let output_path = self.new_sstable_path();
            return Some(CompactionJob {
                input_sstables,
                output_level,
                output_path,
            });
        }

        // Priority 2: Level compaction (when level exceeds size budget)
        let highest_output_level = self.config.max_levels - 1;
        for level in 1..highest_output_level {
            let level_tables: Vec<_> = sstables
                .iter()
                .filter(|s| s.level == level)
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
                let output_path = self.new_sstable_path();
                return Some(CompactionJob {
                    input_sstables,
                    output_level,
                    output_path,
                });
            }
        }

        None
    }

    /// Execute a compaction job using streaming merge
    pub fn execute(&self, job: CompactionJob) -> Result<CompactionResult> {
        info!(
            "Starting compaction: {} inputs -> L{}",
            job.input_sstables.len(),
            job.output_level
        );

        let mut bytes_read = 0u64;
        let mut entries_merged = 0u64;
        let mut entries_dropped = 0u64;
        let mut tombstones_dropped = 0u64;

        // Open all input SSTables
        let readers: Vec<SSTableReader> = job
            .input_sstables
            .iter()
            .map(|entry| {
                bytes_read += entry.size;
                SSTableReader::open(&entry.path)
            })
            .collect::<Result<Vec<_>>>()?;

        // Create output writer
        let mut writer = SSTableWriter::new(&job.output_path, self.sstable_config.clone())?;

        // Streaming k-way merge
        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        let mut iterators: Vec<_> = readers.iter().map(|r| r.iter()).collect();

        // Initialize heap with first entry from each iterator
        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some(result) = iter.next_versioned() {
                let (key, entry) = result?;
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

        while let Some(Reverse(entry)) = heap.pop() {
            // Deduplicate: keep only the newest version of each key
            let is_duplicate = last_key.as_ref().map(|k| k == &entry.key).unwrap_or(false);

            if is_duplicate {
                entries_dropped += 1;
                if entry.value.is_none() {
                    tombstones_dropped += 1;
                }
            } else {
                writer.add_versioned(&entry.key, entry.value.as_deref(), entry.sequence)?;
                if entry.value.is_some() {
                    live_keys.push(entry.key.to_vec());
                }
                entries_merged += 1;
                last_key = Some(entry.key.clone());
            }

            // Advance the iterator that provided this entry
            if let Some(result) = iterators[entry.source].next_versioned() {
                let (key, next_entry) = result?;
                heap.push(Reverse(MergeEntry::new(
                    key,
                    next_entry,
                    &job.input_sstables[entry.source],
                    entry.source,
                )));
            }
        }

        // Finish writing
        let output_info = writer.finish()?;
        let bytes_written = output_info.file_size;

        // Create manifest entry for output
        // Extract the ID from the output path filename (already allocated by new_sstable_path)
        // Filename format: {id}_{timestamp}.sst
        let id = job
            .output_path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.split('_').next())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let output_entry = SSTableManifestEntry {
            id,
            level: job.output_level,
            path: job.output_path,
            size: output_info.file_size,
            entry_count: output_info.entry_count,
            tombstone_count: output_info.tombstone_count,
            min_key: output_info.min_key,
            max_key: output_info.max_key,
            min_sequence: output_info.min_sequence,
            max_sequence: output_info.max_sequence,
            creation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let input_ids: Vec<u64> = job.input_sstables.iter().map(|s| s.id).collect();

        info!(
            "Compaction complete: {} entries merged, {} dropped, {:.2}MB -> {:.2}MB",
            entries_merged,
            entries_dropped,
            bytes_read as f64 / 1024.0 / 1024.0,
            bytes_written as f64 / 1024.0 / 1024.0
        );

        Ok(CompactionResult {
            input_ids,
            output_sstable: Some(output_entry),
            bytes_read,
            bytes_written,
            entries_merged,
            entries_dropped,
            tombstones_dropped,
            live_keys,
        })
    }

    /// Delete old SSTable files after successful compaction
    pub fn cleanup_inputs(&self, paths: &[PathBuf]) -> Result<()> {
        for path in paths {
            if path.exists() {
                std::fs::remove_file(path).map_err(|e| Error::Io {
                    message: format!("Failed to delete compacted SSTable: {:?}", path),
                    source: e,
                })?;
                debug!("Deleted compacted SSTable: {:?}", path);
            }
        }
        Ok(())
    }

    fn max_level_size(&self, level: u32) -> u64 {
        // L1 = target_file_size * 10, L2 = L1 * 10, etc.
        self.config.target_file_size * self.config.level_size_multiplier.pow(level)
    }

    fn new_sstable_path(&self) -> PathBuf {
        let id = self
            .next_sstable_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.data_dir
            .join("sstables")
            .join(format!("{}_{}.sst", id, timestamp))
    }
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

/// A manifest-installed generation waiting for the short asynchronous
/// live-reader/statistics publication step.
struct PreparedCompactionPublication {
    input_sstables: Vec<SSTableManifestEntry>,
    input_ids: HashSet<u64>,
    input_paths: Vec<PathBuf>,
    result: CompactionResult,
    inputs_are_safe_to_unlink: bool,
    publication_error: Option<StorageError>,
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
        sstable_stats: Arc<SstableStatistics>,
        compactions_in_progress: Arc<AtomicU64>,
    ) -> Self {
        Self {
            ownership: Arc::new(AsyncMutex::new(())),
            accepting_requests: AtomicBool::new(true),
            compactor: Compactor::new(config, sstable_config, data_dir.clone(), next_sstable_id),
            data_dir,
            directory_lock,
            sstables,
            manifest,
            sstable_stats,
            compactions_in_progress,
            deferred_cleanup: Arc::new(Mutex::new(Vec::new())),
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
    pub(super) async fn request(self: &Arc<Self>) -> EngineResult<PublicCompactionResult> {
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Ok(PublicCompactionResult::default());
        }

        let claim = Arc::clone(&self.ownership).lock_owned().await;
        if !self.accepting_requests.load(Ordering::Acquire) {
            return Ok(PublicCompactionResult::default());
        }
        let Some(directory_lock) = self.directory_lock.upgrade() else {
            return Ok(PublicCompactionResult::default());
        };

        // Ownership and directory lifetime transfer before the caller reaches
        // another await point. Cancelling the waiter therefore detaches, but
        // never aborts, the accepted job. Shutdown drains `ownership`.
        let coordinator = Arc::clone(self);
        tokio::spawn(async move { coordinator.run_owned(claim, directory_lock).await })
            .await
            .map_err(|error| {
                StorageError::Other(format!("coordinated compaction task failed: {error}"))
            })?
    }

    async fn run_owned(
        self: Arc<Self>,
        _claim: OwnedMutexGuard<()>,
        _directory_lock: Arc<DirectoryLock>,
    ) -> EngineResult<PublicCompactionResult> {
        let _in_progress = InProgressGuard::new(Arc::clone(&self.compactions_in_progress));
        let snapshot = self.live_manifest_entries().await;
        let job = self.compactor.pick_compaction(&snapshot);
        let started = job.as_ref().map(|_| Instant::now());
        let Some(prepared) = self.prepare_claimed(job).await? else {
            return Ok(PublicCompactionResult::default());
        };

        let result = self.publish_prepared(prepared).await?;
        Ok(PublicCompactionResult {
            files_compacted: u32::try_from(result.input_ids.len()).unwrap_or(u32::MAX),
            bytes_reclaimed: result.bytes_read.saturating_sub(result.bytes_written),
            duration_ms: u64::try_from(
                started
                    .expect("a prepared publication always has a selected job")
                    .elapsed()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX),
        })
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
        job: Option<CompactionJob>,
    ) -> EngineResult<Option<PreparedCompactionPublication>> {
        let coordinator = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            coordinator.retry_deferred_cleanup();
            job.map(|job| coordinator.execute_claimed_blocking(job))
                .transpose()
        })
        .await
        .map_err(|error| StorageError::Other(format!("compaction blocking task failed: {error}")))?
    }

    fn execute_claimed_blocking(
        &self,
        job: CompactionJob,
    ) -> EngineResult<PreparedCompactionPublication> {
        let input_sstables = job.input_sstables.clone();
        let input_ids: HashSet<u64> = input_sstables.iter().map(|table| table.id).collect();
        let input_paths = input_sstables
            .iter()
            .map(|table| table.path.clone())
            .collect::<Vec<_>>();
        let selected_input_bytes = input_sstables
            .iter()
            .fold(0_u64, |total, table| total.saturating_add(table.size));
        let mut output_attempt = match CompactionOutputAttempt::claim(
            job.output_path.clone(),
            Arc::clone(&self.deferred_cleanup),
        ) {
            Ok(attempt) => attempt,
            Err(error) => {
                self.sstable_stats
                    .record_compaction_attempt(selected_input_bytes, 0);
                return Err(error);
            }
        };

        let execution = self.compactor.execute(job);
        self.sstable_stats
            .record_compaction_attempt(selected_input_bytes, output_attempt.produced_bytes());
        let result = execution.map_err(|error| StorageError::Compaction(error.to_string()))?;

        #[cfg(test)]
        super::failpoints::check(
            &self.data_dir,
            super::failpoints::PersistenceBoundary::CompactionOutputPublication,
        )?;

        if let Some(output) = &result.output_sstable {
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
            if let Some(output) = &result.output_sstable {
                candidate.sstables.push(output.clone());
            }

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
            *live_manifest = candidate;
        }

        // A process crash reopens from the already durable manifest. Mark the
        // output committed before returning to the asynchronous live-list
        // publication step so the output guard cannot remove it.
        output_attempt.mark_manifest_committed();
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
    ) -> EngineResult<CompactionResult> {
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
            match std::fs::remove_file(&path) {
                Ok(()) => debug!("Deleted obsolete SSTable: {:?}", path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
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
        job: CompactionJob,
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

/// Owns an uncommitted output. Failure paths remove partial output after its
/// bytes have been accounted; an unlink failure is retried by the coordinator.
struct CompactionOutputAttempt {
    path: PathBuf,
    manifest_committed: bool,
    deferred_cleanup: Arc<Mutex<Vec<PathBuf>>>,
}

impl CompactionOutputAttempt {
    fn claim(path: PathBuf, deferred_cleanup: Arc<Mutex<Vec<PathBuf>>>) -> EngineResult<Self> {
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
        Ok(Self {
            path,
            manifest_committed: false,
            deferred_cleanup,
        })
    }

    fn produced_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map_or(0, |metadata| metadata.len())
    }

    fn mark_manifest_committed(&mut self) {
        self.manifest_committed = true;
    }
}

impl Drop for CompactionOutputAttempt {
    fn drop(&mut self) {
        if self.manifest_committed {
            return;
        }
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    "Deferring failed compaction output cleanup for {:?}: {}",
                    self.path, error
                );
                self.deferred_cleanup.lock().push(self.path.clone());
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

    #[test]
    fn test_compaction_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.l0_compaction_trigger, 4);
        assert_eq!(config.max_levels, 7);
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

        let job = compactor.pick_compaction(&sstables);
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

        let job = compactor.pick_compaction(&tables).unwrap();
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

        assert!(compactor.pick_compaction(&top_level_only).is_none());
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
        let result = compactor
            .execute(CompactionJob {
                input_sstables: vec![newest_tombstone, stale_value],
                output_level: 1,
                output_path: sstable_directory.join("3.sst"),
            })
            .unwrap();

        let output = result.output_sstable.unwrap();
        assert_eq!((output.min_sequence, output.max_sequence), (20, 20));
        let reader = SSTableReader::open(output.path).unwrap();
        let entry = reader.get_entry(b"ordered:key").unwrap().unwrap();
        assert_eq!(entry.sequence, Some(20));
        assert!(entry.value.into_option().is_none());
    }
}
