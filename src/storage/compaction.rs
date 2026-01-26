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
use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, info};

use crate::core::error::{Error, Result};

use super::manifest::SSTableManifestEntry;
use super::sstable::{SSTableConfig, SSTableReader, SSTableWriter};

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
    /// Max concurrent compactions
    pub max_concurrent: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            max_levels: 7,
            level_size_multiplier: 10,
            target_file_size: 64 * 1024 * 1024, // 64MB
            max_concurrent: 1,
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
    /// Keys that survived compaction (for vector index filtering)
    pub live_keys: Vec<Vec<u8>>,
}

/// Compactor handles background compaction
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
        // Priority 1: L0 compaction (keeps write path fast)
        let l0_tables: Vec<_> = sstables.iter().filter(|s| s.level == 0).cloned().collect();

        if l0_tables.len() >= self.config.l0_compaction_trigger {
            let output_path = self.new_sstable_path();
            return Some(CompactionJob {
                input_sstables: l0_tables,
                output_level: 1,
                output_path,
            });
        }

        // Priority 2: Level compaction (when level exceeds size budget)
        for level in 1..self.config.max_levels {
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
                sorted.sort_by_key(|s| s.creation_time);
                let to_compact: Vec<_> = sorted.into_iter().take(4).collect();

                let output_path = self.new_sstable_path();
                return Some(CompactionJob {
                    input_sstables: to_compact,
                    output_level: level + 1,
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
            if let Some(result) = iter.next() {
                let (key, value) = result?;
                heap.push(Reverse(MergeEntry {
                    key,
                    value,
                    source: idx,
                }));
            }
        }

        let mut last_key: Option<Bytes> = None;
        let mut live_keys: Vec<Vec<u8>> = Vec::new();

        while let Some(Reverse(entry)) = heap.pop() {
            // Deduplicate: keep only the newest version of each key
            let is_duplicate = last_key.as_ref().map(|k| k == &entry.key).unwrap_or(false);

            if is_duplicate {
                entries_dropped += 1;
            } else {
                // Convert Bytes value to Option<&[u8]>
                // Empty values are treated as tombstones
                let value_opt = if entry.value.is_empty() {
                    None
                } else {
                    Some(entry.value.as_ref())
                };
                writer.add(&entry.key, value_opt)?;
                live_keys.push(entry.key.to_vec());
                entries_merged += 1;
                last_key = Some(entry.key.clone());
            }

            // Advance the iterator that provided this entry
            if let Some(result) = iterators[entry.source].next() {
                let (key, value) = result?;
                heap.push(Reverse(MergeEntry {
                    key,
                    value,
                    source: entry.source,
                }));
            }
        }

        // Finish writing
        let output_info = writer.finish()?;
        let bytes_written = output_info.file_size;

        // Create manifest entry for output
        let output_entry = SSTableManifestEntry {
            id: self
                .next_sstable_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            level: job.output_level,
            path: job.output_path,
            size: output_info.file_size,
            entry_count: output_info.entry_count,
            min_key: output_info.min_key,
            max_key: output_info.max_key,
            min_sequence: job
                .input_sstables
                .iter()
                .map(|s| s.min_sequence)
                .min()
                .unwrap_or(0),
            max_sequence: job
                .input_sstables
                .iter()
                .map(|s| s.max_sequence)
                .max()
                .unwrap_or(0),
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

/// Entry for k-way merge heap
struct MergeEntry {
    key: Bytes,
    value: Bytes,
    source: usize,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
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
        // Compare by key, then by source (lower source = newer data in our case)
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => self.source.cmp(&other.source),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
