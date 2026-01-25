//! # Time-Based Partitioning and Retention
//!
//! Organizes SSTables by time windows for efficient time-range queries
//! and simple retention policy enforcement.
//!
//! ## Partitioning Strategy
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Time-Based Partitioning                     │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                              │
//! │  Incoming Events                                             │
//! │       │                                                      │
//! │       ▼                                                      │
//! │  ┌─────────┐     ┌─────────────────────────────────────┐   │
//! │  │ MemTable│────>│ Flush to time-partitioned SSTable   │   │
//! │  └─────────┘     └─────────────────────────────────────┘   │
//! │                              │                               │
//! │                              ▼                               │
//! │  sstables/                                                   │
//! │  ├── 2024-01-05/                                            │
//! │  │   ├── 00_1704412800_001.sst  (00:00-06:00)              │
//! │  │   ├── 06_1704434400_002.sst  (06:00-12:00)              │
//! │  │   ├── 12_1704456000_003.sst  (12:00-18:00)              │
//! │  │   └── 18_1704477600_004.sst  (18:00-24:00)              │
//! │  ├── 2024-01-04/                                            │
//! │  │   └── ...                                                │
//! │  └── 2024-01-03/                                            │
//! │      └── ...                                                 │
//! │                                                              │
//! │  Query: "last 6 hours" → Only scan 1-2 partitions           │
//! │  Retention: Delete entire date directories                   │
//! │                                                              │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, TimeZone, Timelike, Utc};
use tracing::{debug, info};

use crate::core::error::Result;

/// Time window size for partitioning
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionWindow {
    /// 1-hour partitions (high-volume)
    Hourly,
    /// 6-hour partitions (default)
    SixHour,
    /// 24-hour partitions (low-volume)
    Daily,
}

impl PartitionWindow {
    /// Get window duration in seconds
    pub fn duration_secs(&self) -> u64 {
        match self {
            PartitionWindow::Hourly => 3600,
            PartitionWindow::SixHour => 6 * 3600,
            PartitionWindow::Daily => 24 * 3600,
        }
    }

    /// Get window index for a given hour (0-23)
    pub fn window_index(&self, hour: u32) -> u32 {
        match self {
            PartitionWindow::Hourly => hour,
            PartitionWindow::SixHour => hour / 6,
            PartitionWindow::Daily => 0,
        }
    }

    /// Get window start hour
    pub fn window_start_hour(&self, hour: u32) -> u32 {
        match self {
            PartitionWindow::Hourly => hour,
            PartitionWindow::SixHour => (hour / 6) * 6,
            PartitionWindow::Daily => 0,
        }
    }
}

impl Default for PartitionWindow {
    fn default() -> Self {
        PartitionWindow::SixHour
    }
}

/// Retention policy configuration
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// How long to keep data
    pub retention_period: Duration,
    /// How often to run cleanup
    pub cleanup_interval: Duration,
    /// Minimum partitions to keep regardless of age
    pub min_partitions: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retention_period: Duration::from_secs(30 * 24 * 3600), // 30 days
            cleanup_interval: Duration::from_secs(3600),           // 1 hour
            min_partitions: 24,                                     // Keep at least 24 partitions
        }
    }
}

/// Partition identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    /// Date component (YYYY-MM-DD)
    pub date: NaiveDate,
    /// Window index within the day
    pub window: u32,
}

impl PartitionId {
    /// Create partition ID from timestamp
    pub fn from_timestamp(timestamp_secs: u64, window: PartitionWindow) -> Self {
        let dt = DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(timestamp_secs));
        let date = dt.date_naive();
        let window_idx = window.window_index(dt.hour());
        
        Self {
            date,
            window: window_idx,
        }
    }

    /// Create partition ID from DateTime
    pub fn from_datetime(dt: DateTime<Utc>, window: PartitionWindow) -> Self {
        Self::from_timestamp(dt.timestamp() as u64, window)
    }

    /// Get the start timestamp of this partition
    pub fn start_timestamp(&self, window: PartitionWindow) -> u64 {
        let start_hour = match window {
            PartitionWindow::Hourly => self.window,
            PartitionWindow::SixHour => self.window * 6,
            PartitionWindow::Daily => 0,
        };
        
        let dt = self.date.and_hms_opt(start_hour, 0, 0).unwrap();
        Utc.from_utc_datetime(&dt).timestamp() as u64
    }

    /// Get the end timestamp of this partition (exclusive)
    pub fn end_timestamp(&self, window: PartitionWindow) -> u64 {
        self.start_timestamp(window) + window.duration_secs()
    }

    /// Get directory name for this partition
    pub fn dir_name(&self) -> String {
        self.date.format("%Y-%m-%d").to_string()
    }

    /// Get file prefix for this partition
    pub fn file_prefix(&self, window: PartitionWindow) -> String {
        let start_hour = window.window_start_hour(self.window * match window {
            PartitionWindow::Hourly => 1,
            PartitionWindow::SixHour => 6,
            PartitionWindow::Daily => 24,
        });
        format!("{:02}", start_hour)
    }
}

/// Manages time-based partitioning of SSTables
pub struct PartitionManager {
    /// Base directory for SSTables
    base_dir: PathBuf,
    /// Partition window size
    window: PartitionWindow,
    /// Retention policy
    retention: RetentionPolicy,
}

impl PartitionManager {
    /// Create new partition manager
    pub fn new(base_dir: PathBuf, window: PartitionWindow, retention: RetentionPolicy) -> Self {
        Self {
            base_dir,
            window,
            retention,
        }
    }

    /// Get partition for a given timestamp
    pub fn get_partition(&self, timestamp_secs: u64) -> PartitionId {
        PartitionId::from_timestamp(timestamp_secs, self.window)
    }

    /// Get directory path for a partition
    pub fn partition_dir(&self, partition: &PartitionId) -> PathBuf {
        self.base_dir.join(partition.dir_name())
    }

    /// Generate SSTable filename for a partition
    pub fn generate_sstable_path(
        &self,
        partition: &PartitionId,
        sstable_id: u64,
        timestamp: u64,
    ) -> PathBuf {
        let dir = self.partition_dir(partition);
        let prefix = partition.file_prefix(self.window);
        let filename = format!("{}_{:010}_{:06}.sst", prefix, timestamp, sstable_id);
        dir.join(filename)
    }

    /// List all partitions in the base directory
    pub fn list_partitions(&self) -> Result<Vec<PartitionId>> {
        let mut partitions = Vec::new();

        if !self.base_dir.exists() {
            return Ok(partitions);
        }

        for entry in std::fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if !path.is_dir() {
                continue;
            }

            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Ok(date) = NaiveDate::parse_from_str(name, "%Y-%m-%d") {
                    // Find all windows in this date directory
                    for sst_entry in std::fs::read_dir(&path)? {
                        let sst_entry = sst_entry?;
                        let sst_name = sst_entry.file_name();
                        let sst_str = sst_name.to_string_lossy();
                        
                        if sst_str.ends_with(".sst") {
                            // Parse window from filename prefix (e.g., "06_...")
                            if let Some(hour_str) = sst_str.split('_').next() {
                                if let Ok(hour) = hour_str.parse::<u32>() {
                                    let window_idx = self.window.window_index(hour);
                                    let partition = PartitionId { date, window: window_idx };
                                    if !partitions.contains(&partition) {
                                        partitions.push(partition);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        partitions.sort();
        Ok(partitions)
    }

    /// List SSTables in a specific partition
    pub fn list_sstables_in_partition(&self, partition: &PartitionId) -> Result<Vec<PathBuf>> {
        let dir = self.partition_dir(partition);
        let mut sstables = Vec::new();

        if !dir.exists() {
            return Ok(sstables);
        }

        let prefix = partition.file_prefix(self.window);

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if name.starts_with(&prefix) && name.ends_with(".sst") {
                sstables.push(path);
            }
        }

        sstables.sort();
        Ok(sstables)
    }

    /// Get partitions that overlap with a time range
    pub fn partitions_for_range(
        &self,
        start_secs: u64,
        end_secs: u64,
    ) -> Vec<PartitionId> {
        let mut partitions = Vec::new();
        
        let start_partition = self.get_partition(start_secs);
        let end_partition = self.get_partition(end_secs);

        // Generate all partitions between start and end
        let mut current = start_partition.clone();
        
        loop {
            partitions.push(current.clone());
            
            if current >= end_partition {
                break;
            }

            // Move to next partition
            let next_ts = current.end_timestamp(self.window);
            current = self.get_partition(next_ts);
        }

        partitions
    }

    /// Identify partitions eligible for deletion based on retention policy
    pub fn partitions_to_delete(&self) -> Result<Vec<PartitionId>> {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            - self.retention.retention_period.as_secs();

        let cutoff_partition = self.get_partition(cutoff);
        let all_partitions = self.list_partitions()?;

        // Keep at least min_partitions
        if all_partitions.len() <= self.retention.min_partitions {
            return Ok(Vec::new());
        }

        let to_delete: Vec<_> = all_partitions
            .into_iter()
            .filter(|p| p < &cutoff_partition)
            .collect();

        Ok(to_delete)
    }

    /// Delete a partition and all its SSTables
    pub fn delete_partition(&self, partition: &PartitionId) -> Result<u64> {
        let dir = self.partition_dir(partition);
        
        if !dir.exists() {
            return Ok(0);
        }

        let mut bytes_deleted = 0u64;

        // Delete all SST files in partition
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension() == Some(std::ffi::OsStr::new("sst")) {
                bytes_deleted += entry.metadata()?.len();
                std::fs::remove_file(&path)?;
                debug!("Deleted SSTable: {:?}", path);
            }
        }

        // Remove directory if empty
        if std::fs::read_dir(&dir)?.next().is_none() {
            std::fs::remove_dir(&dir)?;
            debug!("Removed empty partition directory: {:?}", dir);
        }

        info!(
            "Deleted partition {}/{}: {} bytes",
            partition.dir_name(),
            partition.window,
            bytes_deleted
        );

        Ok(bytes_deleted)
    }

    /// Run retention cleanup
    pub fn enforce_retention(&self) -> Result<RetentionResult> {
        let partitions = self.partitions_to_delete()?;
        
        let mut result = RetentionResult {
            partitions_deleted: 0,
            bytes_reclaimed: 0,
            errors: Vec::new(),
        };

        for partition in partitions {
            match self.delete_partition(&partition) {
                Ok(bytes) => {
                    result.partitions_deleted += 1;
                    result.bytes_reclaimed += bytes;
                }
                Err(e) => {
                    result.errors.push(format!(
                        "Failed to delete partition {:?}: {:?}",
                        partition, e
                    ));
                }
            }
        }

        Ok(result)
    }

    /// Merge small SSTables within a partition
    pub fn should_merge_partition(&self, partition: &PartitionId, max_sstables: usize) -> Result<bool> {
        let sstables = self.list_sstables_in_partition(partition)?;
        Ok(sstables.len() > max_sstables)
    }

    /// Ensure partition directory exists
    pub fn ensure_partition_dir(&self, partition: &PartitionId) -> Result<PathBuf> {
        let dir = self.partition_dir(partition);
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    /// Pre-create partition directories for upcoming time windows.
    /// Eliminates directory creation latency during writes.
    pub fn pre_create_partitions(&self, count: usize) -> Result<usize> {
        let now_secs = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let window_secs = self.window.duration_secs();
        let mut created = 0;

        for i in 0..count {
            let future_ts = now_secs + (i as u64 * window_secs);
            let partition = self.get_partition(future_ts);
            let dir = self.partition_dir(&partition);
            
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
                created += 1;
            }
        }

        if created > 0 {
            info!("Pre-created {} partition directories", created);
        }
        Ok(created)
    }
}

/// Result of retention enforcement
#[derive(Debug, Default)]
pub struct RetentionResult {
    pub partitions_deleted: usize,
    pub bytes_reclaimed: u64,
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_id_from_timestamp() {
        // 2024-01-15 14:30:00 UTC
        let ts = 1705329000u64;
        
        let hourly = PartitionId::from_timestamp(ts, PartitionWindow::Hourly);
        assert_eq!(hourly.date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(hourly.window, 14);

        let six_hour = PartitionId::from_timestamp(ts, PartitionWindow::SixHour);
        assert_eq!(six_hour.window, 2); // 12:00-18:00 window

        let daily = PartitionId::from_timestamp(ts, PartitionWindow::Daily);
        assert_eq!(daily.window, 0);
    }

    #[test]
    fn test_partition_timestamps() {
        let partition = PartitionId {
            date: NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            window: 2,
        };

        let start = partition.start_timestamp(PartitionWindow::SixHour);
        let end = partition.end_timestamp(PartitionWindow::SixHour);

        // Window 2 = 12:00-18:00
        assert_eq!(end - start, 6 * 3600);
    }

    #[test]
    fn test_partitions_for_range() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let manager = PartitionManager::new(
            temp_dir.path().to_path_buf(),
            PartitionWindow::SixHour,
            RetentionPolicy::default(),
        );

        // Range spanning 2 days
        let start = 1705329000u64; // 2024-01-15 14:30
        let end = 1705415400u64;   // 2024-01-16 14:30

        let partitions = manager.partitions_for_range(start, end);
        
        // Should cover: 01-15 window 2, 3, 01-16 window 0, 1, 2
        assert!(partitions.len() >= 5);
    }

    #[test]
    fn test_pre_create_partitions() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let manager = PartitionManager::new(
            temp_dir.path().to_path_buf(),
            PartitionWindow::SixHour,
            RetentionPolicy::default(),
        );

        let created = manager.pre_create_partitions(4).unwrap();
        assert!(created > 0);

        // Calling again should create 0 (already exist)
        let created_again = manager.pre_create_partitions(4).unwrap();
        assert_eq!(created_again, 0);

        // Verify directories exist
        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!entries.is_empty());
    }
}
