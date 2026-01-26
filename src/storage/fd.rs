//! # File Descriptor Management
//!
//! Manages file descriptors to prevent exhaustion under sustained load.
//! Provides:
//! - Partitioned LRU pool for SSTable readers (reduces lock contention)
//! - FD usage monitoring
//! - Backpressure when approaching limits

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, info};

use super::cache::BlockCache;
use super::sstable::SSTableReader;
use crate::core::error::{Error, Result};

/// FD pool configuration
#[derive(Debug, Clone)]
pub struct FdConfig {
    /// Max SSTable readers to keep open
    pub max_open_sstables: usize,
    /// Percentage of system limit to use as soft cap (0.0-1.0)
    pub soft_limit_ratio: f64,
    /// Enable backpressure when approaching limit
    pub enable_backpressure: bool,
    /// Number of partitions (0 = auto: 2 per CPU core)
    pub partitions: usize,
}

impl Default for FdConfig {
    fn default() -> Self {
        Self {
            max_open_sstables: 256,
            soft_limit_ratio: 0.8,
            enable_backpressure: true,
            partitions: 0, // auto
        }
    }
}

/// File descriptor statistics
#[derive(Debug, Clone, Default)]
pub struct FdStats {
    pub open_sstables: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub system_limit: u64,
    pub estimated_used: u64,
    pub partitions: usize,
}

/// Single partition of the SSTable pool
struct PoolPartition {
    cache: Mutex<LruCache<PathBuf, Arc<SSTableReader>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// Partitioned LRU pool for SSTable readers - reduces lock contention
pub struct SSTablePool {
    config: FdConfig,
    partitions: Vec<PoolPartition>,
    current_open: AtomicUsize,
    system_fd_limit: u64,
    block_cache: Option<Arc<BlockCache>>,
}

impl SSTablePool {
    pub fn new(config: FdConfig) -> Self {
        Self::with_cache(config, None)
    }

    pub fn with_cache(config: FdConfig, block_cache: Option<Arc<BlockCache>>) -> Self {
        let system_fd_limit = get_fd_limit();
        let max_size = config
            .max_open_sstables
            .min(((system_fd_limit as f64 * config.soft_limit_ratio) as usize).saturating_sub(64));

        // 2 partitions per core, min 4, max 64
        let num_partitions = if config.partitions > 0 {
            config.partitions
        } else {
            (num_cpus::get() * 2).clamp(4, 64)
        };

        let per_partition = (max_size / num_partitions).max(1);

        let partitions: Vec<_> = (0..num_partitions)
            .map(|_| PoolPartition {
                cache: Mutex::new(LruCache::new(NonZeroUsize::new(per_partition).unwrap())),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            })
            .collect();

        info!(
            "SSTable pool: {} partitions, {} per partition, system_limit={}, block_cache={}",
            num_partitions,
            per_partition,
            system_fd_limit,
            block_cache.is_some()
        );

        Self {
            config,
            partitions,
            current_open: AtomicUsize::new(0),
            system_fd_limit,
            block_cache,
        }
    }

    #[inline]
    fn partition_for(&self, path: &Path) -> &PoolPartition {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.partitions.len();
        &self.partitions[idx]
    }

    /// Get or open an SSTable reader
    pub fn get(&self, path: &Path) -> Result<Arc<SSTableReader>> {
        let path_buf = path.to_path_buf();
        let partition = self.partition_for(path);

        // Check cache first
        {
            let mut cache = partition.cache.lock();
            if let Some(reader) = cache.get(&path_buf) {
                partition.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Arc::clone(reader));
            }
        }

        partition.misses.fetch_add(1, Ordering::Relaxed);

        if self.config.enable_backpressure && self.should_backpressure() {
            return Err(Error::Internal {
                message: "FD limit approaching, backpressure active".to_string(),
            });
        }

        let reader = match &self.block_cache {
            Some(cache) => Arc::new(SSTableReader::open_with_cache(path, Arc::clone(cache))?),
            None => Arc::new(SSTableReader::open(path)?),
        };

        {
            let mut cache = partition.cache.lock();
            let old_len = cache.len();
            cache.put(path_buf, Arc::clone(&reader));

            if cache.len() <= old_len && old_len > 0 {
                partition.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                self.current_open.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(reader)
    }

    /// Remove an SSTable from the pool
    pub fn remove(&self, path: &Path) {
        let partition = self.partition_for(path);
        let mut cache = partition.cache.lock();
        if cache.pop(&path.to_path_buf()).is_some() {
            self.current_open.fetch_sub(1, Ordering::Relaxed);
            debug!("Removed SSTable from pool: {:?}", path);
        }
    }

    /// Clear all cached readers
    pub fn clear(&self) {
        for p in &self.partitions {
            p.cache.lock().clear();
        }
        self.current_open.store(0, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn stats(&self) -> FdStats {
        let (hits, misses, evictions) = self.partitions.iter().fold((0, 0, 0), |acc, p| {
            (
                acc.0 + p.hits.load(Ordering::Relaxed),
                acc.1 + p.misses.load(Ordering::Relaxed),
                acc.2 + p.evictions.load(Ordering::Relaxed),
            )
        });

        FdStats {
            open_sstables: self.current_open.load(Ordering::Relaxed),
            cache_hits: hits,
            cache_misses: misses,
            evictions,
            system_limit: self.system_fd_limit,
            estimated_used: estimate_open_fds(),
            partitions: self.partitions.len(),
        }
    }

    fn should_backpressure(&self) -> bool {
        let current = estimate_open_fds();
        let threshold = (self.system_fd_limit as f64 * self.config.soft_limit_ratio) as u64;
        current >= threshold
    }
}

/// Global FD monitor for the entire process
pub struct FdMonitor {
    system_limit: u64,
    soft_limit_ratio: f64,
    #[allow(dead_code)]
    check_interval_ms: u64,
}

impl FdMonitor {
    pub fn new(soft_limit_ratio: f64) -> Self {
        Self {
            system_limit: get_fd_limit(),
            soft_limit_ratio,
            check_interval_ms: 1000,
        }
    }

    /// Check current FD usage
    pub fn check(&self) -> FdStatus {
        let current = estimate_open_fds();
        let threshold = (self.system_limit as f64 * self.soft_limit_ratio) as u64;

        FdStatus {
            current,
            limit: self.system_limit,
            threshold,
            healthy: current < threshold,
            usage_ratio: current as f64 / self.system_limit as f64,
        }
    }

    /// Returns true if it's safe to open more files
    pub fn can_open(&self, count: usize) -> bool {
        let current = estimate_open_fds();
        let threshold = (self.system_limit as f64 * self.soft_limit_ratio) as u64;
        current + count as u64 <= threshold
    }
}

/// FD status report
#[derive(Debug, Clone)]
pub struct FdStatus {
    pub current: u64,
    pub limit: u64,
    pub threshold: u64,
    pub healthy: bool,
    pub usage_ratio: f64,
}

/// Get system file descriptor limit
#[cfg(unix)]
fn get_fd_limit() -> u64 {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    // Try /proc/self/limits first (Linux)
    if let Ok(file) = File::open("/proc/self/limits") {
        let reader = BufReader::new(file);
        for line in reader.lines().flatten() {
            if line.starts_with("Max open files") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    if let Ok(limit) = parts[3].parse::<u64>() {
                        return limit;
                    }
                }
            }
        }
    }

    // Fallback to getrlimit
    unsafe {
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            return rlim.rlim_cur as u64;
        }
    }

    // Default fallback
    1024
}

#[cfg(not(unix))]
fn get_fd_limit() -> u64 {
    // Windows default
    8192
}

/// Estimate current open file descriptors
#[cfg(target_os = "linux")]
fn estimate_open_fds() -> u64 {
    std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn estimate_open_fds() -> u64 {
    // On macOS, use lsof equivalent via proc_pidinfo
    // For simplicity, use a rough estimate based on tracked resources
    // In production, could use libproc
    unsafe {
        let _pid = libc::getpid();
        let mut rusage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut rusage) == 0 {
            // This doesn't give FD count directly, but we can track internally
        }
    }
    // Fallback: count /dev/fd entries
    std::fs::read_dir("/dev/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn estimate_open_fds() -> u64 {
    0 // Can't easily estimate on other platforms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fd_limit_detection() {
        let limit = get_fd_limit();
        assert!(limit > 0, "Should detect a positive FD limit");
        println!("Detected FD limit: {}", limit);
    }

    #[test]
    fn test_fd_monitor() {
        let monitor = FdMonitor::new(0.8);
        let status = monitor.check();

        assert!(status.limit > 0);
        assert!(status.usage_ratio >= 0.0 && status.usage_ratio <= 1.0);
        println!("FD status: {:?}", status);
    }

    #[test]
    fn test_sstable_pool_config() {
        let config = FdConfig {
            max_open_sstables: 128,
            soft_limit_ratio: 0.7,
            enable_backpressure: true,
            partitions: 8,
        };
        let pool = SSTablePool::new(config);
        let stats = pool.stats();

        assert_eq!(stats.open_sstables, 0);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.partitions, 8);
    }
}
