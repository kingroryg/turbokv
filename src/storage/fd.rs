//! # File Descriptor Management
//!
//! Manages file descriptors to prevent exhaustion under sustained load.
//! Provides:
//! - Partitioned LRU pool for SSTable readers (reduces lock contention)
//! - FD usage sampling
//! - Admission errors when configured descriptor thresholds are reached

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, info};

use super::cache::{BlockCache, CacheStats};
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
    /// Number of readers currently retained by the pool.
    pub open_sstables: usize,
    /// Successful reader-pool lookups since creation.
    pub cache_hits: u64,
    /// Reader opens caused by pool misses since creation.
    pub cache_misses: u64,
    /// Readers evicted from full pool partitions since creation.
    pub evictions: u64,
    /// Process file-descriptor limit observed when the pool was created.
    pub system_limit: u64,
    /// Current best-effort count of process file descriptors.
    pub estimated_used: u64,
    /// Number of independent LRU partitions.
    pub partitions: usize,
}

/// Single partition of the SSTable pool
struct PoolPartition {
    cache: Mutex<LruCache<PathBuf, Arc<SSTableReader>>>,
    capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// Partitioned LRU pool for SSTable readers - reduces lock contention
pub struct SSTablePool {
    config: FdConfig,
    partitions: Vec<PoolPartition>,
    open_lock: Mutex<()>,
    current_open: AtomicUsize,
    system_fd_limit: u64,
    block_cache: Option<Arc<BlockCache>>,
    #[cfg(test)]
    opens_in_progress: AtomicUsize,
    #[cfg(test)]
    peak_opens_in_progress: AtomicUsize,
}

impl SSTablePool {
    /// Create a reader pool without a decompressed block cache.
    pub fn new(config: FdConfig) -> Self {
        Self::with_cache(config, None)
    }

    /// Create a reader pool that shares an optional decompressed block cache.
    ///
    /// The effective reader capacity is capped below the sampled process limit.
    /// Configuration that leaves no usable capacity creates a pass-through pool:
    /// readers can still be opened but are not retained.
    pub fn with_cache(config: FdConfig, block_cache: Option<Arc<BlockCache>>) -> Self {
        let system_fd_limit = get_fd_limit();
        let max_size = config
            .max_open_sstables
            .min(((system_fd_limit as f64 * config.soft_limit_ratio) as usize).saturating_sub(64));

        // 2 partitions per core, min 4, max 64
        let requested_partitions = if config.partitions > 0 {
            config.partitions
        } else {
            (num_cpus::get() * 2).clamp(4, 64)
        };
        // More partitions than retained readers create zero-capacity shards
        // without increasing useful concurrency.
        let num_partitions = requested_partitions.min(max_size.max(1));

        let per_partition = max_size / num_partitions;
        let remainder = max_size % num_partitions;

        let partitions: Vec<_> = (0..num_partitions)
            .map(|index| {
                let capacity = per_partition + usize::from(index < remainder);
                PoolPartition {
                    cache: Mutex::new(LruCache::new(NonZeroUsize::new(capacity.max(1)).unwrap())),
                    capacity,
                    hits: AtomicU64::new(0),
                    misses: AtomicU64::new(0),
                    evictions: AtomicU64::new(0),
                }
            })
            .collect();

        info!(
            "SSTable pool: {} partitions, {} total readers, system_limit={}, block_cache={}",
            num_partitions,
            max_size,
            system_fd_limit,
            block_cache.is_some()
        );

        Self {
            config,
            partitions,
            open_lock: Mutex::new(()),
            current_open: AtomicUsize::new(0),
            system_fd_limit,
            block_cache,
            #[cfg(test)]
            opens_in_progress: AtomicUsize::new(0),
            #[cfg(test)]
            peak_opens_in_progress: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn partition_for(&self, path: &Path) -> &PoolPartition {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.partitions.len();
        &self.partitions[idx]
    }

    /// Get or synchronously open and memory-map an SSTable reader.
    ///
    /// Misses serialize file opening across the pool and can return
    /// [`Error::Internal`] when descriptor admission is enabled and the sampled
    /// process use is at its threshold. Format, mmap, and I/O failures are
    /// returned from [`SSTableReader::open`].
    pub fn get(&self, path: &Path) -> Result<Arc<SSTableReader>> {
        let path_buf = path.to_path_buf();
        let partition = self.partition_for(path);

        // Serialize lookup and open within one partition. Besides avoiding a
        // burst of duplicate descriptors on concurrent misses, this makes
        // `remove` a strict invalidation boundary for compaction cleanup.
        let mut cache = partition.cache.lock();
        if let Some(reader) = cache.get(&path_buf) {
            partition.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(reader));
        }

        partition.misses.fetch_add(1, Ordering::Relaxed);

        // Backpressure and the transient descriptor used by `open` share one
        // process-pool admission lane. This closes the check/open race across
        // partitions while cached-reader concurrency remains sharded.
        let _open = self.open_lock.lock();
        #[cfg(test)]
        let _open_attempt = OpenAttemptGuard::new(self);

        if self.config.enable_backpressure && self.should_backpressure() {
            return Err(Error::Internal {
                message: "FD limit approaching, backpressure active".to_string(),
            });
        }

        let reader = match &self.block_cache {
            Some(cache) => Arc::new(SSTableReader::open_with_cache(path, Arc::clone(cache))?),
            None => Arc::new(SSTableReader::open(path)?),
        };

        if partition.capacity > 0 {
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

    /// Invalidate a path so future lookups cannot reuse its retained reader.
    ///
    /// Existing [`Arc`] clones remain usable until their owners drop them.
    pub fn remove(&self, path: &Path) {
        let partition = self.partition_for(path);
        let mut cache = partition.cache.lock();
        if cache.pop(&path.to_path_buf()).is_some() {
            self.current_open.fetch_sub(1, Ordering::Relaxed);
            debug!("Removed SSTable from pool: {:?}", path);
        }
    }

    /// Drop all pool-owned readers while retaining cumulative counters.
    pub fn clear(&self) {
        for p in &self.partitions {
            p.cache.lock().clear();
        }
        self.current_open.store(0, Ordering::Relaxed);
    }

    /// Sample current occupancy, process descriptor use, and cumulative counters.
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

    /// Get decompressed block-cache statistics when that cache is enabled.
    pub fn block_cache_stats(&self) -> Option<CacheStats> {
        self.block_cache.as_ref().map(|cache| cache.stats())
    }

    fn should_backpressure(&self) -> bool {
        let current = estimate_open_fds();
        let threshold = (self.system_fd_limit as f64 * self.config.soft_limit_ratio) as u64;
        current >= threshold
    }
}

#[cfg(test)]
struct OpenAttemptGuard<'a> {
    pool: &'a SSTablePool,
}

#[cfg(test)]
impl<'a> OpenAttemptGuard<'a> {
    fn new(pool: &'a SSTablePool) -> Self {
        let current = pool.opens_in_progress.fetch_add(1, Ordering::SeqCst) + 1;
        pool.peak_opens_in_progress
            .fetch_max(current, Ordering::SeqCst);
        Self { pool }
    }
}

#[cfg(test)]
impl Drop for OpenAttemptGuard<'_> {
    fn drop(&mut self) {
        self.pool.opens_in_progress.fetch_sub(1, Ordering::SeqCst);
    }
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
        if libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut rlim) == 0 {
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
        if libc::getrusage(libc::RUSAGE_SELF, &raw mut rusage) == 0 {
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
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::TempDir;

    use super::*;
    use crate::storage::sstable::{CompressionType, SSTableConfig, SSTableWriter};

    fn write_table(path: &Path, value: &[u8]) {
        let mut writer = SSTableWriter::new(
            path,
            SSTableConfig {
                compression: CompressionType::None,
                ..SSTableConfig::default()
            },
        )
        .unwrap();
        writer.add(b"key", Some(value)).unwrap();
        writer.finish().unwrap();
    }

    fn bounded_config(max_open_sstables: usize, partitions: usize) -> FdConfig {
        FdConfig {
            max_open_sstables,
            soft_limit_ratio: 1.0,
            enable_backpressure: false,
            partitions,
        }
    }

    #[test]
    fn test_fd_limit_detection() {
        let limit = get_fd_limit();
        assert!(limit > 0, "Should detect a positive FD limit");
        println!("Detected FD limit: {}", limit);
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

    #[test]
    fn partition_capacities_sum_to_the_configured_reader_limit() {
        let pool = SSTablePool::new(bounded_config(5, 4));
        assert_eq!(
            pool.partitions
                .iter()
                .map(|partition| partition.capacity)
                .sum::<usize>(),
            5
        );
        assert_eq!(
            pool.partitions
                .iter()
                .map(|partition| partition.capacity)
                .collect::<Vec<_>>(),
            vec![2, 1, 1, 1]
        );
    }

    #[test]
    fn concurrent_misses_never_retain_more_readers_than_configured() {
        const READERS: usize = 3;
        const TABLES: usize = 24;
        let directory = TempDir::new().unwrap();
        let paths = (0..TABLES)
            .map(|index| {
                let path = directory.path().join(format!("{index:02}.sst"));
                write_table(&path, &[index as u8]);
                path
            })
            .collect::<Vec<_>>();
        let pool = Arc::new(SSTablePool::new(bounded_config(READERS, 4)));
        let start = Arc::new(Barrier::new(TABLES));
        let workers = paths
            .into_iter()
            .map(|path| {
                let pool = Arc::clone(&pool);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    assert!(pool.get(&path).unwrap().get(b"key").unwrap().is_some());
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let cached = pool
            .partitions
            .iter()
            .map(|partition| partition.cache.lock().len())
            .sum::<usize>();
        let stats = pool.stats();
        assert_eq!(
            stats.open_sstables, cached,
            "tables={TABLES}, readers={READERS}"
        );
        assert!(stats.open_sstables <= READERS);
        assert_eq!(stats.cache_misses, TABLES as u64);
        assert_eq!(pool.peak_opens_in_progress.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn same_path_same_size_replacement_cannot_reuse_reader_or_block_cache_data() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("table.sst");
        let replacement = directory.path().join("replacement.sst");
        write_table(&path, b"old");
        write_table(&replacement, b"new");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            std::fs::metadata(&replacement).unwrap().len()
        );
        let block_cache = Arc::new(BlockCache::with_shards(1024 * 1024, 1));
        let pool = SSTablePool::with_cache(bounded_config(2, 1), Some(block_cache));

        assert_eq!(
            pool.get(&path).unwrap().get(b"key").unwrap().unwrap(),
            b"old"[..]
        );
        pool.remove(&path);
        std::fs::rename(replacement, &path).unwrap();

        assert_eq!(
            pool.get(&path).unwrap().get(b"key").unwrap().unwrap(),
            b"new"[..]
        );
        assert_eq!(pool.stats().open_sstables, 1);
    }
}
