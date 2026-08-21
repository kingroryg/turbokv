//! Sharded LRU cache for frequently accessed SSTable blocks.
//! Reduces lock contention by partitioning cache across multiple shards.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

/// Cache key identifying a specific block in an SSTable
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub file_id: u64,
    pub block_offset: u64,
}

impl CacheKey {
    pub fn new(file_id: u64, block_offset: u64) -> Self {
        Self {
            file_id,
            block_offset,
        }
    }
}

/// Single shard of the cache
struct CacheShard {
    lru: Mutex<LruCache<CacheKey, Bytes>>,
    max_size_bytes: u64,
    size_bytes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Sharded LRU block cache
pub struct BlockCache {
    shards: Vec<CacheShard>,
    shard_mask: usize,
}

/// Cache statistics
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub entries: usize,
    pub size_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

impl BlockCache {
    /// Create a new block cache with specified max size in bytes.
    /// Uses 16 shards by default for good concurrency.
    pub fn new(max_size_bytes: usize) -> Self {
        Self::with_shards(max_size_bytes, 16)
    }

    /// Create cache with custom shard count (must be power of 2)
    pub fn with_shards(max_size_bytes: usize, num_shards: usize) -> Self {
        assert!(
            num_shards.is_power_of_two(),
            "shard count must be power of 2"
        );

        let per_shard_max_bytes = max_size_bytes / num_shards;
        let remainder = max_size_bytes % num_shards;

        let shards: Vec<_> = (0..num_shards)
            .map(|index| {
                let max_size_bytes = per_shard_max_bytes + usize::from(index < remainder);
                CacheShard {
                    // Manual byte eviction is authoritative. The unbounded
                    // constructor does not eagerly reserve entry metadata;
                    // zero-length values are rejected below so the number of
                    // retained entries is still bounded by the byte budget.
                    lru: Mutex::new(LruCache::unbounded()),
                    max_size_bytes: max_size_bytes as u64,
                    size_bytes: AtomicU64::new(0),
                    hits: AtomicU64::new(0),
                    misses: AtomicU64::new(0),
                }
            })
            .collect();

        Self {
            shards,
            shard_mask: num_shards - 1,
        }
    }

    #[inline]
    fn shard_for(&self, key: &CacheKey) -> &CacheShard {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) & self.shard_mask;
        &self.shards[idx]
    }

    /// Get a block from cache
    #[inline]
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let shard = self.shard_for(key);
        let mut lru = shard.lru.lock();

        match lru.get(key) {
            Some(value) => {
                shard.hits.fetch_add(1, Ordering::Relaxed);
                Some(value.clone())
            }
            None => {
                shard.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a block into cache
    #[inline]
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        let value_len = value.len() as u64;
        let shard = self.shard_for(&key);
        if value_len == 0 || value_len > shard.max_size_bytes {
            return;
        }
        let mut lru = shard.lru.lock();

        // Evict old entry if exists
        if let Some(old) = lru.pop(&key) {
            shard
                .size_bytes
                .fetch_sub(old.len() as u64, Ordering::Relaxed);
        }

        // Insert new entry (LRU handles eviction of oldest)
        if let Some((_, evicted)) = lru.push(key, value) {
            shard
                .size_bytes
                .fetch_sub(evicted.len() as u64, Ordering::Relaxed);
        }
        shard.size_bytes.fetch_add(value_len, Ordering::Relaxed);

        while shard.size_bytes.load(Ordering::Relaxed) > shard.max_size_bytes {
            let Some((_, evicted)) = lru.pop_lru() else {
                break;
            };
            shard
                .size_bytes
                .fetch_sub(evicted.len() as u64, Ordering::Relaxed);
        }
    }

    /// Remove a block from cache (e.g., when SSTable is deleted)
    pub fn remove(&self, key: &CacheKey) {
        let shard = self.shard_for(key);
        let mut lru = shard.lru.lock();
        if let Some(old) = lru.pop(key) {
            shard
                .size_bytes
                .fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut lru = shard.lru.lock();
            lru.clear();
            shard.size_bytes.store(0, Ordering::Relaxed);
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats::default();

        for shard in &self.shards {
            let lru = shard.lru.lock();
            stats.entries += lru.len();
            stats.size_bytes += shard.size_bytes.load(Ordering::Relaxed);
            stats.hits += shard.hits.load(Ordering::Relaxed);
            stats.misses += shard.misses.load(Ordering::Relaxed);
        }

        let total = stats.hits + stats.misses;
        stats.hit_rate = if total > 0 {
            stats.hits as f64 / total as f64
        } else {
            0.0
        };

        stats
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    #[test]
    fn cache_enforces_actual_byte_budget_for_nonstandard_block_sizes() {
        let cache = BlockCache::with_shards(10_000, 1);
        cache.insert(CacheKey::new(1, 0), Bytes::from(vec![1; 6_000]));
        cache.insert(CacheKey::new(1, 1), Bytes::from(vec![2; 6_000]));

        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.size_bytes, 6_000);
        assert!(cache.get(&CacheKey::new(1, 0)).is_none());
        assert!(cache.get(&CacheKey::new(1, 1)).is_some());

        cache.insert(CacheKey::new(1, 2), Bytes::from(vec![3; 10_001]));
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.size_bytes, 6_000);
        assert!(cache.get(&CacheKey::new(1, 2)).is_none());
    }

    #[test]
    fn byte_budget_not_an_estimated_entry_count_controls_eviction() {
        let cache = BlockCache::with_shards(32, 1);
        for offset in 0..33 {
            cache.insert(CacheKey::new(7, offset), Bytes::from_static(b"x"));
        }

        let stats = cache.stats();
        assert_eq!(stats.entries, 32);
        assert_eq!(stats.size_bytes, 32);
        assert!(cache.get(&CacheKey::new(7, 0)).is_none());
        assert!(cache.get(&CacheKey::new(7, 32)).is_some());
        cache.insert(CacheKey::new(7, 33), Bytes::new());
        assert!(cache.get(&CacheKey::new(7, 33)).is_none());
    }

    #[test]
    fn concurrent_replacements_and_evictions_keep_exact_byte_accounting() {
        const THREADS: usize = 8;
        const INSERTS: usize = 96;
        const BUDGET: usize = 4_096;

        let cache = Arc::new(BlockCache::with_shards(BUDGET, 1));
        let start = Arc::new(Barrier::new(THREADS));
        let mut workers = Vec::new();
        for worker in 0..THREADS {
            let cache = Arc::clone(&cache);
            let start = Arc::clone(&start);
            workers.push(thread::spawn(move || {
                start.wait();
                for offset in 0..INSERTS {
                    let len = 1 + (worker * 17 + offset * 29) % 127;
                    // Shared offsets exercise replacement accounting while
                    // worker-specific offsets force byte-budget evictions.
                    let key = CacheKey::new((worker % 2) as u64, offset as u64);
                    cache.insert(key.clone(), Bytes::from(vec![worker as u8; len]));
                    if offset % 7 == 0 {
                        let _ = cache.get(&key);
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        let stats = cache.stats();
        let retained_bytes = (0..2_u64)
            .flat_map(|file_id| {
                (0..INSERTS as u64).map(move |offset| CacheKey::new(file_id, offset))
            })
            .filter_map(|key| cache.get(&key))
            .map(|value| value.len() as u64)
            .sum::<u64>();
        assert_eq!(
            stats.size_bytes, retained_bytes,
            "threads={THREADS}, inserts={INSERTS}"
        );
        assert!(stats.size_bytes <= BUDGET as u64);
        assert!(stats.entries <= INSERTS * 2);
    }
}
