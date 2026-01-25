//! Sharded LRU cache for frequently accessed SSTable blocks.
//! Reduces lock contention by partitioning cache across multiple shards.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

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
        Self { file_id, block_offset }
    }
}

/// Single shard of the cache
struct CacheShard {
    lru: Mutex<LruCache<CacheKey, Bytes>>,
    size_bytes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Sharded LRU block cache
pub struct BlockCache {
    shards: Vec<CacheShard>,
    shard_mask: usize,
    #[allow(dead_code)]
    max_size_bytes: usize,
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
        assert!(num_shards.is_power_of_two(), "shard count must be power of 2");
        
        let per_shard_entries = std::cmp::max(16, max_size_bytes / num_shards / 4096); // ~4KB per block
        
        let shards: Vec<_> = (0..num_shards)
            .map(|_| CacheShard {
                lru: Mutex::new(LruCache::new(NonZeroUsize::new(per_shard_entries).unwrap())),
                size_bytes: AtomicU64::new(0),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            })
            .collect();

        Self {
            shards,
            shard_mask: num_shards - 1,
            max_size_bytes,
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
        let mut lru = shard.lru.lock();
        
        // Evict old entry if exists
        if let Some(old) = lru.pop(&key) {
            shard.size_bytes.fetch_sub(old.len() as u64, Ordering::Relaxed);
        }
        
        // Insert new entry (LRU handles eviction of oldest)
        if let Some((_, evicted)) = lru.push(key, value) {
            shard.size_bytes.fetch_sub(evicted.len() as u64, Ordering::Relaxed);
        }
        shard.size_bytes.fetch_add(value_len, Ordering::Relaxed);
    }

    /// Remove a block from cache (e.g., when SSTable is deleted)
    pub fn remove(&self, key: &CacheKey) {
        let shard = self.shard_for(key);
        let mut lru = shard.lru.lock();
        if let Some(old) = lru.pop(key) {
            shard.size_bytes.fetch_sub(old.len() as u64, Ordering::Relaxed);
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