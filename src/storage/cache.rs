//! Sharded LRU cache for frequently accessed SSTable blocks.
//! Reduces lock contention by partitioning cache across multiple shards.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

/// Validated location of an SSTable block's in-block offset table.
///
/// Offsets remain encoded in the decompressed block. Keeping only this small
/// view avoids a per-read `Vec<u32>` while allowing the cache to account for
/// every byte of additional retained layout metadata.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockLayout {
    // One-based so `Option<BlockLayout>` uses this field as its niche and
    // retains no separate discriminant or padding.
    offsets_start_plus_one: NonZeroUsize,
    entry_count: usize,
}

impl BlockLayout {
    pub(crate) fn new(offsets_start: usize, entry_count: usize) -> Self {
        Self {
            offsets_start_plus_one: NonZeroUsize::new(
                offsets_start
                    .checked_add(1)
                    .expect("slice-backed block offset fits in usize"),
            )
            .expect("one-based block offset is nonzero"),
            entry_count,
        }
    }

    fn offsets_start(self) -> usize {
        self.offsets_start_plus_one.get() - 1
    }

    pub(crate) fn entry_count(self) -> usize {
        self.entry_count
    }

    pub(crate) fn data_end(self) -> usize {
        self.offsets_start()
    }

    pub(crate) fn entry_offset(self, block: &[u8], index: usize) -> usize {
        debug_assert!(index < self.entry_count);
        let start = self
            .offsets_start()
            .checked_add(
                index
                    .checked_mul(std::mem::size_of::<u32>())
                    .expect("validated block offset index"),
            )
            .expect("validated block offset position");
        u32::from_le_bytes(
            block[start..start + std::mem::size_of::<u32>()]
                .try_into()
                .expect("validated four-byte block offset"),
        ) as usize
    }

    pub(crate) fn entry_range(self, block: &[u8], index: usize) -> Range<usize> {
        let start = self.entry_offset(block, index);
        let end = if index + 1 < self.entry_count {
            self.entry_offset(block, index + 1)
        } else {
            self.data_end()
        };
        start..end
    }

    pub(crate) const fn retained_bytes() -> u64 {
        std::mem::size_of::<Option<Self>>() as u64
    }
}

/// A decompressed block and, for SSTable-owned entries, its validated layout.
#[derive(Debug, Clone)]
pub(crate) struct CachedBlock {
    data: Bytes,
    layout: Option<BlockLayout>,
}

impl CachedBlock {
    pub(crate) fn validated(data: Bytes, layout: BlockLayout) -> Self {
        Self {
            data,
            layout: Some(layout),
        }
    }

    fn raw(data: Bytes) -> Self {
        Self { data, layout: None }
    }

    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    pub(crate) fn layout(&self) -> Option<BlockLayout> {
        self.layout
    }

    fn retained_bytes(&self) -> u64 {
        self.data.len() as u64 + self.layout.map_or(0, |_| BlockLayout::retained_bytes())
    }
}

/// Cache key identifying a specific block in an SSTable
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    /// Process-local namespace for the opened SSTable reader.
    pub file_id: u64,
    /// Byte offset of the encoded block within that SSTable.
    pub block_offset: u64,
}

impl CacheKey {
    /// Identify one block within one opened-reader namespace.
    pub fn new(file_id: u64, block_offset: u64) -> Self {
        Self {
            file_id,
            block_offset,
        }
    }
}

/// Single shard of the cache
struct CacheShard {
    lru: Mutex<LruCache<CacheKey, CachedBlock>>,
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
    /// Number of currently retained blocks.
    pub entries: usize,
    /// Retained payload and parsed-layout bytes.
    pub size_bytes: u64,
    /// Successful lookups since this cache was created.
    pub hits: u64,
    /// Unsuccessful lookups since this cache was created.
    pub misses: u64,
    /// `hits / (hits + misses)`, or zero before any lookup.
    pub hit_rate: f64,
}

impl BlockCache {
    /// Create a new block cache with specified max size in bytes.
    /// Uses 16 shards by default for good concurrency.
    pub fn new(max_size_bytes: usize) -> Self {
        Self::with_shards(max_size_bytes, 16)
    }

    /// Create a cache with a custom power-of-two shard count.
    ///
    /// # Panics
    ///
    /// Panics when `num_shards` is zero or is not a power of two. A zero byte
    /// budget creates shards that retain no blocks.
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

    /// Clone the shared bytes for a cached block and update hit/miss counters.
    #[inline]
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.get_cached(key, false).map(|block| block.data)
    }

    pub(crate) fn get_block(&self, key: &CacheKey) -> Option<CachedBlock> {
        self.get_cached(key, true)
    }

    fn get_cached(&self, key: &CacheKey, require_layout: bool) -> Option<CachedBlock> {
        let shard = self.shard_for(key);
        let mut lru = shard.lru.lock();

        let usable = lru
            .peek(key)
            .is_some_and(|value| !require_layout || value.layout.is_some());
        if usable {
            shard.hits.fetch_add(1, Ordering::Relaxed);
            lru.get(key).cloned()
        } else {
            shard.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert shared block bytes, evicting least-recently-used entries as needed.
    ///
    /// Empty blocks and blocks larger than their shard budget are ignored.
    #[inline]
    pub fn insert(&self, key: CacheKey, value: Bytes) {
        self.insert_cached(key, CachedBlock::raw(value));
    }

    pub(crate) fn insert_block(&self, key: CacheKey, value: CachedBlock) {
        debug_assert!(value.layout.is_some());
        self.insert_cached(key, value);
    }

    fn insert_cached(&self, key: CacheKey, value: CachedBlock) {
        let value_len = value.retained_bytes();
        let shard = self.shard_for(&key);
        if value.data.is_empty() || value_len > shard.max_size_bytes {
            return;
        }
        let mut lru = shard.lru.lock();

        // Evict old entry if exists
        if let Some(old) = lru.pop(&key) {
            shard
                .size_bytes
                .fetch_sub(old.retained_bytes(), Ordering::Relaxed);
        }

        // Insert new entry (LRU handles eviction of oldest)
        if let Some((_, evicted)) = lru.push(key, value) {
            shard
                .size_bytes
                .fetch_sub(evicted.retained_bytes(), Ordering::Relaxed);
        }
        shard.size_bytes.fetch_add(value_len, Ordering::Relaxed);

        while shard.size_bytes.load(Ordering::Relaxed) > shard.max_size_bytes {
            let Some((_, evicted)) = lru.pop_lru() else {
                break;
            };
            shard
                .size_bytes
                .fetch_sub(evicted.retained_bytes(), Ordering::Relaxed);
        }
    }

    /// Remove a block if present.
    pub fn remove(&self, key: &CacheKey) {
        let shard = self.shard_for(key);
        let mut lru = shard.lru.lock();
        if let Some(old) = lru.pop(key) {
            shard
                .size_bytes
                .fetch_sub(old.retained_bytes(), Ordering::Relaxed);
        }
    }

    /// Clear all blocks while retaining cumulative hit/miss counters.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut lru = shard.lru.lock();
            lru.clear();
            shard.size_bytes.store(0, Ordering::Relaxed);
        }
    }

    /// Sample current occupancy and cumulative hit/miss counters.
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
    fn dense_validated_layout_metadata_is_charged_to_the_byte_budget() {
        const PAYLOAD: usize = 4_096;
        const ENTRIES: usize = 256;
        assert_eq!(
            std::mem::size_of::<Option<BlockLayout>>(),
            std::mem::size_of::<BlockLayout>(),
            "BlockLayout must retain its niche-optimized representation"
        );
        let charge = PAYLOAD as u64 + BlockLayout::retained_bytes();
        let cache = BlockCache::with_shards(charge as usize, 1);
        let block = CachedBlock::validated(
            Bytes::from(vec![0; PAYLOAD]),
            BlockLayout::new(PAYLOAD - ENTRIES * 4 - 4, ENTRIES),
        );

        cache.insert_block(CacheKey::new(11, 0), block.clone());
        assert_eq!(cache.stats().size_bytes, charge);
        assert!(cache.get_block(&CacheKey::new(11, 0)).is_some());

        cache.insert_block(CacheKey::new(11, 1), block);
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.size_bytes, charge);
        assert!(cache.get_block(&CacheKey::new(11, 0)).is_none());
        assert!(cache.get_block(&CacheKey::new(11, 1)).is_some());

        let too_small = BlockCache::with_shards(charge as usize - 1, 1);
        too_small.insert_block(
            CacheKey::new(12, 0),
            CachedBlock::validated(
                Bytes::from(vec![0; PAYLOAD]),
                BlockLayout::new(PAYLOAD - ENTRIES * 4 - 4, ENTRIES),
            ),
        );
        assert_eq!(too_small.stats().entries, 0);
    }

    #[test]
    fn typed_lookup_counts_incompatible_raw_entries_as_misses() {
        let cache = BlockCache::with_shards(1024, 1);
        let key = CacheKey::new(13, 7);
        cache.insert(key.clone(), Bytes::from_static(b"raw public entry"));

        assert!(cache.get_block(&key).is_none());
        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);

        assert_eq!(
            cache.get(&key).unwrap(),
            Bytes::from_static(b"raw public entry")
        );
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
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
