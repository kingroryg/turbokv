//! # Performance Optimizations
//!
//! This module documents implementation choices that keep TurboKV's write path
//! competitive with RocksDB and fjall while preserving configurable durability.
//!
//! ## Overview
//!
//! Current benchmark notes in the README separate fast mode (no WAL) from
//! durable mode (WAL enabled):
//! - Fast mode is tuned for maximum throughput when recent writes can be lost.
//! - Durable mode uses the WAL for process-crash recovery and remains in the
//!   same performance class as RocksDB's default WAL configuration.
//!
//! Always compare modes with equivalent durability settings.
//!
//! ---
//!
//! ## 1. Zero-Allocation WAL Write Path
//!
//! **Location:** [`crate::storage::wal`]
//!
//! ### Problem
//! Traditional WAL implementations allocate memory for each write:
//! - Create a new `Vec<u8>` for the encoded entry
//! - Create intermediate `Bytes` objects
//! - Multiple small writes to the file
//!
//! ### Solution
//! TurboKV uses a **thread-local pre-allocated buffer**:
//!
//! ```rust,ignore
//! thread_local! {
//!     static WAL_ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
//! }
//! ```
//!
//! Benefits:
//! - **Zero heap allocations** per write in the hot path
//! - Single `write_all()` syscall instead of multiple small writes
//! - Buffer grows once and is reused for all subsequent writes
//!
//! ### Impact
//! ~20% improvement in durable mode throughput.
//!
//! ---
//!
//! ## 2. Lock-Free MemTable with Crossbeam SkipList
//!
//! **Location:** [`crate::storage::memtable`]
//!
//! ### Problem
//! Traditional approach uses `Mutex<BTreeMap>` which creates contention:
//! - Writers block each other
//! - Readers block writers
//! - Poor scalability with concurrent access
//!
//! ### Solution
//! TurboKV uses `crossbeam_skiplist::SkipMap`:
//!
//! ```rust,ignore
//! pub(crate) data: Arc<SkipMap<Vec<u8>, MemTableEntry>>,
//! ```
//!
//! Benefits:
//! - **Lock-free reads and writes** - multiple threads can access simultaneously
//! - O(log n) operations like BTreeMap, but without locking
//! - Excellent cache locality for sequential scans
//!
//! ### Impact
//! Enables high concurrency without mutex contention.
//!
//! ---
//!
//! ## 3. Fast Hashing with gxhash
//!
//! ### Problem
//! Rust's default hasher (SipHash) is designed for security, not speed.
//!
//! ### Solution
//! TurboKV uses `gxhash` in Bloom filters:
//!
//! ```rust,ignore
//! let h = gxhash::gxhash64(key, seed);
//! ```
//!
//! Benefits:
//! - **3-5x faster** than SipHash for short keys
//! - Uses hardware acceleration (AES-NI on x86, AES on ARM)
//! - Still provides good hash distribution
//!
//! ### Impact
//! Faster Bloom filter probes on the SSTable read path.
//!
//! ---
//!
//! ## 4. Atomic Counters for Statistics
//!
//! ### Problem
//! Tracking size and entry count typically requires locking.
//!
//! ### Solution
//! TurboKV uses atomic counters:
//!
//! ```rust,ignore
//! pub(crate) size_bytes: Arc<AtomicUsize>,
//! pub(crate) entry_count: Arc<AtomicU64>,
//! pub(crate) sequence: Arc<AtomicU64>,
//! ```
//!
//! With relaxed ordering for non-critical stats:
//! ```rust,ignore
//! self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
//! ```
//!
//! Benefits:
//! - **No locks** for incrementing counters
//! - Relaxed ordering avoids memory barriers
//! - Stats are eventually consistent (acceptable for monitoring)
//!
//! ---
//!
//! ## 5. LZ4 Compression with lz4_flex
//!
//! **Location:** [`crate::storage::sstable`]
//!
//! ### Problem
//! Need compression for SSTable blocks, but compression is often CPU-bound.
//!
//! ### Solution
//! TurboKV defaults to LZ4 compression using `lz4_flex`:
//!
//! ```rust,ignore
//! CompressionType::Lz4 => {
//!     let compressed = lz4_flex::compress_prepend_size(data);
//!     Ok(compressed)
//! }
//! ```
//!
//! Benefits:
//! - **Fastest compression** algorithm available
//! - Pure Rust implementation (no C dependencies)
//! - Good compression ratio for typical workloads
//!
//! ### Compression Options
//! | Algorithm | Speed | Ratio | Use Case |
//! |-----------|-------|-------|----------|
//! | None | Fastest | 1.0x | When disk space doesn't matter |
//! | LZ4 | Very fast | ~2x | Default - best balance |
//! | Snappy | Fast | ~2x | Alternative to LZ4 |
//! | Zstd | Slow | ~3-4x | Maximum compression |
//!
//! ---
//!
//! ## 6. CRC32 with Hardware Acceleration
//!
//! **Location:** [`crate::core::crypto`]
//!
//! ### Problem
//! Checksums are computed for every WAL entry and SSTable block.
//!
//! ### Solution
//! TurboKV uses `crc32fast`:
//!
//! ```rust,ignore
//! pub fn crc32_checksum(data: &[u8]) -> u32 {
//!     crc32fast::hash(data)
//! }
//! ```
//!
//! Benefits:
//! - Uses **SSE4.2 CRC32C** instruction on x86
//! - Uses **CRC32** instruction on ARM
//! - 10-20x faster than software CRC
//!
//! ---
//!
//! ## 7. Memory-Mapped SSTable Reads
//!
//! **Location:** [`crate::storage::sstable`]
//!
//! ### Problem
//! Reading SSTables with `read()` syscalls is slow.
//!
//! ### Solution
//! TurboKV memory-maps SSTables:
//!
//! ```rust,ignore
//! let mmap = unsafe {
//!     MmapOptions::new()
//!         .map(&file)?
//! };
//! ```
//!
//! Benefits:
//! - **Zero-copy reads** - data goes directly to user space
//! - OS handles caching automatically
//! - Random access without seeking
//!
//! ---
//!
//! ## 8. Bloom Filters for Negative Lookups
//!
//! **Location:** [`crate::storage::sstable`]
//!
//! ### Problem
//! Checking if a key exists requires reading SSTable blocks.
//!
//! ### Solution
//! TurboKV includes bloom filters in each SSTable:
//!
//! ```rust,ignore
//! pub struct BloomFilter {
//!     bits: Vec<u64>,
//!     num_hashes: u32,
//! }
//! ```
//!
//! Benefits:
//! - **Skip entire SSTables** when key definitely doesn't exist
//! - 1% false positive rate with ~10 bits per key
//! - Especially effective for read-heavy workloads
//!
//! ---
//!
//! ## 9. Vectorized Batch Writes
//!
//! **Location:** [`crate::storage::wal`]
//!
//! ### Problem
//! Writing multiple entries one-by-one is inefficient.
//!
//! ### Solution
//! TurboKV batches writes into a single syscall:
//!
//! ```rust,ignore
//! pub fn write_entries_batch(file: &mut WalFile, entries: &[WalEntry]) -> Result<()> {
//!     // Pre-allocate buffer for all entries
//!     let total_size: usize = entries.iter().map(entry_size).sum();
//!     let mut buffer = Vec::with_capacity(total_size);
//!
//!     // Serialize all entries
//!     for entry in entries {
//!         serialize_entry(&mut buffer, entry)?;
//!     }
//!
//!     // Single write syscall
//!     file.file.write_all(&buffer)?;
//! }
//! ```
//!
//! Benefits:
//! - **One syscall** instead of N syscalls
//! - Better I/O scheduling by the kernel
//! - Reduced context switching
//!
//! ---
//!
//! ## 10. Cached Timestamp
//!
//! ### Problem
//! Getting the current time for each write is expensive (syscall).
//!
//! ### Solution
//! TurboKV caches the timestamp:
//!
//! ```rust,ignore
//! thread_local! {
//!     static CACHED_TIME: Cell<u64> = Cell::new(0);
//! }
//!
//! pub fn now_ms() -> u64 {
//!     // Update cache periodically, not on every call
//! }
//! ```
//!
//! Benefits:
//! - **Avoid syscall** on every write
//! - Timestamps are "close enough" for most uses
//! - Critical for high-throughput scenarios
//!
//! ---
//!
//! ## 11. Efficient Serialization with rkyv
//!
//! TurboKV includes `rkyv` for zero-copy deserialization:
//!
//! ```rust,ignore
//! use rkyv::{Archive, Deserialize, Serialize};
//! ```
//!
//! Benefits:
//! - **Zero-copy access** to archived data
//! - No parsing overhead
//! - Useful for complex data structures
//!
//! ---
//!
//! ## 12. Direct I/O (Optional)
//!
//! **Location:** [`crate::storage::direct_io`]
//!
//! TurboKV supports Direct I/O to bypass the OS page cache:
//!
//! ```rust,ignore
//! // Linux: O_DIRECT
//! // macOS: F_NOCACHE
//! let file = open_with_direct_io(path, create, write, direct)?;
//! ```
//!
//! Benefits:
//! - **Bypass OS cache** when application manages its own cache
//! - **Predictable latency** - no page cache eviction delays
//! - **Better for large datasets** that exceed RAM
//!
//! Includes `AlignedBuffer` for proper memory alignment (4KB boundary).
//!
//! ---
//!
//! ## Future Optimizations
//!
//! Potential areas for further improvement:
//! 1. **io_uring** - Async I/O on Linux for higher throughput
//! 2. **Column families** - Better cache utilization for different access patterns
//! 3. **Tiered storage** - Hot/cold data separation
//!
//! ---
//!
//! ## Measuring Performance
//!
//! Run benchmarks:
//!
//! ```bash
//! # Quick comparison (TurboKV vs RocksDB vs fjall)
//! cargo bench --bench large_scale_bench
//!
//! # Detailed statistical analysis
//! cargo bench --bench kv_benchmarks
//! ```
//!
//! Profile with:
//! ```bash
//! # CPU profiling
//! cargo build --release
//! samply record ./target/release/...
//!
//! # Memory profiling
//! MALLOC_CONF=prof:true cargo run --release ...
//! ```

// This module is documentation-only
