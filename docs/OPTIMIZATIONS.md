# TurboKV Performance Optimizations

This document describes the key optimizations that make TurboKV 2x faster than RocksDB and fjall.

## Overview

TurboKV achieves **1.1M ops/sec** for durable writes (WAL enabled), compared to:
- RocksDB: 560K ops/sec
- fjall: 501K ops/sec

This is a **2x improvement** with equivalent durability guarantees.

---

## 1. Zero-Allocation WAL Write Path

**Location:** `src/storage/wal/mod.rs`

### Problem
Traditional WAL implementations allocate memory for each write:
- Create a new `Vec<u8>` for the encoded entry
- Create intermediate `Bytes` objects
- Multiple small writes to the file

### Solution
TurboKV uses a **thread-local pre-allocated buffer**:

```rust
thread_local! {
    static WAL_ENCODE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}
```

Benefits:
- **Zero heap allocations** per write in the hot path
- Single `write_all()` syscall instead of multiple small writes
- Buffer grows once and is reused for all subsequent writes

### Impact
~20% improvement in durable mode throughput.

---

## 2. Lock-Free MemTable with Crossbeam SkipList

**Location:** `src/storage/memtable/table.rs`

### Problem
Traditional approach uses `Mutex<BTreeMap>` which creates contention:
- Writers block each other
- Readers block writers
- Poor scalability with concurrent access

### Solution
TurboKV uses `crossbeam_skiplist::SkipMap`:

```rust
pub(crate) data: Arc<SkipMap<Vec<u8>, MemTableEntry>>,
```

Benefits:
- **Lock-free reads and writes** - multiple threads can access simultaneously
- O(log n) operations like BTreeMap, but without locking
- Excellent cache locality for sequential scans

### Impact
Enables high concurrency without mutex contention.

---

## 3. Fast Hashing with gxhash

**Location:** `src/storage/memtable/table.rs`

### Problem
Rust's default hasher (SipHash) is designed for security, not speed.

### Solution
TurboKV uses `gxhash` for internal hash maps:

```rust
use gxhash::GxBuildHasher;
type FastHashMap<K, V> = HashMap<K, V, GxBuildHasher>;
```

Benefits:
- **3-5x faster** than SipHash for short keys
- Uses hardware acceleration (AES-NI on x86, AES on ARM)
- Still provides good hash distribution

### Impact
Faster internal lookups in index structures.

---

## 4. Atomic Counters for Statistics

**Location:** `src/storage/memtable/table.rs`

### Problem
Tracking size and entry count typically requires locking.

### Solution
TurboKV uses atomic counters:

```rust
pub(crate) size_bytes: Arc<AtomicUsize>,
pub(crate) entry_count: Arc<AtomicU64>,
pub(crate) sequence: Arc<AtomicU64>,
```

With relaxed ordering for non-critical stats:
```rust
self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
```

Benefits:
- **No locks** for incrementing counters
- Relaxed ordering avoids memory barriers
- Stats are eventually consistent (acceptable for monitoring)

---

## 5. LZ4 Compression with lz4_flex

**Location:** `src/storage/sstable/compression.rs`

### Problem
Need compression for SSTable blocks, but compression is often CPU-bound.

### Solution
TurboKV defaults to LZ4 compression using `lz4_flex`:

```rust
CompressionType::Lz4 => {
    let compressed = lz4_flex::compress_prepend_size(data);
    Ok(compressed)
}
```

Benefits:
- **Fastest compression** algorithm available
- Pure Rust implementation (no C dependencies)
- Good compression ratio for typical workloads

### Compression Options
| Algorithm | Speed | Ratio | Use Case |
|-----------|-------|-------|----------|
| None | Fastest | 1.0x | When disk space doesn't matter |
| LZ4 | Very fast | ~2x | Default - best balance |
| Snappy | Fast | ~2x | Alternative to LZ4 |
| Zstd | Slow | ~3-4x | Maximum compression |

---

## 6. CRC32 with Hardware Acceleration

**Location:** `src/core/crypto.rs`

### Problem
Checksums are computed for every WAL entry and SSTable block.

### Solution
TurboKV uses `crc32fast`:

```rust
pub fn crc32_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}
```

Benefits:
- Uses **SSE4.2 CRC32C** instruction on x86
- Uses **CRC32** instruction on ARM
- 10-20x faster than software CRC

---

## 7. Memory-Mapped SSTable Reads

**Location:** `src/storage/sstable/reader.rs`

### Problem
Reading SSTables with `read()` syscalls is slow.

### Solution
TurboKV memory-maps SSTables:

```rust
let mmap = unsafe {
    MmapOptions::new()
        .map(&file)?
};
```

Benefits:
- **Zero-copy reads** - data goes directly to user space
- OS handles caching automatically
- Random access without seeking

---

## 8. Bloom Filters for Negative Lookups

**Location:** `src/storage/sstable/bloom.rs`

### Problem
Checking if a key exists requires reading SSTable blocks.

### Solution
TurboKV includes bloom filters in each SSTable:

```rust
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
}
```

Benefits:
- **Skip entire SSTables** when key definitely doesn't exist
- 1% false positive rate with ~10 bits per key
- Especially effective for read-heavy workloads

---

## 9. Vectorized Batch Writes

**Location:** `src/storage/wal/file.rs`

### Problem
Writing multiple entries one-by-one is inefficient.

### Solution
TurboKV batches writes into a single syscall:

```rust
pub fn write_entries_batch(file: &mut WalFile, entries: &[WalEntry]) -> Result<()> {
    // Pre-allocate buffer for all entries
    let total_size: usize = entries.iter().map(entry_size).sum();
    let mut buffer = Vec::with_capacity(total_size);

    // Serialize all entries
    for entry in entries {
        serialize_entry(&mut buffer, entry)?;
    }

    // Single write syscall
    file.file.write_all(&buffer)?;
}
```

Benefits:
- **One syscall** instead of N syscalls
- Better I/O scheduling by the kernel
- Reduced context switching

---

## 10. Cached Timestamp

**Location:** `src/storage/cached_time.rs`

### Problem
Getting the current time for each write is expensive (syscall).

### Solution
TurboKV caches the timestamp:

```rust
thread_local! {
    static CACHED_TIME: Cell<u64> = Cell::new(0);
}

pub fn now_ms() -> u64 {
    // Update cache periodically, not on every call
}
```

Benefits:
- **Avoid syscall** on every write
- Timestamps are "close enough" for most uses
- Critical for high-throughput scenarios

---

## 11. Efficient Serialization with rkyv

**Location:** `src/core/serialization.rs`

TurboKV includes `rkyv` for zero-copy deserialization:

```rust
use rkyv::{Archive, Deserialize, Serialize};
```

Benefits:
- **Zero-copy access** to archived data
- No parsing overhead
- Useful for complex data structures

---

## Removed Optimizations

### Merkle Chains (Removed)
Originally TurboKV included Merkle chains for tamper detection, but this added **96 bytes per WAL entry**. For most use cases, CRC32 checksums provide sufficient integrity verification.

If you need cryptographic tamper detection, consider:
- Signing batches of entries instead of individual entries
- Using external audit logging

---

## Future Optimizations

Potential areas for further improvement:
1. **Direct I/O** - Bypass OS page cache for more predictable latency
2. **io_uring** - Async I/O on Linux for higher throughput
3. **Column families** - Better cache utilization for different access patterns
4. **Tiered storage** - Hot/cold data separation

---

## Measuring Performance

Run benchmarks:

```bash
# Quick comparison (TurboKV vs RocksDB vs fjall)
cargo bench --bench large_scale_bench

# Detailed statistical analysis
cargo bench --bench kv_benchmarks
```

Profile with:
```bash
# CPU profiling
cargo build --release
samply record ./target/release/...

# Memory profiling
MALLOC_CONF=prof:true cargo run --release ...
```
