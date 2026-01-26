<div align="center">
  <img src="docs/logo.png" alt="TurboKV Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![Build Status](https://github.com/hanshiro-dev/turbokv/workflows/CI/badge.svg)](https://github.com/hanshiro-dev/turbokv/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

</div>


TurboKV is a high-performance, embedded key-value database written in Rust. It provides a clean API with configurable durability guarantees.

## Features

- **Simple API**: Familiar `get`, `insert`, `remove`, `range` operations
- **Configurable Durability**: Choose between fast, durable, or tamper-proof modes
- **LSM-Tree Architecture**: Optimized for write-heavy workloads
- **Async/Await**: Built on Tokio for modern async Rust
- **Batch Operations**: Atomic write batches for transactional writes
- **Range Scans**: Efficient prefix and range queries
- **Block Cache**: Configurable caching for read performance
- **Bloom Filters**: Fast negative lookups
- **Compression**: LZ4, Snappy, and Zstd support

## Quick Start

Add TurboKV to your `Cargo.toml`:

```toml
[dependencies]
turbokv = "0.2"
tokio = { version = "1", features = ["full"] }
```

### Basic Usage

```rust
use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database with default options (durable mode)
    let db = Db::open("./my_data").await?;

    // Insert key-value pairs
    db.insert(b"hello", b"world").await?;
    db.insert(b"user:1", b"alice").await?;

    // Get values
    if let Some(value) = db.get(b"hello").await? {
        println!("Got: {}", String::from_utf8_lossy(&value));
    }

    // Delete keys
    db.remove(b"hello").await?;

    // Range scan
    for (key, value) in db.range(b"user:", b"user:~").await? {
        println!("{}: {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Prefix scan
    let users = db.scan_prefix(b"user:").await?;

    Ok(())
}
```

### Batch Writes

```rust
use turbokv::{Db, WriteBatch};

let db = Db::open("./my_data").await?;

// Atomic batch write
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"old_key");
db.write_batch(&batch).await?;
```

### Configuration Options

TurboKV provides five durability modes to balance speed and safety:

| Mode | WAL | Fsync | Merkle | Survives |
|------|-----|-------|--------|----------|
| `fast()` | No | No | No | Nothing (max speed) |
| `durable()` | Yes | Periodic | No | Process crash |
| `durable_audit()` | Yes | Periodic | Yes | Process crash + tamper detection |
| `paranoid()` | Yes | Yes | No | Power loss |
| `tamper_proof()` | Yes | Yes | Yes | Power loss + tamper detection |

**Recommended for most users:** `fast()` or `durable()` mode.

- Use **`fast()`** when data can be regenerated (caches, derived data, temp files)
- Use **`durable()`** for production data that must survive process crashes

The other modes (`paranoid`, `tamper_proof`) are for specialized use cases where you need power-loss durability or cryptographic tamper detection. These modes are significantly slower due to fsync overhead (~263 ops/sec vs ~100K ops/sec).

```rust
use turbokv::{Db, DbOptions};

// Fast mode - maximum speed, no durability guarantees
// Best for: caches, temporary data, benchmarks
let db = Db::open_with_options("./data", DbOptions::fast()).await?;

// Durable mode (RECOMMENDED) - WAL protects against process crashes
// Best for: most production workloads
let db = Db::open_with_options("./data", DbOptions::durable()).await?;

// Durable audit mode - WAL + Merkle chains (no fsync)
// Best for: audit logs, event sourcing, append-only logs
let db = Db::open_with_options("./data", DbOptions::durable_audit()).await?;

// Paranoid mode - fsync on every write
// Best for: financial transactions, critical records
let db = Db::open_with_options("./data", DbOptions::paranoid()).await?;

// Tamper-proof mode - Merkle chains + fsync
// Best for: compliance data, legal evidence, forensic logs
let db = Db::open_with_options("./data", DbOptions::tamper_proof()).await?;
```

### Custom Configuration

```rust
use turbokv::{DbOptions, Compression};

let options = DbOptions {
    wal_enabled: true,           // Write-ahead log for durability
    merkle_enabled: false,       // Merkle chains for tamper detection
    sync_writes: true,           // Sync to disk on each write
    memtable_size: 64 * 1024 * 1024,   // 64MB memtable
    block_cache_size: 64 * 1024 * 1024, // 64MB block cache
    compression: Compression::Snappy,   // Snappy, Zstd, or None
};

let db = Db::open_with_options("./data", options).await?;

// Or use builder-style configuration
let db = Db::open_with_options(
    "./data",
    DbOptions::durable().with_compression(Compression::Zstd)
).await?;
```

**Compression options:**
- `Compression::None` - No compression (fastest writes, largest files)
- `Compression::Snappy` - Fast compression (default, good balance)
- `Compression::Zstd` - High compression ratio (smaller files, slower)

## Architecture

TurboKV uses an LSM-tree (Log-Structured Merge-tree) architecture:

```
Write Path:
  Incoming Write -> WAL (optional) -> MemTable -> Flush -> SSTable

Read Path:
  Query -> MemTable -> Block Cache -> SSTables (newest first)
           ^                          ^
           |                          |
       Hot data                  Bloom filters
       (fast)                    (skip files)
```

### Components

- **WAL (Write-Ahead Log)**: Ensures durability by logging writes before applying
- **MemTable**: In-memory skip list for fast writes
- **SSTable**: Sorted, immutable files on disk with bloom filters
- **Block Cache**: LRU cache for frequently accessed data blocks
- **Compaction**: Background process to merge SSTables and reclaim space

## Performance

TurboKV is optimized for high write throughput.

**Large-scale benchmark: 1M keys, 400-byte values (400MB total)**

| Operation | Performance |
|-----------|-------------|
| Sequential writes (fast mode) | **1.10M ops/sec** |
| Sequential writes (durable mode) | **899K ops/sec** |
| Sequential writes (durable_audit) | 38K ops/sec |
| Sequential writes (paranoid mode) | ~256 ops/sec |
| Random reads | ~760K ops/sec |
| Range scans | ~1.2M entries/sec |
| Concurrent writes (8 writers, paranoid) | ~1000 ops/sec |

*Benchmarks on Apple Silicon Mac, SSD storage, RocksDB-comparable parameters (20-byte keys, 400-byte values)*

### Understanding the Numbers

**Why is paranoid mode so slow?** Every write calls `fsync()` which takes 3-5ms on SSDs. This is a hardware limitation that affects all databases equally. RocksDB and fjall hit the same bottleneck (~200-300 ops/sec) when configured for power-loss durability.

**Why is durable mode much faster?** It writes to the WAL but relies on the OS to `fsync()` periodically (every few seconds). This "periodic sync" approach means data survives process crashes but not sudden power loss. This is what RocksDB does by default (`sync_wal: false`).

**Comparison note:** RocksDB's published benchmarks use 900M keys, 8 CPU cores, and enterprise NVMe storage. On similar workloads, RocksDB achieves ~87K overwrites/sec and ~137-189K random reads/sec. TurboKV is competitive for an embedded Rust database, though RocksDB's mature C++ implementation has decades of optimization.

## Comparison

| Feature | TurboKV | RocksDB | fjall |
|---------|---------|---------|-------|
| Language | Rust | C++ | Rust |
| Async support | Yes | No | No |
| BTreeMap-like API | Yes | No | Yes |
| Merkle chains | Yes | No | No |
| Maturity | New | Battle-tested | Established |
| Learning curve | Low | High | Low |

**When to use TurboKV:**
- You want a simple, async-native Rust API
- You need Merkle chain tamper detection
- You're building an embedded database into your application

**When to use RocksDB:**
- You need maximum performance at scale (900M+ keys)
- You need advanced features (column families, transactions, compaction tuning)
- You're comfortable with C++ bindings and complex configuration

## API Reference

### Db

| Method | Description |
|--------|-------------|
| `open(path)` | Open database with default options |
| `open_with_options(path, options)` | Open with custom options |
| `insert(key, value)` | Insert or update a key-value pair |
| `get(key)` | Get value by key |
| `remove(key)` | Delete a key |
| `contains_key(key)` | Check if key exists |
| `range(start, end)` | Scan keys in range [start, end) |
| `scan_prefix(prefix)` | Scan all keys with prefix |
| `write_batch(batch)` | Atomic batch write |
| `flush()` | Flush memtable to disk |
| `compact()` | Trigger manual compaction |
| `stats()` | Get database statistics |

### DbOptions

| Field | Default | Description |
|-------|---------|-------------|
| `wal_enabled` | true | Enable write-ahead log |
| `merkle_enabled` | false | Enable Merkle chains |
| `sync_writes` | true | Sync writes to disk |
| `memtable_size` | 64MB | MemTable size before flush |
| `block_cache_size` | 64MB | Block cache size (0 to disable) |

### WriteBatch

| Method | Description |
|--------|-------------|
| `new()` | Create empty batch |
| `put(key, value)` | Add insert operation |
| `delete(key)` | Add delete operation |
| `len()` | Number of operations |
| `clear()` | Clear all operations |

## Development

```bash
# Build
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench

# Format code
cargo fmt

# Lint
cargo clippy
```