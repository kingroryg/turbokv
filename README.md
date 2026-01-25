<div align="center">
  <img src="docs/logo.png" alt="HanshiroDB Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![Build Status](https://github.com/hanshiro-dev/turbokv/workflows/CI/badge.svg)](https://github.com/hanshiro-dev/turbokv/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

</div>
---

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

TurboKV provides preset configurations for common use cases:

```rust
use turbokv::{Db, DbOptions};

// Fast mode - maximum speed, no durability guarantees
// Good for: caches, temporary data, benchmarks
let db = Db::open_with_options("./data", DbOptions::fast()).await?;

// Durable mode (default) - survives crashes
// Good for: production data that must not be lost
let db = Db::open_with_options("./data", DbOptions::durable()).await?;

// Tamper-proof mode - cryptographic integrity
// Good for: audit logs, compliance data, legal evidence
let db = Db::open_with_options("./data", DbOptions::tamper_proof()).await?;
```

### Custom Configuration

```rust
use turbokv::DbOptions;

let options = DbOptions {
    wal_enabled: true,           // Write-ahead log for durability
    merkle_enabled: false,       // Merkle chains for tamper detection
    sync_writes: true,           // Sync to disk on each write
    memtable_size: 64 * 1024 * 1024,   // 64MB memtable
    block_cache_size: 64 * 1024 * 1024, // 64MB block cache
};

let db = Db::open_with_options("./data", options).await?;
```

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

TurboKV is optimized for high write throughput:

| Operation | Performance |
|-----------|-------------|
| Sequential writes (fast mode) | ~1.5M ops/sec |
| Sequential writes (durable mode) | ~60K ops/sec |
| Random reads | ~500K ops/sec |
| Batch writes (1000 per batch) | ~2M ops/sec |
| Range scans | ~100K entries/sec |

*Benchmarks on Apple M1, 16GB RAM, SSD storage*

## Comparison

| Feature | TurboKV | RocksDB | fjall |
|---------|---------|---------|-------|
| Rust-native | Yes | No (C++) | Yes |
| Async | Yes | No | No |
| BTreeMap API | Yes | No | Yes |
| Merkle chains | Yes | No | No |
| Learning curve | Low | High | Low |

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

## License

Apache 2.0

---