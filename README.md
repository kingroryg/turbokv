<div align="center">
  <img src="docs/logo.png" alt="TurboKV Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![Codeberg](https://img.shields.io/badge/repo-Codeberg-blue.svg)](https://codeberg.org/kingroryg/turbokv)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

</div>


TurboKV is a high-performance, embedded key-value database written in Rust. It provides a clean API with configurable durability guarantees.

## Features

- **Simple API**: Familiar `get`, `insert`, `remove`, `range` operations
- **Configurable Durability**: Choose between fast, durable, or paranoid modes
- **LSM-Tree Architecture**: Optimized for write-heavy workloads
- **Async/Await**: Built on Tokio for modern async Rust
- **Batch Operations**: Write batches for grouped operations
- **Range Scans**: Efficient prefix and range queries
- **Block Cache**: Configurable caching for read performance
- **Bloom Filters**: Fast negative lookups
- **Compression**: LZ4, Snappy, and Zstd support

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

## Quick Start

Add TurboKV to your `Cargo.toml`:

```toml
[dependencies]
turbokv = "0.4"
tokio = { version = "1", features = ["full"] }
```

or just run `cargo add turbokv`


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

// Batch write (atomic in WAL, applied sequentially to memtable)
let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"old_key");
db.write_batch(&batch).await?;
```

### Configuration Options

TurboKV provides three durability modes to balance speed and safety:

| Mode | WAL | Fsync | On Crash |
|------|-----|-------|----------|
| `fast()` | No | No | Flushed data survives; unflushed data lost |
| `durable()` | Yes | Periodic | All data survives process crash |
| `paranoid()` | Yes | Every write | All data survives power loss |

**Recommended for most users:** `fast()` or `durable()` mode.

- Use **`fast()`** when data can be regenerated or occasional loss is acceptable
- Use **`durable()`** for production data that must survive process crashes

The `paranoid()` mode is for specialized use cases where you need power-loss durability. This mode is significantly slower due to fsync overhead (~257 ops/sec vs ~1.1M ops/sec).

```rust
use turbokv::{Db, DbOptions};

// Fast mode - maximum speed, no durability guarantees
// Best for: caches, temporary data, benchmarks
let db = Db::open_with_options("./data", DbOptions::fast()).await?;

// Durable mode (RECOMMENDED) - WAL protects against process crashes
// Best for: most production workloads
let db = Db::open_with_options("./data", DbOptions::durable()).await?;

// Paranoid mode - fsync on every write
// Best for: financial transactions, critical records, audit logs
let db = Db::open_with_options("./data", DbOptions::paranoid()).await?;
```

### Custom Configuration

```rust
use turbokv::{DbOptions, Compression};

let options = DbOptions {
    wal_enabled: true,           // Write-ahead log for durability
    sync_writes: false,          // Periodic sync (true = fsync every write)
    memtable_size: 64 * 1024 * 1024,   // 64MB memtable
    block_cache_size: 64 * 1024 * 1024, // 64MB block cache
    compression: Compression::Lz4,      // Lz4, Snappy, Zstd, or None
};

let db = Db::open_with_options("./data", options).await?;
```

## Performance

TurboKV is optimized for high write throughput. In fast mode it significantly outperforms RocksDB and fjall. In durable mode it trades some speed for genuine crash safety.

**Production-scale benchmark: 10M keys, 400-byte values (4.2GB total)**

| Database | Mode | Throughput |
|----------|------|------------|
| **TurboKV** | fast (no WAL) | **1,172K ops/sec** |
| **TurboKV** | durable (WAL) | **438K ops/sec** |
| RocksDB | default (WAL) | 452K ops/sec |
| fjall | default | 360K ops/sec |
| TurboKV | paranoid (fsync/write) | ~249 ops/sec |

| Other Operations | Performance |
|------------------|-------------|
| Batch writes (fast) | ~933K ops/sec |
| Concurrent writes (8 writers, paranoid) | ~1000 ops/sec |
| Mixed read/write (7 readers, 1 writer) | ~292K ops/sec |

*Benchmarks on Apple Silicon M4, SSD storage, 16GB Memory

### Understanding the Numbers

**How does TurboKV compare?**
- TurboKV fast mode is **2.6x faster** than RocksDB and **3.3x faster** than fjall
- TurboKV durable mode is **comparable to RocksDB** with equivalent durability guarantees
- TurboKV durable mode is **1.2x faster** than fjall
- All durability modes are crash-safe: WAL writes go directly to the kernel (no userspace buffering), SSTables are fsync'd before manifest updates

**Why is paranoid mode so slow?** Every write calls `fsync()` which takes 3-5ms on SSDs. This is a hardware limitation that affects all databases equally. RocksDB and fjall hit the same bottleneck (~200-300 ops/sec) when configured for power-loss durability.

**Why is durable mode slower than fast?** It writes every entry to the WAL file before updating the memtable. The OS flushes to disk periodically. Data survives process crashes but not sudden power loss. This is comparable to RocksDB's default (`sync_wal: false`).


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
| `range_iter(start, end)` | Range scan with guard-based value access |
| `scan_prefix_iter(prefix)` | Prefix scan with guard-based value access |
| `write_batch(batch)` | Batch write operations |
| `flush()` | Flush memtable to disk |
| `compact()` | Trigger manual compaction |
| `stats()` | Get database statistics |

### DbOptions

| Field | Default | Description |
|-------|---------|-------------|
| `wal_enabled` | true | Enable write-ahead log |
| `sync_writes` | false | Sync writes to disk (true = paranoid mode) |
| `memtable_size` | 64MB | MemTable size before flush |
| `block_cache_size` | 64MB | Block cache size (0 to disable) |
| `compression` | Lz4 | Compression algorithm |

### WriteBatch

| Method | Description |
|--------|-------------|
| `new()` | Create empty batch |
| `put(key, value)` | Add insert operation |
| `delete(key)` | Add delete operation |
| `len()` | Number of operations |
| `clear()` | Clear all operations |
