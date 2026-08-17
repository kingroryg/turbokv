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

## Exclusive Directory Ownership

An open TurboKV database exclusively owns its canonicalized data directory.
Opening the same directory from another `Db` or `Engine`, in the same process
or another process, fails with a dedicated directory-locked error. Shared
multi-writer access is unsupported.

TurboKV uses a persistent `.turbokv.lock` file with a non-blocking advisory
lock on Unix and Windows. The file is intentionally not deleted on close; the
operating system releases ownership when the database handle closes or the
process terminates. All programs accessing the directory must honor advisory
locks, and the underlying filesystem must implement the platform's locking
semantics. `Db::close` stops background work, flushes pending writes, and then
releases ownership. `Engine::shutdown(&self)` stops and flushes the engine but
retains ownership until the still-usable `Engine` is dropped.

## Development

```bash
# Build
cargo build --release

# Run tests
cargo test

# Run the bounded benchmark protocol
cargo bench --bench benchmarks -- --profile quick

# Format code
cargo fmt

# Lint
cargo clippy
```

## Quick Start

Add TurboKV to your `Cargo.toml`:

```toml
[dependencies]
turbokv = "0.5"
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

// Batch write (atomic in the WAL and published atomically to readers)
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

The `paranoid()` mode is for specialized use cases where you need power-loss durability. It is expected to be significantly slower because every write is synchronized before acknowledgement.

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

Historical performance claims have been removed while the deterministic
production-scale baselines are rebuilt. The current protocol and instructions
live in [`benchmarks/README.md`](benchmarks/README.md); publish numbers only with
the corresponding clean-tree JSON evidence artifact.


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
| `range_iter(start, end)` | Fallible streaming range scan with guard-based value access |
| `scan_prefix_iter(prefix)` | Fallible streaming prefix scan with guard-based value access |
| `write_batch(batch)` | Batch write operations |
| `flush()` | Flush memtable to disk |
| `compact()` | Trigger manual compaction |
| `logical_stats()` | Scan a coherent snapshot for exact unique live-key and logical-byte counts |
| `physical_stats()` | Get cheap physical gauges and counters since open without scanning logical data |
| `stats()` | Legacy mixed physical statistics (deprecated) |

`logical_stats()` counts each live key once and reports key bytes, value bytes,
and their sum. It is a fallible full snapshot scan and can freeze the active
memtable. `physical_stats()` is the monitoring path: it performs no logical
scan, separates WAL/memtable/SSTable/version/tombstone/cache/stall data, and
names process-lifetime counters with a `_since_open` suffix.

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
