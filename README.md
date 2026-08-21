<div align="center">
  <img src="docs/logo.png" alt="TurboKV Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![GitHub](https://img.shields.io/badge/repo-GitHub-blue.svg)](https://github.com/kingroryg/turbokv)
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

The self-cleaning [`basic` example](examples/basic.rs) covers insert, get,
update, and remove operations. Run it with `cargo run --example basic`.

### Batch Writes

The [`batch_writes` example](examples/batch_writes.rs) shows an atomic mix of
puts and deletes. Run it with `cargo run --example batch_writes`.

### Configuration Options

TurboKV provides three durability modes to balance speed and safety:

| Mode | WAL | Fsync | On Crash |
|------|-----|-------|----------|
| `fast()` | No | No | Flushed data survives; unflushed data lost |
| `durable()` | Yes | Not per write | Intended to survive process crashes; no power-loss guarantee for recent writes |
| `paranoid()` | Yes | Before acknowledgement | Strongest mode, subject to filesystem and device sync semantics |

**Recommended for most users:** `fast()` or `durable()` mode.

- Use **`fast()`** when data can be regenerated or occasional loss is acceptable
- Use **`durable()`** for production data that must survive process crashes

The `paranoid()` mode is for specialized use cases where you need power-loss
durability. Concurrent mutations may share an ordered write and sync barrier;
each call returns only after its containing group is durable.
If a barrier fails, every affected caller receives the same causal error and
the open handle rejects later writes. `status()` keeps the WAL failure visible
in the flush-health lane until reopen. Because a complete failed-group record
may be recovered after reopening, treat the failed mutation outcome as
indeterminate before retrying a non-idempotent operation.

The [`persistence` example](examples/persistence.rs) exercises synced WAL
recovery after an unclean drop using `DbOptions::paranoid()`.

### Custom Configuration

The [`configuration` example](examples/configuration.rs) starts from the
supported durable preset, then selects compression and documented cache and
memtable sizes.

See also the [`range_queries`](examples/range_queries.rs) and
[`concurrent`](examples/concurrent.rs) examples for ordered scans and shared
access from multiple tasks.

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
| `compact()` | Drain the captured manual-compaction scope and report actual files, bytes, duration, reclaimed space, and remaining work |
| `status()` | Get bounded maintenance/WAL health plus current write-backpressure causes and counters |
| `close_with_status()` | Close cleanly with a structured unresolved-maintenance error for production monitoring |
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
