<div align="center">
  <img src="docs/logo.png" alt="TurboKV Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![GitHub](https://img.shields.io/badge/repo-GitHub-blue.svg)](https://github.com/kingroryg/turbokv)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
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

## Operational and Durability Assumptions

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
releases that `Db` handle's ownership. A live streaming scan retains an
ownership guard, so the directory remains locked until the iterator is also
dropped. `Engine::shutdown(&self)` stops and flushes the engine but retains
ownership until the still-usable `Engine` and every scan iterator are dropped.

TurboKV uses regular buffered files and memory-mapped SSTables. WAL writes can
bypass a Rust `BufWriter`, but they still pass through the operating-system page
cache; TurboKV does not use `O_DIRECT`. Acknowledged `durable()` writes have
reached that page cache and are intended to survive a database-process crash,
not an operating-system or power failure. `paranoid()` additionally completes
`File::sync_all` before acknowledging the containing WAL group. The hardware,
device controller, kernel, filesystem, and any virtualization layer must honor
that flush for the barrier to survive power loss; TurboKV cannot verify this.

The data directory must provide coherent memory mapping, atomic same-directory
rename or replacement, advisory file locking, and the platform's documented
file-sync behavior. On Unix, TurboKV also synchronizes directories after
durability-critical entry changes. Rust exposes no portable directory fsync on
other targets; Windows uses write-through replacement and synchronized files as
the strongest available ordering. Network, distributed, removable, and
user-space filesystems require their own validation before production use.

Dropping the last handle releases ownership but is not a clean-shutdown API.
Use `Db::close` (or `close_with_status`) to stop maintenance and flush pending
writes before releasing the lock. `Engine::shutdown` is idempotent, stops and
flushes the engine, but deliberately retains directory ownership until every
engine handle and scan guard is dropped. Cancelling an async mutation does not
roll it back: after submission, especially for a grouped paranoid write, its
outcome can be indeterminate and must be checked or retried idempotently.

## Development

```bash
# Build
cargo build --release

# Run tests
cargo test

# Run the bounded benchmark protocol
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- --profile quick

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

The current release evidence is retained as
[`durability-baseline-current.json`](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-current.json)
with a readable companion
[`durability-baseline-current.txt`](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-current.txt).
It was generated from a clean source manifest matching this revision on
**2026-08-21** after the ingestion, read-path, and compaction work in issues
#23–#25.

The equivalent `durable` acknowledgement rows return after each engine's WAL
reaches the OS page cache without an fsync. Sequential fill was noisy enough
that no winner is claimed; its three-repetition median is reported only inside
a **0.40–1.60× fjall** evidence bound. Across random fill and overwrite,
TurboKV delivered **0.50–1.00× fjall's acknowledgement throughput**. In the
deterministic 50/50 mixed read/overwrite workload, TurboKV delivered
**1.20–2.50× fjall's acknowledgement throughput**. These deliberately
conservative median bounds are checked against the retained artifact; its JSON
contains exact rates, latency percentiles, population CV, and every raw
repetition. Cells with high CV are noisy observations, not stable predictions.

No cross-engine claim uses the report's “fully settled” timings. Settlement
forces each engine's own flush and compaction APIs, whose rewrite policies are
not equivalent: fjall's major compaction can rewrite data when TurboKV's
pressure-based compaction correctly reports no work. Those timings remain
useful within one engine but cannot establish that TurboKV is faster than
fjall.

Provenance: Apple M4 (`Mac16,1`), 32 GiB RAM, 10 logical CPUs, aarch64; macOS
15.3.2 build 24D81; APFS; `rustc 1.88.0 (6b00bc388 2025-06-23)`. Each workload
used 1,000 deterministic 20-byte keys and 400-byte values, three repetitions,
one caller, one-entry writes, WAL enabled, 64 MiB memtables, compression off,
block cache zero, and an uncleared OS page cache; scan and recovery settings
were five passes/cycles with seed `0x545552424f4b5604`. The machine was required
to be otherwise idle, but power, thermal state, and background load were
operator-controlled rather than enforced. Exact command:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- --profile release --confirm-release --machine "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2" --output ../target/issue-28-release
```

See [`benchmarks/README.md`](benchmarks/README.md) for the complete equivalence,
workload, settlement, storage-accounting, and rerun protocol.


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
