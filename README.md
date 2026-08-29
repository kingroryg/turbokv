<div align="center">
  <img src="https://raw.githubusercontent.com/kingroryg/turbokv/main/docs/logo.png" alt="TurboKV Logo" width="800"/>

**A fast, embedded key-value store in Rust**

[![GitHub](https://img.shields.io/badge/repo-GitHub-blue.svg)](https://github.com/kingroryg/turbokv)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

</div>

TurboKV is an async embedded key-value database with atomic batches, ordered
range scans, configurable durability, compression, and background compaction.

## Installation

```console
cargo add turbokv
cargo add tokio --features full
```

Or add the dependencies directly:

```toml
[dependencies]
turbokv = "0.6"
tokio = { version = "1", features = ["full"] }
```

TurboKV's persisted Bloom-filter format uses hardware AES. Build x86/x86_64
targets with `RUSTFLAGS="-C target-feature=+aes,+sse2"`, and ARM/AArch64
targets with `RUSTFLAGS="-C target-feature=+aes,+neon"`. You may instead use
`-C target-cpu=native` when the binary will run only on the same CPU model or a
feature superset.

## Quick start

```rust
use turbokv::{Db, DbOptions, WriteBatch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open_with_options("./my-database", DbOptions::durable()).await?;

    db.insert(b"user:1", b"Ada").await?;
    assert_eq!(db.get(b"user:1").await?, Some(b"Ada".to_vec()));

    let mut batch = WriteBatch::new();
    batch.put(b"user:2", b"Grace");
    batch.put(b"user:3", b"Linus");
    batch.delete(b"user:1");
    db.write_batch(&batch).await?;

    for (key, value) in db.scan_prefix(b"user:").await? {
        println!(
            "{} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    db.close().await?;
    Ok(())
}
```

Runnable examples:

- [`basic`](examples/basic.rs): insert, get, update, and remove
- [`batch_writes`](examples/batch_writes.rs): atomic puts and deletes
- [`range_queries`](examples/range_queries.rs): ordered range and prefix scans
- [`concurrent`](examples/concurrent.rs): shared access from Tokio tasks
- [`persistence`](examples/persistence.rs): paranoid WAL recovery
- [`configuration`](examples/configuration.rs): cache, memtable, and compression options

## API breakdown

### Durability presets

| Preset | Acknowledgement boundary | Use case |
|---|---|---|
| `DbOptions::fast()` | In-memory visibility; no WAL | Caches and reproducible data |
| `DbOptions::durable()` | Appended to the WAL without a per-write sync | Process-crash recovery; recommended default |
| `DbOptions::paranoid()` | WAL group completed `sync_all` before return | Strongest mode, subject to filesystem/device guarantees |

One open `Db` or `Engine` exclusively owns its data directory. Use `close()` or
`close_with_status()` for a clean shutdown; dropping a handle is not a clean
shutdown contract.

### Database operations

| API | Purpose |
|---|---|
| `Db::open`, `Db::open_with_options` | Open a database with default or explicit durability/configuration |
| `insert`, `insert_many`, `get`, `remove`, `contains_key` | Point mutations and reads |
| `write_batch` | Atomically publish a batch of puts and deletes |
| `range`, `scan_prefix` | Collect an ordered point-in-time scan |
| `range_iter`, `scan_prefix_iter` | Stream a fallible ordered scan with guarded values |
| `flush`, `compact` | Force persistence or drain manual compaction work |
| `status`, `logical_stats`, `physical_stats` | Inspect maintenance health and logical/physical accounting |
| `close`, `close_with_status` | Stop maintenance, flush pending writes, and close cleanly |

`DbOptions` exposes `memtable_size`, `block_cache_size`, and `compression`.
`Compression` supports LZ4 (default), Snappy, Zstd, and no compression.
`WriteBatch::put` and `WriteBatch::delete` build one atomic visibility update.

## Benchmarks

The benchmark used TurboKV 0.6.0, fjall 2.11.2, and redb 2.6.3 in Durable
mode over three repetitions. Throughput is acknowledged keys per second;
higher is better.

| Workload | Mode | TurboKV ops/s | fjall ops/s | redb ops/s | TurboKV / fjall |
|---|---|---:|---:|---:|---:|
| Sequential fill (1 key/txn) | Durable | 1,407,678 | 485,252 | 1,397 (macOS barrier/txn) | 2.901× |
| Random fill (1 key/txn) | Durable | 834,137 | 456,924 | 1,549 (macOS barrier/txn) | 1.826× |
| Overwrite (1 key/txn) | Durable | 853,083 | 446,733 | 1,516 (macOS barrier/txn) | 1.910× |
| Sequential batch (100 keys/txn) | Durable | 2,272,259 | 511,600 | 80,197 | 4.441× |
| Sequential batch (1,000 keys/txn) | Durable | 2,333,582 | 572,671 | 134,636 | 4.075× |

Protocol: 200,000 deterministic 20-byte keys, 400-byte values (84 MB logical
input, above the 64 MiB memtable), one caller, atomic batches where shown,
compression and block cache disabled, and an uncleared OS page cache. redb
2.6.3's `Durability::Eventual` performs a macOS `F_BARRIERFSYNC` for every
transaction, while the TurboKV and fjall Durable modes stop at their
process-crash-recoverable OS-cache boundaries. Batching amortizes that fixed
redb barrier; its single-key rows are therefore architectural context rather
than a like-for-like durability claim. Cross-engine settled timings are not
compared.

Measured on 2026-08-28 with an Apple M4 (`Mac16,1`), 32 GiB RAM, macOS 15.3.2
(24D81), APFS, and rustc 1.88.0. Exact raw repetitions, latency percentiles,
dispersion, dependency versions, byte accounting, and amplification are in the
[JSON artifact](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-ingest-current.json)
and its [text report](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-ingest-current.txt).
The full methodology and rerun command are in
[`benchmarks/README.md`](benchmarks/README.md).
