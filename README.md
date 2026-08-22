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

This historical retained release benchmark compares only Rust-native embedded stores:
TurboKV 0.5.0, fjall 2.11.2, and redb 2.6.3. The table reports median durable
single-key acknowledgement throughput from three repetitions; higher is
better.

| Workload | TurboKV ops/s | fjall ops/s | redb ops/s | TurboKV / fjall |
|---|---:|---:|---:|---:|
| Sequential fill | 653,814 | 651,383 | 1,559 | 1.004× |
| Random fill | 852,667 | 423,708 | 1,935 | 2.012× |
| Overwrite | 791,618 | 454,874 | 1,795 | 1.740× |

Protocol: 200,000 deterministic 20-byte keys, 400-byte values (84 MB logical
input, above the 64 MiB memtable), one caller, one-entry transactions,
compression and block cache disabled, and an uncleared OS page cache. Each
engine acknowledges its documented process-crash-durable operating-system
persistence boundary. Cross-engine settled timings are not compared.

These numbers were measured before the final production-readiness documentation
and portability changes; they are retained evidence, not a current-HEAD run.
Measured on 2026-08-28 with an Apple M4 (`Mac16,1`), 32 GiB RAM, macOS 15.3.2
(24D81), APFS, and rustc 1.88.0. Sequential-fill dispersion was high, so treat
the near-tie as noise rather than a stable advantage. Exact raw repetitions,
latency percentiles, dispersion, dependency versions, byte accounting, and
amplification are in the [JSON artifact](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-current.json)
and its [text report](benchmarks/results/apple-m4-macos-15.3.2/durability-baseline-current.txt).
The full methodology and rerun command are in
[`benchmarks/README.md`](benchmarks/README.md).
