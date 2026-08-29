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

Keys and values are arbitrary byte sequences supplied through `AsRef<[u8]>`;
strings need to be encoded by the caller. Mutation APIs copy their inputs before
returning. Point and collecting reads return owned `Vec<u8>` values. An empty
value is valid data and is distinct from a deleted key.

#### Opening and configuration

| API | Parameters | Result and behavior |
|---|---|---|
| `Db::open(path)` | `path: AsRef<Path>` | Opens or creates the directory with `DbOptions::durable()`. The open handle exclusively owns the directory. |
| `Db::open_with_options(path, options)` | Database path and a `DbOptions` value | Opens with explicit durability, memory, cache, and compression settings. Rejects contradictory settings such as `sync_writes = true` with the WAL disabled. |
| `DbOptions::fast()` | None | Returns the no-WAL preset. |
| `DbOptions::durable()` | None | Returns the process-crash-recoverable WAL preset. |
| `DbOptions::paranoid()` | None | Returns the sync-before-acknowledgement preset. |
| `options.with_compression(compression)` | A `Compression` variant | Builder-style update that returns the modified options. |

All presets start with a 64 MiB memtable, a 64 MiB block cache, and LZ4
compression. Their public fields can be adjusted before opening:

| `DbOptions` field | Meaning |
|---|---|
| `wal_enabled: bool` | Append mutations to the WAL. Disabling it permits process-crash data loss until a successful flush or close. |
| `sync_writes: bool` | Await a WAL sync barrier before acknowledging each mutation group. Requires `wal_enabled`. |
| `memtable_size: usize` | Approximate in-memory byte threshold that triggers a memtable rotation and background flush. |
| `block_cache_size: usize` | Decompressed SSTable block-cache budget in bytes. Set to `0` to disable the cache. |
| `compression: Compression` | SSTable compression for newly written data: `Lz4`, `Snappy`, `Zstd`, or `None`. Existing tables retain their encoded format. |

#### Point, bulk, and batch operations

| API | Parameters | Returns and semantics |
|---|---|---|
| `insert(key, value)` | Byte-like key and value | `Result<()>`. Inserts or replaces the key. The selected durability boundary is reached before success. |
| `insert_many(entries)` | Any iterator of `(key, value)` pairs | `Result<()>`. Copies the full iterator and applies entries in order; the last duplicate key wins. This is a bulk API, not one atomic visibility transition. |
| `get(key)` | Byte-like key | `Result<Option<Vec<u8>>>`. Returns `None` for missing or deleted keys and `Some(Vec::new())` for a stored empty value. |
| `remove(key)` | Byte-like key | `Result<()>`. Writes a tombstone; deleting a missing key is allowed. |
| `contains_key(key)` | Byte-like key | `Result<bool>`. Resolves the same state as `get` and currently incurs its value allocation. |
| `write_batch(batch)` | `&WriteBatch` | `Result<()>`. Publishes all operations atomically; readers see either the state before the batch or the complete batch. The last operation for a duplicate key wins. |

With the WAL enabled, one record or complete batch must fit in the WAL's
`u32` payload length. A failed or cancelled mutation may already have reached
the WAL; inspect the key or reopen before retrying a non-idempotent operation.

`WriteBatch` owns copies of every key and value:

| API | Parameters | Effect |
|---|---|---|
| `WriteBatch::new()` | None | Creates an empty batch. |
| `WriteBatch::with_capacity(capacity)` | Expected operation count | Preallocates operation slots, but not key or value bytes. |
| `batch.put(key, value)` | Byte-like key and value | Appends an owned put operation. |
| `batch.delete(key)` | Byte-like key | Appends an owned delete operation. |
| `batch.ops()` | None | Borrows the ordered `&[BatchOp]` operation list. |
| `batch.len()` / `batch.is_empty()` | None | Reports the current operation count. |
| `batch.clear()` | None | Removes all operations while retaining the batch allocation for reuse. |

#### Range and prefix scans

Keys are ordered lexicographically by raw bytes. Every scan captures a coherent
point-in-time view. Creating one can freeze a nonempty active memtable, so
frequent small scans may increase later flush work.

| API | Parameters | Returns and allocation |
|---|---|---|
| `range(start, end)` | Inclusive start key and exclusive end key | `Result<Vec<(Vec<u8>, Vec<u8>)>>`; eagerly allocates every returned key and value. |
| `scan_prefix(prefix)` | Byte prefix; an empty prefix matches everything | Eagerly collects all matching key/value pairs in order. |
| `range_iter(start, end)` | The same `[start, end)` bounds | Creates a `RangeIter`. Iterator items are `Result<EntryGuard, ScanError>` because corruption can be discovered while advancing. |
| `scan_prefix_iter(prefix)` | Byte prefix | Creates a `PrefixIter`, an alias of the same streaming implementation. |

Advancing a streaming iterator is synchronous and may perform mmap reads,
checksum validation, decompression, and cache locking. Drop it promptly: the
iterator pins its snapshot readers and database-directory ownership.

| Iterator or guard API | Parameters | Result |
|---|---|---|
| `iter.count()` | None | Consumes the iterator and returns `Result<usize, ScanError>`. |
| `iter.keys()` | None | Consumes the iterator and collects owned keys without materializing memtable values. |
| `iter.collect_pairs()` | None | Consumes the iterator and collects owned key/value pairs. |
| `iter.paginate(offset, limit)` | Number of entries to skip and maximum entries to yield | Returns a lazy iterator; skipped entries are traversed but their memtable values are not copied. |
| `guard.key()` | None | Borrows the key without loading the value. |
| `guard.value()` / `guard.value_len()` | None | Borrows the value, or reports its length; a memtable value is copied only when `value()` is first requested. |
| `guard.into_pair()` / `into_key()` / `into_value()` | None | Consumes the guard and returns the requested owned bytes. |

#### Persistence, maintenance, and statistics

| API | Parameters | Returns and cost |
|---|---|---|
| `flush()` | None | `Result<()>`. Drains pending writes, installs SSTables and the manifest, syncs the WAL, and reclaims eligible WAL segments. Writes that start concurrently may need a later flush. |
| `compact()` | None | `Result<CompactionResult>`. Drains the captured compaction scope and reports actual files, bytes, duration, reclaimed tombstones, and whether work remains. |
| `status()` | None | Cheap `DatabaseStatus` snapshot of maintenance failures, retries, and write backpressure. |
| `logical_stats()` | None | Exact `Result<LogicalStats>` for unique live keys and bytes. It scans physical versions and may perform I/O. |
| `physical_stats()` | None | Cheap `PhysicalStats` gauges and process-lifetime counters for the WAL, memtables, SSTables, cache, stalls, and amplification. |
| `stats()` | None | Deprecated mixed physical counters retained for source compatibility. |
| `close()` | Consumes `Db` | Flushes pending writes, stops maintenance, and releases ownership on success. Dropping `Db` is not a clean-shutdown guarantee. |
| `close_with_status()` | Consumes `Db` | The structured shutdown form; distinguishes storage errors from unresolved flush or compaction health. |

Most database methods return `DbError`. Streaming iterator creation returns
`DbError`, while failures discovered later are yielded as `ScanError`. The
lower-level `Engine` and component configuration types are supported advanced
APIs; their complete field and method contracts are in the
[crate documentation](https://docs.rs/turbokv).

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
