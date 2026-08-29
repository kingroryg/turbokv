# Changelog

## Unreleased

### Added

- Added `Db::take` and `Engine::take`, which atomically return and remove the
  newest live value. Missing keys do not reserve a sequence or write a
  redundant tombstone, and concurrent mutations order wholly before or after
  the operation.

## 0.6.0 - 2026-08-28

### Production readiness

- Corrected the minimum supported Rust version to 1.85, the first toolchain
  that can resolve and parse the current unlocked dependency graph, and added
  an MSRV gate.
- Single-record WAL puts and deletes now reject keys or payloads that cannot be
  represented by the WAL's `u32` fields before allocating a sequence. Internal
  single/group encoders also reject truncation and aggregate-size overflow.
- New WAL segments use format v5. Durable writes publish a framing-and-payload
  checksum plus a tag-last commit marker through bounded, physically reserved
  shared mappings where the platform supports safe reservation, and retain the
  ordered file-write path elsewhere. Recovery treats the header cursor only as
  an acknowledged lower bound, repairs uncommitted active tails, rejects
  committed corruption, and continues to read released v1-v4 segments. Older
  TurboKV versions cannot open v5 segments; back up before upgrading when
  downgrade capability is required.
- Memtable mutations reuse one timestamp sample for capacity and entry-age
  accounting. Exact low-level age statistics now follow timestamps rather than
  key order, while engine monitoring retains a counters-only constant-time path.
- Added ordinary Linux, macOS, and Windows CI for formatting, warnings-denied
  lint and documentation, tests/examples, package verification, storage-format
  compatibility. Scheduled/manual jobs run the longer
  repeated storage, whole-database, and compression suites; dedicated native
  jobs run the selected ASan and TSan coverage.
- The production release benchmark now measures only equivalent durable rows
  with 84,000,000 logical key/value bytes, exceeding the common 64 MiB
  memtable. Bounded sync-per-write evidence moved to an explicit paranoid
  profile and focused group-commit benchmark.

### Breaking changes

TurboKV is pre-1.0. This release removes advanced interfaces that were not
part of the supported database path:

- Removed `storage::PrefixBloomFilter`. SSTables continue to use the exact-key
  Bloom filter persisted in the existing file format; prefix scans use the
  ordered streaming scan interface.
- Removed `storage::BufferPool`, `storage::PooledBuffer`, and the
  `storage::buffer_pool` module. WAL and batch writes manage their buffers at
  the live write-path seam.
- Removed `storage::direct_io`, `AlignedBuffer`, `DirectIoConfig`, and
  `DirectIoWriter`. TurboKV uses buffered or shared-mapped WAL page-cache I/O
  and memory-mapped SSTable reads; there is no direct-I/O database option.
  The corresponding portable-allocation Miri job was removed with that unsafe
  implementation. Miri cannot emulate the remaining native mmap and platform
  filesystem/descriptor calls. AddressSanitizer and ThreadSanitizer exercise
  selected x86_64-Linux paths; ordinary CI builds and runs the test suite on
  Windows and macOS, while native sanitizer coverage remains Linux-specific.
- Removed the time-partitioning and retention interfaces. TurboKV remains a
  generic raw-byte key-value store; TTL and time retention require a separate
  database contract.
- Removed the public `storage::cached_time` interface. Its coarse clock remains
  an internal WAL implementation detail because paired durable measurements
  showed a material write-throughput benefit. WAL construction initializes it
  only after read-only preflight, and failure now returns a WAL-open error
  instead of panicking; WAL bytes and recovery
  compatibility are unchanged.
- Removed `DbConfig` and `CompactionStyle`. Configure the supported database
  interface with `DbOptions`, or configure the advanced engine interface with
  `StorageConfig`.
- Removed the unused synchronous `StorageEngine` and `KvIterator` traits,
  generic serialization helpers, logging configuration types, standalone
  metrics collector, utility functions, `PROTOCOL_VERSION`, and the
  documentation-only `optimizations` module. Use `Db`/`Engine`, the scan
  iterators, application-selected serialization crates, and the database
  statistics interfaces instead.
- Removed root exports `crc32_checksum`, `MemTableManager`, `SSTableInfo`, and
  `WriteAheadLog`, plus duplicate `storage::*` re-exports for memtables, WAL,
  SSTables, cache, descriptor pool, and iterators. The supported advanced root
  interface is now `Engine`, `StorageConfig`, and the configuration types it
  contains. Direct low-level interfaces remain under `storage::wal`,
  `storage::sstable`, `storage::cache`, `storage::fd`, and
  `storage::memtable` for format compatibility, corruption tooling, and
  measured engine integration.
- Removed unused `core::Error` variants `WriteAheadLog`, `MemTable`,
  `Compaction`, `IndexCorruption`, `QueryError`, and `Configuration`, plus the
  unused `ResultExt` trait and classification helpers. Database and engine
  callers should use `DbError` and `StorageError`; low-level WAL callers should
  use `WalError`. The retained `core::Error` variants are those produced by
  live SSTable, I/O, and resource paths.
- Removed the unused standalone `FdMonitor`/`FdStatus`, duplicate SSTable
  footer/index view types, and unused SSTable/Bloom builder helpers. Descriptor
  limits and backpressure remain enforced by the live `SSTablePool`.
- Removed unused crate dependencies (`anyhow`, `async-trait`, `bincode`,
  `rmp-serde`, `rkyv`, `serde_json`, `chrono`, `crossbeam-channel`,
  `crossbeam-utils`, `dashmap`, `rayon`, and `uuid`) and unused root test
  dependencies (`fjall` and `quickcheck`). The standalone benchmark crate keeps
  its own pinned comparison-engine and JSON dependencies.
- Removed the unused `WalConfig::compression`, `WalConfig::buffer_size`, and
  `SSTableConfig::index_interval` fields. WAL compression was never
  implemented; SSTable compression remains configured through
  `SSTableConfig::compression` or `DbOptions::compression`.
- Removed the deprecated, uncoordinated low-level compaction execution
  interface (`Compactor`, its job/result types, and caller-named outputs).
  Use `Db::compact` or `Engine::compact`, which coordinate ownership, publish
  through the manifest, split outputs, and report complete request results.

### Retained-component evidence

The internal cached WAL clock was compared with direct `SystemTime` retrieval
using five alternating samples per variant in the issue-23 durable-ingest
harness: 20,000 400-byte values in sequential, random, and overwrite
workloads. Median acknowledgement throughput (cached versus direct) was
599,648 versus 541,401 operations/second sequentially, 555,569 versus 461,600
randomly, and 512,519 versus 466,546 for overwrites. Direct retrieval also
changed settled throughput by -1.0%, -8.3%, and -2.4%, respectively. The
retained clock therefore remains private to WAL construction and writes.
Format/recovery compatibility tests and the direct-WAL append/reopen contract
cover the persisted timestamp path.
