# Changelog

## Unreleased

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
  `DirectIoWriter`. TurboKV continues to use buffered WAL files and memory-
  mapped SSTable reads; there is no direct-I/O database option.
  The corresponding portable-allocation Miri job was removed with that unsafe
  implementation. Miri cannot emulate the remaining native mmap and platform
  filesystem/descriptor calls. AddressSanitizer and ThreadSanitizer exercise
  selected x86_64-Linux paths; Windows and macOS syscall branches still need
  platform-specific build and runtime coverage.
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
