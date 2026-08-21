# Durability-equivalent benchmark protocol

The isolated `turbokv-benchmarks` Cargo package compares TurboKV 0.5.0,
fjall 2.11.2, and rust-rocksdb 0.22.0/native RocksDB 8.10.0. It is isolated so
RocksDB and its native build dependencies do not enter TurboKV's protected root
lockfile. Exact direct versions are pinned in `benchmarks/Cargo.toml`; every
JSON result also contains the complete resolved dependency set and an FNV-1a
fingerprint of the ignored benchmark `Cargo.lock` used for that run.

Because adding result files changes the final commit hash, each report also
records a durable source-manifest hash. The canonical manifest is the bytewise
path-sorted `mode type blob_oid<TAB>path` output shape from
`git ls-tree -r --full-tree`, excluding only `benchmarks/results/**`, joined and
terminated with LF. The report hashes that manifest with `git hash-object
--stdin` and names the repository's SHA-1 object format. Recomputing it from the
final artifact commit must match, even when the measured pre-artifact commit has
been removed by Git garbage collection.

The root README's current acknowledgement claims are checked against
`results/apple-m4-macos-15.3.2/durability-baseline-current.json`, which must
carry a clean source-manifest hash matching the checked-out revision. Archived
reports remain reproducibility records, not current-head claims. Cross-engine
claims use only equivalent acknowledgement boundaries: the settlement phase
invokes engine-specific compaction policies and is intentionally not compared
between engines.

## Running it

The quick profile is a real, bounded one-repetition smoke across every engine
and workload:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile quick
```

The release profile uses 1,000 keys, three repetitions, five scan passes, and
five WAL-recovery reopens in each of two durability classes. It emits 162 raw
measurements (3 engines × 2 durability classes × 9 workloads × 3 repetitions).
Release runs require both an explicit size acknowledgement and a stable machine
name, and refuse a dirty tracked Git worktree:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile release --confirm-release \
  --machine "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
```

The default output is `target/benchmark-results`; override it with
`--output DIR`. Each invocation writes a human-readable `.txt` file and a
machine-readable `.json` file.

## Equivalence contract

All three engines use one caller, concurrency one, one-entry writes, a 64 MiB
memtable, WAL enabled, compression disabled, block-cache capacity zero, and the
operating-system page cache. Results carry a durability key and must only be
compared within that class:

- `durable`: acknowledgement means the WAL bytes reached the operating-system
  page cache. This protects against process failure but does not promise power-
  loss survival.
- `paranoid`: acknowledgement includes a macOS full-storage synchronization
  barrier for the WAL. This is the power-loss-oriented class.

| Setting | TurboKV | fjall | RocksDB |
|---|---|---|---|
| Durable acknowledgement | durable WAL, `sync_on_write=false`, direct write | insert then keyspace-wide `PersistMode::Buffer` | `sync=false`, then `flush_wal(false)` after every mutation |
| Paranoid acknowledgement | paranoid WAL, group size 1, delay 0, Rust `File::sync_all` | insert then keyspace-wide `PersistMode::SyncAll` | `sync=false`, `flush_wal(false)`, then Rust `File::sync_all` on the pinned active WAL |
| Write batch / callers | 1 / 1 | 1 / 1 | 1 / 1 |
| Compression / block cache | none / 0 | none / 0 | none / 0 |
| Memtable | 64 MiB | 64 MiB | 64 MiB |
| Maintenance workers | one flush task and one compaction task | one flush worker, zero compaction workers | one shared maximum background job |
| Automatic compaction during bounded run | polling interval raised to one hour | disabled | disabled |
| Mutation settlement | forced flush, then two coordinated manual drains | `SyncAll`, rotate-and-wait, then two major compactions | synchronous WAL/data flush, then two full-range compactions; Rust `File::sync_all` on every regular database file and the database directory after each blocking maintenance call |

`Buffer` and `SyncAll` are keyspace-wide in fjall, while TurboKV and RocksDB
persist the WAL for the one database/column family. With one partition and one
caller, fjall's broader API boundary does not allow it to acknowledge less work.

The bundled native RocksDB 8.10.0 build does reach `SyncWAL`/`Fsync` when
`sync=true` and `use_fsync=true`, but its Darwin build does not define
`HAVE_FULLFSYNC`; that path therefore calls ordinary `fsync`. This explains the
surprisingly fast RocksDB result from the original quick smoke. Rust
`File::sync_all` uses macOS `F_FULLFSYNC`, so the paranoid adapter instead uses
`sync=false`, drains the WAL with `flush_wal(false)`, and calls Rust
`File::sync_all` on the pinned active WAL.

RocksDB's write buffer is 64 MiB and `max_total_wal_size` is 1 GiB, while one
bounded write epoch is at most 1,000 entries (420,000 logical bytes in release),
so automatic WAL rotation cannot be driven by the dataset. Each explicit flush
ends the epoch and clears the pin. Within an epoch, the adapter validates the
pinned path/device/inode and requires its length to grow after every WAL drain;
rotation, replacement, or a failed drain aborts the run rather than emitting an
equivalence claim. This validation and full-sync call are a documented adapter
seam. `use_fsync=true` remains configured, but the same Darwin limitation also
affects RocksDB's native data-file settlement. After each blocking flush and
manual compaction, the adapter therefore calls Rust `File::sync_all` on every
regular file that can contain or describe database state and then on the
database directory. Fully-settled timing includes those conservative barriers.
The harness cannot prove that hardware honors any claimed power-loss behavior.

Every measurement uses a fresh temporary directory. The OS page cache is not
cleared, so release runs use a deterministic Latin rotation of engine order to
reduce fixed first/last ordering bias. Run on an otherwise idle host with stable
power and thermal conditions; the report records that these operator controls
are not programmatically enforced.

## Workload boundaries

Prerequisite dataset creation and prerequisite settlement are setup and are
excluded from measured time.

| Workload | Timed acknowledgement boundary | Fully settled boundary / latency sample |
|---|---|---|
| `sequential_fill` | all ordered writes returned at the selected durability boundary | forced flush and compaction fixed point / each write |
| `random_fill` | all seeded-permutation writes returned at the selected durability boundary | forced flush and compaction fixed point / each write |
| `overwrite` | all overwrites of a settled dataset returned at the selected durability boundary | forced flush and compaction fixed point / each overwrite |
| `random_read` | all seeded-order point reads returned | same boundary / each read |
| `sequential_scan` | configured complete ordered scans returned | same boundary / each full scan call |
| `mixed` | deterministic alternating 50% reads and 50% overwrites returned at the selected durability boundary | forced flush and compaction fixed point / each operation |
| `recovery` | configured WAL-only database reopens returned | same boundary; post-open key verification is excluded / each reopen |
| `flush` | explicit synchronous flush returned | same boundary / the flush call |
| `compaction` | full-scope manual compaction returned after five flushed overwrite generations are prepared | same boundary / the compaction call |

Acknowledgement throughput is `operations / acknowledgement time`. Fully
settled throughput is `operations / (acknowledgement + settlement time)`.
Maintenance workloads use live keys in their scope as the operation unit;
recovery uses reopens; scan uses keys visited. The JSON records both operation
and latency units so unlike units are not silently compared.

Release summaries report minimum, median, maximum, mean, population standard
deviation, and coefficient of variation for acknowledgement and fully settled
throughput within each engine/durability/workload key. Raw repetitions retain
p50, p95, p99, and maximum observed latency in nanoseconds, preserving useful
resolution for sub-microsecond reads. No statistic combines `durable` and
`paranoid` samples.

## Storage accounting

Directory size is the recursive sum of file lengths at the measurement
boundary. TurboKV reports exact process-lifetime WAL, flush, and compaction
counter deltas. RocksDB reports exact `WalFileBytes`, `FlushWriteBytes`,
`CompactReadBytes`, and `CompactWriteBytes` ticker deltas. fjall 2.11.2 does not
expose equivalent cumulative byte counters through its public API; its JSON
fields and write amplification are therefore `null`, never estimates.

Recovery closes and reopens each engine, which resets process-lifetime and
statistics counters. Component byte counters for recovery are therefore
`null`, rather than misleading partial deltas.

For timed mutation workloads, write amplification is
`(WAL bytes + flush bytes + compaction output bytes) / logical mutation bytes`.
Disk amplification is `directory bytes / logical live bytes`. Maintenance and
read workloads have no timed logical mutation denominator, so write
amplification is `null`.

## Protocol tests

The lightweight protocol tests remain part of the root test gates and can also
be run against the isolated package:

```console
cargo test --test benchmark_protocol
cargo test --manifest-path benchmarks/Cargo.toml --test benchmark_protocol
```
