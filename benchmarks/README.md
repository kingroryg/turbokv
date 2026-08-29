# Durability-equivalent benchmark protocol

The isolated `turbokv-benchmarks` Cargo package compares three Rust-native
embedded stores: TurboKV 0.6.0, fjall 2.11.2, and redb 2.6.3. Exact direct
versions are pinned in `benchmarks/Cargo.toml`; every JSON result also contains
the complete resolved dependency set and an FNV-1a fingerprint of the ignored
benchmark `Cargo.lock` used for that run.

Each report records a durable source-manifest hash. The canonical manifest is
the bytewise path-sorted `mode type blob_oid<TAB>path` output shape from
`git ls-tree -r --full-tree`, excluding only `benchmarks/results/**`, joined and
terminated with LF. The report hashes that manifest with `git hash-object
--stdin` and names the repository's SHA-1 object format.

`results/apple-m4-macos-15.3.2/durability-baseline-current.json` is the latest
retained TurboKV 0.5.0 release run, not an assertion that the measured source equals the
checked-out revision. The root README labels it historical and derives its
exact table rows from the artifact. Because no single primary workload was
designated before measurement, the retained release check requires TurboKV to
exceed fjall on all three durable single-key ingest shapes: sequential fill,
random fill, and overwrite. It intentionally forbids a fixed-multiple headline
because random-fill throughput and repeated-run dispersion are sensitive enough
that such a floor was not repeatable. Cross-engine claims use only equivalent
acknowledgement boundaries: the settlement phase invokes engine-specific
compaction policies and is intentionally not compared between engines. A fresh
run is required before describing results as current-source evidence.

## Running it

All deterministic datasets use seed `0x545552424f4b5604`.

The quick profile is a real, bounded one-repetition smoke across every engine
and workload:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile quick
```

The ingest profile is the focused production-scale table used by the root
README. It runs the three single-key mutation shapes plus sequential atomic
batches of 100 and 1,000 keys, for 45 measurements (3 engines × 1 durable
class × 5 workloads × 3 repetitions):

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile ingest --confirm-release \
  --machine "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
```

Throughput for batch workloads is inserted keys per second. Their latency
samples are complete atomic batch commits, not individual keys.

The release profile is the complete production-scale durable comparison. It uses
200,000 keys, three repetitions, five scan passes, and five recovery
reopens. Its 84,000,000 logical key/value bytes exceed the common 64 MiB
memtable, crossing the in-memory generation boundary and making the settlement
phase persists more data than one configured memtable. Automatic maintenance is
still deferred by the equivalence contract, so acknowledgement rows do not
claim background-flush or backpressure throughput. The profile emits 81 raw
measurements plus 18 atomic-batch measurements (3 engines × 1 durable class ×
11 workloads × 3 repetitions).
Release runs require both an explicit acknowledgement and a stable machine
name, and refuse a dirty tracked Git worktree:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile release --confirm-release \
  --machine "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
```

Power-loss-oriented single-write measurements are intentionally separate and
bounded: an fsync per acknowledgement is governed primarily by storage sync
latency and does not need to overflow the memtable to measure that boundary.
The paranoid profile uses 1,000 keys and emits the same 99-cell workload matrix
for only the paranoid durability class:

```console
cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- \
  --profile paranoid --confirm-release \
  --machine "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
```

TurboKV's focused concurrency/grouping benchmark is independent of the
cross-engine matrix:

```console
cargo bench --bench paranoid_group_commit
```

A single sequential caller cannot share its current sync barrier with another
in-flight caller, so its throughput is physically bounded by device and
filesystem sync latency. Use an explicit atomic write batch when multiple
mutations form one transaction, or concurrent independent callers when their
acknowledgements may safely share the configured group-commit window. Neither
choice weakens the paranoid acknowledgement boundary.

The default output is `target/benchmark-results`; override it with
`--output DIR`. Each invocation writes a human-readable `.txt` file and a
machine-readable `.json` file.

## Equivalence contract

All three engines use one caller, concurrency one, one-entry transactions,
durable storage, zero configured block-cache capacity, and the operating-system
page cache. Compression is disabled where the engine supports it. TurboKV and
fjall use a 64 MiB memtable; redb is a copy-on-write B-tree and has no memtable.
Results carry a durability key and must only be compared within that class:

- `durable`: acknowledgement means the engine's recovery state reached its
  named non-full-sync operating-system persistence boundary. This protects
  against process failure but does not promise power-loss survival.
- `paranoid`: acknowledgement includes a macOS full-storage synchronization
  barrier over the state required by the commit. This is the power-loss-
  oriented class.

| Setting | TurboKV | fjall | redb |
|---|---|---|---|
| Durable acknowledgement | durable WAL, `sync_on_write=false`, committed v5 record through a physically reserved shared mapping or ordered-write fallback | insert then keyspace-wide `PersistMode::Buffer` | one write transaction committed with `Durability::Eventual` |
| Paranoid acknowledgement | paranoid WAL, group size 1, delay 0, Rust `File::sync_all` | insert then keyspace-wide `PersistMode::SyncAll` | one write transaction committed with `Durability::Immediate`, then Rust `File::sync_all` on the database file |
| Write batch / callers | 1, 100, or 1,000 / 1 | 1, 100, or 1,000 / 1 | 1, 100, or 1,000 / 1 |
| Compression / block cache | none / 0 | none / 0 | not applicable / 0 |
| Memtable | 64 MiB | 64 MiB | not applicable (copy-on-write B-tree) |
| Maintenance workers | one flush task and one compaction task | one flush worker, zero compaction workers | none |
| Automatic compaction during bounded run | polling interval raised to one hour | disabled | not applicable |
| Mutation settlement | forced flush, then two coordinated manual drains | `SyncAll`, rotate-and-wait, then two major compactions | empty Immediate transaction, then two explicit `Database::compact` calls, each followed by a full-file sync |

`Buffer` and `SyncAll` are keyspace-wide in fjall, while TurboKV persists the
WAL for one database. With one partition and one caller, fjall's broader API
boundary does not allow it to acknowledge less work.
TurboKV's mapped path reserves active-segment capacity before exposing it, so
an allocation failure is returned before acknowledgement instead of relying on
sparse pages that could fault later. Filesystems without the required native
reservation API use the same tag-last format through ordered file writes.
Reservation does not turn later media or mapping faults into Rust errors; the
operating system can terminate the process, and externally truncating an open
mapped segment is outside the supported ownership contract.

redb is included as a Rust-native architectural contrast, not as an LSM peer.
Its copy-on-write B-tree has no exact buffer-only durability level. The durable
adapter uses `Durability::Eventual`, which queues persistence and can be a
stronger boundary than TurboKV's process-crash-oriented durable mode, depending
on the platform. The paranoid adapter uses `Durability::Immediate` and adds a
Rust full-file barrier. Cross-engine pass/fail claims remain against fjall,
whose buffer and full-sync journal boundaries directly match TurboKV's WAL
classes. The harness cannot prove that hardware honors any claimed power-loss
behavior.

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
| `recovery` | reopen after a subprocess acknowledged a marker and exited without running database destructors | same boundary; post-open marker verification is excluded / each reopen |
| `flush` | explicit synchronous flush returned | same boundary / the flush call |
| `compaction` | full-scope manual compaction returned after five flushed overwrite generations are prepared | same boundary / the compaction call |

Acknowledgement throughput is `operations / acknowledgement time`. Fully
settled throughput is `operations / (acknowledgement + settlement time)`.
Maintenance workloads use live keys in their scope as the operation unit;
recovery uses reopens; scan uses keys visited. The JSON records both operation
and latency units so unlike units are not silently compared.

Release and paranoid summaries report minimum, median, maximum, mean,
population standard deviation, and coefficient of variation for
acknowledgement and fully settled throughput within each
engine/durability/workload key. Raw repetitions retain
p50, p95, p99, and maximum observed latency in nanoseconds, preserving useful
resolution for sub-microsecond reads. No statistic combines `durable` and
`paranoid` samples.

## Storage accounting

Directory size is the recursive sum of file lengths at the measurement
boundary. TurboKV reports exact process-lifetime WAL, flush, and compaction
counter deltas. fjall 2.11.2 and redb 2.6.3 do not expose equivalent cumulative
component-byte counters through their public APIs; their JSON fields and write
amplification are therefore `null`, never estimates.

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
cargo check --manifest-path benchmarks/Cargo.toml --all-targets
cargo clippy --manifest-path benchmarks/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path benchmarks/Cargo.toml
```
