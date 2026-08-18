# Benchmark protocol

The `benchmarks` Cargo target compares TurboKV durable mode with fjall using a
fresh database, identical deterministic keys and values, WAL-backed buffered
individual writes, disabled compression, equal 64 MiB memtables, one flush
worker, and an explicit final flush followed by compaction to a maintenance
fixed point. Release runs refuse dirty Git worktrees so the recorded commit
identifies the measured source.
It reports write acknowledgement throughput separately from fully settled
throughput. Latency p50, p95, p99, and maximum values measure individual write
acknowledgements.

The default `quick` profile is a developer smoke run:

```console
cargo bench --bench benchmarks -- --profile quick
```

The `release` profile performs three 10-million-operation repetitions. It can
write several gigabytes per engine and must be acknowledged explicitly:

```console
cargo bench --bench benchmarks -- --profile release --confirm-release
```

Each invocation writes a human-readable `.txt` report and a machine-readable
JSON report under `target/benchmark-results` (override with `--output DIR`).
Both include the workload configuration, fixed seed, protocol settings,
environment, the complete locked dependency set and lockfile checksum, and Git
commit/dirty state. Generated results are evidence artifacts and are
intentionally not committed.

Release comparisons should run on an otherwise idle named reference machine,
from a clean checkout, with stable power and thermal settings. Keep the JSON
artifact with any claim derived from the measurements. This harness establishes
the protocol; competitive tuning and README performance claims belong to later
work.

## Paranoid group commit

`paranoid_group_commit` measures the acknowledgement boundary of paranoid
`Engine::insert` calls. It compares a one-caller group (the one-sync-per-call
control) with a maximum of 64 callers and a bounded 200 microsecond collection
window. Both configurations use the same FIFO writer and durability path. Each
row uses a fresh temporary database, disabled compression, a 16-byte value, and
reports acknowledgement throughput plus p50, p95, p99, and maximum latency.

The default run is intentionally bounded and covers both a single writer and
eight concurrent writers:

```console
cargo bench --bench paranoid_group_commit
```

Workload sizes can be overridden without changing the durability settings:

```console
cargo bench --bench paranoid_group_commit -- \
  --single-operations 256 --writers 8 --operations-per-writer 128
```

Results depend strongly on the filesystem, storage device, operating system,
power policy, and background load. Record those alongside any result; do not
generalize a local smoke-run number into a release performance claim.
