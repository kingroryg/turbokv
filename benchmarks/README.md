# Benchmark protocol

The `benchmarks` Cargo target compares TurboKV durable mode with fjall using a
fresh database, identical deterministic keys and values, WAL-backed buffered
individual writes, disabled compression, equal 64 MiB memtables, one flush
worker, and an explicit final flush and manual compaction call.
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
environment, locked dependency versions, and Git commit/dirty state. Generated
results are evidence artifacts and are intentionally not committed.

Release comparisons should run on an otherwise idle named reference machine,
from a clean checkout, with stable power and thermal settings. Keep the JSON
artifact with any claim derived from the measurements. This harness establishes
the protocol; competitive tuning and README performance claims belong to later
work.
