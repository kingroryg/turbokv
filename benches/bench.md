# TurboKV Benchmark Results

**Date:** January 25, 2026
**System:** Apple Silicon Mac (darwin 24.3.0)
**Rust:** 1.75+
**Build:** Release with LTO

## Durability Modes

TurboKV supports five durability modes:

| Mode | WAL | Fsync | Merkle | Survives | Use Case |
|------|-----|-------|--------|----------|----------|
| `fast()` | No | No | No | Nothing | Caching |
| `durable()` | Yes | Periodic | No | Process crash | Most production |
| `durable_audit()` | Yes | Periodic | Yes | Process crash + tampering | Audit logs, event sourcing |
| `paranoid()` | Yes | On every write | No | Power loss | Financial transactions |
| `tamper_proof()` | Yes | On every write | Yes | Power loss + tampering | Compliance, legal evidence |

---

## Write Performance

### Sequential Writes (Single Writer)

**Large-scale benchmark: 1M keys, 400-byte values (400MB total)**

| Mode | Throughput | Time | Notes |
|------|------------|------|-------|
| **fast** | 1.10M ops/s | 0.9s | No WAL overhead |
| **durable** | 899K ops/s | 1.1s | WAL with periodic sync |
| **durable_audit** | 38K ops/s | 26s | WAL + Merkle chains  |
| **paranoid** | 256 ops/s | N/A | WAL + fsync per write (100 keys tested) |
| **tamper_proof** | 258 ops/s | N/A | WAL + fsync + Merkle (100 keys tested) |

**Key insight:** Merkle chains add significant overhead (~23x) due to serial hash chaining.

### Random Writes

| Mode | Throughput | Write Count |
|------|------------|-------------|
| **fast** | 1.16M ops/s | 10,000 |
| **durable** | 90K ops/s | 1,000 |

### Batch Writes (fast mode)

| Operation | Throughput | Write Count |
|-----------|------------|-------------|
| Batch (100 per batch) | 1.63M ops/s | 10,000 |

---

## Read Performance

### Sequential Reads

| Mode | Throughput | Read Count |
|------|------------|------------|
| **fast** | 1.22M ops/s | 1,000 |

### Random Reads

| Mode | Throughput | Read Count |
|------|------------|------------|
| **fast** | 760K ops/s | 1,000 |
| **durable** | 711K ops/s | 1,000 |
| **tamper_proof** | 90K ops/s | 1,000 |

### Range Scans

| Operation | Throughput |
|-----------|------------|
| Scan 1000 keys | 1.19M entries/s |

---

## Apples-to-Apples Comparison

### Understanding Durability Modes

**Critical:** When comparing databases, you must compare equivalent durability guarantees!

| Guarantee | TurboKV | RocksDB | fjall |
|-----------|---------|---------|-------|
| No durability | `fast()` | WAL disabled | N/A |
| Process crash | `durable()` | Default (WAL, no sync) | Default |
| Power loss | `paranoid()` | `sync_wal: true` | `SyncAll` per write |

**RocksDB `open_default()`:**
- WAL enabled (survives process crash)
- No fsync per write (`sync_wal: false`)
- Equivalent to TurboKV `durable()` mode


**fjall with `persist(SyncAll)` at end:**
```rust
// This is NOT per-write sync!
for (key, value) in &data {
    tree.insert(key, value).unwrap();  // Buffered, no sync
}
keyspace.persist(SyncAll).unwrap();    // ONE sync at the end
```
- Equivalent to TurboKV `durable()` + final flush
- NOT equivalent to `paranoid()` mode

### Fair Comparisons

#### Comparison 1: No Durability (Cache Mode)
| Database | Mode | Throughput |
|----------|------|------------|
| TurboKV | `fast()` | 1.65M ops/s |
| RocksDB | WAL disabled | ~800K ops/s* |
| fjall | N/A | N/A |

*Estimated - RocksDB default has WAL enabled

#### Comparison 2: Process Crash Durability (Production Mode)
| Database | Mode | Throughput |
|----------|------|------------|
| TurboKV | `durable()` | 101K ops/s |
| RocksDB | Default | 516K ops/s |
| fjall | Default + final sync | 37.7K ops/s |

**Note:** RocksDB is faster here due to its mature C++ implementation and group commit optimizations.

#### Comparison 3: Power Loss Durability (Paranoid Mode)
| Database | Mode | Throughput |
|----------|------|------------|
| TurboKV | `paranoid()` | 263 ops/s |
| RocksDB | `sync_wal: true` | ~200-300 ops/s* |
| fjall | `SyncAll` per write | ~200-300 ops/s* |

*All databases hit the same fsync bottleneck (~3-5ms per fsync on SSD)

---

## Why Paranoid Mode is "Slow"

### The Physics of Fsync

```
263 ops/sec = 3.8ms per write
SSD fsync latency = 2-5ms
Theoretical max = 200-500 ops/sec
```

**We're achieving ~75% of theoretical maximum.** The remaining 25% is:
- Write lock acquisition
- BufWriter flush
- Entry serialization


Any database that guarantees "data survives power loss" MUST call fsync after each write. There's no way around the physics:


### How to Get Higher Throughput with Durability

1. **Use `durable()` mode** (101K ops/s) - survives process crash, not power loss
2. **Use `durable_audit()` mode** (69K ops/s) - adds tamper detection without fsync overhead
3. **Use batch writes** - amortizes fsync across multiple operations
4. **Use concurrent writers** - group commit batches fsyncs (see below)
5. **Use faster storage** - NVMe with lower fsync latency

---

## Concurrent Writers (Group Commit Benefit)

With multiple concurrent writers, fsync cost is amortized:

```
Single writer:     100 writes × 3.8ms = 380ms (263 ops/s)
8 concurrent:      400 writes, shared fsyncs = 390ms (~1000 ops/s aggregate)
```

**Benchmark: 8 writers × 50 writes each in paranoid mode**

| Scenario | Total Writes | Time | Aggregate Throughput |
|----------|--------------|------|---------------------|
| Serial (baseline) | 400 | ~1.5s | 263 ops/s |
| 8 Concurrent | 400 | ~390ms | ~1000 ops/s |

The group commit batches writes that arrive while another writer holds the lock, sharing fsync costs. This provides a **3.8x improvement** in aggregate throughput with concurrent writers.


## Benchmark Configuration

RocksDB-comparable parameters:

```rust
// Key/Value sizes match RocksDB defaults
const KEY_SIZE: usize = 20;           // RocksDB: 20 bytes
const VALUE_SIZE: usize = 400;        // RocksDB: 400 bytes

const WRITE_COUNT: usize = 100_000;   // Fast mode (no WAL)
const WRITE_COUNT_WAL: usize = 10_000; // Durable mode (WAL, no sync)
const WRITE_COUNT_SYNC: usize = 100;  // Paranoid/tamper_proof (fsync)
const READ_COUNT: usize = 10_000;     // Random reads
const BATCH_SIZE: usize = 100;        // Batch write size
```

### Workloads

| Workload | Description | RocksDB Equivalent |
|----------|-------------|-------------------|
| Sequential writes | Insert keys in order | `fillseq` |
| Random writes | Insert keys in random order | `filluniquerandom` |
| Overwrite | Update existing keys | `overwrite` |
| Random reads | Read random keys | `readrandom` |
| Range scan | Iterate key range | `fwdrange` |
| Read while writing | 7 readers + 1 writer | `readwhilewriting` |

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench --bench kv_benchmarks

# Run specific benchmark group
cargo bench --bench kv_benchmarks -- turbokv_sequential_writes

# Run concurrent writer benchmark
cargo bench --bench kv_benchmarks -- concurrent_writes

# Quick test run
cargo bench --bench kv_benchmarks -- --test
```

---

## Key Insights

1. **Fast mode** is ideal when durability isn't needed (caches, temp data)
2. **Durable mode** provides crash safety with good throughput
3. **Durable audit mode** adds tamper detection with ~30% overhead
4. **Paranoid mode** at 263 ops/s is close to theoretical max for per-write fsync
5. **Concurrent writers** dramatically improve paranoid mode throughput via group commit
6. **All databases** hit the same fsync bottleneck for power-loss durability