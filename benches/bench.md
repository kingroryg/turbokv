# TurboKV Benchmark Results

**System:** Apple Silicon M4 Mac (darwin 24.3.0) 16GB Memory
**Rust:** 1.80+

## Durability Modes

TurboKV supports three durability modes:

| Mode | WAL | Fsync | Crash Behavior | Use Case |
|------|-----|-------|----------------|----------|
| `fast()` | No | No | Flushed data survives; unflushed memtable data lost | Caching, temp data |
| `durable()` | Yes | Periodic | All data survives process crash | Most production |
| `paranoid()` | Yes | Every write | All data survives power loss | Financial transactions |

**Note:** In all modes, data that has been flushed to SSTables on disk is durable. The difference is what happens to in-flight writes that haven't been flushed yet.


## Write Performance

### Sequential Writes (10M keys, 400-byte values, 4.2GB total)

| Database | Mode | Throughput | Total Time |
|----------|------|------------|------------|
| **TurboKV** | fast (no WAL) | **1,132K ops/sec** | 8.8s |
| **TurboKV** | durable (WAL) | **1,094K ops/sec** | 9.1s |
| RocksDB | default (WAL) | 560K ops/sec | 17.9s |
| fjall | default | 501K ops/sec | 20.0s |
| TurboKV | paranoid (fsync) | 257 ops/sec | ~10.8 hours* |

*Paranoid mode extrapolated from 1K key test (fsync-bound)

**Analysis:**
- TurboKV is **2x faster** than RocksDB with equivalent durability (WAL enabled)
- TurboKV is **2.25x faster** than fjall
- All databases configured with default settings (WAL enabled, periodic sync)


### Key Optimizations in TurboKV

1. **Zero-allocation WAL path** - Thread-local pre-allocated buffers eliminate per-write heap allocations
2. **Lock-free MemTable** - Crossbeam skiplist allows concurrent writes without mutex contention
3. **Vectorized I/O** - Batch writes use single syscall with pre-allocated buffer
4. **Fast hashing** - gxhash for internal hash maps (faster than default hasher)

---


## Why Paranoid Mode is Slow

### The Physics of Fsync

```
~263 ops/sec = 3.8ms per write
SSD fsync latency = 2-5ms
Theoretical max = 200-500 ops/sec
```

Any database that guarantees "data survives power loss" MUST call fsync after each write. There's no way around the physics. RocksDB and fjall hit the same bottleneck (~200-300 ops/sec) when configured for power-loss durability.

### How to Get Higher Throughput with Durability

1. **Use `durable()` mode** (~1.1M ops/sec) - survives process crash, not power loss
2. **Use batch writes** - amortizes fsync across multiple operations
3. **Use concurrent writers** - group commit batches fsyncs
4. **Use faster storage** - NVMe with lower fsync latency

---

## Read Performance

| Operation | Throughput |
|-----------|------------|
| Random reads | ~760K ops/sec |
| Sequential reads | ~1.2M ops/sec |
| Range scans | ~1.2M entries/sec |

---

## Benchmark Configuration

RocksDB-comparable parameters:

```rust
const KEY_SIZE: usize = 20;           // RocksDB: 20 bytes
const VALUE_SIZE: usize = 400;        // RocksDB: 400 bytes
const KEY_COUNT: usize = 10_000_000;  // 10M keys
```

### Running Benchmarks

```bash
# Quick benchmark (TurboKV vs RocksDB vs fjall)
cargo bench --bench large_scale_bench

# Detailed criterion benchmarks
cargo bench --bench kv_benchmarks
```

---

## Summary

TurboKV achieves **2x the throughput** of RocksDB and fjall with equivalent durability guarantees. This is achieved through:

- Zero-allocation write paths
- Lock-free concurrent data structures
- Efficient WAL encoding without Merkle overhead
- Optimized memory management with thread-local buffers
