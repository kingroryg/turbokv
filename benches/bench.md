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

**Note:** In all modes, data that has been flushed to SSTables on disk is durable. The difference is what happens to in-flight writes that haven't been flushed yet. SSTables are fsync'd before manifest updates to guarantee on-disk integrity.


## Write Performance

### Sequential Writes (10M keys, 400-byte values, 4.2GB total)

| Database | Mode | Throughput | Total Time |
|----------|------|------------|------------|
| **TurboKV** | fast (no WAL) | **1,172K ops/sec** | 8.5s |
| RocksDB | default (WAL) | 452K ops/sec | 22.1s |
| **TurboKV** | durable (WAL) | **438K ops/sec** | 22.8s |
| fjall | default | 360K ops/sec | 27.8s |
| TurboKV | paranoid (fsync) | 249 ops/sec | ~11.2 hours* |

*Paranoid mode extrapolated from 100-write test (fsync-bound)

**Analysis:**
- TurboKV fast mode is **2.6x faster** than RocksDB
- TurboKV durable mode is **comparable to RocksDB** (~3% slower) with equivalent crash-safety guarantees
- TurboKV durable mode is **1.2x faster** than fjall
- All WAL writes go directly to the kernel (no userspace buffering) for genuine process-crash durability


### Key Optimizations in TurboKV

1. **Zero-allocation WAL path** - Thread-local pre-allocated buffers eliminate per-write heap allocations
2. **Direct kernel writes** - WAL bypasses BufWriter, writes directly to File for crash safety without flush overhead
3. **Lock-free MemTable** - Crossbeam skiplist allows concurrent writes without mutex contention
4. **Vectorized I/O** - Batch writes use single syscall with pre-allocated buffer
5. **Fast hashing** - gxhash for internal hash maps (faster than default hasher)

---


## Why Paranoid Mode is Slow

### The Physics of Fsync

```
~249 ops/sec = 4.0ms per write
SSD fsync latency = 2-5ms
Theoretical max = 200-500 ops/sec
```

Any database that guarantees "data survives power loss" MUST call fsync after each write. There's no way around the physics. RocksDB and fjall hit the same bottleneck (~200-300 ops/sec) when configured for power-loss durability.

### How to Get Higher Throughput with Durability

1. **Use `durable()` mode** (~438K ops/sec) - survives process crash, not power loss
2. **Use batch writes** (~933K ops/sec) - amortizes overhead across multiple operations
3. **Use concurrent writers** - group commit batches fsyncs
4. **Use faster storage** - NVMe with lower fsync latency

---

## Other Benchmarks

| Operation | Throughput |
|-----------|------------|
| Batch writes (fast) | ~933K ops/sec |
| Concurrent writes (8 writers, paranoid) | ~1000 ops/sec |
| Mixed read/write (7 readers, 1 writer) | ~292K ops/sec |
| Overwrite (durable, 10M keys) | ~195K ops/sec |

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

TurboKV fast mode achieves **2.6x the throughput** of RocksDB. In durable mode, TurboKV matches RocksDB while providing genuine crash-safety guarantees (direct kernel writes, fsync'd SSTables). This is achieved through:

- Direct-to-kernel WAL writes (no BufWriter overhead)
- Zero-allocation write paths with thread-local buffers
- Lock-free concurrent data structures
- Efficient WAL encoding without Merkle overhead
- SSTable fsync before manifest updates for durability
