//! Performance benchmarks comparing TurboKV vs fjall vs RocksDB
//!
//! Run with: cargo bench
//!
//! Benchmarks include:
//! - All TurboKV modes: fast, durable, paranoid
//! - Comparison with fjall and RocksDB
//! - Sequential writes, random writes, sequential reads, random reads
//! - Batch operations, range scans
//!
//! Note: Competitor databases (RocksDB, fjall) are set up once and reused
//! across benchmark iterations for fair comparison.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tempfile::TempDir;

use turbokv::{Db, DbOptions, WriteBatch};

const SEED: u64 = 42;

// RocksDB-comparable parameters:
// - RocksDB uses 20-byte keys, 400-byte values
// - We use formatted keys like "key{:016}" = 19 bytes (close enough)
// - RocksDB benchmarks use 900M-8B keys; we use 1M for reasonable runtime
const VALUE_SIZE: usize = 400; // RocksDB default

// Large-scale counts comparable to RocksDB methodology
// 10M keys × 400 bytes = 4GB of data
const WRITE_COUNT: usize = 10_000_000; // For fast mode (no WAL) - ~9 sec at 1.1M ops/s
const WRITE_COUNT_WAL: usize = 10_000_000; // For durable mode - ~11 sec at 900K ops/s
const WRITE_COUNT_SYNC: usize = 100; // For paranoid (fsync-bound)
const READ_COUNT: usize = 100_000;
const BATCH_SIZE: usize = 1000;

// Smaller counts for quick iteration during development
#[allow(dead_code)]
const WRITE_COUNT_SMALL: usize = 10_000;

/// Generate deterministic test data
fn generate_data(count: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(SEED);
    (0..count)
        .map(|i| {
            let key = format!("key{:016}", i).into_bytes();
            let mut value = vec![0u8; value_size];
            rng.fill(&mut value[..]);
            (key, value)
        })
        .collect()
}

/// Generate random read keys (subset of written keys)
fn generate_read_keys(count: usize, max_key: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(SEED + 1);
    (0..count)
        .map(|_| {
            let idx = rng.gen_range(0..max_key);
            format!("key{:016}", idx).into_bytes()
        })
        .collect()
}

/// Generate sequential read keys
fn generate_sequential_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("key{:016}", i).into_bytes())
        .collect()
}

/// Shuffle data for random write order
fn shuffle_data(data: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    use rand::seq::SliceRandom;
    let mut rng = StdRng::seed_from_u64(SEED + 2);
    let mut shuffled = data.to_vec();
    shuffled.shuffle(&mut rng);
    shuffled
}

// ============================================================================
// TurboKV Mode Comparison Benchmarks
// ============================================================================

// ============================================================================
// TurboKV Sequential Writes (all modes)
// ============================================================================

fn bench_turbokv_sequential_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let fast_data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let wal_data = generate_data(WRITE_COUNT_WAL, VALUE_SIZE);
    let sync_data = generate_data(WRITE_COUNT_SYNC, VALUE_SIZE);

    let mut group = c.benchmark_group("turbokv_sequential_writes");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(120)); // Allow 2 min per benchmark

    // Fast mode - no WAL, no sync (10K writes)
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));
    group.bench_function("fast", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::fast())
                    .await
                    .unwrap();

                for (key, value) in &fast_data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // Durable mode - WAL enabled, no sync (1K writes)
    group.throughput(Throughput::Elements(WRITE_COUNT_WAL as u64));
    group.bench_function("durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::durable())
                    .await
                    .unwrap();

                for (key, value) in &wal_data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // Paranoid mode - WAL + sync per write (100 writes - fsync is slow)
    group.throughput(Throughput::Elements(WRITE_COUNT_SYNC as u64));
    group.bench_function("paranoid", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::paranoid())
                    .await
                    .unwrap();

                for (key, value) in &sync_data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Random Writes
// ============================================================================

fn bench_turbokv_random_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let fast_data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let wal_data = generate_data(WRITE_COUNT_WAL, VALUE_SIZE);
    let fast_random = shuffle_data(&fast_data);
    let wal_random = shuffle_data(&wal_data);

    let mut group = c.benchmark_group("turbokv_random_writes");
    group.sample_size(10);

    // Fast mode - random order writes (10K)
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));
    group.bench_function("fast", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::fast())
                    .await
                    .unwrap();

                for (key, value) in &fast_random {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // Durable mode - random order writes (1K)
    group.throughput(Throughput::Elements(WRITE_COUNT_WAL as u64));
    group.bench_function("durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::durable())
                    .await
                    .unwrap();

                for (key, value) in &wal_random {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Random Reads (all modes)
// ============================================================================

fn bench_turbokv_random_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let read_keys = generate_read_keys(READ_COUNT, WRITE_COUNT);

    let mut group = c.benchmark_group("turbokv_random_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup databases once for all modes
    let fast_temp = TempDir::new().unwrap();
    let durable_temp = TempDir::new().unwrap();

    // Pre-populate databases
    rt.block_on(async {
        let db = Db::open_with_options(fast_temp.path(), DbOptions::fast())
            .await
            .unwrap();
        for (key, value) in &data {
            db.insert(key, value).await.unwrap();
        }
        db.flush().await.unwrap();

        let db = Db::open_with_options(durable_temp.path(), DbOptions::durable())
            .await
            .unwrap();
        for (key, value) in &data {
            db.insert(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
    });

    // Fast mode reads
    let fast_path = fast_temp.path().to_path_buf();
    let read_keys_clone = Arc::new(read_keys.clone());
    group.bench_function("fast", |b| {
        let keys = Arc::clone(&read_keys_clone);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&fast_path, DbOptions::fast())
                    .await
                    .unwrap();
                for key in keys.iter() {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    // Durable mode reads
    let durable_path = durable_temp.path().to_path_buf();
    group.bench_function("durable", |b| {
        let keys = Arc::clone(&read_keys_clone);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&durable_path, DbOptions::durable())
                    .await
                    .unwrap();
                for key in keys.iter() {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Sequential Reads
// ============================================================================

fn bench_turbokv_sequential_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let seq_keys = generate_sequential_keys(READ_COUNT);

    let mut group = c.benchmark_group("turbokv_sequential_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup database once
    let temp = TempDir::new().unwrap();
    rt.block_on(async {
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();
        for (key, value) in &data {
            db.insert(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
    });

    let temp_path = temp.path().to_path_buf();
    let seq_keys = Arc::new(seq_keys);

    // Fast mode sequential reads
    group.bench_function("fast", |b| {
        let keys = Arc::clone(&seq_keys);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&temp_path, DbOptions::fast())
                    .await
                    .unwrap();
                for key in keys.iter() {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Batch Write Benchmarks
// ============================================================================

fn bench_turbokv_batch_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("turbokv_batch_writes");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));

    group.bench_function("fast", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::fast())
                    .await
                    .unwrap();

                for chunk in data.chunks(BATCH_SIZE) {
                    let mut batch = WriteBatch::new();
                    for (key, value) in chunk {
                        batch.put(key, value);
                    }
                    db.write_batch(black_box(&batch)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Range Scan Benchmarks
// ============================================================================

fn bench_turbokv_range_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("turbokv_range_scan");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1000));

    // Setup database once
    let temp = TempDir::new().unwrap();
    rt.block_on(async {
        let db = Db::open_with_options(temp.path(), DbOptions::fast())
            .await
            .unwrap();
        for (key, value) in &data {
            db.insert(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
    });

    let temp_path = temp.path().to_path_buf();
    group.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&temp_path, DbOptions::fast())
                    .await
                    .unwrap();
                let start = format!("key{:016}", 50000).into_bytes();
                let end = format!("key{:016}", 51000).into_bytes();
                let _ = db.range(black_box(&start), black_box(&end)).await.unwrap();
            });
        });
    });
    group.finish();
}

// ============================================================================
// RocksDB Benchmarks (Setup Once, Reuse)
// ============================================================================

fn bench_rocksdb_writes(c: &mut Criterion) {
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("rocksdb_writes");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let db = rocksdb::DB::open_default(temp.path()).unwrap();

            for (key, value) in &data {
                db.put(black_box(key), black_box(value)).unwrap();
            }
            db.flush().unwrap();
        });
    });

    group.bench_function("batch", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let db = rocksdb::DB::open_default(temp.path()).unwrap();

            for chunk in data.chunks(BATCH_SIZE) {
                let mut batch = rocksdb::WriteBatch::default();
                for (key, value) in chunk {
                    batch.put(key, value);
                }
                db.write(batch).unwrap();
            }
            db.flush().unwrap();
        });
    });

    group.finish();
}

fn bench_rocksdb_reads(c: &mut Criterion) {
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let read_keys = generate_read_keys(READ_COUNT, WRITE_COUNT);

    let mut group = c.benchmark_group("rocksdb_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup database once
    let temp = TempDir::new().unwrap();
    {
        let db = rocksdb::DB::open_default(temp.path()).unwrap();
        for (key, value) in &data {
            db.put(key, value).unwrap();
        }
        db.flush().unwrap();
    }

    // Reuse for reads
    let temp_path = temp.path().to_path_buf();
    let read_keys = Arc::new(read_keys);
    group.bench_function("random", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            let db = rocksdb::DB::open_default(&temp_path).unwrap();
            for key in keys.iter() {
                let _ = db.get(black_box(key)).unwrap();
            }
        });
    });

    group.finish();
}

// ============================================================================
// fjall Benchmarks (Setup Once, Reuse)
// ============================================================================

fn bench_fjall_writes(c: &mut Criterion) {
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("fjall_writes");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));

    group.bench_function("sequential", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let keyspace = fjall::Config::new(temp.path()).open().unwrap();
            let tree = keyspace
                .open_partition("default", Default::default())
                .unwrap();

            for (key, value) in &data {
                tree.insert(black_box(key), black_box(value)).unwrap();
            }
            keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
        });
    });

    group.bench_function("batch", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let keyspace = fjall::Config::new(temp.path()).open().unwrap();
            let tree = keyspace
                .open_partition("default", Default::default())
                .unwrap();

            for chunk in data.chunks(BATCH_SIZE) {
                let mut batch = keyspace.batch();
                for (key, value) in chunk {
                    batch.insert(&tree, key, value);
                }
                batch.commit().unwrap();
            }
            keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
        });
    });

    group.finish();
}

fn bench_fjall_reads(c: &mut Criterion) {
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let read_keys = generate_read_keys(READ_COUNT, WRITE_COUNT);

    let mut group = c.benchmark_group("fjall_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup database once
    let temp = TempDir::new().unwrap();
    {
        let keyspace = fjall::Config::new(temp.path()).open().unwrap();
        let tree = keyspace
            .open_partition("default", Default::default())
            .unwrap();
        for (key, value) in &data {
            tree.insert(key, value).unwrap();
        }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    // Reuse for reads
    let temp_path = temp.path().to_path_buf();
    let read_keys = Arc::new(read_keys);
    group.bench_function("random", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            let keyspace = fjall::Config::new(&temp_path).open().unwrap();
            let tree = keyspace
                .open_partition("default", Default::default())
                .unwrap();
            for key in keys.iter() {
                let _ = tree.get(black_box(key)).unwrap();
            }
        });
    });

    group.finish();
}

// ============================================================================
// Head-to-head Comparison Benchmarks
// ============================================================================

fn bench_write_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("write_comparison");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));

    // TurboKV fast mode
    group.bench_function("turbokv_fast", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::fast())
                    .await
                    .unwrap();
                for (key, value) in &data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // RocksDB
    group.bench_function("rocksdb", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let db = rocksdb::DB::open_default(temp.path()).unwrap();
            for (key, value) in &data {
                db.put(black_box(key), black_box(value)).unwrap();
            }
            db.flush().unwrap();
        });
    });

    // fjall
    group.bench_function("fjall", |b| {
        b.iter(|| {
            let temp = TempDir::new().unwrap();
            let keyspace = fjall::Config::new(temp.path()).open().unwrap();
            let tree = keyspace
                .open_partition("default", Default::default())
                .unwrap();
            for (key, value) in &data {
                tree.insert(black_box(key), black_box(value)).unwrap();
            }
            keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
        });
    });

    group.finish();
}

fn bench_read_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let read_keys = Arc::new(generate_read_keys(READ_COUNT, WRITE_COUNT));

    let mut group = c.benchmark_group("read_comparison");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup all databases once
    let turbokv_temp = TempDir::new().unwrap();
    let rocksdb_temp = TempDir::new().unwrap();
    let fjall_temp = TempDir::new().unwrap();

    // Populate TurboKV
    rt.block_on(async {
        let db = Db::open_with_options(turbokv_temp.path(), DbOptions::fast())
            .await
            .unwrap();
        for (key, value) in &data {
            db.insert(key, value).await.unwrap();
        }
        db.flush().await.unwrap();
    });

    // Populate RocksDB
    {
        let db = rocksdb::DB::open_default(rocksdb_temp.path()).unwrap();
        for (key, value) in &data {
            db.put(key, value).unwrap();
        }
        db.flush().unwrap();
    }

    // Populate fjall
    {
        let keyspace = fjall::Config::new(fjall_temp.path()).open().unwrap();
        let tree = keyspace
            .open_partition("default", Default::default())
            .unwrap();
        for (key, value) in &data {
            tree.insert(key, value).unwrap();
        }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    // TurboKV reads
    let turbokv_path = turbokv_temp.path().to_path_buf();
    group.bench_function("turbokv", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&turbokv_path, DbOptions::fast())
                    .await
                    .unwrap();
                for key in keys.iter() {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    // RocksDB reads
    let rocksdb_path = rocksdb_temp.path().to_path_buf();
    group.bench_function("rocksdb", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            let db = rocksdb::DB::open_default(&rocksdb_path).unwrap();
            for key in keys.iter() {
                let _ = db.get(black_box(key)).unwrap();
            }
        });
    });

    // fjall reads
    let fjall_path = fjall_temp.path().to_path_buf();
    group.bench_function("fjall", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            let keyspace = fjall::Config::new(&fjall_path).open().unwrap();
            let tree = keyspace
                .open_partition("default", Default::default())
                .unwrap();
            for key in keys.iter() {
                let _ = tree.get(black_box(key)).unwrap();
            }
        });
    });

    group.finish();
}

// ============================================================================
// Concurrent Writer Benchmark (demonstrates group commit benefits)
// ============================================================================

fn bench_concurrent_writes(c: &mut Criterion) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    const CONCURRENT_WRITERS: usize = 8;
    const WRITES_PER_WRITER: usize = 50;
    const TOTAL_WRITES: usize = CONCURRENT_WRITERS * WRITES_PER_WRITER;

    let mut group = c.benchmark_group("concurrent_writes_paranoid");
    group.sample_size(10);
    group.throughput(Throughput::Elements(TOTAL_WRITES as u64));

    // Pre-generate data for each writer
    let all_data: Vec<Vec<(Vec<u8>, Vec<u8>)>> = (0..CONCURRENT_WRITERS)
        .map(|writer_id| {
            (0..WRITES_PER_WRITER)
                .map(|i| {
                    let key = format!("w{:02}key{:08}", writer_id, i).into_bytes();
                    let value = vec![writer_id as u8; VALUE_SIZE];
                    (key, value)
                })
                .collect()
        })
        .collect();

    group.bench_function("8_writers_x_50_writes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Arc::new(
                    Db::open_with_options(temp.path(), DbOptions::paranoid())
                        .await
                        .unwrap(),
                );

                let counter = Arc::new(AtomicUsize::new(0));
                let mut handles = Vec::new();

                for writer_data in all_data.clone() {
                    let db = Arc::clone(&db);
                    let counter = Arc::clone(&counter);

                    handles.push(tokio::spawn(async move {
                        for (key, value) in writer_data {
                            db.insert(black_box(&key), black_box(&value)).await.unwrap();
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                }

                for handle in handles {
                    handle.await.unwrap();
                }

                db.flush().await.unwrap();
                assert_eq!(counter.load(Ordering::Relaxed), TOTAL_WRITES);
            });
        });
    });

    group.finish();
}

// ============================================================================
// Mixed Workload: Read While Writing (RocksDB-style)
// ============================================================================

fn bench_readwhilewriting(c: &mut Criterion) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    const READ_THREADS: usize = 7;
    const WRITE_THREADS: usize = 1;
    const OPS_PER_THREAD: usize = 10_000;
    const TOTAL_OPS: usize = (READ_THREADS + WRITE_THREADS) * OPS_PER_THREAD;
    const PREPOPULATE_KEYS: usize = 100_000;

    let mut group = c.benchmark_group("readwhilewriting");
    group.sample_size(10);
    group.throughput(Throughput::Elements(TOTAL_OPS as u64));

    // Pre-generate data
    let prepop_data = generate_data(PREPOPULATE_KEYS, VALUE_SIZE);
    let write_data: Vec<(Vec<u8>, Vec<u8>)> = (0..OPS_PER_THREAD)
        .map(|i| {
            let key = format!("write{:016}", i).into_bytes();
            let value = vec![0xFFu8; VALUE_SIZE];
            (key, value)
        })
        .collect();

    group.bench_function("durable_7readers_1writer", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Arc::new(
                    Db::open_with_options(temp.path(), DbOptions::durable())
                        .await
                        .unwrap(),
                );

                // Prepopulate
                for (key, value) in &prepop_data {
                    db.insert(key, value).await.unwrap();
                }
                db.flush().await.unwrap();

                let read_counter = Arc::new(AtomicUsize::new(0));
                let write_counter = Arc::new(AtomicUsize::new(0));
                let stop_flag = Arc::new(AtomicBool::new(false));
                let mut handles = Vec::new();

                // Spawn reader threads
                for _ in 0..READ_THREADS {
                    let db = Arc::clone(&db);
                    let counter = Arc::clone(&read_counter);
                    let stop = Arc::clone(&stop_flag);
                    let max_key = PREPOPULATE_KEYS;

                    handles.push(tokio::spawn(async move {
                        let mut rng = StdRng::seed_from_u64(SEED + 100);
                        while !stop.load(Ordering::Relaxed) {
                            let idx = rng.gen_range(0..max_key);
                            let key = format!("key{:016}", idx).into_bytes();
                            let _ = db.get(black_box(&key)).await;
                            if counter.fetch_add(1, Ordering::Relaxed) >= OPS_PER_THREAD {
                                break;
                            }
                        }
                    }));
                }

                // Spawn writer thread
                let db_write = Arc::clone(&db);
                let w_counter = Arc::clone(&write_counter);
                let write_data_clone = write_data.clone();
                handles.push(tokio::spawn(async move {
                    for (key, value) in write_data_clone {
                        db_write
                            .insert(black_box(&key), black_box(&value))
                            .await
                            .unwrap();
                        w_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }));

                // Wait for writer to finish
                for handle in handles {
                    handle.await.unwrap();
                }

                stop_flag.store(true, Ordering::Relaxed);
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

// ============================================================================
// Overwrite Benchmark (RocksDB-style)
// ============================================================================

fn bench_overwrite(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT_WAL, VALUE_SIZE);

    let mut group = c.benchmark_group("overwrite");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT_WAL as u64));

    // Setup: prepopulate, then benchmark overwriting the same keys
    group.bench_function("durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::durable())
                    .await
                    .unwrap();

                // Initial population
                for (key, value) in &data {
                    db.insert(key, value).await.unwrap();
                }
                db.flush().await.unwrap();

                // Overwrite all keys with new values
                let new_value = vec![0xABu8; VALUE_SIZE];
                for (key, _) in &data {
                    db.insert(black_box(key), black_box(&new_value))
                        .await
                        .unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    // TurboKV write benchmarks
    bench_turbokv_sequential_writes,
    bench_turbokv_random_writes,
    bench_turbokv_batch_writes,
    // TurboKV read benchmarks
    bench_turbokv_sequential_reads,
    bench_turbokv_random_reads,
    bench_turbokv_range_scan,
    // Concurrent writes
    bench_concurrent_writes,
    // Mixed workloads (RocksDB-style)
    bench_readwhilewriting,
    bench_overwrite,
    // RocksDB
    bench_rocksdb_writes,
    bench_rocksdb_reads,
    // fjall
    bench_fjall_writes,
    bench_fjall_reads,
    // Head-to-head comparisons
    bench_write_comparison,
    bench_read_comparison,
);
criterion_main!(benches);
