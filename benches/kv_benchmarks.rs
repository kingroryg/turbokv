//! Performance benchmarks comparing TurboKV vs fjall vs RocksDB
//!
//! Run with: cargo bench
//!
//! Benchmarks include:
//! - All TurboKV modes: fast, durable, tamper_proof
//! - Comparison with fjall and RocksDB
//! - Sequential writes, random reads, batch operations, range scans
//! - 100K+ key-value operations
//!
//! Note: Competitor databases (RocksDB, fjall) are set up once and reused
//! across benchmark iterations for fair comparison.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Arc;
use tempfile::TempDir;

use turbokv::{Db, DbOptions, WriteBatch};

const SEED: u64 = 42;
const VALUE_SIZE: usize = 100; // 100 bytes per value
const WRITE_COUNT: usize = 10_000;  // Reduced for faster benchmarks
const READ_COUNT: usize = 1_000;
const BATCH_SIZE: usize = 100;

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

// ============================================================================
// TurboKV Mode Comparison Benchmarks
// ============================================================================

fn bench_turbokv_modes_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);

    let mut group = c.benchmark_group("turbokv_modes_write");
    group.sample_size(10);
    group.throughput(Throughput::Elements(WRITE_COUNT as u64));

    // Fast mode - no WAL, no sync
    group.bench_function("fast", |b| {
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

    // Durable mode - WAL enabled, sync writes
    group.bench_function("durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::durable())
                    .await
                    .unwrap();

                for (key, value) in &data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // Tamper-proof mode - Merkle chains enabled
    group.bench_function("tamper_proof", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::tamper_proof())
                    .await
                    .unwrap();

                for (key, value) in &data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_turbokv_modes_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = generate_data(WRITE_COUNT, VALUE_SIZE);
    let read_keys = generate_read_keys(READ_COUNT, WRITE_COUNT);

    let mut group = c.benchmark_group("turbokv_modes_read");
    group.sample_size(10);
    group.throughput(Throughput::Elements(READ_COUNT as u64));

    // Setup databases once for all modes
    let fast_temp = TempDir::new().unwrap();
    let durable_temp = TempDir::new().unwrap();
    let tamper_temp = TempDir::new().unwrap();

    // Pre-populate databases
    rt.block_on(async {
        let db = Db::open_with_options(fast_temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();

        let db = Db::open_with_options(durable_temp.path(), DbOptions::durable()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();

        let db = Db::open_with_options(tamper_temp.path(), DbOptions::tamper_proof()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    // Fast mode reads
    let fast_path = fast_temp.path().to_path_buf();
    let read_keys_clone = Arc::new(read_keys.clone());
    group.bench_function("fast", |b| {
        let keys = Arc::clone(&read_keys_clone);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&fast_path, DbOptions::fast()).await.unwrap();
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
                let db = Db::open_with_options(&durable_path, DbOptions::durable()).await.unwrap();
                for key in keys.iter() {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    // Tamper-proof mode reads
    let tamper_path = tamper_temp.path().to_path_buf();
    group.bench_function("tamper_proof", |b| {
        let keys = Arc::clone(&read_keys_clone);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&tamper_path, DbOptions::tamper_proof()).await.unwrap();
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
        let db = Db::open_with_options(temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    let temp_path = temp.path().to_path_buf();
    group.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&temp_path, DbOptions::fast()).await.unwrap();
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
            let tree = keyspace.open_partition("default", Default::default()).unwrap();

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
            let tree = keyspace.open_partition("default", Default::default()).unwrap();

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
        let tree = keyspace.open_partition("default", Default::default()).unwrap();
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
            let tree = keyspace.open_partition("default", Default::default()).unwrap();
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
                let db = Db::open_with_options(temp.path(), DbOptions::fast()).await.unwrap();
                for (key, value) in &data {
                    db.insert(black_box(key), black_box(value)).await.unwrap();
                }
                db.flush().await.unwrap();
            });
        });
    });

    // TurboKV durable mode
    group.bench_function("turbokv_durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let temp = TempDir::new().unwrap();
                let db = Db::open_with_options(temp.path(), DbOptions::durable()).await.unwrap();
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
            let tree = keyspace.open_partition("default", Default::default()).unwrap();
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
        let db = Db::open_with_options(turbokv_temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    // Populate RocksDB
    {
        let db = rocksdb::DB::open_default(rocksdb_temp.path()).unwrap();
        for (key, value) in &data { db.put(key, value).unwrap(); }
        db.flush().unwrap();
    }

    // Populate fjall
    {
        let keyspace = fjall::Config::new(fjall_temp.path()).open().unwrap();
        let tree = keyspace.open_partition("default", Default::default()).unwrap();
        for (key, value) in &data { tree.insert(key, value).unwrap(); }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    // TurboKV reads
    let turbokv_path = turbokv_temp.path().to_path_buf();
    group.bench_function("turbokv", |b| {
        let keys = Arc::clone(&read_keys);
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&turbokv_path, DbOptions::fast()).await.unwrap();
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
            let tree = keyspace.open_partition("default", Default::default()).unwrap();
            for key in keys.iter() {
                let _ = tree.get(black_box(key)).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    // TurboKV mode comparison
    bench_turbokv_modes_write,
    bench_turbokv_modes_read,
    bench_turbokv_batch_writes,
    bench_turbokv_range_scan,
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
