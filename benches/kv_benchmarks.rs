//! Performance benchmarks comparing TurboKV vs fjall vs RocksDB
//!
//! Run with: cargo bench
//!
//! Benchmarks include:
//! - All TurboKV modes: fast, durable, tamper_proof
//! - Comparison with fjall and RocksDB
//! - Sequential writes, random reads, batch operations, range scans
//! - 1M+ key-value operations

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tempfile::TempDir;

use turbokv::{Db, DbOptions, WriteBatch};

const SEED: u64 = 42;
const VALUE_SIZE: usize = 100; // 100 bytes per value

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
    let count = 100_000;
    let data = generate_data(count, VALUE_SIZE);

    let mut group = c.benchmark_group("turbokv_modes_write_100k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(count as u64));

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
    let write_count = 100_000;
    let read_count = 10_000;
    let data = generate_data(write_count, VALUE_SIZE);
    let read_keys = generate_read_keys(read_count, write_count);

    let mut group = c.benchmark_group("turbokv_modes_read_10k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(read_count as u64));

    // Setup databases for each mode
    let fast_temp = TempDir::new().unwrap();
    let durable_temp = TempDir::new().unwrap();
    let tamper_temp = TempDir::new().unwrap();

    rt.block_on(async {
        // Fast
        let db = Db::open_with_options(fast_temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();

        // Durable
        let db = Db::open_with_options(durable_temp.path(), DbOptions::durable()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();

        // Tamper-proof
        let db = Db::open_with_options(tamper_temp.path(), DbOptions::tamper_proof()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    let fast_path = fast_temp.path().to_path_buf();
    group.bench_function("fast", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&fast_path, DbOptions::fast()).await.unwrap();
                for key in &read_keys {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    let durable_path = durable_temp.path().to_path_buf();
    group.bench_function("durable", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&durable_path, DbOptions::durable()).await.unwrap();
                for key in &read_keys {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    let tamper_path = tamper_temp.path().to_path_buf();
    group.bench_function("tamper_proof", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&tamper_path, DbOptions::tamper_proof()).await.unwrap();
                for key in &read_keys {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================================
// TurboKV Scalability Benchmarks
// ============================================================================

fn bench_turbokv_sequential_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let counts = [10_000, 100_000, 1_000_000];

    let mut group = c.benchmark_group("turbokv_sequential_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                rt.block_on(async {
                    let temp = TempDir::new().unwrap();
                    let db = Db::open_with_options(temp.path(), DbOptions::fast())
                        .await
                        .unwrap();

                    for (key, value) in data {
                        db.insert(black_box(key), black_box(value)).await.unwrap();
                    }
                    db.flush().await.unwrap();
                });
            });
        });
    }
    group.finish();
}

fn bench_turbokv_batch_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let counts = [10_000, 100_000];
    let batch_size = 1_000;

    let mut group = c.benchmark_group("turbokv_batch_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                rt.block_on(async {
                    let temp = TempDir::new().unwrap();
                    let db = Db::open_with_options(temp.path(), DbOptions::fast())
                        .await
                        .unwrap();

                    for chunk in data.chunks(batch_size) {
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
    }
    group.finish();
}

fn bench_turbokv_range_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let write_count = 100_000;

    let mut group = c.benchmark_group("turbokv_range_scan");
    group.sample_size(10);

    let temp = TempDir::new().unwrap();
    let data = generate_data(write_count, VALUE_SIZE);

    rt.block_on(async {
        let db = Db::open_with_options(temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    group.throughput(Throughput::Elements(1000));
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
// RocksDB Benchmarks
// ============================================================================

fn bench_rocksdb_sequential_writes(c: &mut Criterion) {
    let counts = [10_000, 100_000, 1_000_000];

    let mut group = c.benchmark_group("rocksdb_sequential_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                let temp = TempDir::new().unwrap();
                let db = rocksdb::DB::open_default(temp.path()).unwrap();

                for (key, value) in data {
                    db.put(black_box(key), black_box(value)).unwrap();
                }
                db.flush().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_rocksdb_random_reads(c: &mut Criterion) {
    let write_count = 100_000;
    let read_count = 10_000;

    let mut group = c.benchmark_group("rocksdb_random_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(read_count as u64));

    let temp = TempDir::new().unwrap();
    let data = generate_data(write_count, VALUE_SIZE);
    let read_keys = generate_read_keys(read_count, write_count);

    {
        let db = rocksdb::DB::open_default(temp.path()).unwrap();
        for (key, value) in &data {
            db.put(key, value).unwrap();
        }
        db.flush().unwrap();
    }

    group.bench_function("100k_entries", |b| {
        b.iter(|| {
            let db = rocksdb::DB::open_default(temp.path()).unwrap();
            for key in &read_keys {
                let _ = db.get(black_box(key)).unwrap();
            }
        });
    });
    group.finish();
}

fn bench_rocksdb_batch_writes(c: &mut Criterion) {
    let counts = [10_000, 100_000];
    let batch_size = 1_000;

    let mut group = c.benchmark_group("rocksdb_batch_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                let temp = TempDir::new().unwrap();
                let db = rocksdb::DB::open_default(temp.path()).unwrap();

                for chunk in data.chunks(batch_size) {
                    let mut batch = rocksdb::WriteBatch::default();
                    for (key, value) in chunk {
                        batch.put(key, value);
                    }
                    db.write(batch).unwrap();
                }
                db.flush().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_rocksdb_range_scan(c: &mut Criterion) {
    let write_count = 100_000;

    let mut group = c.benchmark_group("rocksdb_range_scan");
    group.sample_size(10);

    let temp = TempDir::new().unwrap();
    let data = generate_data(write_count, VALUE_SIZE);

    {
        let db = rocksdb::DB::open_default(temp.path()).unwrap();
        for (key, value) in &data {
            db.put(key, value).unwrap();
        }
        db.flush().unwrap();
    }

    group.throughput(Throughput::Elements(1000));
    group.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            let db = rocksdb::DB::open_default(temp.path()).unwrap();
            let start = format!("key{:016}", 50000).into_bytes();
            let end = format!("key{:016}", 51000).into_bytes();
            let iter = db.iterator(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward));
            let results: Vec<_> = iter
                .take_while(|r| r.as_ref().map(|(k, _)| k.as_ref() < end.as_slice()).unwrap_or(false))
                .collect();
            black_box(results);
        });
    });
    group.finish();
}

// ============================================================================
// fjall Benchmarks
// ============================================================================

fn bench_fjall_sequential_writes(c: &mut Criterion) {
    let counts = [10_000, 100_000, 1_000_000];

    let mut group = c.benchmark_group("fjall_sequential_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                let temp = TempDir::new().unwrap();
                let keyspace = fjall::Config::new(temp.path()).open().unwrap();
                let tree = keyspace.open_partition("default", Default::default()).unwrap();

                for (key, value) in data {
                    tree.insert(black_box(key), black_box(value)).unwrap();
                }
                keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_fjall_random_reads(c: &mut Criterion) {
    let write_count = 100_000;
    let read_count = 10_000;

    let mut group = c.benchmark_group("fjall_random_reads");
    group.sample_size(10);
    group.throughput(Throughput::Elements(read_count as u64));

    let temp = TempDir::new().unwrap();
    let data = generate_data(write_count, VALUE_SIZE);
    let read_keys = generate_read_keys(read_count, write_count);

    {
        let keyspace = fjall::Config::new(temp.path()).open().unwrap();
        let tree = keyspace.open_partition("default", Default::default()).unwrap();
        for (key, value) in &data {
            tree.insert(key, value).unwrap();
        }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    group.bench_function("100k_entries", |b| {
        b.iter(|| {
            let keyspace = fjall::Config::new(temp.path()).open().unwrap();
            let tree = keyspace.open_partition("default", Default::default()).unwrap();

            for key in &read_keys {
                let _ = tree.get(black_box(key)).unwrap();
            }
        });
    });
    group.finish();
}

fn bench_fjall_batch_writes(c: &mut Criterion) {
    let counts = [10_000, 100_000];
    let batch_size = 1_000;

    let mut group = c.benchmark_group("fjall_batch_writes");
    group.sample_size(10);

    for count in counts {
        let data = generate_data(count, VALUE_SIZE);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::from_parameter(count), &data, |b, data| {
            b.iter(|| {
                let temp = TempDir::new().unwrap();
                let keyspace = fjall::Config::new(temp.path()).open().unwrap();
                let tree = keyspace.open_partition("default", Default::default()).unwrap();

                for chunk in data.chunks(batch_size) {
                    let mut batch = keyspace.batch();
                    for (key, value) in chunk {
                        batch.insert(&tree, key, value);
                    }
                    batch.commit().unwrap();
                }
                keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_fjall_range_scan(c: &mut Criterion) {
    let write_count = 100_000;

    let mut group = c.benchmark_group("fjall_range_scan");
    group.sample_size(10);

    let temp = TempDir::new().unwrap();
    let data = generate_data(write_count, VALUE_SIZE);

    {
        let keyspace = fjall::Config::new(temp.path()).open().unwrap();
        let tree = keyspace.open_partition("default", Default::default()).unwrap();
        for (key, value) in &data {
            tree.insert(key, value).unwrap();
        }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    group.throughput(Throughput::Elements(1000));
    group.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            let keyspace = fjall::Config::new(temp.path()).open().unwrap();
            let tree = keyspace.open_partition("default", Default::default()).unwrap();
            let start = format!("key{:016}", 50000).into_bytes();
            let end = format!("key{:016}", 51000).into_bytes();
            let results: Vec<_> = tree.range(start..end).collect();
            black_box(results);
        });
    });
    group.finish();
}

// ============================================================================
// Head-to-head comparison benchmarks
// ============================================================================

fn bench_write_throughput_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let count = 100_000;
    let data = generate_data(count, VALUE_SIZE);

    let mut group = c.benchmark_group("write_comparison_100k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(count as u64));

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

fn bench_read_throughput_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let write_count = 100_000;
    let read_count = 10_000;
    let data = generate_data(write_count, VALUE_SIZE);
    let read_keys = generate_read_keys(read_count, write_count);

    // Setup all databases
    let turbokv_temp = TempDir::new().unwrap();
    let rocksdb_temp = TempDir::new().unwrap();
    let fjall_temp = TempDir::new().unwrap();

    rt.block_on(async {
        let db = Db::open_with_options(turbokv_temp.path(), DbOptions::fast()).await.unwrap();
        for (key, value) in &data { db.insert(key, value).await.unwrap(); }
        db.flush().await.unwrap();
    });

    {
        let db = rocksdb::DB::open_default(rocksdb_temp.path()).unwrap();
        for (key, value) in &data { db.put(key, value).unwrap(); }
        db.flush().unwrap();
    }

    {
        let keyspace = fjall::Config::new(fjall_temp.path()).open().unwrap();
        let tree = keyspace.open_partition("default", Default::default()).unwrap();
        for (key, value) in &data { tree.insert(key, value).unwrap(); }
        keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    let mut group = c.benchmark_group("read_comparison_10k");
    group.sample_size(10);
    group.throughput(Throughput::Elements(read_count as u64));

    let turbokv_path = turbokv_temp.path().to_path_buf();
    group.bench_function("turbokv", |b| {
        b.iter(|| {
            rt.block_on(async {
                let db = Db::open_with_options(&turbokv_path, DbOptions::fast()).await.unwrap();
                for key in &read_keys {
                    let _ = db.get(black_box(key)).await.unwrap();
                }
            });
        });
    });

    let rocksdb_path = rocksdb_temp.path().to_path_buf();
    group.bench_function("rocksdb", |b| {
        b.iter(|| {
            let db = rocksdb::DB::open_default(&rocksdb_path).unwrap();
            for key in &read_keys {
                let _ = db.get(black_box(key)).unwrap();
            }
        });
    });

    let fjall_path = fjall_temp.path().to_path_buf();
    group.bench_function("fjall", |b| {
        b.iter(|| {
            let keyspace = fjall::Config::new(&fjall_path).open().unwrap();
            let tree = keyspace.open_partition("default", Default::default()).unwrap();
            for key in &read_keys {
                let _ = tree.get(black_box(key)).unwrap();
            }
        });
    });

    group.finish();
}

fn bench_million_writes_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let count = 1_000_000;
    let data = generate_data(count, VALUE_SIZE);

    let mut group = c.benchmark_group("write_comparison_1M");
    group.sample_size(10);
    group.throughput(Throughput::Elements(count as u64));

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

criterion_group!(
    benches,
    // TurboKV mode comparison
    bench_turbokv_modes_write,
    bench_turbokv_modes_read,
    // TurboKV scalability
    bench_turbokv_sequential_writes,
    bench_turbokv_batch_writes,
    bench_turbokv_range_scan,
    // RocksDB
    bench_rocksdb_sequential_writes,
    bench_rocksdb_random_reads,
    bench_rocksdb_batch_writes,
    bench_rocksdb_range_scan,
    // fjall
    bench_fjall_sequential_writes,
    bench_fjall_random_reads,
    bench_fjall_batch_writes,
    bench_fjall_range_scan,
    // Head-to-head comparisons
    bench_write_throughput_comparison,
    bench_read_throughput_comparison,
    bench_million_writes_comparison,
);
criterion_main!(benches);
