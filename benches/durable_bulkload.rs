//! Durable random bulkload benchmark for TurboKV, RocksDB, and fjall.
//!
//! This is a standalone benchmark binary rather than a Criterion benchmark so
//! long 100M-key runs are measured once, with explicit flush/settle accounting.
//!
//! Example:
//! TURBOKV_BENCH_KEYS=100000000 cargo bench --bench durable_bulkload --no-default-features

use fjall::{Config as FjallConfig, PersistMode};
use rocksdb::{DBCompressionType, Options as RocksOptions, WriteOptions};
use serde::Serialize;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use turbokv::{Compression, Db, DbOptions};

const KEY_SIZE: usize = 20;
const DEFAULT_KEYS: u64 = 100_000_000;
const DEFAULT_VALUE_SIZE: usize = 400;
const DEFAULT_BATCH_SIZE: usize = 1_000;
const LATENCY_SAMPLE_INTERVAL: u64 = 10_000;
const PROGRESS_INTERVAL: u64 = 1_000_000;
const PERMUTATION_MULTIPLIER: u64 = 0xda94_2042_e4dd_58b5;
const PERMUTATION_ADDEND: u64 = 0x9e37_79b9_7f4a_7c15;
const VALUE_SEED: u64 = 0x517c_c1b7_2722_0a95;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchMode {
    DurableFillRandom,
    BulkloadNoWal,
}

impl BenchMode {
    fn from_env(value: &str) -> Result<Self, String> {
        match value {
            "durable_fillrandom" => Ok(Self::DurableFillRandom),
            "bulkload_no_wal" => Ok(Self::BulkloadNoWal),
            other => Err(format!(
                "unsupported TURBOKV_BENCH_MODE={other}; expected durable_fillrandom or bulkload_no_wal"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::DurableFillRandom => "durable_fillrandom",
            Self::BulkloadNoWal => "bulkload_no_wal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    TurboKvInsert,
    TurboKvIngest,
    RocksDb,
    Fjall,
}

impl EngineKind {
    fn from_env(value: &str) -> Result<Self, String> {
        match value {
            "turbokv_insert" => Ok(Self::TurboKvInsert),
            "turbokv_ingest" => Ok(Self::TurboKvIngest),
            "rocksdb" => Ok(Self::RocksDb),
            "fjall" => Ok(Self::Fjall),
            other => Err(format!("unsupported benchmark engine: {other}")),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::TurboKvInsert => "turbokv_insert",
            Self::TurboKvIngest => "turbokv_ingest",
            Self::RocksDb => "rocksdb",
            Self::Fjall => "fjall",
        }
    }
}

#[derive(Debug)]
struct BenchConfig {
    key_count: u64,
    value_size: usize,
    batch_size: usize,
    mode: BenchMode,
    engines: Vec<EngineKind>,
}

#[derive(Debug, Serialize)]
struct BenchResult {
    engine: String,
    mode: String,
    key_count: u64,
    key_size: usize,
    value_size: usize,
    batch_size: usize,
    load_secs: f64,
    flush_secs: f64,
    compaction_settle_secs: f64,
    total_secs: f64,
    acknowledged_ops_per_sec: f64,
    settled_ops_per_sec: f64,
    mb_per_sec: f64,
    p50_latency_micros: u128,
    p99_latency_micros: u128,
    latency_sample_count: usize,
    disk_bytes: u64,
    logical_bytes: u64,
    wal_bytes_written: u64,
    sstable_flush_bytes_written: u64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    immutable_memtables: u64,
    l0_sstable_count: u64,
    write_stall_count: u64,
    write_stall_micros: u64,
    write_amplification: f64,
}

#[derive(Debug, Clone, Copy)]
struct BenchTimings {
    load: Duration,
    flush: Duration,
    settle: Duration,
}

#[derive(Debug, Clone, Copy)]
struct EngineCounters {
    disk_bytes: u64,
    wal_bytes_written: u64,
    sstable_flush_bytes_written: u64,
    compaction_bytes_read: u64,
    compaction_bytes_written: u64,
    immutable_memtables: u64,
    l0_sstable_count: u64,
    write_stall_count: u64,
    write_stall_micros: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BenchConfig::from_env()?;
    let output_dir = PathBuf::from("target/bench-results");
    fs::create_dir_all(&output_dir)?;

    println!("durable_bulkload benchmark");
    println!("  keys: {}", config.key_count);
    println!("  key bytes: {KEY_SIZE}");
    println!("  value bytes: {}", config.value_size);
    println!("  mode: {}", config.mode.as_str());
    println!(
        "  engines: {}",
        config
            .engines
            .iter()
            .map(|engine| engine.as_str())
            .collect::<Vec<_>>()
            .join(",")
    );
    println!();

    let mut results = Vec::with_capacity(config.engines.len());
    for engine in &config.engines {
        println!("running {}...", engine.as_str());
        let result = match engine {
            EngineKind::TurboKvInsert => run_turbokv_insert(&config).await?,
            EngineKind::TurboKvIngest => run_turbokv_ingest(&config).await?,
            EngineKind::RocksDb => run_rocksdb(&config)?,
            EngineKind::Fjall => run_fjall(&config)?,
        };
        print_result(&result);
        results.push(result);
    }

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let stem = format!("durable_bulkload-{}-{timestamp}", config.mode.as_str());
    let json_path = output_dir.join(format!("{stem}.json"));
    let tsv_path = output_dir.join(format!("{stem}.tsv"));

    fs::write(&json_path, serde_json::to_vec_pretty(&results)?)?;
    fs::write(&tsv_path, to_tsv(&results))?;

    println!("wrote {}", json_path.display());
    println!("wrote {}", tsv_path.display());

    Ok(())
}

impl BenchConfig {
    fn from_env() -> Result<Self, String> {
        let key_count = parse_env_u64("TURBOKV_BENCH_KEYS", DEFAULT_KEYS)?;
        let value_size = parse_env_usize("TURBOKV_BENCH_VALUE_SIZE", DEFAULT_VALUE_SIZE)?;
        let batch_size = parse_env_usize("TURBOKV_BENCH_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
        let mode = env::var("TURBOKV_BENCH_MODE")
            .map(|value| BenchMode::from_env(&value))
            .unwrap_or(Ok(BenchMode::DurableFillRandom))?;
        let engines = env::var("TURBOKV_BENCH_ENGINES")
            .unwrap_or_else(|_| "turbokv_insert,turbokv_ingest,rocksdb,fjall".to_string())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(EngineKind::from_env)
            .collect::<Result<Vec<_>, _>>()?;

        if key_count == 0 {
            return Err("TURBOKV_BENCH_KEYS must be > 0".to_string());
        }
        if batch_size == 0 {
            return Err("TURBOKV_BENCH_BATCH_SIZE must be > 0".to_string());
        }
        if engines.is_empty() {
            return Err("TURBOKV_BENCH_ENGINES must include at least one engine".to_string());
        }

        Ok(Self {
            key_count,
            value_size,
            batch_size,
            mode,
            engines,
        })
    }

    const fn logical_bytes(&self) -> u64 {
        self.key_count * (KEY_SIZE as u64 + self.value_size as u64)
    }
}

async fn run_turbokv_insert(
    config: &BenchConfig,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db = Db::open_with_options(temp.path(), turbokv_options(config.mode)).await?;
    let mut samples = Vec::new();
    let mut value = vec![0; config.value_size];

    let load_start = Instant::now();
    for (written, index) in PermutedKeys::new(config.key_count).enumerate() {
        fill_value(index, &mut value);
        let key = make_key(index);

        if should_sample(written as u64) {
            let sample_start = Instant::now();
            db.insert(&key, &value).await?;
            samples.push(sample_start.elapsed().as_micros());
        } else {
            db.insert(&key, &value).await?;
        }

        print_progress(written as u64 + 1, config.key_count, load_start);
    }
    let load_time = load_start.elapsed();

    let flush_start = Instant::now();
    db.flush().await?;
    let flush_time = flush_start.elapsed();

    let settle_start = Instant::now();
    db.compact().await?;
    let settle_time = settle_start.elapsed();

    let stats = db.stats();
    let disk_bytes = dir_size(temp.path())?;

    Ok(make_result(
        "turbokv_insert",
        config,
        BenchTimings {
            load: load_time,
            flush: flush_time,
            settle: settle_time,
        },
        &samples,
        EngineCounters {
            disk_bytes,
            wal_bytes_written: stats.wal_bytes_written,
            sstable_flush_bytes_written: stats.sstable_flush_bytes_written,
            compaction_bytes_read: stats.compaction_bytes_read,
            compaction_bytes_written: stats.compaction_bytes_written,
            immutable_memtables: stats.immutable_memtables,
            l0_sstable_count: stats.l0_sstable_count,
            write_stall_count: stats.write_stall_count,
            write_stall_micros: stats.write_stall_micros,
        },
    ))
}

async fn run_turbokv_ingest(
    config: &BenchConfig,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db = Db::open_with_options(temp.path(), turbokv_options(config.mode)).await?;
    let mut samples = Vec::new();
    let mut chunk = Vec::with_capacity(config.batch_size);

    let load_start = Instant::now();
    for (written, index) in PermutedKeys::new(config.key_count).enumerate() {
        let mut value = vec![0; config.value_size];
        fill_value(index, &mut value);
        chunk.push((make_key(index).to_vec(), value));

        if chunk.len() == config.batch_size {
            write_turbokv_chunk(&db, &chunk, written as u64, &mut samples).await?;
            chunk.clear();
        }

        print_progress(written as u64 + 1, config.key_count, load_start);
    }
    if !chunk.is_empty() {
        write_turbokv_chunk(&db, &chunk, config.key_count - 1, &mut samples).await?;
    }
    let load_time = load_start.elapsed();

    let flush_start = Instant::now();
    db.flush().await?;
    let flush_time = flush_start.elapsed();

    let settle_start = Instant::now();
    db.compact().await?;
    let settle_time = settle_start.elapsed();

    let stats = db.stats();
    let disk_bytes = dir_size(temp.path())?;

    Ok(make_result(
        "turbokv_ingest",
        config,
        BenchTimings {
            load: load_time,
            flush: flush_time,
            settle: settle_time,
        },
        &samples,
        EngineCounters {
            disk_bytes,
            wal_bytes_written: stats.wal_bytes_written,
            sstable_flush_bytes_written: stats.sstable_flush_bytes_written,
            compaction_bytes_read: stats.compaction_bytes_read,
            compaction_bytes_written: stats.compaction_bytes_written,
            immutable_memtables: stats.immutable_memtables,
            l0_sstable_count: stats.l0_sstable_count,
            write_stall_count: stats.write_stall_count,
            write_stall_micros: stats.write_stall_micros,
        },
    ))
}

fn run_rocksdb(config: &BenchConfig) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut options = RocksOptions::default();
    options.create_if_missing(true);
    options.set_compression_type(DBCompressionType::None);
    options.increase_parallelism(num_cpus::get() as i32);

    let db = rocksdb::DB::open(&options, temp.path())?;
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(matches!(config.mode, BenchMode::BulkloadNoWal));

    let mut samples = Vec::new();
    let mut value = vec![0; config.value_size];
    let load_start = Instant::now();
    for (written, index) in PermutedKeys::new(config.key_count).enumerate() {
        fill_value(index, &mut value);
        let key = make_key(index);

        if should_sample(written as u64) {
            let sample_start = Instant::now();
            db.put_opt(key, &value, &write_options)?;
            samples.push(sample_start.elapsed().as_micros());
        } else {
            db.put_opt(key, &value, &write_options)?;
        }

        print_progress(written as u64 + 1, config.key_count, load_start);
    }
    let load_time = load_start.elapsed();

    let flush_start = Instant::now();
    db.flush()?;
    let flush_time = flush_start.elapsed();

    let settle_start = Instant::now();
    db.compact_range::<&[u8], &[u8]>(None, None);
    let settle_time = settle_start.elapsed();
    drop(db);

    let disk_bytes = dir_size(temp.path())?;

    Ok(make_result(
        "rocksdb",
        config,
        BenchTimings {
            load: load_time,
            flush: flush_time,
            settle: settle_time,
        },
        &samples,
        EngineCounters {
            disk_bytes,
            wal_bytes_written: 0,
            sstable_flush_bytes_written: 0,
            compaction_bytes_read: 0,
            compaction_bytes_written: 0,
            immutable_memtables: 0,
            l0_sstable_count: 0,
            write_stall_count: 0,
            write_stall_micros: 0,
        },
    ))
}

fn run_fjall(config: &BenchConfig) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let keyspace = FjallConfig::new(temp.path()).open()?;
    let partition = keyspace.open_partition("bench", Default::default())?;

    let mut samples = Vec::new();
    let mut value = vec![0; config.value_size];
    let load_start = Instant::now();
    for (written, index) in PermutedKeys::new(config.key_count).enumerate() {
        fill_value(index, &mut value);
        let key = make_key(index);

        if should_sample(written as u64) {
            let sample_start = Instant::now();
            partition.insert(key, value.as_slice())?;
            samples.push(sample_start.elapsed().as_micros());
        } else {
            partition.insert(key, value.as_slice())?;
        }

        print_progress(written as u64 + 1, config.key_count, load_start);
    }
    let load_time = load_start.elapsed();

    let flush_start = Instant::now();
    keyspace.persist(PersistMode::SyncAll)?;
    let flush_time = flush_start.elapsed();

    let settle_time = Duration::ZERO;
    drop(partition);
    drop(keyspace);
    let disk_bytes = dir_size(temp.path())?;

    Ok(make_result(
        "fjall",
        config,
        BenchTimings {
            load: load_time,
            flush: flush_time,
            settle: settle_time,
        },
        &samples,
        EngineCounters {
            disk_bytes,
            wal_bytes_written: 0,
            sstable_flush_bytes_written: 0,
            compaction_bytes_read: 0,
            compaction_bytes_written: 0,
            immutable_memtables: 0,
            l0_sstable_count: 0,
            write_stall_count: 0,
            write_stall_micros: 0,
        },
    ))
}

async fn write_turbokv_chunk(
    db: &Db,
    chunk: &[(Vec<u8>, Vec<u8>)],
    written: u64,
    samples: &mut Vec<u128>,
) -> Result<(), Box<dyn std::error::Error>> {
    if samples.is_empty() || should_sample(written + 1) {
        let sample_start = Instant::now();
        db.insert_many(
            chunk
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .await?;
        let per_entry = sample_start
            .elapsed()
            .as_micros()
            .div_ceil(chunk.len() as u128)
            .max(1);
        samples.push(per_entry);
    } else {
        db.insert_many(
            chunk
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )
        .await?;
    }

    Ok(())
}

fn turbokv_options(mode: BenchMode) -> DbOptions {
    match mode {
        BenchMode::DurableFillRandom => DbOptions::durable().with_compression(Compression::None),
        BenchMode::BulkloadNoWal => DbOptions::fast().with_compression(Compression::None),
    }
}

fn make_result(
    engine: &str,
    config: &BenchConfig,
    timings: BenchTimings,
    samples: &[u128],
    counters: EngineCounters,
) -> BenchResult {
    let total_time = timings.load + timings.flush + timings.settle;
    let logical_bytes = config.logical_bytes();
    let acknowledged_ops_per_sec = rate(config.key_count, timings.load);
    let settled_ops_per_sec = rate(config.key_count, total_time);
    let measured_write_bytes =
        counters.sstable_flush_bytes_written + counters.compaction_bytes_written;
    let write_amplification = if measured_write_bytes == 0 {
        counters.disk_bytes as f64 / logical_bytes as f64
    } else {
        measured_write_bytes as f64 / logical_bytes as f64
    };

    BenchResult {
        engine: engine.to_string(),
        mode: config.mode.as_str().to_string(),
        key_count: config.key_count,
        key_size: KEY_SIZE,
        value_size: config.value_size,
        batch_size: config.batch_size,
        load_secs: timings.load.as_secs_f64(),
        flush_secs: timings.flush.as_secs_f64(),
        compaction_settle_secs: timings.settle.as_secs_f64(),
        total_secs: total_time.as_secs_f64(),
        acknowledged_ops_per_sec,
        settled_ops_per_sec,
        mb_per_sec: logical_bytes as f64 / timings.load.as_secs_f64() / 1_000_000.0,
        p50_latency_micros: percentile(samples, 50),
        p99_latency_micros: percentile(samples, 99),
        latency_sample_count: samples.len(),
        disk_bytes: counters.disk_bytes,
        logical_bytes,
        wal_bytes_written: counters.wal_bytes_written,
        sstable_flush_bytes_written: counters.sstable_flush_bytes_written,
        compaction_bytes_read: counters.compaction_bytes_read,
        compaction_bytes_written: counters.compaction_bytes_written,
        immutable_memtables: counters.immutable_memtables,
        l0_sstable_count: counters.l0_sstable_count,
        write_stall_count: counters.write_stall_count,
        write_stall_micros: counters.write_stall_micros,
        write_amplification,
    }
}

fn parse_env_u64(name: &str, default: u64) -> Result<u64, String> {
    env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|err| format!("invalid {name}={value}: {err}"))
    })
}

fn parse_env_usize(name: &str, default: usize) -> Result<usize, String> {
    env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|err| format!("invalid {name}={value}: {err}"))
    })
}

fn should_sample(written: u64) -> bool {
    written % LATENCY_SAMPLE_INTERVAL == 0
}

fn print_progress(written: u64, total: u64, start: Instant) {
    if written % PROGRESS_INTERVAL == 0 || written == total {
        let rate = written as f64 / start.elapsed().as_secs_f64();
        println!("  {:>10}/{:<10} {:>8.0} ops/sec", written, total, rate);
    }
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() - 1) * percentile) / 100;
    sorted[rank]
}

fn make_key(index: u64) -> [u8; KEY_SIZE] {
    let mut key = [b'0'; KEY_SIZE];
    let mut value = index;
    for byte in key.iter_mut().rev() {
        *byte = b'0' + (value % 10) as u8;
        value /= 10;
    }
    key
}

fn fill_value(index: u64, value: &mut [u8]) {
    let mut state = index ^ VALUE_SEED;
    for byte in value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
}

fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

fn print_result(result: &BenchResult) {
    println!(
        "  load={:.2}s flush={:.2}s settle={:.2}s ack={:.0} ops/sec settled={:.0} ops/sec p99={}us disk={:.2}GB wa={:.2}",
        result.load_secs,
        result.flush_secs,
        result.compaction_settle_secs,
        result.acknowledged_ops_per_sec,
        result.settled_ops_per_sec,
        result.p99_latency_micros,
        result.disk_bytes as f64 / 1_000_000_000.0,
        result.write_amplification,
    );
    println!();
}

fn to_tsv(results: &[BenchResult]) -> String {
    let mut output = String::from(
        "engine\tmode\tkey_count\tkey_size\tvalue_size\tbatch_size\tload_secs\tflush_secs\tcompaction_settle_secs\ttotal_secs\tacknowledged_ops_per_sec\tsettled_ops_per_sec\tmb_per_sec\tp50_latency_micros\tp99_latency_micros\tlatency_sample_count\tdisk_bytes\tlogical_bytes\twal_bytes_written\tsstable_flush_bytes_written\tcompaction_bytes_read\tcompaction_bytes_written\timmutable_memtables\tl0_sstable_count\twrite_stall_count\twrite_stall_micros\twrite_amplification\n",
    );

    for result in results {
        writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{}\t{:.6}\t{:.6}\t{:.6}\t{:.6}\t{:.3}\t{:.3}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.6}",
            result.engine,
            result.mode,
            result.key_count,
            result.key_size,
            result.value_size,
            result.batch_size,
            result.load_secs,
            result.flush_secs,
            result.compaction_settle_secs,
            result.total_secs,
            result.acknowledged_ops_per_sec,
            result.settled_ops_per_sec,
            result.mb_per_sec,
            result.p50_latency_micros,
            result.p99_latency_micros,
            result.latency_sample_count,
            result.disk_bytes,
            result.logical_bytes,
            result.wal_bytes_written,
            result.sstable_flush_bytes_written,
            result.compaction_bytes_read,
            result.compaction_bytes_written,
            result.immutable_memtables,
            result.l0_sstable_count,
            result.write_stall_count,
            result.write_stall_micros,
            result.write_amplification,
        )
        .expect("write to String cannot fail");
    }

    output
}

struct PermutedKeys {
    count: u64,
    modulus_mask: u64,
    step: u64,
    yielded: u64,
}

impl PermutedKeys {
    fn new(count: u64) -> Self {
        let modulus = count.next_power_of_two();
        Self {
            count,
            modulus_mask: modulus - 1,
            step: 0,
            yielded: 0,
        }
    }
}

impl Iterator for PermutedKeys {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        while self.yielded < self.count {
            let candidate = self
                .step
                .wrapping_mul(PERMUTATION_MULTIPLIER)
                .wrapping_add(PERMUTATION_ADDEND)
                & self.modulus_mask;
            self.step += 1;

            if candidate < self.count {
                self.yielded += 1;
                return Some(candidate);
            }
        }

        None
    }
}
