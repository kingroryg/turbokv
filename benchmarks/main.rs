mod protocol;

use fjall::{
    CompressionType as FjallCompression, Config as FjallConfig, PartitionCreateOptions, PersistMode,
};
use protocol::{
    ensure_release_reproducible, locked_package_versions, package_version, percentile, Cli,
    Profile, WorkloadConfig,
};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use turbokv::{Compression, Db, DbOptions, DbStats};

type DynError = Box<dyn std::error::Error>;
const MEMTABLE_BYTES: u32 = 64 * 1024 * 1024;
const SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    generated_unix_seconds: u64,
    profile: Profile,
    workload: WorkloadConfig,
    protocol: Protocol,
    environment: Environment,
    dependencies: BTreeMap<String, String>,
    locked_dependencies: BTreeMap<String, Vec<String>>,
    cargo_lock_crc32: String,
    results: Vec<Measurement>,
}

#[derive(Debug, Serialize)]
struct Protocol {
    workload: &'static str,
    key_order: &'static str,
    acknowledgement_boundary: &'static str,
    settled_boundary: &'static str,
    durability: &'static str,
    compression: &'static str,
    cache_state: &'static str,
    batch_size: u32,
    memtable_bytes: u32,
    flush_workers: u32,
}

#[derive(Debug, Serialize)]
struct Environment {
    os: String,
    architecture: String,
    cpu_parallelism: usize,
    rustc: String,
    git_commit: String,
    git_dirty: bool,
}

#[derive(Debug, Serialize)]
struct Measurement {
    engine: EngineName,
    repetition: u32,
    operations: u64,
    acknowledgement_seconds: f64,
    settlement_seconds: f64,
    total_seconds: f64,
    acknowledgement_ops_per_second: f64,
    fully_settled_ops_per_second: f64,
    latency_micros: Latencies,
    logical_bytes: u64,
    disk_bytes: u64,
    wal_bytes_written: Option<u64>,
    flush_bytes_written: Option<u64>,
    compaction_bytes_read: Option<u64>,
    compaction_bytes_written: Option<u64>,
    write_amplification: Option<f64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum EngineName {
    #[serde(rename = "turbokv")]
    TurboKv,
    Fjall,
}

impl EngineName {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TurboKv => "turbokv",
            Self::Fjall => "fjall",
        }
    }
}

#[derive(Debug, Serialize)]
struct Latencies {
    samples: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    maximum: u64,
}

#[derive(Default)]
struct StorageMetrics {
    disk_bytes: u64,
    wal_bytes_written: Option<u64>,
    flush_bytes_written: Option<u64>,
    compaction_bytes_read: Option<u64>,
    compaction_bytes_written: Option<u64>,
    physical_write_bytes: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let cli = match Cli::parse(env::args().skip(1)) {
        Ok(cli) => cli,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let config = cli.profile.defaults();
    let report = run(cli.profile, config).await?;
    fs::create_dir_all(&cli.output)?;
    let stamp = report.generated_unix_seconds;
    let profile = match cli.profile {
        Profile::Quick => "quick",
        Profile::Release => "release",
    };
    let json_path = cli
        .output
        .join(format!("benchmarks-{profile}-{stamp}.json"));
    let text_path = cli.output.join(format!("benchmarks-{profile}-{stamp}.txt"));
    let human = human_report(&report);
    fs::write(&json_path, serde_json::to_vec_pretty(&report)?)?;
    fs::write(&text_path, &human)?;
    print!("{human}");
    println!("machine report: {}", json_path.display());
    println!("human report:   {}", text_path.display());
    Ok(())
}

async fn run(profile: Profile, config: WorkloadConfig) -> Result<Report, DynError> {
    let generated_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let environment = environment()?;
    ensure_release_reproducible(profile, environment.git_dirty)?;
    let keys = deterministic_keys(&config);
    let value = deterministic_value(config.value_bytes, config.seed);
    let mut results = Vec::with_capacity(config.repetitions as usize * 2);
    for repetition in 1..=config.repetitions {
        results.push(run_turbokv(repetition, &config, &keys, &value).await?);
        results.push(run_fjall(repetition, &config, &keys, &value)?);
    }
    Ok(Report {
        schema_version: 1,
        generated_unix_seconds,
        profile,
        workload: config,
        protocol: Protocol {
            workload: "seeded random unique-key fill",
            key_order: "SplitMix64 permutation rendered as fixed-width hexadecimal bytes",
            acknowledgement_boundary: "all individual insert calls have returned successfully",
            settled_boundary:
                "acknowledgement followed by a synchronous forced flush and compaction to a stable maintenance fixed point",
            durability:
                "WAL enabled with buffered acknowledgements; final settlement synchronizes pending data",
            compression: "disabled for both engines",
            cache_state: "new empty temporary database per engine and repetition",
            batch_size: 1,
            memtable_bytes: MEMTABLE_BYTES,
            flush_workers: 1,
        },
        environment,
        dependencies: dependencies(),
        locked_dependencies: locked_dependencies(),
        cargo_lock_crc32: format!("{:08x}", crc32fast::hash(include_bytes!("../Cargo.lock"))),
        results,
    })
}

async fn run_turbokv(
    repetition: u32,
    config: &WorkloadConfig,
    keys: &[Vec<u8>],
    value: &[u8],
) -> Result<Measurement, DynError> {
    let temp = TempDir::new()?;
    let mut options = DbOptions::durable().with_compression(Compression::None);
    options.memtable_size = MEMTABLE_BYTES as usize;
    let db = Db::open_with_options(temp.path(), options).await?;
    let mut samples = Vec::with_capacity(keys.len());
    let acknowledgement_start = Instant::now();
    for key in keys {
        let started = Instant::now();
        db.insert(key, value).await?;
        samples.push(micros(started.elapsed()));
    }
    let acknowledgement = acknowledgement_start.elapsed();
    let settlement_start = Instant::now();
    let stats = settle_turbokv(&db, config.operations).await?;
    let settlement = settlement_start.elapsed();
    let disk_bytes = directory_size(temp.path())?;
    let write_bytes = stats.wal_bytes_written
        + stats.sstable_flush_bytes_written
        + stats.compaction_bytes_written;
    Ok(measurement(
        EngineName::TurboKv,
        repetition,
        config,
        acknowledgement,
        settlement,
        samples,
        StorageMetrics {
            disk_bytes,
            wal_bytes_written: Some(stats.wal_bytes_written),
            flush_bytes_written: Some(stats.sstable_flush_bytes_written),
            compaction_bytes_read: Some(stats.compaction_bytes_read),
            compaction_bytes_written: Some(stats.compaction_bytes_written),
            physical_write_bytes: Some(write_bytes),
        },
    ))
}

async fn settle_turbokv(db: &Db, expected_keys: u64) -> Result<DbStats, DynError> {
    let deadline = Instant::now() + SETTLEMENT_TIMEOUT;
    db.flush().await?;

    loop {
        let before = wait_for_accounted_keys(db, expected_keys, deadline).await?;
        db.compact().await?;
        let after = wait_for_accounted_keys(db, expected_keys, deadline).await?;
        if maintenance_signature(&before) == maintenance_signature(&after) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let confirmed = wait_for_accounted_keys(db, expected_keys, deadline).await?;
            if maintenance_signature(&after) == maintenance_signature(&confirmed) {
                return Ok(confirmed);
            }
        }
        if Instant::now() >= deadline {
            return Err(
                "TurboKV did not reach a maintenance fixed point within 300 seconds".into(),
            );
        }
    }
}

async fn wait_for_accounted_keys(
    db: &Db,
    expected_keys: u64,
    deadline: Instant,
) -> Result<DbStats, DynError> {
    loop {
        let stats = db.stats();
        if stats.total_keys == expected_keys
            && stats.memtable_size == 0
            && stats.immutable_memtables == 0
            && stats.compactions_in_progress == 0
        {
            return Ok(stats);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "TurboKV settlement timed out with {} of {expected_keys} keys accounted for",
                stats.total_keys
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

const fn maintenance_signature(stats: &DbStats) -> (u64, u64, u64, u64, u64) {
    (
        stats.sstable_count,
        stats.l0_sstable_count,
        stats.compaction_bytes_read,
        stats.compaction_bytes_written,
        stats.compactions_in_progress,
    )
}

fn run_fjall(
    repetition: u32,
    config: &WorkloadConfig,
    keys: &[Vec<u8>],
    value: &[u8],
) -> Result<Measurement, DynError> {
    let temp = TempDir::new()?;
    let keyspace = FjallConfig::new(temp.path())
        .manual_journal_persist(true)
        .flush_workers(1)
        .open()?;
    let partition = keyspace.open_partition(
        "bench",
        PartitionCreateOptions::default()
            .compression(FjallCompression::None)
            .manual_journal_persist(true)
            .max_memtable_size(MEMTABLE_BYTES),
    )?;
    let mut samples = Vec::with_capacity(keys.len());
    let acknowledgement_start = Instant::now();
    for key in keys {
        let started = Instant::now();
        partition.insert(key, value)?;
        samples.push(micros(started.elapsed()));
    }
    let acknowledgement = acknowledgement_start.elapsed();
    let settlement_start = Instant::now();
    keyspace.persist(PersistMode::SyncAll)?;
    partition.rotate_memtable_and_wait()?;
    partition.major_compact()?;
    keyspace.persist(PersistMode::SyncAll)?;
    let settlement = settlement_start.elapsed();
    let stored_keys = u64::try_from(partition.len()?)?;
    if stored_keys != config.operations {
        return Err(format!(
            "fjall settlement accounted for {stored_keys} of {} keys",
            config.operations
        )
        .into());
    }
    drop(partition);
    drop(keyspace);
    let disk_bytes = directory_size(temp.path())?;
    Ok(measurement(
        EngineName::Fjall,
        repetition,
        config,
        acknowledgement,
        settlement,
        samples,
        StorageMetrics {
            disk_bytes,
            ..StorageMetrics::default()
        },
    ))
}

fn measurement(
    engine: EngineName,
    repetition: u32,
    config: &WorkloadConfig,
    acknowledgement: Duration,
    settlement: Duration,
    mut samples: Vec<u64>,
    storage: StorageMetrics,
) -> Measurement {
    samples.sort_unstable();
    let total = acknowledgement + settlement;
    let logical_bytes = config.operations * (config.key_bytes + config.value_bytes) as u64;
    Measurement {
        engine,
        repetition,
        operations: config.operations,
        acknowledgement_seconds: acknowledgement.as_secs_f64(),
        settlement_seconds: settlement.as_secs_f64(),
        total_seconds: total.as_secs_f64(),
        acknowledgement_ops_per_second: rate(config.operations, acknowledgement),
        fully_settled_ops_per_second: rate(config.operations, total),
        latency_micros: Latencies {
            samples: samples.len(),
            p50: percentile(&samples, 50),
            p95: percentile(&samples, 95),
            p99: percentile(&samples, 99),
            maximum: samples.last().copied().unwrap_or_default(),
        },
        logical_bytes,
        disk_bytes: storage.disk_bytes,
        wal_bytes_written: storage.wal_bytes_written,
        flush_bytes_written: storage.flush_bytes_written,
        compaction_bytes_read: storage.compaction_bytes_read,
        compaction_bytes_written: storage.compaction_bytes_written,
        write_amplification: storage
            .physical_write_bytes
            .map(|bytes| bytes as f64 / logical_bytes as f64),
    }
}

fn deterministic_keys(config: &WorkloadConfig) -> Vec<Vec<u8>> {
    (0..config.operations)
        .map(|index| {
            let mixed = splitmix64(index ^ config.seed);
            let mut key = format!("{mixed:016x}").into_bytes();
            key.resize(config.key_bytes, b'0');
            key
        })
        .collect()
}

fn deterministic_value(size: usize, seed: u64) -> Vec<u8> {
    let mut value = vec![0; size];
    StdRng::seed_from_u64(seed).fill_bytes(&mut value);
    value
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn rate(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn directory_size(path: &Path) -> std::io::Result<u64> {
    fs::read_dir(path)?.try_fold(0, |total, entry| {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            Ok(total + directory_size(&entry.path())?)
        } else {
            Ok(total + metadata.len())
        }
    })
}

fn environment() -> Result<Environment, DynError> {
    let git_commit = command_output("git", &["rev-parse", "HEAD"])?;
    let git_dirty = !command_output("git", &["status", "--porcelain"])?.is_empty();
    Ok(Environment {
        os: command_output("uname", &["-srv"]).unwrap_or_else(|_| env::consts::OS.to_string()),
        architecture: env::consts::ARCH.to_string(),
        cpu_parallelism: std::thread::available_parallelism()?.get(),
        rustc: command_output("rustc", &["--version"]).unwrap_or_else(|_| "unknown".to_string()),
        git_commit,
        git_dirty,
    })
}

fn command_output(command: &str, args: &[&str]) -> Result<String, DynError> {
    let output = Command::new(command).args(args).output()?;
    if !output.status.success() {
        return Err(format!("{command} exited with {}", output.status).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn dependencies() -> BTreeMap<String, String> {
    let lockfile = include_str!("../Cargo.lock");
    ["turbokv", "fjall", "rand", "serde", "tokio"]
        .into_iter()
        .map(|name| {
            let version = package_version(lockfile, name).unwrap_or_else(|| "unknown".to_string());
            (name.to_string(), version)
        })
        .collect()
}

fn locked_dependencies() -> BTreeMap<String, Vec<String>> {
    locked_package_versions(include_str!("../Cargo.lock"))
}

fn human_report(report: &Report) -> String {
    let mut output = format!(
        "TurboKV benchmark protocol v{} ({:?})\ncommit: {}{}\nenvironment: {} / {} / {} CPUs / {}\nworkload: {} operations, {}-byte keys, {}-byte values, seed {:#x}, {} repetition(s)\nprotocol: {}; {}; compression {}; batch size {}; memtable {} bytes; {} flush worker\n",
        report.schema_version,
        report.profile,
        report.environment.git_commit,
        if report.environment.git_dirty { " (dirty)" } else { "" },
        report.environment.os,
        report.environment.architecture,
        report.environment.cpu_parallelism,
        report.environment.rustc,
        report.workload.operations,
        report.workload.key_bytes,
        report.workload.value_bytes,
        report.workload.seed,
        report.workload.repetitions,
        report.protocol.acknowledgement_boundary,
        report.protocol.settled_boundary,
        report.protocol.compression,
        report.protocol.batch_size,
        report.protocol.memtable_bytes,
        report.protocol.flush_workers,
    );
    output.push_str("dependencies:");
    for (name, version) in &report.dependencies {
        write!(output, " {name}={version}").expect("writing to a String cannot fail");
    }
    writeln!(
        output,
        "\nCargo.lock crc32: {}\nlocked dependencies:",
        report.cargo_lock_crc32
    )
    .expect("writing to a String cannot fail");
    for (name, versions) in &report.locked_dependencies {
        writeln!(output, "  {name}={}", versions.join(","))
            .expect("writing to a String cannot fail");
    }
    output.push('\n');
    output.push_str(
        "engine    run  ack ops/s  settled ops/s  p50 us  p95 us  p99 us  max us  disk bytes\n",
    );
    for result in &report.results {
        writeln!(
            output,
            "{:<9} {:>3} {:>10.0} {:>14.0} {:>7} {:>7} {:>7} {:>7} {:>11}",
            result.engine.as_str(),
            result.repetition,
            result.acknowledgement_ops_per_second,
            result.fully_settled_ops_per_second,
            result.latency_micros.p50,
            result.latency_micros.p95,
            result.latency_micros.p99,
            result.latency_micros.maximum,
            result.disk_bytes,
        )
        .expect("writing to a String cannot fail");
    }
    output.push('\n');
    output
}
