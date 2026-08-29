use crate::engine::{Database, DynError, EngineName, StorageCounters};
use crate::protocol::{
    deterministic_value, percentile, random_keys, sequential_keys, Durability, WorkloadConfig,
    KEY_BYTES,
};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CRASH_CHILD_ENGINE: &str = "TURBOKV_BENCH_CRASH_ENGINE";
const CRASH_CHILD_DURABILITY: &str = "TURBOKV_BENCH_CRASH_DURABILITY";
const CRASH_CHILD_PATH: &str = "TURBOKV_BENCH_CRASH_PATH";
const CRASH_CHILD_CYCLE: &str = "TURBOKV_BENCH_CRASH_CYCLE";
const CRASH_CHILD_EXIT_CODE: i32 = 86;
const RECOVERY_VALUE: &[u8] = b"acknowledged-before-exit";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    SequentialFill,
    RandomFill,
    Overwrite,
    #[serde(rename = "sequential_batch_100")]
    SequentialBatch100,
    #[serde(rename = "sequential_batch_1000")]
    SequentialBatch1000,
    RandomRead,
    SequentialScan,
    Mixed,
    Recovery,
    Flush,
    Compaction,
}

impl Workload {
    pub const ALL: [Self; 11] = [
        Self::SequentialFill,
        Self::RandomFill,
        Self::Overwrite,
        Self::SequentialBatch100,
        Self::SequentialBatch1000,
        Self::RandomRead,
        Self::SequentialScan,
        Self::Mixed,
        Self::Recovery,
        Self::Flush,
        Self::Compaction,
    ];

    pub const INGEST: [Self; 5] = [
        Self::SequentialFill,
        Self::RandomFill,
        Self::Overwrite,
        Self::SequentialBatch100,
        Self::SequentialBatch1000,
    ];

    pub const fn as_str(self) -> &'static str {
        self.metadata().name
    }

    pub const fn operation_unit(self) -> &'static str {
        self.metadata().operation_unit
    }

    pub const fn latency_unit(self) -> &'static str {
        self.metadata().latency_unit
    }

    const fn metadata(self) -> WorkloadMetadata {
        match self {
            Self::SequentialFill => WorkloadMetadata::database("sequential_fill"),
            Self::RandomFill => WorkloadMetadata::database("random_fill"),
            Self::Overwrite => WorkloadMetadata::database("overwrite"),
            Self::SequentialBatch100 => WorkloadMetadata {
                name: "sequential_batch_100",
                operation_unit: "keys inserted",
                latency_unit: "100-key atomic batch commits",
            },
            Self::SequentialBatch1000 => WorkloadMetadata {
                name: "sequential_batch_1000",
                operation_unit: "keys inserted",
                latency_unit: "1,000-key atomic batch commits",
            },
            Self::RandomRead => WorkloadMetadata::database("random_read"),
            Self::SequentialScan => WorkloadMetadata {
                name: "sequential_scan",
                operation_unit: "keys visited",
                latency_unit: "full scan calls",
            },
            Self::Mixed => WorkloadMetadata::database("mixed"),
            Self::Recovery => WorkloadMetadata {
                name: "recovery",
                operation_unit: "database reopens",
                latency_unit: "database reopen calls",
            },
            Self::Flush => WorkloadMetadata {
                name: "flush",
                operation_unit: "keys flushed",
                latency_unit: "explicit flush calls",
            },
            Self::Compaction => WorkloadMetadata {
                name: "compaction",
                operation_unit: "live keys in compaction scope",
                latency_unit: "manual compaction calls",
            },
        }
    }
}

struct WorkloadMetadata {
    name: &'static str,
    operation_unit: &'static str,
    latency_unit: &'static str,
}

impl WorkloadMetadata {
    const fn database(name: &'static str) -> Self {
        Self {
            name,
            operation_unit: "database operations",
            latency_unit: "individual database operations",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Latencies {
    pub samples: usize,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub maximum: u64,
}

#[derive(Debug, Serialize)]
pub struct Measurement {
    pub engine: EngineName,
    pub durability: Durability,
    pub workload: Workload,
    pub repetition: u32,
    pub operations: u64,
    pub operation_unit: &'static str,
    pub acknowledgement_seconds: f64,
    pub settlement_seconds: f64,
    pub total_seconds: f64,
    pub acknowledgement_ops_per_second: f64,
    pub fully_settled_ops_per_second: f64,
    pub latency_unit: &'static str,
    pub latency_nanoseconds: Latencies,
    pub logical_mutation_bytes: u64,
    pub logical_live_bytes: u64,
    pub disk_bytes: u64,
    pub wal_bytes_written: Option<u64>,
    pub flush_bytes_written: Option<u64>,
    pub compaction_bytes_read: Option<u64>,
    pub compaction_bytes_written: Option<u64>,
    pub write_amplification: Option<f64>,
    pub disk_amplification: Option<f64>,
}

struct Outcome {
    operations: u64,
    acknowledgement: Duration,
    settlement: Duration,
    samples: Vec<u64>,
    logical_mutation_bytes: u64,
    counters: StorageCounters,
}

pub async fn run(
    engine: EngineName,
    durability: Durability,
    workload: Workload,
    repetition: u32,
    config: &WorkloadConfig,
) -> Result<Measurement, DynError> {
    let temporary = TempDir::new()?;
    let mut database = Database::open(engine, durability, temporary.path()).await?;
    let keys = random_keys(config);
    let value = deterministic_value(config.value_bytes, config.seed);
    let replacement = deterministic_value(config.value_bytes, !config.seed);

    let outcome = match workload {
        Workload::SequentialFill => {
            let keys = sequential_keys(config.keys);
            run_fill(&database, &keys, &value, config).await?
        }
        Workload::RandomFill => run_fill(&database, &keys, &value, config).await?,
        Workload::Overwrite => {
            write_setup(&database, &keys, &value).await?;
            database.settle(config.keys).await?;
            let before = database.counters()?;
            let (acknowledgement, samples) = timed_puts(&database, &keys, &replacement).await?;
            let settlement = database.settle(config.keys).await?;
            Outcome {
                operations: config.keys,
                acknowledgement,
                settlement,
                samples,
                logical_mutation_bytes: logical_bytes(config, config.keys),
                counters: database.counters()?.delta(before),
            }
        }
        Workload::SequentialBatch100 => {
            let keys = sequential_keys(config.keys);
            run_batch_fill(&database, &keys, &value, config, 100).await?
        }
        Workload::SequentialBatch1000 => {
            let keys = sequential_keys(config.keys);
            run_batch_fill(&database, &keys, &value, config, 1_000).await?
        }
        Workload::RandomRead => {
            write_setup(&database, &keys, &value).await?;
            database.settle(config.keys).await?;
            let before = database.counters()?;
            let started = Instant::now();
            let mut samples = Vec::with_capacity(keys.len());
            for key in &keys {
                let operation = Instant::now();
                if database.get(key).await?.as_deref() != Some(value.as_slice()) {
                    return Err("random read returned the wrong value".into());
                }
                samples.push(nanos(operation.elapsed()));
            }
            Outcome {
                operations: config.keys,
                acknowledgement: started.elapsed(),
                settlement: Duration::ZERO,
                samples,
                logical_mutation_bytes: 0,
                counters: database.counters()?.delta(before),
            }
        }
        Workload::SequentialScan => {
            let ordered = sequential_keys(config.keys);
            write_setup(&database, &ordered, &value).await?;
            database.settle(config.keys).await?;
            let before = database.counters()?;
            let mut elapsed = Duration::ZERO;
            let mut samples = Vec::with_capacity(config.scan_passes as usize);
            let mut expected_checksum = None;
            for _ in 0..config.scan_passes {
                let operation = Instant::now();
                let (count, checksum) = database.scan_all().await?;
                let duration = operation.elapsed();
                if count != config.keys {
                    return Err(format!("scan visited {count} of {} keys", config.keys).into());
                }
                if expected_checksum
                    .replace(checksum)
                    .is_some_and(|value| value != checksum)
                {
                    return Err("repeated scan checksum changed".into());
                }
                elapsed += duration;
                samples.push(nanos(duration));
            }
            Outcome {
                operations: config.keys * u64::from(config.scan_passes),
                acknowledgement: elapsed,
                settlement: Duration::ZERO,
                samples,
                logical_mutation_bytes: 0,
                counters: database.counters()?.delta(before),
            }
        }
        Workload::Mixed => {
            write_setup(&database, &keys, &value).await?;
            database.settle(config.keys).await?;
            let before = database.counters()?;
            let started = Instant::now();
            let mut samples = Vec::with_capacity(keys.len());
            let mut writes = 0_u64;
            for (index, key) in keys.iter().enumerate() {
                let operation = Instant::now();
                if index % 2 == 0 {
                    if database.get(key).await?.is_none() {
                        return Err("mixed read missed an existing key".into());
                    }
                } else {
                    database.put(key, &replacement).await?;
                    writes += 1;
                }
                samples.push(nanos(operation.elapsed()));
            }
            let acknowledgement = started.elapsed();
            let settlement = database.settle(config.keys).await?;
            Outcome {
                operations: config.keys,
                acknowledgement,
                settlement,
                samples,
                logical_mutation_bytes: logical_bytes(config, writes),
                counters: database.counters()?.delta(before),
            }
        }
        Workload::Recovery => {
            write_setup(&database, &keys, &value).await?;
            database.drop_without_settlement();
            let mut elapsed = Duration::ZERO;
            let mut samples = Vec::with_capacity(config.recovery_cycles as usize);
            for cycle in 0..config.recovery_cycles {
                run_crash_writer(engine, durability, temporary.path(), cycle)?;
                let operation = Instant::now();
                database.reopen().await?;
                let duration = operation.elapsed();
                let marker = recovery_marker(cycle);
                if database.get(&marker).await?.as_deref() != Some(RECOVERY_VALUE) {
                    return Err("recovery did not restore the crash-writer marker".into());
                }
                if database.live_keys().await? != config.keys + u64::from(cycle) + 1 {
                    return Err("recovery did not restore every acknowledged key".into());
                }
                elapsed += duration;
                samples.push(nanos(duration));
                database.drop_without_settlement();
            }
            Outcome {
                operations: u64::from(config.recovery_cycles),
                acknowledgement: elapsed,
                settlement: Duration::ZERO,
                samples,
                logical_mutation_bytes: 0,
                counters: StorageCounters::default(),
            }
        }
        Workload::Flush => {
            write_setup(&database, &keys, &value).await?;
            let before = database.counters()?;
            let operation = Instant::now();
            database.flush().await?;
            let acknowledgement = operation.elapsed();
            if database.live_keys().await? != config.keys {
                return Err("flush did not retain every acknowledged key".into());
            }
            Outcome {
                operations: config.keys,
                acknowledgement,
                settlement: Duration::ZERO,
                samples: vec![nanos(acknowledgement)],
                logical_mutation_bytes: 0,
                counters: database.counters()?.delta(before),
            }
        }
        Workload::Compaction => {
            for round in 0..5_u64 {
                let round_value = deterministic_value(config.value_bytes, config.seed ^ round);
                write_setup(&database, &keys, &round_value).await?;
                database.flush().await?;
            }
            let before = database.counters()?;
            let operation = Instant::now();
            database.compact().await?;
            let acknowledgement = operation.elapsed();
            if database.live_keys().await? != config.keys {
                return Err("compaction did not retain every live key".into());
            }
            Outcome {
                operations: config.keys,
                acknowledgement,
                settlement: Duration::ZERO,
                samples: vec![nanos(acknowledgement)],
                logical_mutation_bytes: 0,
                counters: database.counters()?.delta(before),
            }
        }
    };

    let disk_bytes = directory_size(temporary.path())?;
    database.close().await?;
    Ok(make_measurement(
        engine, durability, workload, repetition, config, outcome, disk_bytes,
    ))
}

pub async fn run_crash_child_if_requested() -> Result<(), DynError> {
    let Some(engine) = env::var_os(CRASH_CHILD_ENGINE) else {
        return Ok(());
    };
    let engine = match engine.to_str() {
        Some("turbokv") => EngineName::TurboKv,
        Some("fjall") => EngineName::Fjall,
        Some("redb") => EngineName::Redb,
        _ => return Err("invalid crash-child engine".into()),
    };
    let durability = match env::var(CRASH_CHILD_DURABILITY)?.as_str() {
        "fast" => Durability::Fast,
        "durable" => Durability::Durable,
        "paranoid" => Durability::Paranoid,
        _ => return Err("invalid crash-child durability".into()),
    };
    let cycle = env::var(CRASH_CHILD_CYCLE)?.parse::<u32>()?;
    let path = env::var_os(CRASH_CHILD_PATH).ok_or("missing crash-child path")?;
    let database = Database::open_after_handoff(engine, durability, Path::new(&path)).await?;
    database
        .put(&recovery_marker(cycle), RECOVERY_VALUE)
        .await?;
    std::process::exit(CRASH_CHILD_EXIT_CODE);
}

fn run_crash_writer(
    engine: EngineName,
    durability: Durability,
    path: &Path,
    cycle: u32,
) -> Result<(), DynError> {
    let status = Command::new(env::current_exe()?)
        .env(CRASH_CHILD_ENGINE, engine.as_str())
        .env(CRASH_CHILD_DURABILITY, durability.as_str())
        .env(CRASH_CHILD_PATH, path)
        .env(CRASH_CHILD_CYCLE, cycle.to_string())
        .status()?;
    if status.code() != Some(CRASH_CHILD_EXIT_CODE) {
        return Err(format!("crash writer exited with {status}").into());
    }
    Ok(())
}

fn recovery_marker(cycle: u32) -> Vec<u8> {
    let mut marker = b"recovery-marker-".to_vec();
    marker.extend_from_slice(&cycle.to_be_bytes());
    debug_assert_eq!(marker.len(), KEY_BYTES);
    marker
}

async fn run_fill(
    database: &Database,
    keys: &[Vec<u8>],
    value: &[u8],
    config: &WorkloadConfig,
) -> Result<Outcome, DynError> {
    let before = database.counters()?;
    let (acknowledgement, samples) = timed_puts(database, keys, value).await?;
    let settlement = database.settle(config.keys).await?;
    Ok(Outcome {
        operations: config.keys,
        acknowledgement,
        settlement,
        samples,
        logical_mutation_bytes: logical_bytes(config, config.keys),
        counters: database.counters()?.delta(before),
    })
}

async fn run_batch_fill(
    database: &Database,
    keys: &[Vec<u8>],
    value: &[u8],
    config: &WorkloadConfig,
    batch_size: usize,
) -> Result<Outcome, DynError> {
    let before = database.counters()?;
    let started = Instant::now();
    let mut samples = Vec::with_capacity(keys.len().div_ceil(batch_size));
    for batch in keys.chunks(batch_size) {
        let operation = Instant::now();
        database.put_batch(batch, value).await?;
        samples.push(nanos(operation.elapsed()));
    }
    let acknowledgement = started.elapsed();
    let settlement = database.settle(config.keys).await?;
    Ok(Outcome {
        operations: config.keys,
        acknowledgement,
        settlement,
        samples,
        logical_mutation_bytes: logical_bytes(config, config.keys),
        counters: database.counters()?.delta(before),
    })
}

async fn write_setup(database: &Database, keys: &[Vec<u8>], value: &[u8]) -> Result<(), DynError> {
    for key in keys {
        database.put(key, value).await?;
    }
    Ok(())
}

async fn timed_puts(
    database: &Database,
    keys: &[Vec<u8>],
    value: &[u8],
) -> Result<(Duration, Vec<u64>), DynError> {
    let started = Instant::now();
    let mut samples = Vec::with_capacity(keys.len());
    for key in keys {
        let operation = Instant::now();
        database.put(key, value).await?;
        samples.push(nanos(operation.elapsed()));
    }
    Ok((started.elapsed(), samples))
}

fn make_measurement(
    engine: EngineName,
    durability: Durability,
    workload: Workload,
    repetition: u32,
    config: &WorkloadConfig,
    outcome: Outcome,
    disk_bytes: u64,
) -> Measurement {
    let mut samples = outcome.samples;
    samples.sort_unstable();
    let total = outcome.acknowledgement + outcome.settlement;
    let logical_live_bytes = logical_live_bytes(workload, config);
    Measurement {
        engine,
        durability,
        workload,
        repetition,
        operations: outcome.operations,
        operation_unit: workload.operation_unit(),
        acknowledgement_seconds: outcome.acknowledgement.as_secs_f64(),
        settlement_seconds: outcome.settlement.as_secs_f64(),
        total_seconds: total.as_secs_f64(),
        acknowledgement_ops_per_second: rate(outcome.operations, outcome.acknowledgement),
        fully_settled_ops_per_second: rate(outcome.operations, total),
        latency_unit: workload.latency_unit(),
        latency_nanoseconds: Latencies {
            samples: samples.len(),
            p50: percentile(&samples, 50),
            p95: percentile(&samples, 95),
            p99: percentile(&samples, 99),
            maximum: samples.last().copied().unwrap_or_default(),
        },
        logical_mutation_bytes: outcome.logical_mutation_bytes,
        logical_live_bytes,
        disk_bytes,
        wal_bytes_written: outcome.counters.wal_bytes_written,
        flush_bytes_written: outcome.counters.flush_bytes_written,
        compaction_bytes_read: outcome.counters.compaction_bytes_read,
        compaction_bytes_written: outcome.counters.compaction_bytes_written,
        write_amplification: if outcome.logical_mutation_bytes == 0 {
            None
        } else {
            outcome
                .counters
                .physical_write_bytes()
                .map(|bytes| bytes as f64 / outcome.logical_mutation_bytes as f64)
        },
        disk_amplification: if logical_live_bytes == 0 {
            None
        } else {
            Some(disk_bytes as f64 / logical_live_bytes as f64)
        },
    }
}

const fn logical_bytes(config: &WorkloadConfig, operations: u64) -> u64 {
    operations * (KEY_BYTES + config.value_bytes) as u64
}

pub(crate) const fn logical_live_bytes(workload: Workload, config: &WorkloadConfig) -> u64 {
    let base = logical_bytes(config, config.keys);
    if matches!(workload, Workload::Recovery) {
        base + config.recovery_cycles as u64 * (KEY_BYTES + RECOVERY_VALUE.len()) as u64
    } else {
        base
    }
}

fn rate(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
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
