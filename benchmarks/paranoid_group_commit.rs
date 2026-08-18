use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::sync::Barrier;
use turbokv::{Engine, StorageConfig, WalConfig};

type DynError = Box<dyn std::error::Error>;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    writers: usize,
    operations_per_writer: usize,
    collection_delay: Duration,
    maximum_group_size: usize,
}

struct Measurement {
    scenario: Scenario,
    operations: usize,
    elapsed: Duration,
    latencies: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let (single_operations, writers, operations_per_writer) = parse_args()?;
    let scenarios = [
        Scenario {
            name: "single/group-1",
            writers: 1,
            operations_per_writer: single_operations,
            collection_delay: Duration::ZERO,
            maximum_group_size: 1,
        },
        Scenario {
            name: "single/group-64",
            writers: 1,
            operations_per_writer: single_operations,
            collection_delay: Duration::from_micros(200),
            maximum_group_size: 64,
        },
        Scenario {
            name: "concurrent/group-1",
            writers,
            operations_per_writer,
            collection_delay: Duration::ZERO,
            maximum_group_size: 1,
        },
        Scenario {
            name: "concurrent/group-64",
            writers,
            operations_per_writer,
            collection_delay: Duration::from_micros(200),
            maximum_group_size: 64,
        },
    ];

    let mut measurements = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        measurements.push(measure(scenario).await?);
    }

    println!("Paranoid group-commit benchmark");
    println!("- each operation is one Engine::insert acknowledgement");
    println!("- fresh temporary database per row; 16-byte value; compression disabled");
    println!("- group-1 is the one-fsync-per-caller control");
    println!("- group-64 uses a 200us bounded collection window");
    println!();
    println!("| scenario | operations | writers | ops/s | p50 us | p95 us | p99 us | max us |");
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");
    for mut measurement in measurements {
        measurement.latencies.sort_unstable();
        let throughput = measurement.operations as f64 / measurement.elapsed.as_secs_f64();
        println!(
            "| {} | {} | {} | {:.0} | {} | {} | {} | {} |",
            measurement.scenario.name,
            measurement.operations,
            measurement.scenario.writers,
            throughput,
            percentile(&measurement.latencies, 50),
            percentile(&measurement.latencies, 95),
            percentile(&measurement.latencies, 99),
            measurement.latencies.last().copied().unwrap_or(0),
        );
    }
    Ok(())
}

async fn measure(scenario: Scenario) -> Result<Measurement, DynError> {
    let directory = TempDir::new()?;
    let mut config = StorageConfig::paranoid(directory.path().to_path_buf());
    config.wal_config = WalConfig::paranoid()
        .with_group_commit_delay(scenario.collection_delay)
        .with_max_group_size(scenario.maximum_group_size);
    config.flush_interval = Duration::from_secs(3_600);
    config.compaction_interval = Duration::from_secs(3_600);
    config.memtable_config.max_size = 256 * 1024 * 1024;
    config.sstable_config.compression = turbokv::storage::sstable::CompressionType::None;
    let engine = Arc::new(Engine::open(config).await?);
    let start = Arc::new(Barrier::new(scenario.writers + 1));
    let mut writers = Vec::with_capacity(scenario.writers);
    for writer in 0..scenario.writers {
        let writer_engine = Arc::clone(&engine);
        let writer_start = Arc::clone(&start);
        writers.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(scenario.operations_per_writer);
            writer_start.wait().await;
            for operation in 0..scenario.operations_per_writer {
                let key = format!("writer-{writer:04}-operation-{operation:08}");
                let started = Instant::now();
                writer_engine
                    .insert(key.as_bytes(), b"0123456789abcdef")
                    .await?;
                latencies.push(micros(started.elapsed()));
            }
            Ok::<_, turbokv::StorageError>(latencies)
        }));
    }

    let started = Instant::now();
    start.wait().await;
    let mut latencies = Vec::with_capacity(scenario.writers * scenario.operations_per_writer);
    for writer in writers {
        latencies.extend(writer.await??);
    }
    let elapsed = started.elapsed();
    engine.shutdown().await?;

    Ok(Measurement {
        scenario,
        operations: scenario.writers * scenario.operations_per_writer,
        elapsed,
        latencies,
    })
}

fn parse_args() -> Result<(usize, usize, usize), DynError> {
    let mut single_operations = 128;
    let mut writers = 8;
    let mut operations_per_writer = 64;
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        let target = match argument.as_str() {
            "--single-operations" => &mut single_operations,
            "--writers" => &mut writers,
            "--operations-per-writer" => &mut operations_per_writer,
            "--bench" => continue,
            "--help" | "-h" => return Err(usage().into()),
            _ => return Err(format!("unknown argument {argument:?}\n{}", usage()).into()),
        };
        *target = args
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?
            .parse()?;
    }
    if single_operations == 0 || writers == 0 || operations_per_writer == 0 {
        return Err("all workload sizes must be greater than zero".into());
    }
    Ok((single_operations, writers, operations_per_writer))
}

fn usage() -> &'static str {
    "usage: cargo bench --bench paranoid_group_commit -- [--single-operations N] [--writers N] [--operations-per-writer N]"
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}
