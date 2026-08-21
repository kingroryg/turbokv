mod engine;
mod protocol;
mod workloads;

use engine::{DynError, EngineName};
use protocol::{
    benchmark_source_manifest, distribution, ensure_release_reproducible, fnv1a64,
    locked_package_versions, Cli, Distribution, Durability, Profile, WorkloadConfig, KEY_BYTES,
    MEMTABLE_BYTES,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use workloads::{Measurement, Workload};

const BENCHMARK_LOCKFILE: &str = include_str!("Cargo.lock");

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    generated_unix_seconds: u64,
    profile: Profile,
    dataset: WorkloadConfig,
    protocol: Protocol,
    engine_settings: Vec<EngineSettings>,
    counter_availability: Vec<CounterAvailability>,
    environment: Environment,
    direct_dependencies: BTreeMap<String, String>,
    resolved_dependencies: BTreeMap<String, Vec<String>>,
    benchmark_lock_fnv1a64: String,
    results: Vec<Measurement>,
    summaries: Vec<Summary>,
}

#[derive(Debug, Serialize)]
struct Protocol {
    durability_classes: Vec<DurabilityClass>,
    workloads: Vec<&'static str>,
    workload_order: &'static str,
    engine_order: &'static str,
    setup_boundary: &'static str,
    acknowledgement_boundary: &'static str,
    settled_boundary: &'static str,
    recovery_boundary: &'static str,
    common: CommonSettings,
    os_page_cache: &'static str,
    mixed_read_percent: u32,
    mixed_write_percent: u32,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct CommonSettings {
    wal_enabled: bool,
    batch_size: u32,
    concurrency: u32,
    key_bytes: usize,
    memtable_bytes: usize,
    compression: &'static str,
    block_cache_bytes: u64,
}

#[derive(Debug, Serialize)]
struct DurabilityClass {
    durability: Durability,
    acknowledgement: &'static str,
    comparison_rule: &'static str,
}

#[derive(Debug, Serialize)]
struct EngineSettings {
    engine: EngineName,
    version: &'static str,
    wal: &'static str,
    acknowledgement_modes: Vec<EngineDurabilitySettings>,
    settlement: &'static str,
    automatic_flush_workers: &'static str,
    automatic_compaction_workers: &'static str,
    differences: &'static str,
}

#[derive(Debug, Serialize)]
struct EngineDurabilitySettings {
    durability: Durability,
    acknowledgement: &'static str,
    conservative_difference: &'static str,
}

#[derive(Debug, Serialize)]
struct CounterAvailability {
    engine: EngineName,
    disk_bytes: &'static str,
    wal_bytes: &'static str,
    flush_bytes: &'static str,
    compaction_bytes: &'static str,
    amplification: &'static str,
}

#[derive(Debug, Serialize)]
struct Environment {
    machine_name: String,
    cpu: String,
    hardware_model: String,
    memory_bytes: Option<u64>,
    os: String,
    architecture: String,
    cpu_parallelism: usize,
    filesystem: String,
    rustc: String,
    git_commit: String,
    source_manifest_git_hash: String,
    source_manifest_hash_algorithm: &'static str,
    source_manifest_scope: &'static str,
    git_dirty: bool,
    power_policy: &'static str,
    thermal_policy: &'static str,
    background_load: &'static str,
}

#[derive(Debug, Serialize)]
struct Summary {
    engine: EngineName,
    durability: Durability,
    workload: Workload,
    acknowledgement_ops_per_second: Distribution,
    fully_settled_ops_per_second: Distribution,
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
    let environment = environment(cli.machine.as_deref())?;
    ensure_release_reproducible(cli.profile, environment.git_dirty)?;
    let report = run(cli.profile, environment).await?;
    fs::create_dir_all(&cli.output)?;
    let stem = format!(
        "durability-baseline-{}-{}",
        cli.profile.as_str(),
        report.generated_unix_seconds
    );
    let json_path = cli.output.join(format!("{stem}.json"));
    let text_path = cli.output.join(format!("{stem}.txt"));
    let human = human_report(&report);
    fs::write(&json_path, serde_json::to_vec_pretty(&report)?)?;
    fs::write(&text_path, &human)?;
    print!("{human}");
    println!("machine report: {}", json_path.display());
    println!("human report:   {}", text_path.display());
    Ok(())
}

async fn run(profile: Profile, environment: Environment) -> Result<Report, DynError> {
    let generated_unix_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let dataset = profile.defaults();
    let mut results = Vec::with_capacity(
        Durability::ALL.len()
            * dataset.repetitions as usize
            * Workload::ALL.len()
            * EngineName::ALL.len(),
    );
    for (durability_index, durability) in Durability::ALL.into_iter().enumerate() {
        for repetition in 1..=dataset.repetitions {
            for (workload_index, workload) in Workload::ALL.into_iter().enumerate() {
                let rotation = (durability_index + repetition as usize - 1 + workload_index)
                    % EngineName::ALL.len();
                for offset in 0..EngineName::ALL.len() {
                    let engine = EngineName::ALL[(rotation + offset) % EngineName::ALL.len()];
                    println!(
                        "run {repetition}/{}: {} / {} / {}",
                        dataset.repetitions,
                        durability.as_str(),
                        workload.as_str(),
                        engine.as_str()
                    );
                    results.push(
                        workloads::run(engine, durability, workload, repetition, &dataset).await?,
                    );
                }
            }
        }
    }
    let summaries = summaries(&results);
    Ok(Report {
        schema_version: 4,
        generated_unix_seconds,
        profile,
        dataset,
        protocol: protocol_settings(),
        engine_settings: engine_settings(),
        counter_availability: counter_availability(),
        environment,
        direct_dependencies: direct_dependencies(),
        resolved_dependencies: locked_package_versions(BENCHMARK_LOCKFILE),
        benchmark_lock_fnv1a64: fnv1a64(BENCHMARK_LOCKFILE.as_bytes()),
        results,
        summaries,
    })
}

fn protocol_settings() -> Protocol {
    Protocol {
        durability_classes: vec![
            DurabilityClass {
                durability: Durability::Durable,
                acknowledgement:
                    "the mutation's WAL bytes have been flushed from engine/process buffering to the operating-system page cache; no fsync is performed",
                comparison_rule:
                    "process-crash durability only; compare throughput only with other durable rows",
            },
            DurabilityClass {
                durability: Durability::Paranoid,
                acknowledgement:
                    "the mutation's WAL bytes have crossed a macOS full-storage synchronization barrier before acknowledgement",
                comparison_rule:
                    "power-loss-oriented full-sync boundary; compare throughput only with other paranoid rows",
            },
        ],
        workloads: Workload::ALL
            .into_iter()
            .map(Workload::as_str)
            .collect(),
        workload_order: "fixed documented order",
        engine_order: "deterministic Latin rotation by workload and repetition",
        setup_boundary:
            "dataset preparation and prerequisite settlement are excluded from measured workload time",
        acknowledgement_boundary:
            "all measured operations returned successfully; mutation acknowledgement uses the row's named durability class",
        settled_boundary:
            "acknowledgement followed by synchronous forced flush and two manual compaction drains for mutation workloads",
        recovery_boundary:
            "time to hand off in-process directory ownership and reopen a WAL-only database; verification is after and excluded from the timed reopen",
        common: CommonSettings {
            wal_enabled: true,
            batch_size: 1,
            concurrency: 1,
            key_bytes: KEY_BYTES,
            memtable_bytes: MEMTABLE_BYTES,
            compression: "disabled",
            block_cache_bytes: 0,
        },
        os_page_cache: "enabled and not cleared between runs; every measurement uses a fresh directory",
        mixed_read_percent: 50,
        mixed_write_percent: 50,
    }
}

fn engine_settings() -> Vec<EngineSettings> {
    vec![
        EngineSettings {
            engine: EngineName::TurboKv,
            version: "0.5.0 (path source measured at report git commit)",
            wal: "enabled; one record per mutation",
            acknowledgement_modes: vec![
                EngineDurabilitySettings {
                    durability: Durability::Durable,
                    acknowledgement:
                        "StorageConfig/WalConfig::durable; direct WAL write with sync_on_write=false",
                    conservative_difference: "none",
                },
                EngineDurabilitySettings {
                    durability: Durability::Paranoid,
                    acknowledgement:
                        "StorageConfig/WalConfig::paranoid; Rust File::sync_all; group size 1 and zero collection delay",
                    conservative_difference: "none",
                },
            ],
            settlement: "foreground flush then two coordinated manual compaction drains",
            automatic_flush_workers:
                "one async task installed; polling interval raised to 3600 seconds",
            automatic_compaction_workers:
                "one async task installed; polling interval raised to 3600 seconds",
            differences:
                "async API; bounded runs use explicit foreground flush/compaction before deferred polling",
        },
        EngineSettings {
            engine: EngineName::Fjall,
            version: "fjall 2.11.2",
            wal: "manual journal persistence enabled on keyspace and partition",
            acknowledgement_modes: vec![
                EngineDurabilitySettings {
                    durability: Durability::Durable,
                    acknowledgement:
                        "partition insert then Keyspace::persist(PersistMode::Buffer), flushing the journal BufWriter to OS buffers without fsync",
                    conservative_difference:
                        "persistence call is keyspace-wide; the benchmark has one partition and one caller",
                },
                EngineDurabilitySettings {
                    durability: Durability::Paranoid,
                    acknowledgement:
                        "partition insert then Keyspace::persist(PersistMode::SyncAll), which calls File::sync_all on the journal",
                    conservative_difference:
                        "full-sync call is keyspace-wide; the benchmark has one partition and one caller",
                },
            ],
            settlement: "SyncAll, rotate_memtable_and_wait, major_compact",
            automatic_flush_workers: "one",
            automatic_compaction_workers: "zero; all compaction is explicit and foreground",
            differences:
                "manual compaction has no public byte counters in the pinned version",
        },
        EngineSettings {
            engine: EngineName::RocksDb,
            version: "rust-rocksdb 0.22.0; native RocksDB 8.10.0",
            wal: "enabled (disable_wal=false); one Put per mutation",
            acknowledgement_modes: vec![
                EngineDurabilitySettings {
                    durability: Durability::Durable,
                    acknowledgement:
                        "WriteOptions::sync=false followed by DB::flush_wal(false), moving WAL bytes from RocksDB buffering to OS buffers without fsync",
                    conservative_difference: "none",
                },
                EngineDurabilitySettings {
                    durability: Durability::Paranoid,
                    acknowledgement:
                        "WriteOptions::sync=false followed by DB::flush_wal(false), then Rust File::sync_all on a pinned and identity/size-validated active WAL",
                    conservative_difference:
                        "the adapter seam is necessary because bundled RocksDB 8.10.0 omits HAVE_FULLFSYNC on Darwin; a 64 MiB write buffer and 1 GiB max total WAL size prevent automatic rotation within the bounded write epoch, and any pinned-file identity or growth anomaly aborts the run",
                },
            ],
            settlement:
                "FlushWAL(sync=true), blocking Flush, and manual full-range compaction, with Rust File::sync_all applied to every regular database file and the database directory after each blocking maintenance call",
            automatic_flush_workers: "one shared maximum background job",
            automatic_compaction_workers:
                "zero automatic compaction; manual compaction uses the one shared background job",
            differences:
                "use_fsync=true remains configured, but both acknowledgement and settlement add explicit Rust full-sync adapter seams because the native Darwin build has the weaker fsync",
        },
    ]
}

fn counter_availability() -> Vec<CounterAvailability> {
    vec![
        CounterAvailability {
            engine: EngineName::TurboKv,
            disk_bytes: "recursive exact file length at measurement boundary",
            wal_bytes: "process-lifetime exact engine counter delta",
            flush_bytes: "process-lifetime exact engine counter delta",
            compaction_bytes: "process-lifetime exact input/output counter delta",
            amplification:
                "WAL + flush output + compaction output divided by timed logical mutation bytes",
        },
        CounterAvailability {
            engine: EngineName::Fjall,
            disk_bytes: "recursive exact file length at measurement boundary",
            wal_bytes: "unavailable in fjall 2.11.2 public API; JSON null",
            flush_bytes: "unavailable in fjall 2.11.2 public API; JSON null",
            compaction_bytes: "unavailable in fjall 2.11.2 public API; JSON null",
            amplification: "unavailable because component byte counters are unavailable; JSON null",
        },
        CounterAvailability {
            engine: EngineName::RocksDb,
            disk_bytes: "recursive exact file length at measurement boundary",
            wal_bytes: "exact RocksDB WalFileBytes ticker delta",
            flush_bytes: "exact RocksDB FlushWriteBytes ticker delta",
            compaction_bytes: "exact RocksDB CompactReadBytes/CompactWriteBytes ticker deltas",
            amplification:
                "WAL + flush output + compaction output divided by timed logical mutation bytes",
        },
    ]
}

fn direct_dependencies() -> BTreeMap<String, String> {
    [
        ("turbokv", "0.5.0 path"),
        ("fjall", "2.11.2"),
        ("rocksdb", "0.22.0"),
        ("native-rocksdb", "8.10.0"),
        ("serde", "1.0.228"),
        ("serde_json", "1.0.148"),
        ("tempfile", "3.24.0"),
        ("tokio", "1.49.0"),
    ]
    .into_iter()
    .map(|(name, version)| (name.to_string(), version.to_string()))
    .collect()
}

fn summaries(results: &[Measurement]) -> Vec<Summary> {
    let mut output =
        Vec::with_capacity(EngineName::ALL.len() * Durability::ALL.len() * Workload::ALL.len());
    for engine in EngineName::ALL {
        for durability in Durability::ALL {
            for workload in Workload::ALL {
                let matching = results
                    .iter()
                    .filter(|result| {
                        result.engine == engine
                            && result.durability == durability
                            && result.workload == workload
                    })
                    .collect::<Vec<_>>();
                output.push(Summary {
                    engine,
                    durability,
                    workload,
                    acknowledgement_ops_per_second: distribution(
                        &matching
                            .iter()
                            .map(|result| result.acknowledgement_ops_per_second)
                            .collect::<Vec<_>>(),
                    ),
                    fully_settled_ops_per_second: distribution(
                        &matching
                            .iter()
                            .map(|result| result.fully_settled_ops_per_second)
                            .collect::<Vec<_>>(),
                    ),
                });
            }
        }
    }
    output
}

fn environment(machine: Option<&str>) -> Result<Environment, DynError> {
    let git_commit = command_output("git", &["rev-parse", "HEAD"])?;
    let tree_listing = command_output("git", &["ls-tree", "-r", "--full-tree", "HEAD"])?;
    let source_manifest_git_hash = git_blob_hash(&benchmark_source_manifest(&tree_listing))?;
    let git_dirty = !command_output("git", &["status", "--porcelain"])?.is_empty();
    Ok(Environment {
        machine_name: machine.unwrap_or("local developer smoke host").to_string(),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
            .unwrap_or_else(|_| "unknown".to_string()),
        hardware_model: command_output("sysctl", &["-n", "hw.model"])
            .unwrap_or_else(|_| "unknown".to_string()),
        memory_bytes: command_output("sysctl", &["-n", "hw.memsize"])
            .ok()
            .and_then(|value| value.parse().ok()),
        os: command_output("sw_vers", &[])
            .or_else(|_| command_output("uname", &["-srv"]))
            .unwrap_or_else(|_| env::consts::OS.to_string()),
        architecture: env::consts::ARCH.to_string(),
        cpu_parallelism: std::thread::available_parallelism()?.get(),
        filesystem: filesystem(),
        rustc: command_output("rustc", &["--version"]).unwrap_or_else(|_| "unknown".to_string()),
        git_commit,
        source_manifest_git_hash,
        source_manifest_hash_algorithm:
            "Git blob object ID using this repository's SHA-1 object format",
        source_manifest_scope:
            "bytewise path-sorted `mode type blob_oid<TAB>path` records from `git ls-tree -r --full-tree HEAD`, excluding only paths under benchmarks/results/, joined with LF and terminated by LF",
        git_dirty,
        power_policy: "operator-controlled; harness does not change macOS power settings",
        thermal_policy: "operator-controlled; harness does not read or change fan/thermal policy",
        background_load:
            "release protocol requires an otherwise idle host; not programmatically enforced",
    })
}

fn filesystem() -> String {
    command_output("diskutil", &["info", "/"])
        .ok()
        .and_then(|output| {
            output
                .lines()
                .find(|line| line.contains("File System Personality"))
                .and_then(|line| line.split_once(':'))
                .map(|(_, value)| value.trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn command_output(command: &str, args: &[&str]) -> Result<String, DynError> {
    let output = Command::new(command).args(args).output()?;
    if !output.status.success() {
        return Err(format!("{command} exited with {}", output.status).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn git_blob_hash(content: &str) -> Result<String, DynError> {
    use std::process::Stdio;

    let mut child = Command::new("git")
        .args(["hash-object", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    std::io::Write::write_all(
        child.stdin.as_mut().ok_or("git hash-object has no stdin")?,
        content.as_bytes(),
    )?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!("git hash-object exited with {}", output.status).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn human_report(report: &Report) -> String {
    let mut output = format!(
        "TurboKV durability-equivalent baseline protocol v{} ({})\ncommit: {}{}\nsource manifest git hash: {}\nmachine: {} / {} / {} / {} bytes RAM\nenvironment: {} / {} / {} CPUs / filesystem {} / {}\ndataset: {} keys, {}-byte keys, {}-byte values, seed {:#x}, {} repetition(s)\nequivalence: WAL {}; durable and paranoid classes reported separately; compression {}; block cache {} bytes; batch {}; concurrency {}; memtable {} bytes\nbenchmark Cargo.lock fnv1a64: {}\n\n",
        report.schema_version,
        report.profile.as_str(),
        report.environment.git_commit,
        if report.environment.git_dirty { " (dirty)" } else { "" },
        report.environment.source_manifest_git_hash,
        report.environment.machine_name,
        report.environment.cpu,
        report.environment.hardware_model,
        report.environment.memory_bytes.unwrap_or_default(),
        report.environment.os.replace('\n', "; "),
        report.environment.architecture,
        report.environment.cpu_parallelism,
        report.environment.filesystem,
        report.environment.rustc,
        report.dataset.keys,
        KEY_BYTES,
        report.dataset.value_bytes,
        report.dataset.seed,
        report.dataset.repetitions,
        if report.protocol.common.wal_enabled {
            "on"
        } else {
            "off"
        },
        report.protocol.common.compression,
        report.protocol.common.block_cache_bytes,
        report.protocol.common.batch_size,
        report.protocol.common.concurrency,
        report.protocol.common.memtable_bytes,
        report.benchmark_lock_fnv1a64,
    );
    output.push_str("engine    mode      workload          run  ack ops/s  settled ops/s  p50 ns  p95 ns  p99 ns  max ns  disk bytes  wal bytes  flush bytes  compact r/w\n");
    for result in &report.results {
        writeln!(
            output,
            "{:<9} {:<9} {:<17} {:>3} {:>10.0} {:>14.0} {:>7} {:>7} {:>7} {:>7} {:>11} {:>10} {:>12} {}/{}",
            result.engine.as_str(),
            result.durability.as_str(),
            result.workload.as_str(),
            result.repetition,
            result.acknowledgement_ops_per_second,
            result.fully_settled_ops_per_second,
            result.latency_nanoseconds.p50,
            result.latency_nanoseconds.p95,
            result.latency_nanoseconds.p99,
            result.latency_nanoseconds.maximum,
            result.disk_bytes,
            optional(result.wal_bytes_written),
            optional(result.flush_bytes_written),
            optional(result.compaction_bytes_read),
            optional(result.compaction_bytes_written),
        )
        .expect("writing to a String cannot fail");
    }
    output.push_str("\nrepeated-run throughput dispersion (population CV)\n");
    output.push_str(
        "engine    mode      workload          ack median  ack CV%  settled median  settled CV%\n",
    );
    for summary in &report.summaries {
        writeln!(
            output,
            "{:<9} {:<9} {:<17} {:>10.0} {:>8.2} {:>15.0} {:>12.2}",
            summary.engine.as_str(),
            summary.durability.as_str(),
            summary.workload.as_str(),
            summary.acknowledgement_ops_per_second.median,
            summary
                .acknowledgement_ops_per_second
                .coefficient_of_variation_percent,
            summary.fully_settled_ops_per_second.median,
            summary
                .fully_settled_ops_per_second
                .coefficient_of_variation_percent,
        )
        .expect("writing to a String cannot fail");
    }
    output.push_str("\nNull byte counters mean unavailable, never estimated. Setup is excluded from each timed workload. Durable and paranoid rows are different durability classes and must not be compared with each other. These are baseline observations, not performance claims.\n");
    output
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}
