use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const SEED: u64 = 0x5455_5242_4f4b_5604;
pub const MEMTABLE_BYTES: usize = 64 * 1024 * 1024;
pub const KEY_BYTES: usize = 20;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Durable,
    Paranoid,
}

impl Durability {
    pub const ALL: [Self; 2] = [Self::Durable, Self::Paranoid];
    pub const DURABLE_ONLY: [Self; 1] = [Self::Durable];
    pub const PARANOID_ONLY: [Self; 1] = [Self::Paranoid];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Paranoid => "paranoid",
        }
    }

    pub const fn acknowledgement_boundary(self) -> AcknowledgementBoundary {
        match self {
            Self::Durable => AcknowledgementBoundary::ProcessCrashRecoverable,
            Self::Paranoid => AcknowledgementBoundary::PowerLossDurable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgementBoundary {
    ProcessCrashRecoverable,
    PowerLossDurable,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Quick,
    Ingest,
    Release,
    Paranoid,
}

impl Profile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "quick" => Ok(Self::Quick),
            "ingest" => Ok(Self::Ingest),
            "release" => Ok(Self::Release),
            "paranoid" => Ok(Self::Paranoid),
            _ => Err(format!(
                "unknown profile {value:?}; expected quick, ingest, release, or paranoid"
            )),
        }
    }

    pub const fn defaults(self) -> WorkloadConfig {
        match self {
            Self::Quick => WorkloadConfig {
                keys: 32,
                value_bytes: 128,
                repetitions: 1,
                scan_passes: 3,
                recovery_cycles: 3,
                seed: SEED,
            },
            Self::Ingest | Self::Release => WorkloadConfig {
                keys: 200_000,
                value_bytes: 400,
                repetitions: 3,
                scan_passes: 5,
                recovery_cycles: 5,
                seed: SEED,
            },
            Self::Paranoid => WorkloadConfig {
                keys: 1_000,
                value_bytes: 400,
                repetitions: 3,
                scan_passes: 5,
                recovery_cycles: 5,
                seed: SEED,
            },
        }
    }

    pub const fn durabilities(self) -> &'static [Durability] {
        match self {
            Self::Quick => &Durability::ALL,
            Self::Ingest | Self::Release => &Durability::DURABLE_ONLY,
            Self::Paranoid => &Durability::PARANOID_ONLY,
        }
    }

    const fn requires_release_controls(self) -> bool {
        matches!(self, Self::Ingest | Self::Release | Self::Paranoid)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Ingest => "ingest",
            Self::Release => "release",
            Self::Paranoid => "paranoid",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkloadConfig {
    pub keys: u64,
    pub value_bytes: usize,
    pub repetitions: u32,
    pub scan_passes: u32,
    pub recovery_cycles: u32,
    pub seed: u64,
}

#[derive(Debug)]
pub struct Cli {
    pub profile: Profile,
    pub output: PathBuf,
    pub machine: Option<String>,
}

impl Cli {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut profile = Profile::Quick;
        let mut output = PathBuf::from("../target/benchmark-results");
        let mut machine = None;
        let mut confirm_release = false;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--profile" => {
                    let value = args.next().ok_or("--profile requires a value")?;
                    profile = Profile::parse(&value)?;
                }
                "--output" => {
                    output = PathBuf::from(args.next().ok_or("--output requires a value")?);
                }
                "--machine" => {
                    let value = args.next().ok_or("--machine requires a value")?;
                    if value.trim().is_empty() {
                        return Err("--machine must not be empty".to_string());
                    }
                    machine = Some(value);
                }
                "--confirm-release" => confirm_release = true,
                "--bench" => {}
                "--help" | "-h" => return Err(usage().to_string()),
                _ => return Err(format!("unknown argument {arg:?}\n{}", usage())),
            }
        }
        if profile.requires_release_controls() && !confirm_release {
            return Err(
                "release and paranoid profiles perform repeated persistence workloads; rerun with --confirm-release"
                    .to_string(),
            );
        }
        if profile.requires_release_controls() && machine.is_none() {
            return Err(
                "release and paranoid benchmarks require a stable name via --machine".to_string(),
            );
        }
        Ok(Self {
            profile,
            output,
            machine,
        })
    }
}

pub const fn usage() -> &'static str {
    "usage: cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- --profile quick|ingest|release|paranoid [--output DIR] [--machine NAME] [--confirm-release]"
}

pub fn percentile(sorted_values: &[u64], percentile: u32) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let index = ((sorted_values.len() - 1) * percentile as usize).div_ceil(100);
    sorted_values[index]
}

pub fn ensure_release_reproducible(profile: Profile, git_dirty: bool) -> Result<(), String> {
    if profile.requires_release_controls() && git_dirty {
        return Err(
            "release and paranoid benchmarks require a clean Git worktree so the recorded commit identifies the measured source"
                .to_string(),
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct Distribution {
    pub samples: usize,
    pub minimum: f64,
    pub median: f64,
    pub maximum: f64,
    pub mean: f64,
    pub standard_deviation: f64,
    pub coefficient_of_variation_percent: f64,
}

pub fn distribution(values: &[f64]) -> Distribution {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let samples = sorted.len();
    let mean = sorted.iter().sum::<f64>() / samples as f64;
    let variance = sorted
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / samples as f64;
    Distribution {
        samples,
        minimum: sorted[0],
        median: sorted[(samples - 1).div_ceil(2)],
        maximum: sorted[samples - 1],
        mean,
        standard_deviation: variance.sqrt(),
        coefficient_of_variation_percent: if mean == 0.0 {
            0.0
        } else {
            variance.sqrt() / mean * 100.0
        },
    }
}

pub fn locked_package_versions(lockfile: &str) -> BTreeMap<String, Vec<String>> {
    let mut packages = BTreeMap::<String, Vec<String>>::new();
    for block in lockfile.split("[[package]]").skip(1) {
        let name = lock_field(block, "name");
        let version = lock_field(block, "version");
        if let (Some(name), Some(version)) = (name, version) {
            let versions = packages.entry(name).or_default();
            if !versions.contains(&version) {
                versions.push(version);
                versions.sort();
            }
        }
    }
    packages
}

fn lock_field(block: &str, field: &str) -> Option<String> {
    let prefix = format!("{field} = \"");
    block.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix('"'))
            .map(str::to_string)
    })
}

pub fn fnv1a64(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!("{hash:016x}")
}

pub fn benchmark_source_manifest(tree_listing: &str) -> String {
    let mut entries = tree_listing
        .lines()
        .filter(|line| match line.split_once('\t') {
            Some((_, path)) => !path.starts_with("benchmarks/results/"),
            None => true,
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| {
        let left_path = left.split_once('\t').map_or(*left, |(_, path)| path);
        let right_path = right.split_once('\t').map_or(*right, |(_, path)| path);
        left_path.as_bytes().cmp(right_path.as_bytes())
    });
    let mut manifest = entries.join("\n");
    manifest.push('\n');
    manifest
}

pub fn sequential_keys(count: u64) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| format!("{index:0KEY_BYTES$}").into_bytes())
        .collect()
}

pub fn random_keys(config: &WorkloadConfig) -> Vec<Vec<u8>> {
    (0..config.keys)
        .map(|index| {
            let mixed = splitmix64(index ^ config.seed);
            let mut key = format!("{mixed:016x}").into_bytes();
            key.resize(KEY_BYTES, b'0');
            key
        })
        .collect()
}

pub fn deterministic_value(size: usize, seed: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    let mut state = seed;
    for _ in 0..size {
        state = splitmix64(state);
        value.push(state as u8);
    }
    value
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
