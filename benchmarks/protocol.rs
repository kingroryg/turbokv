use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const SEED: u64 = 0x5455_5242_4f4b_5604;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Quick,
    Release,
}

impl Profile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "quick" => Ok(Self::Quick),
            "release" => Ok(Self::Release),
            _ => Err(format!(
                "unknown profile {value:?}; expected quick or release"
            )),
        }
    }

    pub const fn defaults(self) -> WorkloadConfig {
        match self {
            Self::Quick => WorkloadConfig {
                operations: 1_000,
                key_bytes: 20,
                value_bytes: 128,
                repetitions: 1,
                seed: SEED,
            },
            Self::Release => WorkloadConfig {
                operations: 10_000_000,
                key_bytes: 20,
                value_bytes: 400,
                repetitions: 3,
                seed: SEED,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkloadConfig {
    pub operations: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub repetitions: u32,
    pub seed: u64,
}

#[derive(Debug)]
pub struct Cli {
    pub profile: Profile,
    pub output: PathBuf,
}

impl Cli {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut profile = Profile::Quick;
        let mut output = PathBuf::from("target/benchmark-results");
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
                "--confirm-release" => confirm_release = true,
                // Cargo appends this marker to custom benchmark harnesses.
                "--bench" => {}
                "--help" | "-h" => return Err(usage().to_string()),
                _ => return Err(format!("unknown argument {arg:?}\n{}", usage())),
            }
        }
        if profile == Profile::Release && !confirm_release {
            return Err(
                "the release profile writes a multi-gigabyte dataset; rerun with --confirm-release"
                    .to_string(),
            );
        }
        Ok(Self { profile, output })
    }
}

pub const fn usage() -> &'static str {
    "usage: cargo bench --bench benchmarks -- --profile quick|release [--output DIR] [--confirm-release]"
}

pub fn percentile(sorted_micros: &[u64], percentile: u32) -> u64 {
    if sorted_micros.is_empty() {
        return 0;
    }
    let index = ((sorted_micros.len() - 1) * percentile as usize).div_ceil(100);
    sorted_micros[index]
}

pub fn package_version(lockfile: &str, package: &str) -> Option<String> {
    locked_package_versions(lockfile)
        .remove(package)
        .and_then(|versions| versions.into_iter().next())
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

pub fn ensure_release_reproducible(profile: Profile, git_dirty: bool) -> Result<(), String> {
    if profile == Profile::Release && git_dirty {
        return Err(
            "release benchmarks require a clean Git worktree so the recorded commit identifies the measured source"
                .to_string(),
        );
    }
    Ok(())
}
