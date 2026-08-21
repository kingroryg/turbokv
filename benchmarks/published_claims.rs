#[allow(dead_code)]
mod protocol;

use std::io::Write as _;
use std::process::{Command, Stdio};

use protocol::benchmark_source_manifest;
use serde_json::Value;

const README: &str = include_str!("../README.md");
const ARTIFACT: &str = include_str!(
    "results/apple-m4-macos-15.3.2/durability-baseline-current.json"
);

#[test]
fn readme_acknowledgement_claims_are_bounded_by_current_release_evidence() {
    let report = report();
    let sequential_ratio = acknowledgement_ratio(&report, "sequential_fill");
    assert!(
        (0.40..=1.60).contains(&sequential_ratio),
        "sequential-fill acknowledgement ratio {sequential_ratio:.4} is outside the published evidence bound"
    );
    for workload in ["random_fill", "overwrite"] {
        let ratio = acknowledgement_ratio(&report, workload);
        assert!(
            (0.50..=1.00).contains(&ratio),
            "{workload} acknowledgement ratio {ratio:.4} is outside the published bucket"
        );
    }
    let mixed_ratio = acknowledgement_ratio(&report, "mixed");
    assert!(
        (1.20..=2.50).contains(&mixed_ratio),
        "mixed acknowledgement ratio {mixed_ratio:.4} is outside the published bucket"
    );

    for claim in [
        "0.40–1.60× fjall",
        "0.50–1.00× fjall's acknowledgement",
        "1.20–2.50× fjall's acknowledgement",
        "Sequential fill was noisy enough",
        "that no winner is claimed",
        "No cross-engine claim uses the report's “fully settled” timings",
    ] {
        assert!(README.contains(claim), "README is missing claim: {claim}");
    }
}

#[test]
fn current_release_evidence_matches_the_documented_source_and_protocol() {
    let report = report();
    let environment = &report["environment"];
    let protocol = &report["protocol"];
    let common = &protocol["common"];
    let dataset = &report["dataset"];

    assert_eq!(report["profile"], "release");
    assert_eq!(environment["git_dirty"], false);
    assert_eq!(
        environment["source_manifest_git_hash"],
        current_source_manifest_git_hash()
    );
    assert_eq!(environment["cpu"], "Apple M4");
    assert_eq!(environment["hardware_model"], "Mac16,1");
    assert_eq!(environment["memory_bytes"], 34_359_738_368_u64);
    assert_eq!(environment["cpu_parallelism"], 10);
    assert_eq!(environment["architecture"], "aarch64");
    assert_eq!(environment["filesystem"], "APFS");
    assert_eq!(
        environment["os"],
        "ProductName:\t\tmacOS\nProductVersion:\t\t15.3.2\nBuildVersion:\t\t24D81"
    );
    assert_eq!(
        environment["machine_name"],
        "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
    );
    assert_eq!(
        environment["rustc"],
        "rustc 1.88.0 (6b00bc388 2025-06-23)"
    );
    assert!(
        (1_787_270_400_u64..1_787_356_800_u64)
            .contains(&report["generated_unix_seconds"].as_u64().unwrap()),
        "release evidence was not generated on 2026-08-21 UTC"
    );

    assert_eq!(dataset["keys"], 1_000);
    assert_eq!(dataset["value_bytes"], 400);
    assert_eq!(dataset["repetitions"], 3);
    assert_eq!(dataset["scan_passes"], 5);
    assert_eq!(dataset["recovery_cycles"], 5);
    assert_eq!(dataset["seed"], 6_076_853_716_958_008_836_u64);
    assert_eq!(common["key_bytes"], 20);
    assert_eq!(common["wal_enabled"], true);
    assert_eq!(common["batch_size"], 1);
    assert_eq!(common["concurrency"], 1);
    assert_eq!(common["memtable_bytes"], 67_108_864);
    assert_eq!(common["compression"], "disabled");
    assert_eq!(common["block_cache_bytes"], 0);
    assert_eq!(
        protocol["acknowledgement_boundary"],
        "all measured operations returned successfully; mutation acknowledgement uses the row's named durability class"
    );
    assert_eq!(
        protocol["settled_boundary"],
        "acknowledgement followed by synchronous forced flush and two manual compaction drains for mutation workloads"
    );

    for required in [
        "2026-08-21",
        "15.3.2 build 24D81",
        "1,000 deterministic 20-byte keys and 400-byte values",
        "seed `0x545552424f4b5604`",
        "cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks -- --profile release --confirm-release --machine \"Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2\" --output ../target/issue-28-release",
    ] {
        assert!(
            README.contains(required),
            "README is missing artifact provenance: {required}"
        );
    }
}

fn report() -> Value {
    serde_json::from_str(ARTIFACT).expect("retained current artifact must be JSON")
}

fn acknowledgement_ratio(report: &Value, workload: &str) -> f64 {
    median(report, "turbo_kv", workload, "acknowledgement_ops_per_second")
        / median(report, "fjall", workload, "acknowledgement_ops_per_second")
}

fn median(report: &Value, engine: &str, workload: &str, boundary: &str) -> f64 {
    report["summaries"]
        .as_array()
        .expect("summaries must be an array")
        .iter()
        .find(|summary| {
            summary["engine"] == engine
                && summary["durability"] == "durable"
                && summary["workload"] == workload
        })
        .unwrap_or_else(|| panic!("missing durable summary for {engine}/{workload}"))[boundary]
        ["median"]
        .as_f64()
        .unwrap_or_else(|| panic!("missing median {boundary} for {engine}/{workload}"))
}

fn current_source_manifest_git_hash() -> String {
    let listing = Command::new("git")
        .args(["ls-tree", "-r", "--full-tree", "HEAD"])
        .output()
        .expect("git ls-tree must run");
    assert!(listing.status.success(), "git ls-tree failed");
    let manifest = benchmark_source_manifest(
        std::str::from_utf8(&listing.stdout).expect("git tree listing must be UTF-8"),
    );

    let mut child = Command::new("git")
        .args(["hash-object", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("git hash-object must start");
    child
        .stdin
        .as_mut()
        .expect("git hash-object stdin must exist")
        .write_all(manifest.as_bytes())
        .expect("source manifest must be writable");
    let output = child.wait_with_output().expect("git hash-object must finish");
    assert!(output.status.success(), "git hash-object failed");
    String::from_utf8(output.stdout)
        .expect("git hash must be UTF-8")
        .trim()
        .to_string()
}
