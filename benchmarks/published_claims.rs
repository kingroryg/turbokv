#[allow(dead_code)]
mod protocol;

use serde_json::Value;

const README: &str = include_str!("../README.md");
const BENCHMARK_README: &str = include_str!("README.md");
const ARTIFACT: &str =
    include_str!("results/apple-m4-macos-15.3.2/durability-baseline-current.json");
const REQUIRED_DURABLE_INGEST: [&str; 3] = ["sequential_fill", "random_fill", "overwrite"];

#[test]
fn readme_reports_the_predeclared_durable_ingest_gate_from_retained_evidence() {
    let report = report();
    let headings = README
        .lines()
        .filter(|line| line.starts_with("## "))
        .collect::<Vec<_>>();
    assert_eq!(
        headings,
        ["## Installation", "## Quick start", "## API breakdown", "## Benchmarks"]
    );

    for (workload, label) in [
        ("sequential_fill", "Sequential fill"),
        ("random_fill", "Random fill"),
        ("overwrite", "Overwrite"),
    ] {
        let turbo = median(&report, "turbo_kv", workload, "acknowledgement_ops_per_second");
        let fjall = median(&report, "fjall", workload, "acknowledgement_ops_per_second");
        let redb = median(&report, "redb", workload, "acknowledgement_ops_per_second");
        let ratio = turbo / fjall;
        assert!(ratio.is_finite() && ratio > 0.0, "invalid {workload} ratio");
        let row = format!(
            "| {label} | {} | {} | {} | {ratio:.3}× |",
            grouped_integer(turbo),
            grouped_integer(fjall),
            grouped_integer(redb),
        );
        assert!(README.contains(&row), "README is missing benchmark row: {row}");
    }

    assert!(REQUIRED_DURABLE_INGEST
        .iter()
        .all(|workload| acknowledgement_ratio(&report, workload) > 1.0));
    assert!(README.contains("Cross-engine settled timings are not compared"));
    assert!(README.contains("Sequential-fill dispersion was high"));
    assert!(README.contains("historical retained release benchmark"));
    assert!(README.contains("not a current-HEAD run"));
}

#[test]
fn retained_release_evidence_matches_the_documented_protocol() {
    let report = report();
    let environment = &report["environment"];
    let protocol = &report["protocol"];
    let common = &protocol["common"];
    let dataset = &report["dataset"];

    assert_eq!(report["schema_version"], 6);
    assert_eq!(report["profile"], "release");
    assert_eq!(environment["git_dirty"], false);
    assert_eq!(environment["git_commit"], "f95a7872e84db029acc3a769d62f59bdb4c1ccb8");
    assert_eq!(
        environment["source_manifest_scope"],
        "bytewise path-sorted `mode type blob_oid<TAB>path` records from `git ls-tree -r --full-tree HEAD`, excluding only paths under benchmarks/results/, joined with LF and terminated by LF"
    );
    assert_eq!(
        environment["machine_name"],
        "Apple M4 (Mac16,1), 32 GiB, macOS 15.3.2"
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
    assert_eq!(environment["rustc"], "rustc 1.88.0 (6b00bc388 2025-06-23)");
    assert!(
        (1_787_875_200_u64..1_787_961_600_u64)
            .contains(&report["generated_unix_seconds"].as_u64().unwrap()),
        "release evidence was not generated on 2026-08-28 UTC"
    );

    assert_eq!(dataset["keys"], 200_000);
    assert_eq!(dataset["value_bytes"], 400);
    assert_eq!(dataset["repetitions"], 3);
    assert_eq!(dataset["scan_passes"], 5);
    assert_eq!(dataset["recovery_cycles"], 5);
    assert_eq!(dataset["seed"], 6_076_853_716_958_008_836_u64);
    assert_eq!(common["key_bytes"], 20);
    assert_eq!(common["durable_storage_enabled"], true);
    assert_eq!(common["batch_size"], 1);
    assert_eq!(common["concurrency"], 1);
    assert_eq!(common["memtable_bytes"], 67_108_864);
    assert_eq!(common["compression"], "disabled");
    assert_eq!(common["block_cache_bytes"], 0);
    assert!(
        dataset["keys"].as_u64().unwrap()
            * (common["key_bytes"].as_u64().unwrap() + dataset["value_bytes"].as_u64().unwrap())
            > common["memtable_bytes"].as_u64().unwrap()
    );
    assert_eq!(protocol["durability_classes"].as_array().unwrap().len(), 1);
    assert_eq!(protocol["durability_classes"][0]["durability"], "durable");
    assert_eq!(report["results"].as_array().unwrap().len(), 81);
    assert_eq!(report["summaries"].as_array().unwrap().len(), 27);
    assert!(report["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|measurement| measurement["durability"] == "durable"));
    assert_eq!(
        protocol["acknowledgement_boundary"],
        "all measured operations returned successfully; mutation acknowledgement uses the row's named durability class"
    );
    assert_eq!(
        protocol["settled_boundary"],
        "acknowledgement followed by synchronous forced flush and two manual compaction drains for mutation workloads"
    );
    assert_eq!(
        protocol["production_scale_rule"],
        "release durable logical key-plus-value bytes exceed the configured LSM memtable; quick and paranoid profiles are explicitly bounded and are not production-scale ingest evidence"
    );

    for required in [
        "2026-08-28",
        "macOS 15.3.2",
        "24D81",
        "rustc 1.88.0",
        "200,000 deterministic 20-byte keys, 400-byte values",
        "84 MB logical",
    ] {
        assert!(
            README.contains(required),
            "README is missing artifact provenance: {required}"
        );
    }
    assert!(BENCHMARK_README.contains("seed `0x545552424f4b5604`"));
    assert!(BENCHMARK_README.contains(
        "cargo bench --manifest-path benchmarks/Cargo.toml --bench benchmarks --"
    ));
}

fn grouped_integer(value: f64) -> String {
    let digits = format!("{value:.0}");
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index != 0 && (digits.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(character);
    }
    grouped
}

fn report() -> Value {
    serde_json::from_str(ARTIFACT).expect("retained current artifact must be JSON")
}

fn acknowledgement_ratio(report: &Value, workload: &str) -> f64 {
    median(
        report,
        "turbo_kv",
        workload,
        "acknowledgement_ops_per_second",
    ) / median(report, "fjall", workload, "acknowledgement_ops_per_second")
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
