// Compile the benchmark entry point under Rust's test harness so its private
// report serializer and human renderer stay tested without widening their API.
include!("main.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_and_human_reports_include_dependencies_and_storage_accounting() {
        let measurement = |workload,
                           logical_mutation_bytes,
                           logical_live_bytes,
                           write_amplification,
                           disk_amplification| {
            Measurement {
                engine: EngineName::TurboKv,
                durability: Durability::Durable,
                workload,
                repetition: 1,
                operations: 1,
                operation_unit: "fixture operations",
                acknowledgement_seconds: 1.0,
                settlement_seconds: 0.0,
                total_seconds: 1.0,
                acknowledgement_ops_per_second: 1.0,
                fully_settled_ops_per_second: 1.0,
                latency_unit: "fixture latency",
                latency_nanoseconds: workloads::Latencies {
                    samples: 1,
                    p50: 1,
                    p95: 1,
                    p99: 1,
                    maximum: 1,
                },
                logical_mutation_bytes,
                logical_live_bytes,
                disk_bytes: 1,
                wal_bytes_written: None,
                flush_bytes_written: None,
                compaction_bytes_read: None,
                compaction_bytes_written: None,
                write_amplification,
                disk_amplification,
            }
        };
        let report = Report {
            schema_version: 8,
            generated_unix_seconds: 1,
            profile: Profile::Quick,
            dataset: Profile::Quick.defaults(),
            protocol: protocol_settings(Profile::Quick),
            engine_settings: engine_settings(&EngineName::ALL),
            counter_availability: counter_availability(&EngineName::ALL),
            environment: Environment {
                machine_name: "fixture".to_string(),
                cpu: "fixture-cpu".to_string(),
                hardware_model: "fixture-model".to_string(),
                memory_bytes: Some(1),
                os: "fixture-os".to_string(),
                architecture: "fixture-arch".to_string(),
                cpu_parallelism: 1,
                filesystem: "fixture-fs".to_string(),
                rustc: "fixture-rustc".to_string(),
                git_commit: "fixture-commit".to_string(),
                source_manifest_git_hash: "fixture-manifest".to_string(),
                source_manifest_hash_algorithm: "fixture-hash",
                source_manifest_scope: "fixture-scope",
                git_dirty: false,
                power_policy: "fixture-power",
                thermal_policy: "fixture-thermal",
                background_load: "fixture-load",
            },
            direct_dependencies: direct_dependencies(),
            resolved_dependencies: BTreeMap::from([
                ("engine".to_string(), vec!["2.6.3".to_string()]),
                (
                    "shared".to_string(),
                    vec!["1.0.0".to_string(), "2.0.0".to_string()],
                ),
            ]),
            benchmark_lock_fnv1a64: "fixture-lock".to_string(),
            results: vec![
                measurement(
                    Workload::SequentialFill,
                    84_000_000,
                    84_000_000,
                    Some(2.139_028),
                    Some(2.139_034),
                ),
                measurement(Workload::RandomRead, 0, 84_000_000, None, None),
            ],
            summaries: Vec::new(),
        };

        let json = serde_json::to_value(&report).expect("fixture report must serialize");
        assert_eq!(json["resolved_dependencies"]["engine"][0], "2.6.3");
        assert_eq!(json["results"][0]["logical_mutation_bytes"], 84_000_000);
        assert_eq!(json["results"][0]["logical_live_bytes"], 84_000_000);
        assert_eq!(json["results"][0]["write_amplification"], 2.139_028);
        assert_eq!(json["results"][0]["disk_amplification"], 2.139_034);
        assert!(json["results"][1]["write_amplification"].is_null());
        assert!(json["results"][1]["disk_amplification"].is_null());
        assert_eq!(
            serde_json::to_value(Workload::SequentialBatch100).unwrap(),
            "sequential_batch_100"
        );
        assert_eq!(
            serde_json::to_value(Workload::SequentialBatch1000).unwrap(),
            "sequential_batch_1000"
        );

        let human = human_report(&report);
        assert!(human.contains("resolved dependencies (benchmark Cargo.lock):"));
        assert!(human.contains("  engine: 2.6.3"));
        assert!(human.contains("  shared: 1.0.0, 2.0.0"));
        assert!(human.contains("logical mutation bytes  logical live bytes  write amp  disk amp"));
        let accounting = human
            .split_once("storage accounting\n")
            .expect("human report must contain storage accounting")
            .1
            .split_once("\nrepeated-run throughput dispersion")
            .expect("storage accounting must end before summaries")
            .0;
        assert!(accounting.lines().any(|line| {
            line.contains("sequential_fill")
                && line.contains("84000000")
                && line.contains("2.139028")
                && line.contains("2.139034")
        }));
        assert!(accounting.lines().any(|line| {
            line.contains("random_read")
                && line.contains("84000000")
                && line
                    .split_whitespace()
                    .rev()
                    .take(2)
                    .all(|field| field == "-")
        }));
    }
}
