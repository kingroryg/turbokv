#[path = "protocol.rs"]
mod protocol;

use protocol::{
    benchmark_source_manifest, deterministic_value, distribution, ensure_release_reproducible,
    fnv1a64, locked_package_versions, percentile, random_keys, sequential_keys,
    AcknowledgementPolicy, Cli, Durability, Profile, WorkloadConfig, KEY_BYTES, MEMTABLE_BYTES,
    SEED,
};

#[test]
fn quick_is_the_bounded_default() {
    let cli = Cli::parse(Vec::<String>::new()).unwrap();
    let defaults = cli.profile.defaults();
    assert_eq!(cli.profile, Profile::Quick);
    assert_eq!(cli.profile.as_str(), "quick");
    assert_eq!(cli.output.to_str(), Some("../target/benchmark-results"));
    assert!(cli.machine.is_none());
    assert_eq!(defaults.keys, 32);
    assert_eq!(defaults.repetitions, 1);
    assert_eq!(defaults.seed, SEED);
    assert_eq!(KEY_BYTES, 20);
    assert_eq!(MEMTABLE_BYTES, 64 * 1024 * 1024);
}

#[test]
fn release_requires_explicit_confirmation_and_machine_name() {
    let error = Cli::parse(["--profile".into(), "release".into()]).unwrap_err();
    assert!(error.contains("--confirm-release"));
    let error = Cli::parse([
        "--profile".into(),
        "release".into(),
        "--confirm-release".into(),
    ])
    .unwrap_err();
    assert!(error.contains("--machine"));
    let cli = Cli::parse([
        "--profile".into(),
        "release".into(),
        "--confirm-release".into(),
        "--machine".into(),
        "Apple M4 reference".into(),
    ])
    .unwrap();
    assert_eq!(cli.profile, Profile::Release);
}

#[test]
fn durability_classes_select_distinct_acknowledgement_policies() {
    assert_eq!(Durability::ALL, [Durability::Durable, Durability::Paranoid]);
    assert_eq!(Durability::Durable.as_str(), "durable");
    assert_eq!(
        Durability::Durable.acknowledgement_policy(),
        AcknowledgementPolicy::WalInOsCache
    );
    assert_eq!(
        Durability::Paranoid.acknowledgement_policy(),
        AcknowledgementPolicy::WalFsync
    );
}

#[test]
fn measured_adapter_dispatch_is_allocation_free() {
    let adapter = include_str!("engine.rs");
    assert!(adapter.contains("enum State"));
    assert!(!adapter.contains("async_trait"));
    assert!(!adapter.contains("Box<dyn Backend"));
}

#[test]
fn rocks_adapter_adds_full_barriers_at_acknowledgement_and_settlement() {
    let adapter = include_str!("engine.rs");
    assert!(adapter.contains("write_options.set_sync(false)"));
    assert!(adapter.contains("state.db.flush_wal(false)"));
    assert!(adapter.contains("sync_active_wal_with_full_barrier"));
    assert!(adapter.contains("state.full_sync_database_files()?"));
    assert!(adapter.contains("File::open(&self.path)?.sync_all()?"));
}

#[test]
fn percentiles_select_observed_latency_samples() {
    let samples = [10, 20, 30, 40, 50];
    assert_eq!(percentile(&samples, 50), 30);
    assert_eq!(percentile(&samples, 95), 50);
    assert_eq!(percentile(&samples, 99), 50);
}

#[test]
fn repeated_run_dispersion_is_population_standard_deviation() {
    let summary = distribution(&[10.0, 20.0, 30.0]);
    assert_eq!(summary.samples, 3);
    assert!((summary.minimum - 10.0).abs() < f64::EPSILON);
    assert!((summary.median - 20.0).abs() < f64::EPSILON);
    assert!((summary.maximum - 30.0).abs() < f64::EPSILON);
    assert!((summary.mean - 20.0).abs() < f64::EPSILON);
    assert!((summary.standard_deviation - 8.164_965_809).abs() < 1e-9);
}

#[test]
fn release_requires_a_clean_source_tree() {
    assert!(ensure_release_reproducible(Profile::Quick, true).is_ok());
    assert!(ensure_release_reproducible(Profile::Release, false).is_ok());
    assert!(ensure_release_reproducible(Profile::Release, true).is_err());
}

#[test]
fn lockfile_audit_preserves_all_resolved_versions() {
    let lockfile = concat!(
        "[[package]]\nname = \"shared\"\nversion = \"1.0.0\"\n",
        "[[package]]\nname = \"shared\"\nversion = \"2.0.0\"\n",
        "[[package]]\nname = \"engine\"\nversion = \"3.0.0\"\n",
    );
    let packages = locked_package_versions(lockfile);
    assert_eq!(packages["shared"], ["1.0.0", "2.0.0"]);
    assert_eq!(packages["engine"], ["3.0.0"]);
    assert_eq!(fnv1a64(b""), "cbf29ce484222325");
}

#[test]
fn source_manifest_excludes_only_result_artifacts_and_is_path_sorted() {
    let listing = concat!(
        // Blob IDs deliberately sort in the opposite order from their paths.
        // This prevents a whole-record sort from masquerading as a path sort.
        "100644 blob 0000\tbenchmarks/workloads.rs\n",
        "100644 blob bbbb\tbenchmarks/results/host/result.json\n",
        "100755 blob ffff\tbenchmarks/run.sh\n",
        "100644 blob 1111\tdocs/design.md\n",
    );
    assert_eq!(
        benchmark_source_manifest(listing),
        concat!(
            "100755 blob ffff\tbenchmarks/run.sh\n",
            "100644 blob 0000\tbenchmarks/workloads.rs\n",
            "100644 blob 1111\tdocs/design.md\n",
        )
    );
}

#[test]
fn seeded_datasets_are_unique_ordered_and_repeatable() {
    let config = WorkloadConfig {
        keys: 10_000,
        value_bytes: 8,
        repetitions: 1,
        scan_passes: 1,
        recovery_cycles: 1,
        seed: 42,
    };
    let random = random_keys(&config);
    let unique = random.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), random.len());
    assert_eq!(random, random_keys(&config));
    assert_eq!(deterministic_value(8, 42), deterministic_value(8, 42));

    let sequential = sequential_keys(100);
    let mut sorted = sequential.clone();
    sorted.sort();
    assert_eq!(sequential, sorted);
    assert!(sequential.iter().all(|key| key.len() == KEY_BYTES));
}
