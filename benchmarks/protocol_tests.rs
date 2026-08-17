#[path = "protocol.rs"]
mod protocol;

use protocol::{
    ensure_release_reproducible, locked_package_versions, package_version, percentile, Cli,
    Profile, SEED,
};

#[test]
fn quick_is_the_bounded_default() {
    let cli = Cli::parse(Vec::<String>::new()).unwrap();
    assert_eq!(cli.profile, Profile::Quick);
    assert_eq!(cli.output.to_str(), Some("target/benchmark-results"));
    assert_eq!(cli.profile.defaults().operations, 1_000);
    assert_eq!(cli.profile.defaults().seed, SEED);
}

#[test]
fn release_requires_an_explicit_large_run_confirmation() {
    let error = Cli::parse(["--profile".into(), "release".into()]).unwrap_err();
    assert!(error.contains("--confirm-release"));
    let cli = Cli::parse([
        "--profile".into(),
        "release".into(),
        "--confirm-release".into(),
    ])
    .unwrap();
    assert_eq!(cli.profile, Profile::Release);
}

#[test]
fn percentiles_select_observed_latency_samples() {
    let samples = [10, 20, 30, 40, 50];
    assert_eq!(percentile(&samples, 50), 30);
    assert_eq!(percentile(&samples, 95), 50);
    assert_eq!(percentile(&samples, 99), 50);
}

#[test]
fn dependency_version_comes_from_the_lockfile() {
    let lockfile = "[[package]]\nname = \"fjall\"\nversion = \"2.11.2\"\n";
    assert_eq!(
        package_version(lockfile, "fjall").as_deref(),
        Some("2.11.2")
    );
}

#[test]
fn complete_locked_dependency_set_preserves_multiple_versions() {
    let lockfile = concat!(
        "[[package]]\nname = \"shared\"\nversion = \"1.0.0\"\n",
        "[[package]]\nname = \"shared\"\nversion = \"2.0.0\"\n",
        "[[package]]\nname = \"engine\"\nversion = \"3.0.0\"\n",
    );
    let packages = locked_package_versions(lockfile);
    assert_eq!(packages["shared"], ["1.0.0", "2.0.0"]);
    assert_eq!(packages["engine"], ["3.0.0"]);
}

#[test]
fn release_requires_a_clean_source_tree() {
    assert!(ensure_release_reproducible(Profile::Quick, true).is_ok());
    assert!(ensure_release_reproducible(Profile::Release, false).is_ok());
    assert!(ensure_release_reproducible(Profile::Release, true).is_err());
}
