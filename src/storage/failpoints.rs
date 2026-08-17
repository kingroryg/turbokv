//! Test-only persistence boundary failure injection.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;

use super::engine::{Result, StorageError};

/// Persistence boundaries exercised by crash and failure tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PersistenceBoundary {
    Wal,
    WalFlush,
    WalTruncation,
    MemtableFreeze,
    SstablePublication,
    ManifestInstallation,
    Checkpoint,
    CompactionOutputPublication,
    CompactionManifestPublication,
    ManifestDirectorySync,
    CompactionManifestDirectoryResync,
    SstableCleanupScan,
    SstableCleanup,
}

impl PersistenceBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::WalFlush => "WAL flush",
            Self::WalTruncation => "WAL truncation",
            Self::MemtableFreeze => "memtable freeze",
            Self::SstablePublication => "SSTable publication",
            Self::ManifestInstallation => "manifest installation",
            Self::Checkpoint => "checkpoint",
            Self::CompactionOutputPublication => "compaction output publication",
            Self::CompactionManifestPublication => "compaction manifest publication",
            Self::ManifestDirectorySync => "manifest directory sync",
            Self::CompactionManifestDirectoryResync => "compaction manifest directory resync",
            Self::SstableCleanupScan => "SSTable cleanup scan",
            Self::SstableCleanup => "SSTable cleanup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionCrashBoundary {
    OutputPublication,
    ManifestPublication,
}

const COMPACTION_CRASH_BOUNDARY_TOKENS: [(CompactionCrashBoundary, &str); 2] = [
    (
        CompactionCrashBoundary::OutputPublication,
        "compaction-output-publication",
    ),
    (
        CompactionCrashBoundary::ManifestPublication,
        "compaction-manifest-publication",
    ),
];

impl CompactionCrashBoundary {
    fn stable_token(self) -> &'static str {
        COMPACTION_CRASH_BOUNDARY_TOKENS
            .iter()
            .find(|(boundary, _)| *boundary == self)
            .map(|(_, token)| *token)
            .expect("every compaction crash boundary has a stable token")
    }

    fn from_stable_token(token: &str) -> Option<Self> {
        COMPACTION_CRASH_BOUNDARY_TOKENS
            .iter()
            .find(|(_, stable_token)| *stable_token == token)
            .map(|(boundary, _)| *boundary)
    }

    fn persistence_boundary(self) -> PersistenceBoundary {
        match self {
            Self::OutputPublication => PersistenceBoundary::CompactionOutputPublication,
            Self::ManifestPublication => PersistenceBoundary::CompactionManifestPublication,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FailureTarget {
    data_dir: PathBuf,
    boundary: PersistenceBoundary,
}

struct ArmedState {
    remaining_hits: AtomicUsize,
    hit: AtomicBool,
    crash: bool,
}

static ARMED: LazyLock<Mutex<HashMap<FailureTarget, Arc<ArmedState>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn canonical_data_dir(data_dir: &Path) -> PathBuf {
    std::fs::canonicalize(data_dir).unwrap_or_else(|_| data_dir.to_path_buf())
}

/// RAII registration for one injected failure.
pub(crate) struct ArmedFailure {
    target: FailureTarget,
    state: Arc<ArmedState>,
}

impl ArmedFailure {
    pub(crate) fn assert_hit(&self) {
        assert!(
            self.state.hit.load(Ordering::Acquire),
            "persistence boundary was not reached: {}",
            self.target.boundary.name()
        );
    }
}

impl Drop for ArmedFailure {
    fn drop(&mut self) {
        let mut armed = ARMED.lock();
        if armed
            .get(&self.target)
            .is_some_and(|registered| Arc::ptr_eq(registered, &self.state))
        {
            armed.remove(&self.target);
        }
    }
}

/// Arms a one-shot failure for one database and one persistence boundary.
pub(crate) fn arm(data_dir: &Path, boundary: PersistenceBoundary) -> ArmedFailure {
    arm_on_hit(data_dir, boundary, 1)
}

/// Arms a failure on the specified visit to a persistence boundary.
pub(crate) fn arm_on_hit(
    data_dir: &Path,
    boundary: PersistenceBoundary,
    hit_number: usize,
) -> ArmedFailure {
    arm_with_action(data_dir, boundary, hit_number, false)
}

fn arm_crash_on_hit(
    data_dir: &Path,
    boundary: PersistenceBoundary,
    hit_number: usize,
) -> ArmedFailure {
    arm_with_action(data_dir, boundary, hit_number, true)
}

fn arm_with_action(
    data_dir: &Path,
    boundary: PersistenceBoundary,
    hit_number: usize,
    crash: bool,
) -> ArmedFailure {
    assert!(hit_number > 0, "failure hit number must be positive");
    let target = FailureTarget {
        data_dir: canonical_data_dir(data_dir),
        boundary,
    };
    let state = Arc::new(ArmedState {
        remaining_hits: AtomicUsize::new(hit_number),
        hit: AtomicBool::new(false),
        crash,
    });
    let previous = ARMED.lock().insert(target.clone(), Arc::clone(&state));
    assert!(previous.is_none(), "persistence boundary already armed");
    ArmedFailure { target, state }
}

/// Returns an injected error exactly once when an armed boundary is reached.
pub(crate) fn check(data_dir: &Path, boundary: PersistenceBoundary) -> Result<()> {
    let target = FailureTarget {
        data_dir: canonical_data_dir(data_dir),
        boundary,
    };
    let mut armed = ARMED.lock();
    let should_fail = armed
        .get(&target)
        .is_some_and(|state| state.remaining_hits.fetch_sub(1, Ordering::AcqRel) == 1);
    if should_fail {
        let state = armed
            .remove(&target)
            .expect("armed failure remains registered until its selected hit");
        state.hit.store(true, Ordering::Release);
        if state.crash {
            drop(armed);
            std::process::abort();
        }
        return Err(StorageError::Other(format!(
            "injected test failure at {} boundary",
            boundary.name()
        )));
    }
    Ok(())
}

/// Abort at a crash-only boundary. Unlike [`check`], this can be called after
/// a manifest commit because it never returns an ordinary error that could
/// unwind output guards and delete newly authoritative files.
pub(crate) fn crash_if_armed(data_dir: &Path, boundary: PersistenceBoundary) {
    let target = FailureTarget {
        data_dir: canonical_data_dir(data_dir),
        boundary,
    };
    let mut armed = ARMED.lock();
    let should_crash = armed.get(&target).is_some_and(|state| {
        state.crash && state.remaining_hits.fetch_sub(1, Ordering::AcqRel) == 1
    });
    if should_crash {
        let state = armed
            .remove(&target)
            .expect("armed crash remains registered until its selected hit");
        state.hit.store(true, Ordering::Release);
        drop(armed);
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{arm, arm_crash_on_hit, arm_on_hit, CompactionCrashBoundary, PersistenceBoundary};
    use crate::storage::engine::{Engine, Result, StorageConfig};
    use crate::storage::manifest::Manifest;
    use crate::storage::sstable::CompressionType;

    fn test_config(path: &Path) -> StorageConfig {
        let mut config = StorageConfig::durable(path.to_path_buf());
        config.flush_interval = Duration::from_secs(24 * 60 * 60);
        config.compaction_interval = Duration::from_secs(24 * 60 * 60);
        config.sstable_config.compression = CompressionType::None;
        config.background_tasks_enabled = false;
        config
    }

    fn assert_injected<T>(result: Result<T>, boundary: PersistenceBoundary) {
        let error = match result {
            Ok(_) => panic!("boundary should inject a failure"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            format!(
                "Storage error: injected test failure at {} boundary",
                boundary.name()
            )
        );
    }

    async fn insert_then_fail_flush(boundary: PersistenceBoundary) -> TempDir {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();

        let failure = arm(temp.path(), boundary);
        assert_injected(engine.flush().await, boundary);
        failure.assert_hit();

        drop(engine);
        temp
    }

    fn split_compaction_config(path: &Path) -> StorageConfig {
        let mut config = test_config(path);
        config.compaction_config.l0_compaction_trigger = 2;
        config.compaction_config.max_levels = 2;
        config.compaction_config.target_file_size = 20 * 1024;
        config.sstable_config.block_size = 512;
        config
    }

    async fn split_compaction_fixture(path: &Path) -> (Engine, StorageConfig) {
        let config = split_compaction_config(path);
        let engine = Engine::open(config.clone()).await.unwrap();
        for generation in 0_usize..2 {
            for key_index in 0_usize..48 {
                let value = (0_usize..512)
                    .map(|offset| {
                        generation
                            .wrapping_mul(53)
                            .wrapping_add(key_index.wrapping_mul(97))
                            .wrapping_add(offset) as u8
                    })
                    .collect::<Vec<_>>();
                let key = format!("split:{key_index:04}");
                if generation == 1 && key_index == 0 {
                    engine.delete(key.as_bytes()).await.unwrap();
                } else {
                    engine.insert(key.as_bytes(), &value).await.unwrap();
                }
            }
            engine.flush().await.unwrap();
        }
        (engine, config)
    }

    struct SplitCompactionBaseline {
        output_count: usize,
        boundary_hits: Vec<usize>,
    }

    async fn split_compaction_baseline() -> SplitCompactionBaseline {
        let baseline = TempDir::new().unwrap();
        let (engine, _) = split_compaction_fixture(baseline.path()).await;
        let output_count = usize::try_from(engine.compact().await.unwrap().output_files).unwrap();
        assert!(output_count >= 3);
        engine.shutdown().await.unwrap();
        drop(engine);

        let mut boundary_hits = vec![1, output_count.div_ceil(2), output_count];
        boundary_hits.sort_unstable();
        boundary_hits.dedup();
        SplitCompactionBaseline {
            output_count,
            boundary_hits,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compaction_crash_child() {
        let Ok(data_dir) = std::env::var("TURBOKV_COMPACTION_CRASH_DIR") else {
            return;
        };
        let boundary_token = std::env::var("TURBOKV_COMPACTION_CRASH_BOUNDARY").unwrap();
        let crash_boundary = CompactionCrashBoundary::from_stable_token(&boundary_token)
            .unwrap_or_else(|| panic!("unknown compaction crash boundary: {boundary_token}"));
        let hit = std::env::var("TURBOKV_COMPACTION_CRASH_HIT")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let (engine, _) = split_compaction_fixture(Path::new(&data_dir)).await;
        let _crash = arm_crash_on_hit(
            Path::new(&data_dir),
            crash_boundary.persistence_boundary(),
            hit,
        );
        let result = engine.compact().await;
        panic!("crash boundary returned instead of aborting: {result:?}");
    }

    async fn run_compaction_crash_child(
        path: &Path,
        boundary: CompactionCrashBoundary,
        hit: usize,
    ) {
        let executable = std::env::current_exe().unwrap();
        let path = path.to_path_buf();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(executable)
                .arg("--exact")
                .arg("storage::failpoints::tests::compaction_crash_child")
                .arg("--nocapture")
                .env("TURBOKV_COMPACTION_CRASH_DIR", path)
                .env("TURBOKV_COMPACTION_CRASH_BOUNDARY", boundary.stable_token())
                .env("TURBOKV_COMPACTION_CRASH_HIT", hit.to_string())
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            !output.status.success(),
            "crash child unexpectedly succeeded:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    fn uncommitted_compaction_file_count(path: &Path) -> usize {
        std::fs::read_dir(path.join("sstables"))
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
            .count()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wal_only_sequence_bounds_a_later_flush_checkpoint_and_replays_on_reopen() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::Wal);

        assert_injected(
            engine.insert(b"wal-key", b"wal-value").await,
            PersistenceBoundary::Wal,
        );
        failure.assert_hit();
        assert_eq!(engine.get(b"wal-key").await.unwrap(), None);
        engine.insert(b"later-key", b"later-value").await.unwrap();
        engine.flush().await.unwrap();
        assert_eq!(
            Manifest::load_or_create(temp.path())
                .unwrap()
                .wal_checkpoint,
            0
        );
        drop(engine);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(
            reopened.get(b"wal-key").await.unwrap(),
            Some(b"wal-value".to_vec())
        );
        assert_eq!(
            reopened.get(b"later-key").await.unwrap(),
            Some(b"later-value".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memtable_freeze_boundary_leaves_the_immutable_generation_readable() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::MemtableFreeze);

        assert_injected(engine.flush().await, PersistenceBoundary::MemtableFreeze);
        failure.assert_hit();
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sstable_publication_failure_recovers_the_write_from_wal() {
        let temp = insert_then_fail_flush(PersistenceBoundary::SstablePublication).await;
        let final_files = std::fs::read_dir(temp.path().join("sstables/L0"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .count();
        assert_eq!(final_files, 0);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.stats().sstable_count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manifest_publication_failure_retains_and_retries_the_generation_once() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::ManifestInstallation);

        assert_injected(
            engine.flush().await,
            PersistenceBoundary::ManifestInstallation,
        );
        failure.assert_hit();
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert!(Manifest::load_or_create(temp.path())
            .unwrap()
            .sstables
            .is_empty());
        let installed_file = std::fs::read_dir(temp.path().join("sstables/L0"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .find(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .expect("manifest failure leaves one complete unreferenced SSTable")
            .path();
        crate::storage::sstable::SSTableReader::open(&installed_file).unwrap();

        engine.flush().await.unwrap();
        let manifest = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(manifest.sstables.len(), 1);
        assert_eq!(manifest.wal_checkpoint, 1);
        assert_eq!(engine.stats().immutable_memtables, 0);
        assert_eq!(engine.stats().sstable_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn installed_checkpoint_survives_before_wal_reclamation() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::Checkpoint);

        assert_injected(engine.flush().await, PersistenceBoundary::Checkpoint);
        failure.assert_hit();
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.stats().sstable_count, 1);
        let flush_bytes = engine.stats().sstable_flush_bytes_written;
        let manifest = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(manifest.wal_checkpoint, 1);
        assert_eq!(manifest.sstables.len(), 1);

        engine.flush().await.unwrap();
        assert_eq!(engine.stats().immutable_memtables, 0);
        assert_eq!(engine.stats().sstable_count, 1);
        assert_eq!(engine.stats().sstable_flush_bytes_written, flush_bytes);
        assert_eq!(
            Manifest::load_or_create(temp.path())
                .unwrap()
                .sstables
                .len(),
            1
        );
        drop(engine);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.stats().sstable_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn second_generation_failure_keeps_checkpoint_behind_every_unflushed_generation() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path());
        config.memtable_config.max_entries = 1;
        let engine = Engine::open(config.clone()).await.unwrap();
        for sequence in 0..3 {
            engine
                .insert(
                    format!("key-{sequence}").as_bytes(),
                    format!("value-{sequence}").as_bytes(),
                )
                .await
                .unwrap();
        }

        let failure = arm_on_hit(temp.path(), PersistenceBoundary::ManifestInstallation, 2);
        assert_injected(
            engine.flush().await,
            PersistenceBoundary::ManifestInstallation,
        );
        failure.assert_hit();

        let manifest = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(manifest.sstables.len(), 1);
        assert_eq!(manifest.sstables[0].max_sequence, 0);
        assert_eq!(manifest.wal_checkpoint, 1);
        assert_eq!(engine.stats().immutable_memtables, 2);
        for sequence in 0..3 {
            assert_eq!(
                engine
                    .get(format!("key-{sequence}").as_bytes())
                    .await
                    .unwrap(),
                Some(format!("value-{sequence}").into_bytes())
            );
        }
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        for sequence in 0..3 {
            assert_eq!(
                reopened
                    .get(format!("key-{sequence}").as_bytes())
                    .await
                    .unwrap(),
                Some(format!("value-{sequence}").into_bytes())
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manifest_io_failure_does_not_poison_the_live_manifest_before_retry() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        std::fs::create_dir(temp.path().join("MANIFEST.tmp")).unwrap();

        assert!(engine.flush().await.is_err());
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        std::fs::remove_dir(temp.path().join("MANIFEST.tmp")).unwrap();

        engine.flush().await.unwrap();
        let manifest = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(manifest.sstables.len(), 1);
        assert_eq!(engine.stats().sstable_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_wal_mode_keeps_a_failed_generation_live_until_retry() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(temp.path());
        config.wal_enabled = false;
        let engine = Engine::open(config).await.unwrap();
        engine.insert(b"key", b"value").await.unwrap();
        engine.flush_write_buffers().unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::SstablePublication);

        assert_injected(
            engine.flush().await,
            PersistenceBoundary::SstablePublication,
        );
        failure.assert_hit();
        assert_eq!(engine.stats().immutable_memtables, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));

        engine.flush().await.unwrap();
        assert_eq!(engine.stats().immutable_memtables, 0);
        assert_eq!(engine.stats().sstable_count, 1);
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compaction_output_publication_failure_keeps_installed_inputs_authoritative() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();

        for generation in 0..4 {
            let key = format!("key-{generation}");
            let value = format!("value-{generation}");
            engine
                .insert(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
            engine.flush().await.unwrap();
        }
        assert_eq!(engine.stats().l0_sstable_count, 4);

        let failure = arm(
            temp.path(),
            PersistenceBoundary::CompactionOutputPublication,
        );
        assert_injected(
            engine.compact().await,
            PersistenceBoundary::CompactionOutputPublication,
        );
        failure.assert_hit();
        assert_eq!(engine.stats().l0_sstable_count, 4);
        assert_eq!(engine.stats().sstable_count, 4);
        assert_eq!(
            std::fs::read_dir(temp.path().join("sstables"))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "sst")
                })
                .count(),
            0,
            "an output rejected before directory durability must be removed"
        );
        drop(engine);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        for generation in 0..4 {
            let key = format!("key-{generation}");
            let value = format!("value-{generation}");
            assert_eq!(
                reopened.get(key.as_bytes()).await.unwrap(),
                Some(value.into_bytes())
            );
        }
        assert_eq!(reopened.stats().l0_sstable_count, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_split_output_and_manifest_boundary_reopens_from_the_input_generation() {
        let baseline = split_compaction_baseline().await;
        for hit in baseline.boundary_hits {
            let temp = TempDir::new().unwrap();
            let (engine, config) = split_compaction_fixture(temp.path()).await;
            let before = engine.physical_stats();
            let input_ids = Manifest::load_or_create(temp.path())
                .unwrap()
                .sstables
                .into_iter()
                .map(|table| table.id)
                .collect::<Vec<_>>();
            let failure = arm_on_hit(
                temp.path(),
                PersistenceBoundary::CompactionOutputPublication,
                hit,
            );

            assert_injected(
                engine.compact().await,
                PersistenceBoundary::CompactionOutputPublication,
            );
            failure.assert_hit();
            assert_eq!(engine.physical_stats().sstables.files, 2);
            assert_eq!(
                engine
                    .physical_stats()
                    .amplification
                    .compaction_input_bytes_since_open,
                before.sstables.bytes
            );
            assert!(
                engine
                    .physical_stats()
                    .amplification
                    .compaction_output_bytes_since_open
                    > 0
            );
            assert_eq!(uncommitted_compaction_file_count(temp.path()), 0);
            assert_eq!(
                Manifest::load_or_create(temp.path())
                    .unwrap()
                    .sstables
                    .into_iter()
                    .map(|table| table.id)
                    .collect::<Vec<_>>(),
                input_ids
            );
            drop(engine);

            let reopened = Engine::open(config).await.unwrap();
            assert_eq!(reopened.get(b"split:0000").await.unwrap(), None);
            assert!(reopened.get(b"split:0001").await.unwrap().is_some());
            assert_eq!(reopened.physical_stats().sstables.files, 2);
            reopened.shutdown().await.unwrap();
        }

        let temp = TempDir::new().unwrap();
        let (engine, config) = split_compaction_fixture(temp.path()).await;
        let input_ids = Manifest::load_or_create(temp.path())
            .unwrap()
            .sstables
            .into_iter()
            .map(|table| table.id)
            .collect::<Vec<_>>();
        let failure = arm(temp.path(), PersistenceBoundary::ManifestInstallation);
        assert_injected(
            engine.compact().await,
            PersistenceBoundary::ManifestInstallation,
        );
        failure.assert_hit();
        assert_eq!(uncommitted_compaction_file_count(temp.path()), 0);
        assert_eq!(
            Manifest::load_or_create(temp.path())
                .unwrap()
                .sstables
                .into_iter()
                .map(|table| table.id)
                .collect::<Vec<_>>(),
            input_ids
        );
        drop(engine);

        let reopened = Engine::open(config).await.unwrap();
        assert_eq!(reopened.get(b"split:0000").await.unwrap(), None);
        assert!(reopened.get(b"split:0047").await.unwrap().is_some());
        assert_eq!(reopened.physical_stats().sstables.files, 2);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_crashes_at_split_and_manifest_boundaries_reopen_atomically() {
        let baseline = split_compaction_baseline().await;
        for hit in baseline.boundary_hits {
            let temp = TempDir::new().unwrap();
            run_compaction_crash_child(
                temp.path(),
                CompactionCrashBoundary::OutputPublication,
                hit,
            )
            .await;

            let durable = Manifest::load_or_create(temp.path()).unwrap();
            assert_eq!(durable.sstables.len(), 2);
            assert_eq!(uncommitted_compaction_file_count(temp.path()), hit);

            let reopened = Engine::open(split_compaction_config(temp.path()))
                .await
                .unwrap();
            assert_eq!(uncommitted_compaction_file_count(temp.path()), 0);
            assert_eq!(reopened.physical_stats().sstables.files, 2);
            assert_eq!(reopened.get(b"split:0000").await.unwrap(), None);
            assert!(reopened.get(b"split:0001").await.unwrap().is_some());
            assert!(reopened.get(b"split:0047").await.unwrap().is_some());
            reopened.shutdown().await.unwrap();
        }

        let temp = TempDir::new().unwrap();
        run_compaction_crash_child(temp.path(), CompactionCrashBoundary::ManifestPublication, 1)
            .await;
        let durable = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(durable.sstables.len(), baseline.output_count);
        assert_eq!(
            uncommitted_compaction_file_count(temp.path()),
            baseline.output_count
        );

        let reopened = Engine::open(split_compaction_config(temp.path()))
            .await
            .unwrap();
        assert_eq!(
            reopened.physical_stats().sstables.files,
            baseline.output_count as u64
        );
        assert_eq!(
            uncommitted_compaction_file_count(temp.path()),
            baseline.output_count
        );
        assert_eq!(reopened.get(b"split:0000").await.unwrap(), None);
        assert!(reopened.get(b"split:0001").await.unwrap().is_some());
        assert!(reopened.get(b"split:0047").await.unwrap().is_some());
        reopened.shutdown().await.unwrap();
    }

    #[test]
    fn unarmed_boundary_has_no_behavior() {
        let temp = TempDir::new().unwrap();
        super::check(temp.path(), PersistenceBoundary::Wal).unwrap();
    }

    #[test]
    fn armed_failure_is_path_scoped_and_fires_once() {
        let armed_path = TempDir::new().unwrap();
        let other_path = TempDir::new().unwrap();
        let failure = arm(armed_path.path(), PersistenceBoundary::Wal);

        super::check(other_path.path(), PersistenceBoundary::Wal).unwrap();
        assert_injected(
            super::check(armed_path.path(), PersistenceBoundary::Wal),
            PersistenceBoundary::Wal,
        );
        super::check(armed_path.path(), PersistenceBoundary::Wal).unwrap();
        failure.assert_hit();
    }

    #[test]
    fn armed_failure_can_select_a_later_boundary_visit() {
        let temp = TempDir::new().unwrap();
        let failure = arm_on_hit(temp.path(), PersistenceBoundary::Checkpoint, 2);

        super::check(temp.path(), PersistenceBoundary::Checkpoint).unwrap();
        assert_injected(
            super::check(temp.path(), PersistenceBoundary::Checkpoint),
            PersistenceBoundary::Checkpoint,
        );
        failure.assert_hit();
    }
}
