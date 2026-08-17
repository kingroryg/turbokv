//! Test-only persistence boundary failure injection.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;

use super::engine::{Result, StorageError};

/// Persistence boundaries exercised by crash and failure tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PersistenceBoundary {
    Wal,
    MemtableFreeze,
    SstablePublication,
    ManifestInstallation,
    Checkpoint,
    Compaction,
}

impl PersistenceBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::MemtableFreeze => "memtable freeze",
            Self::SstablePublication => "SSTable publication",
            Self::ManifestInstallation => "manifest installation",
            Self::Checkpoint => "checkpoint",
            Self::Compaction => "compaction",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FailureTarget {
    data_dir: PathBuf,
    boundary: PersistenceBoundary,
}

static ARMED: LazyLock<Mutex<HashMap<FailureTarget, Arc<AtomicBool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// RAII registration for one injected failure.
pub(crate) struct ArmedFailure {
    target: FailureTarget,
    hit: Arc<AtomicBool>,
}

impl ArmedFailure {
    pub(crate) fn assert_hit(&self) {
        assert!(
            self.hit.load(Ordering::Acquire),
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
            .is_some_and(|registered| Arc::ptr_eq(registered, &self.hit))
        {
            armed.remove(&self.target);
        }
    }
}

/// Arms a one-shot failure for one database and one persistence boundary.
pub(crate) fn arm(data_dir: &Path, boundary: PersistenceBoundary) -> ArmedFailure {
    let target = FailureTarget {
        data_dir: data_dir.to_path_buf(),
        boundary,
    };
    let hit = Arc::new(AtomicBool::new(false));
    let previous = ARMED.lock().insert(target.clone(), Arc::clone(&hit));
    assert!(previous.is_none(), "persistence boundary already armed");
    ArmedFailure { target, hit }
}

/// Returns an injected error exactly once when an armed boundary is reached.
pub(crate) fn check(data_dir: &Path, boundary: PersistenceBoundary) -> Result<()> {
    let target = FailureTarget {
        data_dir: data_dir.to_path_buf(),
        boundary,
    };
    let hit = ARMED.lock().remove(&target);
    if let Some(hit) = hit {
        hit.store(true, Ordering::Release);
        return Err(StorageError::Other(format!(
            "injected test failure at {} boundary",
            boundary.name()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{arm, PersistenceBoundary};
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

    #[tokio::test(flavor = "multi_thread")]
    async fn wal_boundary_stops_before_memtable_application_and_replays_on_reopen() {
        let temp = TempDir::new().unwrap();
        let engine = Engine::open(test_config(temp.path())).await.unwrap();
        let failure = arm(temp.path(), PersistenceBoundary::Wal);

        assert_injected(
            engine.insert(b"wal-key", b"wal-value").await,
            PersistenceBoundary::Wal,
        );
        failure.assert_hit();
        assert_eq!(engine.get(b"wal-key").await.unwrap(), None);
        drop(engine);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(
            reopened.get(b"wal-key").await.unwrap(),
            Some(b"wal-value".to_vec())
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

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.stats().sstable_count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn installed_manifest_survives_an_injected_post_install_failure() {
        let temp = insert_then_fail_flush(PersistenceBoundary::ManifestInstallation).await;

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.stats().sstable_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn installed_checkpoint_survives_before_wal_reclamation() {
        let temp = insert_then_fail_flush(PersistenceBoundary::Checkpoint).await;
        let manifest = Manifest::load_or_create(temp.path()).unwrap();
        assert_eq!(manifest.wal_checkpoint, 1);

        let reopened = Engine::open(test_config(temp.path())).await.unwrap();
        assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.stats().sstable_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compaction_boundary_keeps_installed_inputs_authoritative() {
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

        let failure = arm(temp.path(), PersistenceBoundary::Compaction);
        assert_injected(engine.compact().await, PersistenceBoundary::Compaction);
        failure.assert_hit();
        assert_eq!(engine.stats().l0_sstable_count, 4);
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
}
