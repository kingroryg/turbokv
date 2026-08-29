//! Executable production contracts exercised through TurboKV's public API.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use tempfile::TempDir;
use turbokv::storage::cache::BlockCache;
use turbokv::storage::sstable::{CompressionType, SSTableReader, SSTableWriter};
use turbokv::storage::wal::WriteAheadLog;
use turbokv::{
    CompactionConfig, Db, DbError, DbOptions, Engine, FdConfig, MemTableConfig, SSTableConfig,
    StorageConfig, StorageError, WalConfig, WriteBatch,
};

const CRASH_WRITER_PATH: &str = "TURBOKV_CONTRACT_CRASH_WRITER_PATH";
const CRASH_WRITER_MODE: &str = "TURBOKV_CONTRACT_CRASH_WRITER_MODE";
const CRASH_WRITER_READY: &str = "TURBOKV_CONTRACT_CRASH_WRITER_READY";
const LOCK_HOLDER_PATH: &str = "TURBOKV_CONTRACT_LOCK_HOLDER_PATH";
const LOCK_HOLDER_READY: &str = "TURBOKV_CONTRACT_LOCK_HOLDER_READY";

fn durability_modes() -> [(&'static str, DbOptions); 3] {
    [
        ("fast", DbOptions::fast()),
        ("durable", DbOptions::durable()),
        ("paranoid", DbOptions::paranoid()),
    ]
}

enum TakeRaceMutation {
    Insert(Vec<u8>),
    Remove,
    Batch(Vec<u8>),
}

async fn race_take_with_mutation(
    db: &Arc<Db>,
    key: &[u8],
    old_value: &[u8],
    mutation: TakeRaceMutation,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    db.insert(key, old_value).await.unwrap();

    let key = key.to_vec();
    let take_key = key.clone();
    let final_key = key.clone();
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let taker = {
        let db = Arc::clone(db);
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            db.take(take_key).await.unwrap()
        })
    };
    let writer = {
        let db = Arc::clone(db);
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            match mutation {
                TakeRaceMutation::Insert(value) => db.insert(key, value).await.unwrap(),
                TakeRaceMutation::Remove => db.remove(key).await.unwrap(),
                TakeRaceMutation::Batch(value) => {
                    let mut batch = WriteBatch::new();
                    batch.put(key, value);
                    db.write_batch(&batch).await.unwrap();
                }
            }
        })
    };
    start.wait().await;

    let taken = taker.await.unwrap();
    writer.await.unwrap();
    (taken, db.get(final_key).await.unwrap())
}

fn wait_until_ready(child: &mut Child, marker: &str, context: &str) {
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).unwrap();
        if bytes_read == 0 {
            let status = child.wait().unwrap();
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("{context}: child exited before readiness ({status}): {stderr}");
        }
        if line.contains(marker) {
            return;
        }
    }
}

fn expect_directory_locked(result: Result<Db, DbError>) -> std::path::PathBuf {
    match result {
        Err(DbError::DirectoryLocked { path }) => path,
        Err(error) => panic!("expected a directory-locked error, got: {error}"),
        Ok(_) => panic!("a second database unexpectedly acquired the directory"),
    }
}

#[test]
fn removed_dormant_surfaces_stay_absent_from_package_sources() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/optimizations.rs",
        "src/core/config.rs",
        "src/core/metrics.rs",
        "src/core/serialization.rs",
        "src/core/traits.rs",
        "src/core/utils.rs",
        "src/storage/buffer_pool.rs",
        "src/storage/direct_io.rs",
        "src/storage/partitioning.rs",
    ] {
        assert!(!root.join(relative).exists(), "{relative} was reintroduced");
    }

    let root_api = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
    for removed in [
        "DbConfig",
        "CompactionStyle",
        "crc32_checksum",
        "MemTableManager",
        "SSTableInfo",
        "WriteAheadLog",
    ] {
        assert!(
            !root_api.contains(removed),
            "removed root export {removed} was reintroduced"
        );
    }

    let storage_api = std::fs::read_to_string(root.join("src/storage/mod.rs")).unwrap();
    for removed in [
        "pub mod buffer_pool",
        "pub mod cached_time",
        "pub mod direct_io",
        "pub mod partitioning",
        "pub use buffer_pool",
        "pub use cache",
        "pub use compaction",
        "pub use direct_io",
    ] {
        assert!(
            !storage_api.contains(removed),
            "removed storage surface {removed} was reintroduced"
        );
    }

    let bloom = std::fs::read_to_string(root.join("src/storage/sstable/bloom.rs")).unwrap();
    assert!(!bloom.contains("PrefixBloomFilter"));
    let core_error = std::fs::read_to_string(root.join("src/core/error.rs")).unwrap();
    for removed in [
        "WriteAheadLog",
        "MemTable",
        "Compaction {",
        "IndexCorruption",
        "QueryError",
        "Configuration",
    ] {
        assert!(
            !core_error.contains(removed),
            "removed core error variant {removed} was reintroduced"
        );
    }
    let compaction = std::fs::read_to_string(root.join("src/storage/compaction.rs")).unwrap();
    for removed in [
        "pub struct Compactor",
        "pub struct CompactionJob",
        "pub fn pick_compaction",
        "pub fn execute",
        "pub fn cleanup_inputs",
    ] {
        assert!(!compaction.contains(removed), "{removed} was reintroduced");
    }
    let wal_config = std::fs::read_to_string(root.join("src/storage/wal/types.rs")).unwrap();
    for removed in ["pub compression: bool", "pub buffer_size: usize"] {
        assert!(!wal_config.contains(removed), "{removed} was reintroduced");
    }
    let sstable_config =
        std::fs::read_to_string(root.join("src/storage/sstable/types.rs")).unwrap();
    assert!(!sstable_config.contains("pub index_interval:"));

    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    for dependency in [
        "anyhow",
        "async-trait",
        "bincode",
        "chrono",
        "crossbeam-channel",
        "crossbeam-utils",
        "dashmap",
        "fjall",
        "quickcheck",
        "rayon",
        "rkyv",
        "rmp-serde",
        "serde_json",
        "uuid",
    ] {
        assert!(
            !manifest
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{dependency} ="))),
            "removed dependency {dependency} was reintroduced"
        );
    }
}

#[test]
fn direct_sstable_reader_writer_and_cache_form_a_supported_storage_seam() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("direct.sst");
    let mut writer = SSTableWriter::new(
        &path,
        SSTableConfig {
            block_size: 512,
            compression: CompressionType::Lz4,
            ..SSTableConfig::default()
        },
    )
    .unwrap();
    for index in 0_u32..64 {
        writer
            .add(
                format!("direct:{index:04}").as_bytes(),
                Some(&index.to_le_bytes()),
            )
            .unwrap();
    }
    writer.finish().unwrap();

    let cache = Arc::new(BlockCache::new(128 * 1024));
    let reader = SSTableReader::open_with_cache(&path, Arc::clone(&cache)).unwrap();
    assert_eq!(
        reader.get(b"direct:0032").unwrap(),
        Some(bytes::Bytes::copy_from_slice(&32_u32.to_le_bytes()))
    );
    let after_first_read = cache.stats();
    assert!(after_first_read.misses > 0);
    assert!(after_first_read.entries > 0);

    assert_eq!(
        reader.get(b"direct:0032").unwrap(),
        Some(bytes::Bytes::copy_from_slice(&32_u32.to_le_bytes()))
    );
    assert!(cache.stats().hits > after_first_read.hits);
    assert_eq!(reader.iter().count(), 64);
}

#[tokio::test]
async fn advanced_engine_configuration_runs_through_flush_compaction_and_recovery() {
    let directory = TempDir::new().unwrap();
    let database_path = directory.path().join("advanced-engine");
    let mut config = StorageConfig::durable(database_path.clone());
    config.wal_config = WalConfig {
        max_file_size: 512,
        ..WalConfig::durable()
    };
    config.memtable_config = MemTableConfig {
        max_size: 2 * 1024,
        max_entries: 8,
        ..MemTableConfig::default()
    };
    config.sstable_config = SSTableConfig {
        block_size: 512,
        ..SSTableConfig::default()
    };
    config.compaction_config = CompactionConfig {
        l0_compaction_trigger: 2,
        max_levels: 3,
        target_file_size: 4 * 1024,
        ..CompactionConfig::default()
    };
    config.fd_config = FdConfig {
        max_open_sstables: 8,
        partitions: 2,
        ..FdConfig::default()
    };

    let engine = Engine::open(config.clone()).await.unwrap();
    for index in 0_u32..32 {
        engine
            .insert(
                format!("advanced:{index:04}").as_bytes(),
                &index.to_le_bytes(),
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    let compaction = engine.compact().await.unwrap();
    assert!(compaction.is_complete());
    engine.shutdown().await.unwrap();
    drop(engine);

    let reopened = Engine::open(config).await.unwrap();
    for index in 0_u32..32 {
        assert_eq!(
            reopened
                .get(format!("advanced:{index:04}").as_bytes())
                .await
                .unwrap(),
            Some(index.to_le_bytes().to_vec())
        );
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_wal_constructor_appends_and_recovers_through_the_public_storage_seam() {
    let directory = TempDir::new().unwrap();
    let wal = WriteAheadLog::new(directory.path(), WalConfig::durable())
        .await
        .unwrap();

    assert_eq!(wal.append(b"direct", b"value").await.unwrap(), 0);
    wal.flush().await.unwrap();
    drop(wal);

    let reopened = WriteAheadLog::new(directory.path(), WalConfig::durable())
        .await
        .unwrap();
    let entries = reopened.read_from(0).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].decode_kv(),
        Some((&b"direct"[..], Some(&b"value"[..])))
    );
}

#[tokio::test]
async fn sync_writes_without_wal_is_rejected() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("invalid-options");
    let options = DbOptions {
        wal_enabled: false,
        sync_writes: true,
        ..DbOptions::default()
    };

    let result = Db::open_with_options(&database_path, options).await;

    assert!(matches!(result, Err(DbError::InvalidOptions(_))));
    assert!(!database_path.exists());
}

#[tokio::test]
async fn same_process_second_opener_fails_and_clean_close_releases_the_lock() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    let first = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();

    let locked_path =
        expect_directory_locked(Db::open_with_options(&database_path, DbOptions::fast()).await);
    assert_eq!(locked_path, database_path.canonicalize().unwrap());

    let second_error = match Db::open_with_options(&database_path, DbOptions::fast()).await {
        Err(error) => error,
        Ok(_) => panic!("a second database unexpectedly acquired the directory"),
    };
    assert!(second_error.to_string().contains("close or drop"));
    assert!(second_error
        .to_string()
        .contains("shared multi-writer access is unsupported"));

    first.close().await.unwrap();
    let reopened = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn engine_shutdown_retains_ownership_until_the_engine_is_dropped() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    let config = StorageConfig::fast(database_path.clone());
    let engine = Engine::open(config.clone()).await.unwrap();

    engine.shutdown().await.unwrap();
    let error = match Engine::open(config.clone()).await {
        Err(error) => error,
        Ok(_) => panic!("shutdown unexpectedly released a live engine's directory lock"),
    };
    assert!(matches!(error, StorageError::DirectoryLocked { .. }));

    drop(engine);
    let reopened = Engine::open(config).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_a_database_releases_its_directory_lock() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    let db = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();

    drop(db);

    let reopened = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn a_partial_open_failure_releases_the_directory_lock() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    std::fs::create_dir(&database_path).unwrap();
    let invalid_sstable_path = database_path.join("sstables");
    std::fs::write(&invalid_sstable_path, b"not a directory").unwrap();

    let error = match Db::open_with_options(&database_path, DbOptions::fast()).await {
        Err(error) => error,
        Ok(_) => panic!("database unexpectedly opened with a file at its SSTable path"),
    };
    assert!(matches!(error, DbError::Storage(StorageError::Io(_))));
    assert!(database_path.join(".turbokv.lock").is_file());

    std::fs::remove_file(invalid_sstable_path).unwrap();
    let reopened = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn lock_io_failures_are_distinct_from_lock_contention() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    std::fs::create_dir(&database_path).unwrap();
    std::fs::create_dir(database_path.join(".turbokv.lock")).unwrap();

    let error = match Db::open_with_options(&database_path, DbOptions::fast()).await {
        Err(error) => error,
        Ok(_) => panic!("database unexpectedly opened with a directory at its lock-file path"),
    };
    assert!(matches!(
        error,
        DbError::Storage(StorageError::DirectoryLockIo { .. })
    ));
    assert!(!database_path.join("wal").exists());
    assert!(!database_path.join("sstables").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_path_aliases_share_one_same_process_lock() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("database");
    let alias_path = temp.path().join("database-alias");
    let first = Db::open_with_options(&database_path, DbOptions::fast())
        .await
        .unwrap();
    symlink(&database_path, &alias_path).unwrap();

    let locked_path =
        expect_directory_locked(Db::open_with_options(&alias_path, DbOptions::fast()).await);
    assert_eq!(locked_path, database_path.canonicalize().unwrap());

    first.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn retargeting_an_opened_symlink_cannot_redirect_database_writes() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let owned_path = temp.path().join("owned");
    let other_path = temp.path().join("other");
    let alias_path = temp.path().join("database-alias");
    std::fs::create_dir(&owned_path).unwrap();
    std::fs::create_dir(&other_path).unwrap();
    symlink(&owned_path, &alias_path).unwrap();

    let db = Db::open_with_options(&alias_path, DbOptions::fast())
        .await
        .unwrap();
    std::fs::remove_file(&alias_path).unwrap();
    symlink(&other_path, &alias_path).unwrap();

    db.insert(b"key", b"value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    assert!(owned_path.join("MANIFEST").is_file());
    assert!(!other_path.join("MANIFEST").exists());
    assert!(!other_path.join("sstables").exists());
}

#[tokio::test]
async fn another_process_cannot_open_the_directory_until_its_owner_terminates() {
    let temp = TempDir::new().unwrap();

    for iteration in 0..3 {
        let database_path = temp.path().join(format!("database-{iteration}"));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("directory_lock_holder_process")
            .arg("--nocapture")
            .env(LOCK_HOLDER_PATH, &database_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        wait_until_ready(&mut child, LOCK_HOLDER_READY, "directory-lock holder");
        let locked_path =
            expect_directory_locked(Db::open_with_options(&database_path, DbOptions::fast()).await);
        assert_eq!(locked_path, database_path.canonicalize().unwrap());

        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "lock holder must be killed by its parent"
        );

        let reopened = Db::open_with_options(&database_path, DbOptions::fast())
            .await
            .unwrap();
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn clean_close_persists_fast_mode_writes() {
    let temp = TempDir::new().unwrap();

    let db = Db::open_with_options(temp.path(), DbOptions::fast())
        .await
        .unwrap();
    db.insert(b"session", b"complete").await.unwrap();
    db.close().await.unwrap();

    let reopened = Db::open_with_options(temp.path(), DbOptions::fast())
        .await
        .unwrap();
    assert_eq!(
        reopened.get(b"session").await.unwrap(),
        Some(b"complete".to_vec())
    );
}

#[tokio::test]
async fn acknowledged_mutations_are_immediately_visible_in_every_mode() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in durability_modes() {
        let database_path = temp.path().join(mode);
        let db = Db::open_with_options(&database_path, options)
            .await
            .unwrap();

        let binary_key = [0x00, 0xff, 0x10, 0x80];
        let binary_value = [0xff, 0x00, 0xfe, 0x80];
        db.insert([], binary_value).await.unwrap();
        assert_eq!(
            db.get([]).await.unwrap(),
            Some(binary_value.to_vec()),
            "{mode}: keys and values must not require UTF-8"
        );

        db.insert(binary_key, []).await.unwrap();
        assert_eq!(
            db.get(binary_key).await.unwrap(),
            Some(Vec::new()),
            "{mode}: an empty value must remain distinct from a tombstone"
        );
        assert!(
            db.contains_key(binary_key).await.unwrap(),
            "{mode}: an acknowledged insert must be visible"
        );

        db.insert_many([
            (b"bulk:a".as_slice(), b"one".as_slice()),
            (b"bulk:a".as_slice(), b"last".as_slice()),
            (b"bulk:b".as_slice(), b"two".as_slice()),
        ])
        .await
        .unwrap();
        assert_eq!(
            db.scan_prefix(b"bulk:").await.unwrap(),
            vec![
                (b"bulk:a".to_vec(), b"last".to_vec()),
                (b"bulk:b".to_vec(), b"two".to_vec()),
            ],
            "{mode}: acknowledged bulk writes must be visible in key order, with the last duplicate winning"
        );

        let mut batch = WriteBatch::new();
        batch.put(b"batch:kept", b"present");
        batch.put(b"bulk:a", b"replaced");
        batch.delete(b"bulk:b");
        db.write_batch(&batch).await.unwrap();

        assert_eq!(
            db.get(b"batch:kept").await.unwrap(),
            Some(b"present".to_vec()),
            "{mode}: a successful batch must expose its puts"
        );
        assert_eq!(
            db.get(b"bulk:a").await.unwrap(),
            Some(b"replaced".to_vec()),
            "{mode}: batch operations must be applied in order"
        );
        assert_eq!(
            db.get(b"bulk:b").await.unwrap(),
            None,
            "{mode}: a successful batch must expose its deletes"
        );

        db.remove(b"missing-key").await.unwrap();
        db.remove(binary_key).await.unwrap();
        assert_eq!(
            db.get(binary_key).await.unwrap(),
            None,
            "{mode}: an acknowledged delete must be visible"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_take_returns_and_removes_each_value_exactly_once_in_every_mode() {
    const CALLERS: usize = 16;
    let temp = TempDir::new().unwrap();

    for (mode, options) in durability_modes() {
        let database_path = temp.path().join(mode);
        let db = Arc::new(
            Db::open_with_options(&database_path, options)
                .await
                .unwrap(),
        );

        assert_eq!(db.take(b"missing").await.unwrap(), None, "{mode}");

        db.insert(b"empty", []).await.unwrap();
        assert_eq!(
            db.take(b"empty").await.unwrap(),
            Some(Vec::new()),
            "{mode}: an empty value must remain distinct from absence"
        );
        assert_eq!(db.take(b"empty").await.unwrap(), None, "{mode}");

        db.insert(b"raced", b"one winner").await.unwrap();
        let start = Arc::new(tokio::sync::Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let db = Arc::clone(&db);
            let start = Arc::clone(&start);
            callers.push(tokio::spawn(async move {
                start.wait().await;
                db.take(b"raced").await.unwrap()
            }));
        }
        start.wait().await;

        let mut returned = Vec::new();
        for caller in callers {
            if let Some(value) = caller.await.unwrap() {
                returned.push(value);
            }
        }
        assert_eq!(returned, [b"one winner".to_vec()], "{mode}");
        assert_eq!(db.get(b"raced").await.unwrap(), None, "{mode}");

        db.insert(b"sstable", b"persisted value").await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(
            db.take(b"sstable").await.unwrap(),
            Some(b"persisted value".to_vec()),
            "{mode}: take must resolve values outside the active memtable"
        );
        assert_eq!(db.get(b"sstable").await.unwrap(), None, "{mode}");

        Arc::try_unwrap(db)
            .unwrap_or_else(|_| panic!("{mode}: all take callers must release the database"))
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_take_orders_wholly_before_or_after_an_atomic_batch() {
    let temp = TempDir::new().unwrap();
    for (mode, options) in durability_modes() {
        let db = Arc::new(
            Db::open_with_options(temp.path().join(mode), options)
                .await
                .unwrap(),
        );

        for iteration in 0..8 {
            let old = format!("old:{iteration}").into_bytes();
            let new = format!("new:{iteration}").into_bytes();
            let (taken, final_value) = race_take_with_mutation(
                &db,
                b"ordered",
                &old,
                TakeRaceMutation::Batch(new.clone()),
            )
            .await;
            assert!(
                (taken == Some(old.clone()) && final_value == Some(new.clone()))
                    || (taken == Some(new) && final_value.is_none()),
                "{mode} iteration {iteration}: take and batch exposed a partial ordering: taken={taken:?}, final={final_value:?}"
            );
        }
        Arc::try_unwrap(db)
            .unwrap_or_else(|_| panic!("{mode}: batch race retained the database"))
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_take_orders_wholly_before_or_after_insert_and_remove() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in durability_modes() {
        let db = Arc::new(
            Db::open_with_options(temp.path().join(mode), options)
                .await
                .unwrap(),
        );
        for iteration in 0..8 {
            let old = format!("old:{iteration}").into_bytes();
            let new = format!("new:{iteration}").into_bytes();
            let (taken, final_value) = race_take_with_mutation(
                &db,
                b"insert-race",
                &old,
                TakeRaceMutation::Insert(new.clone()),
            )
            .await;
            assert!(
                (taken == Some(old.clone()) && final_value == Some(new.clone()))
                    || (taken == Some(new) && final_value.is_none()),
                "{mode} iteration {iteration}: take and insert exposed a partial ordering: taken={taken:?}, final={final_value:?}"
            );
            let (taken, final_value) =
                race_take_with_mutation(&db, b"remove-race", &old, TakeRaceMutation::Remove).await;
            assert!(
                taken == Some(old.clone()) || taken.is_none(),
                "{mode} iteration {iteration}: take returned an impossible value: {taken:?}"
            );
            assert_eq!(final_value, None, "{mode}");
        }
        Arc::try_unwrap(db)
            .unwrap_or_else(|_| panic!("{mode}: mutation race retained the database"))
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn wal_backed_take_recovers_its_tombstone_after_unclean_drop() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in [
        ("durable", DbOptions::durable()),
        ("paranoid", DbOptions::paranoid()),
    ] {
        let database_path = temp.path().join(mode);
        let db = Db::open_with_options(&database_path, options.clone())
            .await
            .unwrap();
        db.insert(b"taken", b"before").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let reopened = Db::open_with_options(&database_path, options.clone())
            .await
            .unwrap();
        assert_eq!(
            reopened.take(b"taken").await.unwrap(),
            Some(b"before".to_vec())
        );
        drop(reopened);

        let recovered = Db::open_with_options(&database_path, options)
            .await
            .unwrap();
        assert_eq!(recovered.get(b"taken").await.unwrap(), None, "{mode}");
        assert_eq!(recovered.take(b"taken").await.unwrap(), None, "{mode}");
        recovered.close().await.unwrap();
    }
}

#[tokio::test]
async fn scans_are_lexicographic_with_inclusive_start_and_exclusive_end() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in durability_modes() {
        let database_path = temp.path().join(mode);
        let db = Db::open_with_options(&database_path, options)
            .await
            .unwrap();

        db.insert_many([
            (b"scan:\x00".as_slice(), b"zero".as_slice()),
            (b"scan:\x7f".as_slice(), b"middle".as_slice()),
            (b"scan:\xff".as_slice(), b"end".as_slice()),
            (b"other".as_slice(), b"outside".as_slice()),
        ])
        .await
        .unwrap();

        assert_eq!(
            db.range(b"scan:\x00", b"scan:\xff").await.unwrap(),
            vec![
                (b"scan:\x00".to_vec(), b"zero".to_vec()),
                (b"scan:\x7f".to_vec(), b"middle".to_vec()),
            ],
            "{mode}: range bounds must be [start, end)"
        );
        assert_eq!(
            db.scan_prefix(b"scan:").await.unwrap(),
            vec![
                (b"scan:\x00".to_vec(), b"zero".to_vec()),
                (b"scan:\x7f".to_vec(), b"middle".to_vec()),
                (b"scan:\xff".to_vec(), b"end".to_vec()),
            ],
            "{mode}: prefix scans must accept arbitrary binary suffixes"
        );
    }
}

#[tokio::test]
async fn flush_and_clean_close_persist_acknowledged_mutations_in_every_mode() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in durability_modes() {
        let database_path = temp.path().join(mode);
        let db = Db::open_with_options(&database_path, options.clone())
            .await
            .unwrap();

        db.insert(b"single", b"value").await.unwrap();
        db.insert_many([(b"bulk".as_slice(), b"value".as_slice())])
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"batch", b"value");
        batch.put(b"deleted", b"temporary");
        batch.delete(b"deleted");
        db.write_batch(&batch).await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let reopened = Db::open_with_options(&database_path, options)
            .await
            .unwrap();
        assert_eq!(
            reopened.get(b"single").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: a flushed insert must survive reopen"
        );
        assert_eq!(
            reopened.get(b"bulk").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: a flushed bulk insert must survive reopen"
        );
        assert_eq!(
            reopened.get(b"batch").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: a flushed batch put must survive reopen"
        );
        assert_eq!(
            reopened.get(b"deleted").await.unwrap(),
            None,
            "{mode}: a flushed batch delete must survive reopen"
        );
    }
}

#[tokio::test]
async fn wal_modes_recover_after_writer_process_exits_without_destructors() {
    let temp = TempDir::new().unwrap();

    for (mode, options) in [
        ("durable", DbOptions::durable()),
        ("paranoid", DbOptions::paranoid()),
    ] {
        let database_path = temp.path().join(mode);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("crash_writer_process")
            .arg("--nocapture")
            .env(CRASH_WRITER_PATH, &database_path)
            .env(CRASH_WRITER_MODE, mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        wait_until_ready(&mut child, CRASH_WRITER_READY, mode);

        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "{mode}: crash writer must be terminated by the parent"
        );

        let recovered = Db::open_with_options(&database_path, options)
            .await
            .unwrap();
        assert_eq!(
            recovered.get(b"wal:single").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: acknowledged inserts must be replayed from the WAL"
        );
        assert_eq!(
            recovered.get(b"wal:bulk").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: acknowledged bulk inserts must be replayed from the WAL"
        );
        assert_eq!(
            recovered.get(b"wal:batch").await.unwrap(),
            Some(b"value".to_vec()),
            "{mode}: acknowledged batch puts must be replayed from the WAL"
        );
        assert_eq!(
            recovered.get(b"wal:deleted").await.unwrap(),
            None,
            "{mode}: acknowledged batch deletes must be replayed from the WAL"
        );
    }
}

#[test]
fn directory_lock_holder_process() {
    let Some(database_path) = std::env::var_os(LOCK_HOLDER_PATH) else {
        return;
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let _db = Db::open_with_options(database_path, DbOptions::fast())
            .await
            .unwrap();
        println!("{LOCK_HOLDER_READY}");
        std::io::stdout().flush().unwrap();

        loop {
            std::thread::park();
        }
    });
}

#[test]
fn crash_writer_process() {
    let Some(database_path) = std::env::var_os(CRASH_WRITER_PATH) else {
        return;
    };
    let mode = std::env::var(CRASH_WRITER_MODE).unwrap();
    let options = match mode.as_str() {
        "durable" => DbOptions::durable(),
        "paranoid" => DbOptions::paranoid(),
        other => panic!("unsupported crash-writer mode: {other}"),
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let db = Db::open_with_options(database_path, options).await.unwrap();
        db.insert(b"wal:single", b"value").await.unwrap();
        db.insert_many([(b"wal:bulk".as_slice(), b"value".as_slice())])
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"wal:batch", b"value");
        batch.delete(b"wal:deleted");
        db.write_batch(&batch).await.unwrap();

        println!("{CRASH_WRITER_READY}");
        std::io::stdout().flush().unwrap();

        // Wait for the parent to terminate the process. No Db, runtime, WAL, or
        // other Rust value gets a graceful drop before recovery is verified.
        loop {
            std::thread::park();
        }
    });
}
