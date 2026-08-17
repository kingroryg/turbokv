//! Executable production contracts exercised through TurboKV's public API.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tempfile::TempDir;
use turbokv::storage::compaction::{CompactionConfig, CompactionJob};
use turbokv::storage::manifest::SSTableManifestEntry;
use turbokv::storage::{Compactor, SSTableConfig, SSTableReader, SSTableWriter};
use turbokv::{Db, DbError, DbOptions, Engine, StorageConfig, StorageError, WriteBatch};

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
#[allow(deprecated)]
fn low_level_compactor_keeps_its_legacy_single_output_contract() {
    let directory = TempDir::new().unwrap();
    let sstable_directory = directory.path().join("sstables");
    std::fs::create_dir_all(&sstable_directory).unwrap();
    let config = SSTableConfig::default();
    let input_path = sstable_directory.join("input.sst");
    let mut writer = SSTableWriter::new(&input_path, config.clone()).unwrap();
    for index in 0_u64..32 {
        let value = [index as u8; 256];
        writer
            .add(format!("key-{index:04}").as_bytes(), Some(&value))
            .unwrap();
    }
    let input_info = writer.finish().unwrap();
    let input = SSTableManifestEntry {
        id: 1,
        level: 0,
        path: input_info.path,
        size: input_info.file_size,
        entry_count: input_info.entry_count,
        tombstone_count: input_info.tombstone_count,
        min_key: input_info.min_key,
        max_key: input_info.max_key,
        min_sequence: input_info.min_sequence,
        max_sequence: input_info.max_sequence,
        creation_time: input_info.creation_time,
    };
    let compactor = Compactor::new(
        CompactionConfig {
            target_file_size: 1,
            ..CompactionConfig::default()
        },
        config,
        directory.path().to_path_buf(),
        Arc::new(AtomicU64::new(2)),
    );
    let selectable = (0_u64..4)
        .map(|index| SSTableManifestEntry {
            id: index + 10,
            creation_time: index,
            ..input.clone()
        })
        .collect::<Vec<_>>();
    let picked = compactor.pick_compaction(&selectable).unwrap();
    assert_eq!(picked.input_sstables.len(), 4);
    assert_eq!(picked.output_level, 1);
    assert_eq!(
        picked.output_path.parent(),
        Some(sstable_directory.as_path())
    );
    assert!(picked
        .output_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("2_"));
    assert!(!picked.output_path.exists());

    let output_path = sstable_directory.join("42_legacy.sst");
    std::fs::write(&output_path, b"replace this existing caller-owned file").unwrap();

    let result = compactor
        .execute(CompactionJob {
            input_sstables: vec![input],
            output_level: 1,
            output_path: output_path.clone(),
        })
        .unwrap();

    let output = result.output_sstable.unwrap();
    assert_eq!(output.id, 42);
    assert_eq!(output.path, output_path);
    assert_eq!(output.entry_count, 32);
    assert_eq!(result.bytes_written, output.size);
    assert!(SSTableReader::open(output.path).is_ok());

    let empty_path = sstable_directory.join("43_empty.sst");
    let empty = compactor
        .execute(CompactionJob {
            input_sstables: Vec::new(),
            output_level: 1,
            output_path: empty_path.clone(),
        })
        .unwrap()
        .output_sstable
        .unwrap();
    assert_eq!(empty.id, 43);
    assert_eq!(empty.path, empty_path);
    assert_eq!(empty.entry_count, 0);
    assert!(SSTableReader::open(empty.path).is_ok());
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
