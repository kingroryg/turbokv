//! Executable production contracts exercised through TurboKV's public API.

use std::process::Command;

use tempfile::TempDir;
use turbokv::{Db, DbError, DbOptions, WriteBatch};

const CRASH_WRITER_PATH: &str = "TURBOKV_CONTRACT_CRASH_WRITER_PATH";
const CRASH_WRITER_MODE: &str = "TURBOKV_CONTRACT_CRASH_WRITER_MODE";

fn durability_modes() -> [(&'static str, DbOptions); 3] {
    [
        ("fast", DbOptions::fast()),
        ("durable", DbOptions::durable()),
        ("paranoid", DbOptions::paranoid()),
    ]
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
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("crash_writer_process")
            .arg("--nocapture")
            .env(CRASH_WRITER_PATH, &database_path)
            .env(CRASH_WRITER_MODE, mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: crash writer failed: {}",
            String::from_utf8_lossy(&output.stderr)
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

        // Terminate without dropping Db, the runtime, the WAL, or any other
        // Rust value. The parent reopens the database and verifies recovery.
        std::process::exit(0);
    });
}
