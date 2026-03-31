//! Crash-recovery and correctness integration tests for TurboKV.
//!
//! These tests exercise the database through the public `Db` API,
//! covering tombstone semantics, flush/reopen durability, batch
//! writes, range/prefix scans with deletions, compaction, and stats.

use tempfile::TempDir;
use turbokv::{Db, DbOptions, WriteBatch};

// ---------------------------------------------------------------------------
// 1. Tombstone point-get
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tombstone_point_get() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Insert and verify
    db.insert(b"key1", b"value1").await.unwrap();
    assert_eq!(db.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));

    // Delete the key
    db.remove(b"key1").await.unwrap();

    // Flush so the tombstone lands in an SSTable
    db.flush().await.unwrap();

    // get() must return None — not Some(vec![])
    assert_eq!(db.get(b"key1").await.unwrap(), None);
}

// ---------------------------------------------------------------------------
// 2. Empty-value handling
// ---------------------------------------------------------------------------

/// Known limitation: empty values are treated as tombstones at the SSTable
/// level because SSTable readers interpret `value.is_empty()` as a delete
/// marker.  At the memtable level (before any flush) the empty value is
/// distinguishable from a tombstone.
#[tokio::test]
async fn test_empty_value_handling() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Before flush: empty value is returned as-is from the memtable.
    db.insert(b"key_empty", b"").await.unwrap();
    assert_eq!(
        db.get(b"key_empty").await.unwrap(),
        Some(b"".to_vec()),
        "Empty value should be returned from the memtable layer"
    );

    // After flush: the SSTable treats the empty value as a tombstone.
    db.flush().await.unwrap();
    assert_eq!(
        db.get(b"key_empty").await.unwrap(),
        None,
        "Known limitation: empty values become tombstones after flush to SSTable"
    );
}

// ---------------------------------------------------------------------------
// 3. Flush and reopen
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_flush_and_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: write data and flush
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        db.insert(b"alpha", b"1").await.unwrap();
        db.insert(b"beta", b"2").await.unwrap();
        db.insert(b"gamma", b"3").await.unwrap();
        db.insert(b"delta", b"4").await.unwrap();

        db.flush().await.unwrap();
        // db is dropped here
    }

    // Phase 2: reopen and verify
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        assert_eq!(db.get(b"alpha").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"beta").await.unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"gamma").await.unwrap(), Some(b"3".to_vec()));
        assert_eq!(db.get(b"delta").await.unwrap(), Some(b"4".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// 4. Batch write recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_write_recovery() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // Phase 1: batch write, flush, drop
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(b"batch_a", b"val_a");
        batch.put(b"batch_b", b"val_b");
        batch.put(b"batch_c", b"val_c");
        db.write_batch(&batch).await.unwrap();

        db.flush().await.unwrap();
    }

    // Phase 2: reopen and verify the batch survived
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        assert_eq!(db.get(b"batch_a").await.unwrap(), Some(b"val_a".to_vec()));
        assert_eq!(db.get(b"batch_b").await.unwrap(), Some(b"val_b".to_vec()));
        assert_eq!(db.get(b"batch_c").await.unwrap(), Some(b"val_c".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// 5. Range scan with tombstones
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_range_scan_with_tombstones() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"a", b"1").await.unwrap();
    db.insert(b"b", b"2").await.unwrap();
    db.insert(b"c", b"3").await.unwrap();
    db.insert(b"d", b"4").await.unwrap();

    // Delete "b"
    db.remove(b"b").await.unwrap();

    // Range [a, e) — should skip the deleted key
    let results = db.range(b"a", b"e").await.unwrap();

    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec(), b"d".to_vec()]);
}

// ---------------------------------------------------------------------------
// 6. Prefix scan with tombstones
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_prefix_scan_with_tombstones() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"user:1", b"alice").await.unwrap();
    db.insert(b"user:2", b"bob").await.unwrap();
    db.insert(b"user:3", b"charlie").await.unwrap();

    // Delete user:2
    db.remove(b"user:2").await.unwrap();

    let results = db.scan_prefix(b"user:").await.unwrap();

    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"user:1".to_vec(), b"user:3".to_vec()]);
}

// ---------------------------------------------------------------------------
// 7. Compaction preserves data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compaction_preserves_data() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Insert enough keys across multiple flushes to create several SSTables.
    let total_keys = 200;
    for i in 0..total_keys {
        let key = format!("ckey:{:04}", i);
        let val = format!("cval:{:04}", i);
        db.insert(key.as_bytes(), val.as_bytes()).await.unwrap();

        // Flush every 50 keys to create multiple SSTables
        if (i + 1) % 50 == 0 {
            db.flush().await.unwrap();
        }
    }

    // Delete a handful of keys before compaction
    let deleted_keys: Vec<usize> = vec![10, 50, 100, 150];
    for &i in &deleted_keys {
        let key = format!("ckey:{:04}", i);
        db.remove(key.as_bytes()).await.unwrap();
    }
    db.flush().await.unwrap();

    // Trigger compaction
    db.compact().await.unwrap();

    // Verify all non-deleted keys still readable
    for i in 0..total_keys {
        let key = format!("ckey:{:04}", i);
        let expected_val = format!("cval:{:04}", i);
        let result = db.get(key.as_bytes()).await.unwrap();

        if deleted_keys.contains(&i) {
            assert_eq!(result, None, "Deleted key {} should be None", i);
        } else {
            assert_eq!(
                result,
                Some(expected_val.into_bytes()),
                "Key {} should still be readable after compaction",
                i
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 8. WAL size in stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_wal_size() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Insert some data so the WAL has content
    db.insert(b"stat_key_1", b"stat_val_1").await.unwrap();
    db.insert(b"stat_key_2", b"stat_val_2").await.unwrap();
    db.insert(b"stat_key_3", b"stat_val_3").await.unwrap();

    let stats = db.stats();
    assert!(
        stats.wal_size > 0,
        "WAL size should be > 0 after inserting data in durable mode, got {}",
        stats.wal_size
    );
}
