//! Crash-recovery and correctness integration tests for TurboKV.
//!
//! These tests exercise the database through the public `Db` API,
//! covering tombstone semantics, flush/reopen durability, batch
//! writes, range/prefix scans with deletions, compaction, and stats.

use std::sync::Arc;
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

// ---------------------------------------------------------------------------
// 9. Overwrite key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_overwrite_key() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        db.insert(b"foo", b"bar").await.unwrap();
        assert_eq!(db.get(b"foo").await.unwrap(), Some(b"bar".to_vec()));

        // Overwrite with new value
        db.insert(b"foo", b"baz").await.unwrap();
        assert_eq!(db.get(b"foo").await.unwrap(), Some(b"baz".to_vec()));

        db.flush().await.unwrap();
    }

    // Reopen and verify the overwritten value persisted
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        assert_eq!(db.get(b"foo").await.unwrap(), Some(b"baz".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// 10. Large values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_large_values() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    // 1 MB value filled with a repeating pattern
    let large_value: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();

    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        db.insert(b"big", &large_value).await.unwrap();
        assert_eq!(db.get(b"big").await.unwrap(), Some(large_value.clone()));

        db.flush().await.unwrap();
    }

    // Reopen and verify the large value survived
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        assert_eq!(db.get(b"big").await.unwrap(), Some(large_value));
    }
}

// ---------------------------------------------------------------------------
// 11. Many keys sequential
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_many_keys_sequential() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Insert 1000 keys
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        db.insert(key.as_bytes(), val.as_bytes()).await.unwrap();
    }

    // Verify all 1000 can be retrieved
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        assert_eq!(
            db.get(key.as_bytes()).await.unwrap(),
            Some(val.into_bytes()),
            "Key {} should be retrievable",
            i,
        );
    }

    // Range scan the entire space and verify count
    let results = db.range(b"key_0000", b"key_9999").await.unwrap();
    assert_eq!(
        results.len(),
        1000,
        "Range scan should return all 1000 keys",
    );
}

// ---------------------------------------------------------------------------
// 12. Delete nonexistent key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delete_nonexistent_key() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Delete a key that was never inserted — should not error
    db.remove(b"never_existed").await.unwrap();

    // Verify get returns None
    assert_eq!(db.get(b"never_existed").await.unwrap(), None);
}

// ---------------------------------------------------------------------------
// 13. Contains key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_contains_key() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"exists", b"value").await.unwrap();

    assert!(
        db.contains_key(b"exists").await.unwrap(),
        "contains_key should return true for inserted key",
    );
    assert!(
        !db.contains_key(b"missing").await.unwrap(),
        "contains_key should return false for absent key",
    );

    // Delete and verify contains_key returns false
    db.remove(b"exists").await.unwrap();
    assert!(
        !db.contains_key(b"exists").await.unwrap(),
        "contains_key should return false after deletion",
    );
}

// ---------------------------------------------------------------------------
// 14. Multiple flushes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_flushes() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Batch 1
    for i in 1..=10 {
        let key = format!("a{}", i);
        let val = format!("aval{}", i);
        db.insert(key.as_bytes(), val.as_bytes()).await.unwrap();
    }
    db.flush().await.unwrap();

    // Batch 2
    for i in 1..=10 {
        let key = format!("b{}", i);
        let val = format!("bval{}", i);
        db.insert(key.as_bytes(), val.as_bytes()).await.unwrap();
    }
    db.flush().await.unwrap();

    // Verify all 20 keys
    for i in 1..=10 {
        let akey = format!("a{}", i);
        let aval = format!("aval{}", i);
        assert_eq!(
            db.get(akey.as_bytes()).await.unwrap(),
            Some(aval.into_bytes()),
        );
        let bkey = format!("b{}", i);
        let bval = format!("bval{}", i);
        assert_eq!(
            db.get(bkey.as_bytes()).await.unwrap(),
            Some(bval.into_bytes()),
        );
    }

    // Range scan all keys and verify 20 results
    // Keys are: a1..a10, a2..a9 sorted lexicographically, then b1..b10
    // Use a range that covers everything
    let results = db.range(b"a", b"c").await.unwrap();
    assert_eq!(
        results.len(),
        20,
        "Range scan should return all 20 keys from both batches",
    );
}

// ---------------------------------------------------------------------------
// 15. Scan empty database
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scan_empty_db() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    let range_results = db.range(b"a", b"z").await.unwrap();
    assert!(range_results.is_empty(), "Range scan on empty DB should return empty vec");

    let prefix_results = db.scan_prefix(b"anything").await.unwrap();
    assert!(prefix_results.is_empty(), "Prefix scan on empty DB should return empty vec");
}

// ---------------------------------------------------------------------------
// 16. Reopen preserves deletes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reopen_preserves_deletes() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        db.insert(b"x", b"1").await.unwrap();
        db.insert(b"y", b"2").await.unwrap();
        db.insert(b"z", b"3").await.unwrap();

        db.remove(b"y").await.unwrap();

        db.flush().await.unwrap();
        // db is dropped here
    }

    // Reopen and verify
    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();

        assert_eq!(db.get(b"x").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"y").await.unwrap(), None, "Deleted key y should remain deleted after reopen");
        assert_eq!(db.get(b"z").await.unwrap(), Some(b"3".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// 17. Batch mixed operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_mixed_operations() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    // Pre-insert key "c" so we can delete it in the batch
    db.insert(b"c", b"pre_existing").await.unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1");
    batch.put(b"b", b"2");
    batch.delete(b"c");
    batch.put(b"d", b"4");
    db.write_batch(&batch).await.unwrap();

    assert_eq!(db.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").await.unwrap(), None, "Batch-deleted key should be None");
    assert_eq!(db.get(b"d").await.unwrap(), Some(b"4".to_vec()));
}

// ---------------------------------------------------------------------------
// 18. Concurrent reads and writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_reads_and_writes() {
    let tmp = TempDir::new().unwrap();
    let db = Arc::new(
        Db::open_with_options(tmp.path(), DbOptions::durable())
            .await
            .unwrap(),
    );

    let mut handles = Vec::new();

    // Spawn 4 tasks, each writing 100 keys with a unique prefix
    for task_id in 0..4u32 {
        let db = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            for i in 0..100u32 {
                let key = format!("t{}:key_{:04}", task_id, i);
                let val = format!("t{}:val_{:04}", task_id, i);
                db.insert(key.as_bytes(), val.as_bytes()).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all 400 keys are present
    for task_id in 0..4u32 {
        for i in 0..100u32 {
            let key = format!("t{}:key_{:04}", task_id, i);
            let val = format!("t{}:val_{:04}", task_id, i);
            assert_eq!(
                db.get(key.as_bytes()).await.unwrap(),
                Some(val.into_bytes()),
                "Key {} should exist after concurrent writes",
                key,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 19. Stats basic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_basic() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"stat_a", b"value_a").await.unwrap();
    db.insert(b"stat_b", b"value_b").await.unwrap();
    db.insert(b"stat_c", b"value_c").await.unwrap();

    let stats = db.stats();
    assert!(
        stats.total_keys > 0,
        "total_keys should be > 0 after inserting data, got {}",
        stats.total_keys,
    );
    assert!(
        stats.memtable_size > 0,
        "memtable_size should be > 0 after inserting data, got {}",
        stats.memtable_size,
    );
}

// ---------------------------------------------------------------------------
// 20. Fast mode basic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fast_mode_basic() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::fast())
        .await
        .unwrap();

    // Insert
    db.insert(b"fk1", b"fv1").await.unwrap();
    db.insert(b"fk2", b"fv2").await.unwrap();
    db.insert(b"fk3", b"fv3").await.unwrap();

    // Get
    assert_eq!(db.get(b"fk1").await.unwrap(), Some(b"fv1".to_vec()));
    assert_eq!(db.get(b"fk2").await.unwrap(), Some(b"fv2".to_vec()));
    assert_eq!(db.get(b"fk3").await.unwrap(), Some(b"fv3".to_vec()));

    // Delete
    db.remove(b"fk2").await.unwrap();
    assert_eq!(db.get(b"fk2").await.unwrap(), None);

    // Range scan remaining keys
    let results = db.range(b"fk1", b"fk9").await.unwrap();
    assert_eq!(results.len(), 2);
    let keys: Vec<Vec<u8>> = results.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"fk1".to_vec(), b"fk3".to_vec()]);
}
