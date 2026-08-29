//! Crash-recovery and correctness integration tests for TurboKV.
//!
//! These tests exercise the database through the public `Db` API,
//! covering tombstone and atomic-take semantics, flush/reopen durability, batch
//! writes, range/prefix scans with deletions, compaction, and stats.

use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;
use turbokv::{Db, DbOptions, WriteBatch};

fn seeded_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

async fn run_seeded_mutation_mix(options: DbOptions, mut seed: u64) {
    let tmp = TempDir::new().unwrap();
    let mut expected = BTreeMap::new();
    let db = Db::open_with_options(tmp.path(), options.clone())
        .await
        .unwrap();

    // Deterministically cross every fast-buffer boundary with every other
    // mutation/read/flush entry point before the randomized mix.
    db.insert(b"mix:00", b"buffered-before-delete")
        .await
        .unwrap();
    db.remove(b"mix:00").await.unwrap();
    db.insert(b"mix:01", b"buffered-before-bulk").await.unwrap();
    db.insert_many([(b"mix:01".as_slice(), b"bulk".as_slice())])
        .await
        .unwrap();
    expected.insert(b"mix:01".to_vec(), b"bulk".to_vec());
    db.insert(b"mix:02", b"buffered-before-batch")
        .await
        .unwrap();
    let mut opening_batch = WriteBatch::new();
    opening_batch.delete(b"mix:02");
    opening_batch.put(b"mix:03", b"batch");
    db.write_batch(&opening_batch).await.unwrap();
    expected.insert(b"mix:03".to_vec(), b"batch".to_vec());
    db.insert(b"mix:04", b"buffered-before-read").await.unwrap();
    expected.insert(b"mix:04".to_vec(), b"buffered-before-read".to_vec());
    assert_eq!(
        db.get(b"mix:04").await.unwrap(),
        expected.get(b"mix:04".as_slice()).cloned()
    );
    db.insert(b"mix:05", b"buffered-before-flush")
        .await
        .unwrap();
    expected.insert(b"mix:05".to_vec(), b"buffered-before-flush".to_vec());
    db.flush().await.unwrap();
    db.insert(b"mix:06", b"buffered-before-take").await.unwrap();
    assert_eq!(
        db.take(b"mix:06").await.unwrap(),
        Some(b"buffered-before-take".to_vec())
    );

    for step in 0..240_u64 {
        let random = seeded_next(&mut seed);
        let key_index = (random >> 8) % 24;
        let key = format!("mix:{key_index:02}").into_bytes();
        let value = format!("value:{step:03}:{random:016x}").into_bytes();
        match random % 8 {
            0 => {
                db.insert(&key, &value).await.unwrap();
                expected.insert(key, value);
            }
            1 => {
                db.remove(&key).await.unwrap();
                expected.remove(&key);
            }
            2 => {
                let second_key = format!("mix:{:02}", (key_index + 7) % 24).into_bytes();
                let second_value = format!("bulk:{step:03}").into_bytes();
                db.insert_many([
                    (key.clone(), value.clone()),
                    (second_key.clone(), second_value.clone()),
                ])
                .await
                .unwrap();
                expected.insert(key, value);
                expected.insert(second_key, second_value);
            }
            3 => {
                let second_key = format!("mix:{:02}", (key_index + 11) % 24).into_bytes();
                let mut batch = WriteBatch::new();
                batch.put(&key, b"batch-first");
                batch.delete(&key);
                batch.put(&key, &value);
                batch.delete(&second_key);
                db.write_batch(&batch).await.unwrap();
                expected.insert(key, value);
                expected.remove(&second_key);
            }
            4 => {
                assert_eq!(db.get(&key).await.unwrap(), expected.get(&key).cloned());
            }
            5 => {
                let actual = db.range(b"mix:", b"mix;").await.unwrap();
                let modeled: Vec<_> = expected
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                assert_eq!(actual, modeled);
            }
            6 => {
                let modeled = expected.remove(&key);
                assert_eq!(db.take(&key).await.unwrap(), modeled);
            }
            _ => db.flush().await.unwrap(),
        }
    }

    db.flush().await.unwrap();
    let modeled: Vec<_> = expected.into_iter().collect();
    assert_eq!(db.scan_prefix(b"mix:").await.unwrap(), modeled);
    drop(db);

    let reopened = Db::open_with_options(tmp.path(), options).await.unwrap();
    assert_eq!(reopened.scan_prefix(b"mix:").await.unwrap(), modeled);
}

#[tokio::test]
async fn seeded_mutation_entry_point_mix_preserves_order_in_every_mode() {
    for (options, seed) in [
        (DbOptions::fast(), 0x9f4a_7c15_d3e2_b801),
        (DbOptions::durable(), 0x62de_41b7_a590_c3f8),
        (DbOptions::paranoid(), 0xe13b_805c_4ad7_296f),
    ] {
        run_seeded_mutation_mix(options, seed).await;
    }
}

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

#[tokio::test]
async fn test_empty_value_handling() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"key_empty", b"").await.unwrap();
    assert_eq!(
        db.get(b"key_empty").await.unwrap(),
        Some(b"".to_vec()),
        "Empty value should be returned from the memtable"
    );

    db.flush().await.unwrap();
    assert_eq!(
        db.get(b"key_empty").await.unwrap(),
        Some(b"".to_vec()),
        "Empty value should survive flush to SSTable"
    );
    drop(db);

    let reopened = Db::open_with_options(&path, DbOptions::durable())
        .await
        .unwrap();
    assert_eq!(
        reopened.get(b"key_empty").await.unwrap(),
        Some(b"".to_vec()),
        "Empty value should survive reopen from SSTable"
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
// 4b. Bulk insert recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_many_persists_after_reopen() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let db = Db::open_with_options(&path, DbOptions::durable())
            .await
            .unwrap();
        db.insert_many([
            (b"bulk:a".as_slice(), b"1".as_slice()),
            (b"bulk:b".as_slice(), b"2".as_slice()),
            (b"bulk:empty".as_slice(), b"".as_slice()),
        ])
        .await
        .unwrap();
        db.flush().await.unwrap();
    }

    let reopened = Db::open_with_options(&path, DbOptions::durable())
        .await
        .unwrap();
    assert_eq!(reopened.get(b"bulk:a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(reopened.get(b"bulk:b").await.unwrap(), Some(b"2".to_vec()));
    assert_eq!(reopened.get(b"bulk:empty").await.unwrap(), Some(Vec::new()));
}

#[tokio::test]
async fn test_insert_many_overwrites_existing_values() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert(b"bulk:overwrite", b"old").await.unwrap();
    db.insert_many([
        (b"bulk:overwrite".as_slice(), b"new".as_slice()),
        (b"bulk:other".as_slice(), b"value".as_slice()),
    ])
    .await
    .unwrap();

    assert_eq!(
        db.get(b"bulk:overwrite").await.unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        db.get(b"bulk:other").await.unwrap(),
        Some(b"value".to_vec())
    );
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

    let stats = db.physical_stats();
    assert!(
        stats.wal.retained_valid_bytes > 0,
        "WAL size should be > 0 after inserting data in durable mode, got {}",
        stats.wal.retained_valid_bytes
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
    assert!(
        range_results.is_empty(),
        "Range scan on empty DB should return empty vec"
    );

    let prefix_results = db.scan_prefix(b"anything").await.unwrap();
    assert!(
        prefix_results.is_empty(),
        "Prefix scan on empty DB should return empty vec"
    );
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
        assert_eq!(
            db.get(b"y").await.unwrap(),
            None,
            "Deleted key y should remain deleted after reopen"
        );
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
    assert_eq!(
        db.get(b"c").await.unwrap(),
        None,
        "Batch-deleted key should be None"
    );
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

    let logical = db.logical_stats().await.unwrap();
    assert_eq!(logical.live_keys, 3);
    assert_eq!(logical.key_bytes, 18);
    assert_eq!(logical.value_bytes, 21);
    assert_eq!(logical.total_bytes, 39);
    let stats = db.physical_stats();
    assert!(
        stats.memtables.active_bytes + stats.memtables.immutable_bytes > 0,
        "memtable bytes should be > 0 after inserting data, got active={} immutable={}",
        stats.memtables.active_bytes,
        stats.memtables.immutable_bytes,
    );
}

// ---------------------------------------------------------------------------
// 20. Durable bulkload stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stats_include_durable_bulkload_counters() {
    let tmp = TempDir::new().unwrap();
    let db = Db::open_with_options(tmp.path(), DbOptions::durable())
        .await
        .unwrap();

    db.insert_many((0..128).map(|i| (format!("bulk-stats:{i:04}").into_bytes(), vec![b'x'; 128])))
        .await
        .unwrap();

    let before_flush = db.physical_stats();
    assert!(before_flush.amplification.wal_bytes_written_since_open > 0);
    assert_eq!(before_flush.memtables.immutable_tables, 0);

    db.flush().await.unwrap();
    let after_flush = db.physical_stats();
    assert!(after_flush.amplification.flush_bytes_written_since_open > 0);
    assert!(after_flush.sstables.level_zero_files >= 1);
    assert_eq!(after_flush.stalls.count_since_open, 0);
}

// ---------------------------------------------------------------------------
// 21. Fast mode basic
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
