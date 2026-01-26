//! # MemTable Tests for TurboKV
//!
//! Tests for the generic key-value memtable implementation.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use turbokv::storage::memtable::{MemTable, MemTableConfig, MemTableManager};

fn test_config() -> MemTableConfig {
    MemTableConfig {
        max_size: 1024 * 1024,
        max_entries: 1000,
        max_age: Duration::from_secs(3600),
    }
}

#[test]
fn test_memtable_basic_operations() {
    let memtable = MemTable::new(test_config());

    // Insert key-value pairs
    let seq1 = memtable.insert(b"key1", b"value1").unwrap();
    assert_eq!(seq1, 0);

    let seq2 = memtable.insert(b"key2", b"value2").unwrap();
    assert_eq!(seq2, 1);

    // Get values
    assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(memtable.get(b"key2"), Some(b"value2".to_vec()));
    assert_eq!(memtable.get(b"key3"), None);

    // Contains key
    assert!(memtable.contains_key(b"key1"));
    assert!(!memtable.contains_key(b"key3"));
}

#[test]
fn test_memtable_delete() {
    let memtable = MemTable::new(test_config());

    memtable.insert(b"key1", b"value1").unwrap();
    assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));

    // Delete
    memtable.delete(b"key1").unwrap();

    // Should return None (tombstone)
    assert_eq!(memtable.get(b"key1"), None);
    assert!(!memtable.contains_key(b"key1"));
}

#[test]
fn test_memtable_update() {
    let memtable = MemTable::new(test_config());

    memtable.insert(b"key1", b"value1").unwrap();
    assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));

    // Update
    memtable.insert(b"key1", b"value2").unwrap();
    assert_eq!(memtable.get(b"key1"), Some(b"value2".to_vec()));
}

#[test]
fn test_memtable_range() {
    let memtable = MemTable::new(test_config());

    memtable.insert(b"a", b"1").unwrap();
    memtable.insert(b"b", b"2").unwrap();
    memtable.insert(b"c", b"3").unwrap();
    memtable.insert(b"d", b"4").unwrap();

    let range = memtable.range(b"b", b"d");
    assert_eq!(range.len(), 2);
    assert_eq!(range[0], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(range[1], (b"c".to_vec(), b"3".to_vec()));
}

#[test]
fn test_memtable_scan_prefix() {
    let memtable = MemTable::new(test_config());

    memtable.insert(b"user:1", b"alice").unwrap();
    memtable.insert(b"user:2", b"bob").unwrap();
    memtable.insert(b"user:3", b"charlie").unwrap();
    memtable.insert(b"item:1", b"apple").unwrap();

    let users = memtable.scan_prefix(b"user:");
    assert_eq!(users.len(), 3);

    let items = memtable.scan_prefix(b"item:");
    assert_eq!(items.len(), 1);
}

#[test]
fn test_memtable_size_limit() {
    let config = MemTableConfig {
        max_size: 1000,
        max_entries: 1000,
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config);

    // Insert until size limit
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = vec![b'x'; 100]; // 100 bytes each
        if memtable.should_flush() {
            break;
        }
        let _ = memtable.insert(key.as_bytes(), &value);
    }

    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_entry_limit() {
    let config = MemTableConfig {
        max_size: 1024 * 1024,
        max_entries: 10,
        max_age: Duration::from_secs(3600),
    };
    let memtable = MemTable::new(config);

    for i in 0..10 {
        memtable
            .insert(format!("key{}", i).as_bytes(), b"value")
            .unwrap();
    }

    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_read_only() {
    let memtable = MemTable::new(test_config());

    memtable.insert(b"key1", b"value1").unwrap();
    memtable.set_read_only();

    // Insert should fail
    assert!(memtable.insert(b"key2", b"value2").is_err());

    // Read should still work
    assert_eq!(memtable.get(b"key1"), Some(b"value1".to_vec()));
}

#[test]
fn test_memtable_stats() {
    let memtable = MemTable::new(test_config());

    let stats = memtable.stats();
    assert_eq!(stats.entry_count, 0);
    assert_eq!(stats.size_bytes, 0);

    memtable.insert(b"key1", b"value1").unwrap();
    memtable.insert(b"key2", b"value2").unwrap();

    let stats = memtable.stats();
    assert_eq!(stats.entry_count, 2);
    assert!(stats.size_bytes > 0);
}

#[test]
fn test_memtable_concurrent_writes() {
    let memtable = Arc::new(MemTable::new(test_config()));
    let num_threads = 4;
    let writes_per_thread = 100;

    let mut handles = vec![];
    for t in 0..num_threads {
        let memtable_clone = Arc::clone(&memtable);
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let key = format!("t{}k{}", t, i);
                let value = format!("value{}", i);
                memtable_clone
                    .insert(key.as_bytes(), value.as_bytes())
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = memtable.stats();
    assert_eq!(stats.entry_count, num_threads * writes_per_thread);
}

// MemTableManager tests

#[test]
fn test_manager_basic_operations() {
    let manager = MemTableManager::new(test_config());

    manager.insert(b"key1", b"value1").unwrap();
    manager.insert(b"key2", b"value2").unwrap();

    assert_eq!(manager.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(manager.get(b"key2"), Some(b"value2".to_vec()));
    assert_eq!(manager.get(b"key3"), None);
}

#[test]
fn test_manager_delete() {
    let manager = MemTableManager::new(test_config());

    manager.insert(b"key1", b"value1").unwrap();
    manager.delete(b"key1").unwrap();

    assert_eq!(manager.get(b"key1"), None);
}

#[test]
fn test_manager_range() {
    let manager = MemTableManager::new(test_config());

    manager.insert(b"a", b"1").unwrap();
    manager.insert(b"b", b"2").unwrap();
    manager.insert(b"c", b"3").unwrap();

    let range = manager.range(b"a", b"c");
    assert_eq!(range.len(), 2);
}

#[test]
fn test_manager_rotation() {
    let config = MemTableConfig {
        max_entries: 10,
        max_size: 1024 * 1024,
        max_age: Duration::from_secs(3600),
    };
    let manager = MemTableManager::new(config);

    // Insert enough entries to trigger rotation
    for i in 0..25 {
        manager
            .insert(format!("key{}", i).as_bytes(), b"value")
            .unwrap();
    }

    // Should have immutable tables
    assert!(manager.has_immutable());

    // All entries should still be accessible
    for i in 0..25 {
        assert!(manager.get(format!("key{}", i).as_bytes()).is_some());
    }
}

#[test]
fn test_manager_flush() {
    let config = MemTableConfig {
        max_entries: 10,
        max_size: 1024 * 1024,
        max_age: Duration::from_secs(3600),
    };
    let manager = MemTableManager::new(config);

    // Insert and rotate
    for i in 0..15 {
        manager
            .insert(format!("key{}", i).as_bytes(), b"value")
            .unwrap();
    }

    // Get immutable for flush
    let immutable = manager.get_immutable_for_flush();
    assert!(immutable.is_some());

    let table = immutable.unwrap();
    assert!(!table.is_empty());
}

#[test]
fn test_manager_stats() {
    let manager = MemTableManager::new(test_config());

    manager.insert(b"key1", b"value1").unwrap();
    manager.insert(b"key2", b"value2").unwrap();

    let stats = manager.stats();
    assert_eq!(stats.active.entry_count, 2);
}
