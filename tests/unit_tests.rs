//! Unit tests for TurboKV core types

use turbokv::core::types::{
    BatchOp, CompactionResult, CompactionStyle, Compression, DbConfig, StorageStats, WriteBatch,
};

mod types_tests {
    use super::*;

    #[test]
    fn test_db_config_default() {
        let config = DbConfig::default();
        assert!(config.wal_enabled);
        assert!(config.sync_writes);
        assert_eq!(config.compression, Compression::Lz4);
        assert_eq!(config.compaction_style, CompactionStyle::SizeTiered);
    }

    #[test]
    fn test_db_config_fast() {
        let config = DbConfig::fast();
        assert!(!config.wal_enabled);
        assert!(!config.sync_writes);
        assert_eq!(config.compression, Compression::None);
    }

    #[test]
    fn test_db_config_durable() {
        let config = DbConfig::durable();
        assert!(config.wal_enabled);
        assert!(config.sync_writes);
        assert_eq!(config.compression, Compression::Lz4);
    }

    #[test]
    fn test_write_batch() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key3");

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 3);

        let ops = batch.ops();
        assert!(matches!(ops[0], BatchOp::Put { .. }));
        assert!(matches!(ops[1], BatchOp::Put { .. }));
        assert!(matches!(ops[2], BatchOp::Delete { .. }));
    }

    #[test]
    fn test_write_batch_with_capacity() {
        let batch = WriteBatch::with_capacity(100);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_write_batch_clear() {
        let mut batch = WriteBatch::new();
        batch.put(b"key", b"value");
        assert!(!batch.is_empty());

        batch.clear();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_storage_stats_default() {
        let stats = StorageStats::default();
        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.total_bytes, 0);
        assert!(!stats.compaction_pending);
    }

    #[test]
    fn test_compaction_result_default() {
        let result = CompactionResult::default();
        assert_eq!(result.files_compacted, 0);
        assert_eq!(result.bytes_reclaimed, 0);
    }
}

mod utils_tests {
    use turbokv::core::utils::{align_to, format_bytes, is_power_of_two, next_power_of_two};

    #[test]
    fn test_power_of_two() {
        assert_eq!(next_power_of_two(0), 1);
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);

        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(is_power_of_two(4));
        assert!(!is_power_of_two(3));
        assert!(!is_power_of_two(0));
    }

    #[test]
    fn test_align() {
        assert_eq!(align_to(0, 8), 0);
        assert_eq!(align_to(1, 8), 8);
        assert_eq!(align_to(7, 8), 8);
        assert_eq!(align_to(8, 8), 8);
        assert_eq!(align_to(9, 8), 16);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
}

mod crypto_tests {
    use turbokv::core::crypto::MerkleChain;

    #[test]
    fn test_merkle_chain() {
        let mut chain = MerkleChain::new();

        let node1 = chain.add(b"data1");
        let node2 = chain.add(b"data2");
        let node3 = chain.add(b"data3");

        // Each hash should be different
        assert_ne!(node1.hash, node2.hash);
        assert_ne!(node2.hash, node3.hash);

        // Chain should link properly
        assert!(node1.prev_hash.is_none());
        assert_eq!(node2.prev_hash, Some(node1.hash.clone()));
        assert_eq!(node3.prev_hash, Some(node2.hash.clone()));
    }

    #[test]
    fn test_merkle_chain_deterministic() {
        let mut chain1 = MerkleChain::new();
        let mut chain2 = MerkleChain::new();

        let node1a = chain1.add(b"same data");
        let node1b = chain2.add(b"same data");

        // Same input should produce same output
        assert_eq!(node1a.data_hash, node1b.data_hash);
    }
}

mod error_tests {
    use turbokv::core::error::{Error, Result};

    #[test]
    fn test_error_display() {
        let err = Error::Internal {
            message: "test error".to_string(),
        };
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: Result<i32> = Err(Error::Internal {
            message: "test".to_string(),
        });
        assert!(err_result.is_err());
    }

    #[test]
    fn test_error_is_recoverable() {
        let recoverable = Error::QueryTimeout { seconds: 30 };
        assert!(recoverable.is_recoverable());

        let non_recoverable = Error::TamperingDetected { position: 0 };
        assert!(!non_recoverable.is_recoverable());
    }
}
