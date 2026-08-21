//! Unit tests for TurboKV core types.

use turbokv::core::types::{BatchOp, CompactionResult, StorageStats, WriteBatch};

mod types_tests {
    use super::*;

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
        assert_eq!(result.input_files, 0);
        assert_eq!(result.output_files, 0);
        assert_eq!(result.bytes_read, 0);
        assert_eq!(result.bytes_written, 0);
        assert_eq!(result.bytes_reclaimed, 0);
        assert!(result.is_complete());
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
}
