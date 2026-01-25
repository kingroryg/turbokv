//! # TurboKV
//!
//! A fast, embedded key-value store with BTreeMap-like API.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Open with default options (durable)
//! let db = Db::open("./my_data").await?;
//!
//! // Insert key-value pairs
//! db.insert(b"hello", b"world").await?;
//! db.insert(b"foo", b"bar").await?;
//!
//! // Get values
//! if let Some(value) = db.get(b"hello").await? {
//!     println!("Got: {:?}", String::from_utf8_lossy(&value));
//! }
//!
//! // Delete keys
//! db.remove(b"hello").await?;
//!
//! // Range scan
//! for (key, value) in db.range(b"a", b"z").await? {
//!     println!("{:?} -> {:?}", key, value);
//! }
//!
//! // Prefix scan
//! for (key, value) in db.scan_prefix(b"user:").await? {
//!     println!("{:?} -> {:?}", key, value);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Configuration
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Fast mode - no WAL, no fsync (for benchmarks/non-critical data)
//! let db = Db::open_with_options("./data", DbOptions::fast()).await?;
//!
//! // Durable mode - WAL enabled, fsync on writes (default)
//! let db = Db::open_with_options("./data", DbOptions::durable()).await?;
//!
//! // Tamper-proof mode - Merkle chains for integrity verification
//! let db = Db::open_with_options("./data", DbOptions::tamper_proof()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Operations
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions, WriteBatch};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Db::open("./data").await?;
//!
//! // Atomic batch write
//! let mut batch = WriteBatch::new();
//! batch.put(b"key1", b"value1");
//! batch.put(b"key2", b"value2");
//! batch.delete(b"old_key");
//! db.write_batch(&batch).await?;
//! # Ok(())
//! # }
//! ```

// Core types and traits
pub mod core;

// Storage engine implementation
pub mod storage;

// ============================================================================
// Public API - Primary exports
// ============================================================================

// Database API (main interface)
pub use storage::db::{Db, DbError, DbOptions, DbStats};

// Write batch for atomic operations
pub use core::types::WriteBatch;

// Configuration
pub use core::types::{DbConfig, Compression, CompactionStyle};

// ============================================================================
// Advanced API - For power users
// ============================================================================

// Storage engine (low-level)
pub use storage::engine::{Engine, StorageConfig, StorageError};
pub use storage::engine::Result as StorageResult;

// WAL
pub use storage::wal::{WalConfig, WriteAheadLog};

// MemTable
pub use storage::memtable::{MemTableConfig, MemTableManager};

// SSTable
pub use storage::sstable::{SSTableConfig, SSTableInfo};

// Compaction
pub use storage::compaction::CompactionConfig;

// Cryptographic utilities
pub use core::crypto::{crc32_checksum, MerkleChain, MerkleNode};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
