//! # TurboKV
//!
//! A fast, embedded key-value store with a BTreeMap-like API.
//!
//! TurboKV is optimized for high write throughput, with fast mode tuned for
//! benchmark-heavy workloads and durable mode using a WAL for crash recovery.
//! See the [`optimizations`] module for implementation notes.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Db::open("./my_data").await?;
//!
//! db.insert(b"hello", b"world").await?;
//! db.insert(b"foo", b"bar").await?;
//!
//! if let Some(value) = db.get(b"hello").await? {
//!     println!("Got: {:?}", String::from_utf8_lossy(&value));
//! }
//!
//! db.remove(b"hello").await?;
//!
//! for (key, value) in db.range(b"a", b"z").await? {
//!     println!("{:?} -> {:?}", key, value);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Durability Modes
//!
//! ```rust,no_run
//! use turbokv::{Db, DbOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Fast: no WAL, no fsync (for caches, temporary data)
//! let db = Db::open_with_options("./data", DbOptions::fast()).await?;
//!
//! // Durable: WAL enabled, periodic sync (survives process crash)
//! let db = Db::open_with_options("./data", DbOptions::durable()).await?;
//!
//! // Paranoid: WAL + fsync every write (survives power loss)
//! let db = Db::open_with_options("./data", DbOptions::paranoid()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Operations
//!
//! ```rust,no_run
//! use turbokv::{Db, WriteBatch};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Db::open("./data").await?;
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"key1", b"value1");
//! batch.put(b"key2", b"value2");
//! batch.delete(b"old_key");
//! db.write_batch(&batch).await?;
//! # Ok(())
//! # }
//! ```

pub mod core;
pub mod optimizations;
pub mod storage;

// Primary API
pub use core::types::{CompactionStyle, Compression, DbConfig, WriteBatch};
pub use storage::db::{Db, DbError, DbOptions, DbStats};
pub use storage::iter::{EntryGuard, PrefixIter, RangeIter};

// Advanced API
pub use core::crypto::crc32_checksum;
pub use storage::compaction::CompactionConfig;
pub use storage::engine::{Engine, Result as StorageResult, StorageConfig, StorageError};
pub use storage::memtable::{MemTableConfig, MemTableManager};
pub use storage::sstable::{SSTableConfig, SSTableInfo};
pub use storage::wal::{WalConfig, WriteAheadLog};

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
