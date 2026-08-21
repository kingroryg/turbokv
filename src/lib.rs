#![warn(missing_docs)]

//! # TurboKV
//!
//! A fast, embedded key-value store with a BTreeMap-like API.
//!
//! TurboKV is optimized for high write throughput, with fast mode tuned for
//! benchmark-heavy workloads and durable mode using a WAL for crash recovery.
//!
//! A TurboKV database exclusively owns its canonicalized data directory while
//! open. Shared multi-writer access, whether from the same process or another
//! process, is unsupported and rejected with [`DbError::DirectoryLocked`].
//!
//! ## Quick Start
//!
//! ```rust
//! use turbokv::Db;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let directory = tempfile::tempdir()?;
//! let db = Db::open(directory.path()).await?;
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
//! db.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Durability Modes
//!
//! ```rust
//! use turbokv::{Db, DbOptions};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let directory = tempfile::tempdir()?;
//!
//! // Fast: no WAL, no fsync (for caches, temporary data)
//! let fast = Db::open_with_options(directory.path().join("fast"), DbOptions::fast()).await?;
//! fast.close().await?;
//!
//! // Durable: append to the WAL before acknowledgement without per-write sync.
//! // Intended for process-crash recovery; it does not guarantee recent writes
//! // against power loss.
//! let durable =
//!     Db::open_with_options(directory.path().join("durable"), DbOptions::durable()).await?;
//! durable.close().await?;
//!
//! // Paranoid: sync the WAL before acknowledgement. This is the strongest
//! // mode, subject to the filesystem and device honoring sync.
//! let paranoid =
//!     Db::open_with_options(directory.path().join("paranoid"), DbOptions::paranoid()).await?;
//! paranoid.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Operations
//!
//! ```rust
//! use turbokv::{Db, WriteBatch};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let directory = tempfile::tempdir()?;
//! let db = Db::open(directory.path()).await?;
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"key1", b"value1");
//! batch.put(b"key2", b"value2");
//! batch.delete(b"old_key");
//! db.write_batch(&batch).await?;
//! db.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod core;
pub mod storage;

// Primary API
pub use core::types::{
    CompactionResult, Compression, DatabaseStatus, LogicalStats, MaintenanceFailure,
    MaintenanceOperationStatus, MaintenanceOrigin, MaintenanceStatus, PhysicalCacheStats,
    PhysicalMemTableStats, PhysicalSSTableStats, PhysicalStats, PhysicalVersionStats, WalStats,
    WriteAmplificationStats, WriteBackpressureCauseStatus, WriteBackpressureStatus, WriteBatch,
    WriteStallStats,
};
pub use storage::db::{Db, DbError, DbOptions, DbStats};
pub use storage::iter::{EntryGuard, PrefixIter, RangeIter, ScanEntry, ScanError, ScanResult};

// Advanced API
pub use storage::compaction::CompactionConfig;
pub use storage::engine::{
    Engine, MaintenanceShutdownError, Result as StorageResult, StorageConfig, StorageError,
};
pub use storage::fd::FdConfig;
pub use storage::memtable::MemTableConfig;
pub use storage::sstable::SSTableConfig;
pub use storage::wal::WalConfig;

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
