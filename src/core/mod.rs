//! Shared types and errors for TurboKV.
//!
//! This module provides:
//! - Database value and status types ([`Compression`], [`DatabaseStatus`])
//! - Error handling ([`Error`], [`Result`])
//! - Data integrity primitives ([`crypto`])

pub(crate) mod crypto;
pub mod error;
pub mod types;

pub use error::{Error, Result};
pub use types::{
    BatchOp, CompactionResult, Compression, DatabaseStatus, LogicalStats, MaintenanceFailure,
    MaintenanceOperationStatus, MaintenanceOrigin, MaintenanceStatus, PhysicalCacheStats,
    PhysicalMemTableStats, PhysicalSSTableStats, PhysicalStats, PhysicalVersionStats, StorageStats,
    WalStats, WriteAmplificationStats, WriteBackpressureCauseStatus, WriteBackpressureStatus,
    WriteBatch, WriteStallStats,
};
