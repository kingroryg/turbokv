//! Core types and traits for TurboKV.
//!
//! This module provides:
//! - Configuration types ([`DbConfig`], [`Compression`])
//! - Error handling ([`Error`], [`Result`])
//! - Storage engine trait ([`StorageEngine`])
//! - Data integrity primitives ([`crypto`])
//! - Serialization utilities ([`serialization`])

pub mod config;
pub mod crypto;
pub mod error;
pub mod metrics;
pub mod serialization;
pub mod traits;
pub mod types;
pub mod utils;

pub use error::{Error, Result};
pub use serialization::{
    decode_delete, decode_kv, encode_delete, encode_kv, from_bytes, from_msgpack, to_bytes,
    to_msgpack,
};
pub use traits::StorageEngine;
pub use types::{
    BatchOp, CompactionResult, CompactionStyle, Compression, DbConfig, LogicalStats,
    PhysicalCacheStats, PhysicalMemTableStats, PhysicalSSTableStats, PhysicalStats,
    PhysicalVersionStats, StorageStats, WalStats, WriteAmplificationStats, WriteBatch,
    WriteStallStats,
};

/// Protocol version for WAL format compatibility.
pub const PROTOCOL_VERSION: u32 = 2;
