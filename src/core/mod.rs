//! # TurboKV Core
//!
//! This crate provides the fundamental building blocks for TurboKV:
//! - Configuration types
//! - Error types
//! - Storage engine trait
//! - Cryptographic primitives (Merkle chains, checksums)
//! - Serialization utilities
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                   turbokv-core                  │
//! ├─────────────────────────────────────────────────┤
//! │  • types         - DbConfig, WriteBatch, etc.  │
//! │  • traits        - StorageEngine interface     │
//! │  • error         - Error handling              │
//! │  • crypto        - Merkle chains & checksums   │
//! │  • serialization - Encoding utilities          │
//! │  • utils         - Common utilities            │
//! └─────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```ignore
//! use turbokv_core::{DbConfig, WriteBatch};
//!
//! // Use preset configurations
//! let config = DbConfig::fast();      // Maximum speed
//! let config = DbConfig::durable();   // Crash-safe
//! let config = DbConfig::tamper_proof(); // With Merkle chains
//!
//! // Batch operations
//! let mut batch = WriteBatch::new();
//! batch.put(b"key1", b"value1");
//! batch.put(b"key2", b"value2");
//! batch.delete(b"old_key");
//! ```

pub mod config;
pub mod crypto;
pub mod error;
pub mod metrics;
pub mod serialization;
pub mod traits;
pub mod types;
pub mod utils;

// Re-export commonly used types
pub use error::{Error, Result};
pub use serialization::{
    decode_delete, decode_kv, encode_delete, encode_kv, from_bytes, from_msgpack, to_bytes,
    to_msgpack,
};
pub use traits::StorageEngine;
pub use types::{
    BatchOp, CompactionResult, CompactionStyle, Compression, DbConfig, StorageStats, WriteBatch,
};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_VERSION: u32 = 2; // Bumped for TurboKV
