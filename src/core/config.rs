//! Configuration types for TurboKV.
//!
//! Most users should use [`DbConfig`](crate::core::types::DbConfig) or
//! [`DbOptions`](crate::storage::db::DbOptions) instead of these lower-level types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Compression algorithms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
    Snappy,
}

/// Logging levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Logging formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    Text,
    Json,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub format: LogFormat,
    pub file: Option<PathBuf>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Text,
            file: None,
        }
    }
}
