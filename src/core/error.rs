//! Error types for TurboKV.

use thiserror::Error;

/// Result type alias for TurboKV operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Primary error type for TurboKV.
#[derive(Error, Debug)]
pub enum Error {
    #[error("WAL error: {message}")]
    WriteAheadLog {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("SSTable error: {message}")]
    SSTable {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("MemTable error: {message}")]
    MemTable { message: String },

    #[error("Compaction failed: {reason}")]
    Compaction { reason: String },

    #[error("Index corruption detected: {details}")]
    IndexCorruption { details: String },

    #[error("Query error: {message}")]
    QueryError { message: String },

    #[error("IO error: {message}")]
    Io {
        message: String,
        source: std::io::Error,
    },

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl Error {
    /// Check if error is recoverable.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Error::ResourceExhausted { .. })
    }

    /// Get error code for monitoring.
    pub fn error_code(&self) -> &'static str {
        match self {
            Error::WriteAheadLog { .. } => "WAL_ERROR",
            Error::SSTable { .. } => "SSTABLE_ERROR",
            Error::MemTable { .. } => "MEMTABLE_ERROR",
            Error::Compaction { .. } => "COMPACTION_ERROR",
            Error::IndexCorruption { .. } => "INDEX_CORRUPTION",
            Error::QueryError { .. } => "QUERY_ERROR",
            Error::Io { .. } => "IO_ERROR",
            Error::Configuration { .. } => "CONFIG_ERROR",
            Error::ResourceExhausted { .. } => "RESOURCE_EXHAUSTED",
            Error::Internal { .. } => "INTERNAL_ERROR",
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io {
            message: err.to_string(),
            source: err,
        }
    }
}

/// Extension trait for adding context to results.
pub trait ResultExt<T> {
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T> ResultExt<T> for Result<T> {
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| Error::Internal {
            message: format!("{}: {}", f(), e),
        })
    }
}
