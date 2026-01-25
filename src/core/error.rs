//! # Error Handling
//!
//! Comprehensive error types for HanshiroDB operations.
//!
//! ## Design Principles
//!
//! 1. **Actionable**: Every error should guide the user toward resolution
//! 2. **Contextual**: Errors include relevant context (file paths, values)
//! 3. **Traceable**: Errors can be traced through the system
//! 4. **Recoverable**: Distinguish between fatal and recoverable errors

use thiserror::Error;

/// Result type alias for HanshiroDB operations
pub type Result<T> = std::result::Result<T, Error>;

/// Primary error type for HanshiroDB
#[derive(Error, Debug)]
pub enum Error {
    // Storage Errors
    #[error("WAL error: {message}")]
    WriteAheadLog { message: String, source: Option<Box<dyn std::error::Error + Send + Sync>> },
    
    #[error("SSTable error: {message}")]
    SSTable { message: String, source: Option<Box<dyn std::error::Error + Send + Sync>> },
    
    #[error("MemTable error: {message}")]
    MemTable { message: String },
    
    #[error("Compaction failed: {reason}")]
    Compaction { reason: String },
    
    // Index Errors
    #[error("Vector index error: {message}")]
    VectorIndex { message: String },
    
    #[error("Metadata index error: {message}")]
    MetadataIndex { message: String },
    
    #[error("Index corruption detected: {details}")]
    IndexCorruption { details: String },
    
    // Ingestion Errors
    #[error("Parsing error for format {format}: {message}")]
    ParseError { format: String, message: String },
    
    #[error("Vectorization failed: {message}")]
    VectorizationError { message: String },
    
    #[error("Schema validation error: {message}")]
    SchemaValidation { message: String },
    
    // Query Errors
    #[error("Query error: {message}")]
    QueryError { message: String },
    
    #[error("Invalid query syntax: {message}")]
    InvalidQuery { message: String },
    
    #[error("Query timeout after {seconds}s")]
    QueryTimeout { seconds: u64 },
    
    // Security Errors
    #[error("Merkle chain validation failed: expected {expected}, got {actual}")]
    MerkleValidation { expected: String, actual: String },
    
    #[error("Tampering detected at position {position}")]
    TamperingDetected { position: u64 },
    
    #[error("Authentication failed: {reason}")]
    Authentication { reason: String },
    
    #[error("Authorization failed: {reason}")]
    Authorization { reason: String },
    
    // System Errors
    #[error("IO error: {message}")]
    Io { message: String, source: std::io::Error },
    
    #[error("Configuration error: {message}")]
    Configuration { message: String },
    
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },
    
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl Error {
    /// Check if error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            Error::QueryTimeout { .. } => true,
            Error::ResourceExhausted { .. } => true,
            Error::Io { .. } => false,
            Error::IndexCorruption { .. } => false,
            Error::TamperingDetected { .. } => false,
            _ => true,
        }
    }
    
    /// Get error code for monitoring
    pub fn error_code(&self) -> &'static str {
        match self {
            Error::WriteAheadLog { .. } => "WAL_ERROR",
            Error::SSTable { .. } => "SSTABLE_ERROR",
            Error::MemTable { .. } => "MEMTABLE_ERROR",
            Error::Compaction { .. } => "COMPACTION_ERROR",
            Error::VectorIndex { .. } => "VECTOR_INDEX_ERROR",
            Error::MetadataIndex { .. } => "METADATA_INDEX_ERROR",
            Error::IndexCorruption { .. } => "INDEX_CORRUPTION",
            Error::ParseError { .. } => "PARSE_ERROR",
            Error::VectorizationError { .. } => "VECTORIZATION_ERROR",
            Error::SchemaValidation { .. } => "SCHEMA_VALIDATION_ERROR",
            Error::QueryError { .. } => "QUERY_ERROR",
            Error::InvalidQuery { .. } => "INVALID_QUERY",
            Error::QueryTimeout { .. } => "QUERY_TIMEOUT",
            Error::MerkleValidation { .. } => "MERKLE_VALIDATION_FAILED",
            Error::TamperingDetected { .. } => "TAMPERING_DETECTED",
            Error::Authentication { .. } => "AUTH_FAILED",
            Error::Authorization { .. } => "AUTHZ_FAILED",
            Error::Io { .. } => "IO_ERROR",
            Error::Configuration { .. } => "CONFIG_ERROR",
            Error::ResourceExhausted { .. } => "RESOURCE_EXHAUSTED",
            Error::Internal { .. } => "INTERNAL_ERROR",
        }
    }
}

// Conversion from std::io::Error
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io {
            message: err.to_string(),
            source: err,
        }
    }
}

/// Error context builder for adding context to errors
pub struct ErrorContext<T> {
    result: Result<T>,
}

impl<T> ErrorContext<T> {
    pub fn new(result: Result<T>) -> Self {
        Self { result }
    }
    
    pub fn context(self, message: &str) -> Result<T> {
        self.result.map_err(|e| Error::Internal {
            message: format!("{}: {}", message, e),
        })
    }
}

/// Extension trait for adding context to results
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