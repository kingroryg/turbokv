//! Error types for TurboKV.

use thiserror::Error;

/// Result type alias for TurboKV operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Primary error type for TurboKV.
#[derive(Error, Debug)]
pub enum Error {
    /// An SSTable could not be encoded, decoded, validated, or accessed.
    #[error("SSTable error: {message}")]
    SSTable {
        /// Operation-specific failure description.
        message: String,
        /// Optional underlying codec or I/O error.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A filesystem operation failed.
    #[error("IO error: {message}")]
    Io {
        /// Operation-specific failure description.
        message: String,
        /// Underlying operating-system error.
        source: std::io::Error,
    },

    /// A bounded process resource could not admit more work.
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted {
        /// Name of the exhausted resource.
        resource: String,
    },

    /// An internal invariant or persisted metadata contract failed.
    #[error("Internal error: {message}")]
    Internal {
        /// Invariant-specific failure description.
        message: String,
    },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io {
            message: err.to_string(),
            source: err,
        }
    }
}
