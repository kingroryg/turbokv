//! Error types for TurboKV.

use thiserror::Error;

/// Result type alias for TurboKV operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Primary error type for TurboKV.
#[derive(Error, Debug)]
pub enum Error {
    #[error("SSTable error: {message}")]
    SSTable {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("IO error: {message}")]
    Io {
        message: String,
        source: std::io::Error,
    },

    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io {
            message: err.to_string(),
            source: err,
        }
    }
}
