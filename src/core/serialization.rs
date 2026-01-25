//! Serialization utilities for TurboKV.
//!
//! For a generic key-value store, keys and values are raw bytes (`&[u8]`).
//! Serialization is the caller's responsibility - you can use any format
//! (bincode, MessagePack, JSON, protobuf, etc.).
//!
//! This module provides optional helper functions for common serialization
//! patterns using bincode (fast binary format) and MessagePack.

use super::error::{Error, Result};

/// Serialize any serde-compatible type to bytes using bincode.
///
/// Bincode is a fast, compact binary format ideal for internal storage.
///
/// # Example
/// ```ignore
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String, age: u32 }
///
/// let user = User { name: "Alice".into(), age: 30 };
/// let bytes = to_bytes(&user)?;
/// db.insert(b"user:1", &bytes)?;
/// ```
#[inline]
pub fn to_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| Error::Internal {
        message: format!("Serialization failed: {}", e),
    })
}

/// Deserialize bytes to any serde-compatible type using bincode.
///
/// # Example
/// ```ignore
/// let bytes = db.get(b"user:1")?.unwrap();
/// let user: User = from_bytes(&bytes)?;
/// ```
#[inline]
pub fn from_bytes<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(|e| Error::Internal {
        message: format!("Deserialization failed: {}", e),
    })
}

/// Serialize using MessagePack (more compact, cross-language compatible).
#[inline]
pub fn to_msgpack<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec(value).map_err(|e| Error::Internal {
        message: format!("MessagePack serialization failed: {}", e),
    })
}

/// Deserialize from MessagePack.
#[inline]
pub fn from_msgpack<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| Error::Internal {
        message: format!("MessagePack deserialization failed: {}", e),
    })
}

/// Encode a key-value pair for WAL storage.
///
/// Format: [key_len: u32 LE][key bytes][value bytes]
#[inline]
pub fn encode_kv(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len() + value.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

/// Decode a key-value pair from WAL storage.
///
/// Returns (key, value) or an error if the format is invalid.
#[inline]
pub fn decode_kv(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    if bytes.len() < 4 {
        return Err(Error::Internal {
            message: "KV encoding too short".to_string(),
        });
    }

    let key_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

    if bytes.len() < 4 + key_len {
        return Err(Error::Internal {
            message: "KV encoding truncated".to_string(),
        });
    }

    let key = &bytes[4..4 + key_len];
    let value = &bytes[4 + key_len..];

    Ok((key, value))
}

/// Encode a deletion marker for WAL storage.
///
/// Format: [key_len: u32 LE][key bytes] (no value)
#[inline]
pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Decode a deletion marker from WAL storage.
#[inline]
pub fn decode_delete(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(Error::Internal {
            message: "Delete encoding too short".to_string(),
        });
    }

    let key_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

    if bytes.len() < 4 + key_len {
        return Err(Error::Internal {
            message: "Delete encoding truncated".to_string(),
        });
    }

    Ok(&bytes[4..4 + key_len])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestStruct {
        name: String,
        value: u64,
    }

    #[test]
    fn test_bincode_roundtrip() {
        let original = TestStruct {
            name: "test".to_string(),
            value: 42,
        };
        let bytes = to_bytes(&original).unwrap();
        let recovered: TestStruct = from_bytes(&bytes).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_msgpack_roundtrip() {
        let original = TestStruct {
            name: "test".to_string(),
            value: 42,
        };
        let bytes = to_msgpack(&original).unwrap();
        let recovered: TestStruct = from_msgpack(&bytes).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_kv_encoding() {
        let key = b"hello";
        let value = b"world";
        let encoded = encode_kv(key, value);
        let (dec_key, dec_value) = decode_kv(&encoded).unwrap();
        assert_eq!(dec_key, key);
        assert_eq!(dec_value, value);
    }

    #[test]
    fn test_delete_encoding() {
        let key = b"delete_me";
        let encoded = encode_delete(key);
        let dec_key = decode_delete(&encoded).unwrap();
        assert_eq!(dec_key, key);
    }
}
