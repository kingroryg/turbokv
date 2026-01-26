//! # Cryptographic Primitives
//!
//! Provides cryptographic functions for data integrity and security.
//!
//! ## Key Components
//!
//! 1. **Merkle Chaining**: Tamper-proof event linking
//! 2. **Checksums**: Fast data integrity verification
//! 3. **Hashing**: Cryptographic hashing with BLAKE3
//!
//! ## Merkle Chain Visualization
//!
//! ```text
//! Entry 1: H(data1)                     = hash1
//! Entry 2: H(hash1 || data2)           = hash2
//! Entry 3: H(hash2 || data3)           = hash3
//! ...
//! Entry N: H(hashN-1 || dataN)         = hashN
//!
//! If any entry is modified, all subsequent hashes change.
//! This creates a tamper-evident chain of entries.
//! ```

use blake3::Hasher;
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Merkle chain node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleNode {
    pub hash: String,
    pub prev_hash: Option<String>,
    pub data_hash: String,
    pub sequence: u64,
}

impl MerkleNode {
    /// Create an empty node (used when Merkle chains are disabled)
    pub fn empty(sequence: u64) -> Self {
        Self {
            hash: String::new(),
            prev_hash: None,
            data_hash: String::new(),
            sequence,
        }
    }
}

/// Merkle chain builder
pub struct MerkleChain {
    current_hash: Option<String>,
    sequence: u64,
}

impl Default for MerkleChain {
    fn default() -> Self {
        Self::new()
    }
}

impl MerkleChain {
    /// Create new Merkle chain
    pub fn new() -> Self {
        Self {
            current_hash: None,
            sequence: 0,
        }
    }

    /// Get the last hash in the chain
    pub fn get_last_hash(&self) -> Option<String> {
        self.current_hash.clone()
    }

    /// Set the last hash (used for parallel processing)
    pub fn set_last_hash(&mut self, hash: String) {
        self.current_hash = Some(hash);
        self.sequence += 1;
    }

    /// Fast chain hash when data_hash is pre-computed (avoids re-hashing data)
    #[inline]
    pub fn chain_hash_fast(&self, data_hash: &str, prev_hash: Option<&str>) -> String {
        let mut chain_hasher = Hasher::new();
        if let Some(prev) = prev_hash {
            chain_hasher.update(prev.as_bytes());
            chain_hasher.update(b"|");
        }
        chain_hasher.update(data_hash.as_bytes());
        chain_hasher.update(&self.sequence.to_le_bytes());
        hex::encode(chain_hasher.finalize().as_bytes())
    }

    /// Compute hash for data with optional previous hash
    pub fn compute_hash(&self, data: &[u8], prev_hash: Option<&str>) -> String {
        let data_hash = blake3::hash(data).to_hex().to_string();
        self.chain_hash_fast(&data_hash, prev_hash)
    }

    /// Add data to chain
    pub fn add(&mut self, data: &[u8]) -> MerkleNode {
        // Hash the data
        let mut data_hasher = Hasher::new();
        data_hasher.update(data);
        let data_hash = hex::encode(data_hasher.finalize().as_bytes());

        // Create chain hash
        let mut chain_hasher = Hasher::new();

        // Include previous hash if exists
        if let Some(ref prev) = self.current_hash {
            chain_hasher.update(prev.as_bytes());
            chain_hasher.update(b"|");
        }

        // Include data hash
        chain_hasher.update(data_hash.as_bytes());

        // Include sequence number
        chain_hasher.update(&self.sequence.to_le_bytes());

        // Finalize hash
        let hash = hex::encode(chain_hasher.finalize().as_bytes());

        let node = MerkleNode {
            hash: hash.clone(),
            prev_hash: self.current_hash.clone(),
            data_hash,
            sequence: self.sequence,
        };

        // Update state
        self.current_hash = Some(hash);
        self.sequence += 1;

        node
    }

    /// Verify a chain of nodes
    pub fn verify_chain(nodes: &[MerkleNode]) -> Result<(), MerkleError> {
        if nodes.is_empty() {
            return Ok(());
        }

        // First node should have no previous hash
        if nodes[0].prev_hash.is_some() {
            return Err(MerkleError::InvalidChain {
                position: 0,
                reason: "First node has previous hash".to_string(),
            });
        }

        // Verify each subsequent node
        for i in 1..nodes.len() {
            let node = &nodes[i];
            let prev_node = &nodes[i - 1];

            // Check sequence
            if node.sequence != prev_node.sequence + 1 {
                return Err(MerkleError::InvalidChain {
                    position: i,
                    reason: format!(
                        "Invalid sequence: expected {}, got {}",
                        prev_node.sequence + 1,
                        node.sequence
                    ),
                });
            }

            // Check previous hash reference
            match (&node.prev_hash, &prev_node.hash) {
                (Some(prev_hash), actual_prev) if prev_hash == actual_prev => {}
                _ => {
                    return Err(MerkleError::InvalidChain {
                        position: i,
                        reason: "Previous hash mismatch".to_string(),
                    })
                }
            }

            // Recompute and verify hash
            let mut hasher = Hasher::new();
            if let Some(ref prev_hash) = node.prev_hash {
                hasher.update(prev_hash.as_bytes());
                hasher.update(b"|");
            }
            hasher.update(node.data_hash.as_bytes());
            hasher.update(&node.sequence.to_le_bytes());

            let computed_hash = hex::encode(hasher.finalize().as_bytes());
            if computed_hash != node.hash {
                return Err(MerkleError::TamperingDetected {
                    position: i,
                    expected: node.hash.clone(),
                    computed: computed_hash,
                });
            }
        }

        Ok(())
    }
}

/// Merkle chain errors
#[derive(Debug, Clone)]
pub enum MerkleError {
    InvalidChain {
        position: usize,
        reason: String,
    },
    TamperingDetected {
        position: usize,
        expected: String,
        computed: String,
    },
}

impl fmt::Display for MerkleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MerkleError::InvalidChain { position, reason } => {
                write!(f, "Invalid chain at position {}: {}", position, reason)
            }
            MerkleError::TamperingDetected {
                position,
                expected,
                computed,
            } => {
                write!(
                    f,
                    "Tampering detected at position {}: expected hash {}, computed {}",
                    position, expected, computed
                )
            }
        }
    }
}

impl std::error::Error for MerkleError {}

/// Fast CRC32 checksum for data integrity
#[inline]
pub fn crc32_checksum(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify CRC32 checksum
#[inline]
pub fn verify_crc32(data: &[u8], expected: u32) -> bool {
    crc32_checksum(data) == expected
}

/// BLAKE3 hash (fast and secure)
#[inline]
pub fn blake3_hash(data: &[u8]) -> Vec<u8> {
    blake3::hash(data).as_bytes().to_vec()
}

/// BLAKE3 hash as hex string
#[inline]
pub fn blake3_hash_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_chain() {
        let mut chain = MerkleChain::new();

        let node1 = chain.add(b"entry1");
        let node2 = chain.add(b"entry2");
        let node3 = chain.add(b"entry3");

        // Verify chain is valid
        assert!(MerkleChain::verify_chain(&[node1.clone(), node2.clone(), node3.clone()]).is_ok());

        // Tamper with data
        let mut tampered = node2.clone();
        tampered.data_hash = "tampered".to_string();

        // Chain should be invalid
        assert!(MerkleChain::verify_chain(&[node1, tampered, node3]).is_err());
    }

    #[test]
    fn test_checksums() {
        let data = b"test data";
        let checksum = crc32_checksum(data);
        assert!(verify_crc32(data, checksum));
        assert!(!verify_crc32(b"tampered", checksum));
    }

    #[test]
    fn test_blake3() {
        let hash1 = blake3_hash(b"hello");
        let hash2 = blake3_hash(b"hello");
        let hash3 = blake3_hash(b"world");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }
}
