//! # Data Integrity Primitives
//!
//! Provides fast checksums for detecting accidental data corruption.

use crc32fast::Hasher as Crc32Hasher;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksums() {
        let data = b"test data";
        let checksum = crc32_checksum(data);
        assert!(verify_crc32(data, checksum));
        assert!(!verify_crc32(b"tampered", checksum));
    }
}
