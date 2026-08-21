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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksums() {
        let data = b"test data";
        let checksum = crc32_checksum(data);
        assert_eq!(crc32_checksum(data), checksum);
        assert_ne!(crc32_checksum(b"tampered"), checksum);
    }
}
