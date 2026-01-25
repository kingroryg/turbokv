use crate::core::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Zstd = 1,
    Snappy = 2,
}

impl TryFrom<u8> for CompressionType {
    type Error = Error;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Zstd),
            2 => Ok(CompressionType::Snappy),
            _ => Err(Error::SSTable {
                message: format!("Invalid compression type: {}", value),
                source: None,
            }),
        }
    }
}

pub fn compress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            let compressed = zstd::encode_all(data, 3)
                .map_err(|e| Error::SSTable {
                    message: format!("Zstd compression failed: {}", e),
                    source: None,
                })?;
            Ok(compressed)
        }
        CompressionType::Snappy => {
            let compressed = snap::raw::Encoder::new().compress_vec(data)
                .map_err(|e| Error::SSTable {
                    message: format!("Snappy compression failed: {}", e),
                    source: None,
                })?;
            Ok(compressed)
        }
    }
}

pub fn decompress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            let decompressed = zstd::decode_all(data)
                .map_err(|e| Error::SSTable {
                    message: format!("Zstd decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
        CompressionType::Snappy => {
            let decompressed = snap::raw::Decoder::new().decompress_vec(data)
                .map_err(|e| Error::SSTable {
                    message: format!("Snappy decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
    }
}