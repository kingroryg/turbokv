use crate::core::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Zstd = 1,
    Snappy = 2,
    Lz4 = 3,
}

impl TryFrom<u8> for CompressionType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Zstd),
            2 => Ok(CompressionType::Snappy),
            3 => Ok(CompressionType::Lz4),
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
            let compressed = zstd::encode_all(data, 3).map_err(|e| Error::SSTable {
                message: format!("Zstd compression failed: {}", e),
                source: None,
            })?;
            Ok(compressed)
        }
        CompressionType::Snappy => {
            let compressed =
                snap::raw::Encoder::new()
                    .compress_vec(data)
                    .map_err(|e| Error::SSTable {
                        message: format!("Snappy compression failed: {}", e),
                        source: None,
                    })?;
            Ok(compressed)
        }
        CompressionType::Lz4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            Ok(compressed)
        }
    }
}

/// Maximum bytes the configured block compressor can emit for an input size.
pub(crate) fn max_compressed_block_size(input_size: usize, compression: CompressionType) -> usize {
    match compression {
        CompressionType::None => input_size,
        CompressionType::Zstd => zstd::zstd_safe::compress_bound(input_size),
        CompressionType::Snappy => snap::raw::max_compress_len(input_size),
        CompressionType::Lz4 => lz4_flex::block::get_maximum_output_size(input_size)
            .saturating_add(std::mem::size_of::<u32>()),
    }
}

pub fn decompress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            let decompressed = zstd::decode_all(data).map_err(|e| Error::SSTable {
                message: format!("Zstd decompression failed: {}", e),
                source: None,
            })?;
            Ok(decompressed)
        }
        CompressionType::Snappy => {
            let decompressed = snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| Error::SSTable {
                    message: format!("Snappy decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
        CompressionType::Lz4 => {
            let decompressed =
                lz4_flex::decompress_size_prepended(data).map_err(|e| Error::SSTable {
                    message: format!("LZ4 decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
    }
}
