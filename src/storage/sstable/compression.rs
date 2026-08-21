use std::io::Read;

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

/// Decode a block without imposing a limit below what the existing format can
/// represent. The format stores no decoded-size bound, so a smaller ceiling
/// would make previously valid, highly compressible tables unreadable. Codec
/// declarations are still checked for platform representation and impossible
/// expansion ratios before decoders can allocate from them.
pub fn decompress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(data) {
                usize::try_from(size).map_err(|_| Error::SSTable {
                    message: format!(
                        "Zstd declared block size {size} cannot be represented on this platform"
                    ),
                    source: None,
                })?;
            }
            let mut decoder =
                zstd::stream::read::Decoder::new(data).map_err(|e| Error::SSTable {
                    message: format!("Zstd decompression failed: {}", e),
                    source: None,
                })?;
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::SSTable {
                    message: format!("Zstd decompression failed: {e}"),
                    source: None,
                })?;
            Ok(decompressed)
        }
        CompressionType::Snappy => {
            let size = snap::raw::decompress_len(data).map_err(|e| Error::SSTable {
                message: format!("Snappy decompression failed: {e}"),
                source: None,
            })?;
            reject_implausible_decompression(size, data.len(), 256, compression)?;
            let decompressed = snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| Error::SSTable {
                    message: format!("Snappy decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
        CompressionType::Lz4 => {
            let declared_size = data
                .get(..4)
                .map(|size| u32::from_le_bytes(size.try_into().expect("four-byte slice")) as usize)
                .unwrap_or(0);
            reject_implausible_decompression(
                declared_size,
                data.len().saturating_sub(4),
                512,
                compression,
            )?;
            let decompressed =
                lz4_flex::decompress_size_prepended(data).map_err(|e| Error::SSTable {
                    message: format!("LZ4 decompression failed: {}", e),
                    source: None,
                })?;
            Ok(decompressed)
        }
    }
}

fn reject_implausible_decompression(
    declared_size: usize,
    payload_size: usize,
    conservative_max_ratio: usize,
    compression: CompressionType,
) -> Result<()> {
    let plausible_size = payload_size
        .saturating_mul(conservative_max_ratio)
        .saturating_add(64);
    if declared_size > plausible_size {
        return Err(Error::SSTable {
            message: format!(
                "{compression:?} declared block size {declared_size} is implausible for {payload_size} compressed bytes"
            ),
            source: None,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn compression_type_strategy(
) -> impl proptest::strategy::Strategy<Value = CompressionType> {
    proptest::sample::select(vec![
        CompressionType::None,
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ])
}

#[cfg(test)]
fn compressed_type_strategy() -> impl proptest::strategy::Strategy<Value = CompressionType> {
    proptest::sample::select(vec![
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ])
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn every_compression_mode_round_trips_arbitrary_bytes(
            data in prop::collection::vec(any::<u8>(), 0..16 * 1024),
            compression in compression_type_strategy(),
        ) {
            let input_len = data.len();
            let compressed = compress_block(&data, compression).unwrap();
            let decompressed = decompress_block(&compressed, compression).unwrap();
            prop_assert_eq!(
                decompressed,
                data,
                "compression={:?}, input_len={}",
                compression,
                input_len
            );
        }
    }

    #[test]
    fn retained_codecs_reject_truncated_streams_without_panicking() {
        let data = (0..32 * 1024)
            .map(|offset| (offset * 31 + offset / 7) as u8)
            .collect::<Vec<_>>();
        for compression in [
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let mut compressed = compress_block(&data, compression).unwrap();
            compressed.pop().unwrap();
            let result = std::panic::catch_unwind(|| decompress_block(&compressed, compression));
            assert!(
                result.is_ok(),
                "compression={compression:?}: decoder panicked"
            );
            assert!(
                result.unwrap().is_err(),
                "compression={compression:?}: truncated stream decoded"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn bounded_arbitrary_malformed_codec_input_never_panics(
            data in prop::collection::vec(any::<u8>(), 0..512),
            compression in compressed_type_strategy(),
        ) {
            let result = std::panic::catch_unwind(|| decompress_block(&data, compression));
            prop_assert!(result.is_ok(), "compression={compression:?}, data={data:?}");
            let _ = result.unwrap();
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn mutated_codec_streams_never_panic(
            data in prop::collection::vec(any::<u8>(), 0..8 * 1024),
            compression in compressed_type_strategy(),
            mutation in any::<usize>(),
            mask in 1_u8..=u8::MAX,
        ) {
            let mut compressed = compress_block(&data, compression).unwrap();
            let index = mutation % compressed.len();
            compressed[index] ^= mask;
            let result = std::panic::catch_unwind(|| decompress_block(&compressed, compression));
            prop_assert!(
                result.is_ok(),
                "compression={compression:?}, input_len={}, mutation={index}, mask={mask}",
                data.len()
            );
            let _ = result.unwrap();
        }
    }

    #[test]
    fn hostile_size_headers_are_rejected_before_allocation() {
        let snappy_bomb = [0xff, 0xff, 0xff, 0xff, 0x0f];
        let mut lz4_bomb = u32::MAX.to_le_bytes().to_vec();
        lz4_bomb.push(0);
        for (compression, data) in [
            (CompressionType::Snappy, snappy_bomb.as_slice()),
            (CompressionType::Lz4, lz4_bomb.as_slice()),
        ] {
            let error = decompress_block(data, compression).unwrap_err();
            assert!(
                error.to_string().contains("implausible"),
                "compression={compression:?}: {error}"
            );
        }
        assert!(decompress_block(&[0; 16], CompressionType::Zstd).is_err());
    }

    #[test]
    #[ignore = "extended arbitrary-codec stress; run explicitly before releases"]
    fn heavy_arbitrary_compression_roundtrip() {
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(2_048));
        let strategy = (
            prop::collection::vec(any::<u8>(), 0..256 * 1024),
            compression_type_strategy(),
        );
        runner
            .run(&strategy, |(data, compression)| {
                let compressed = compress_block(&data, compression).unwrap();
                prop_assert_eq!(decompress_block(&compressed, compression).unwrap(), data);
                Ok(())
            })
            .unwrap();
    }
}
