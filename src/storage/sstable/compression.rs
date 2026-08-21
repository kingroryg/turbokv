use std::io::Read;

use crate::core::error::{Error, Result};

const MAX_BULK_ZSTD_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MAX_BULK_ZSTD_EXPANSION_RATIO: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
/// Compression tags persisted in SSTable data-block footers.
pub enum CompressionType {
    /// Store the block without compression.
    None = 0,
    /// Zstandard level 3.
    Zstd = 1,
    /// Raw Snappy framing.
    Snappy = 2,
    /// LZ4 block encoding with a prepended decoded-size field.
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

/// Allocates and encodes one uncompressed SSTable block.
///
/// The call is synchronous and CPU-bound. `None` still copies the input so the
/// caller always owns the result. Returns an SSTable error if the selected
/// compressor rejects the input.
pub fn compress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            let compressed = zstd::bulk::compress(data, 3).map_err(|e| Error::SSTable {
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

/// Decodes one block into a newly allocated buffer.
///
/// The call is synchronous and CPU-bound. The format stores no independent
/// decoded-size bound, so Zstandard frames with unknown, very large, or highly
/// compressible sizes are decoded incrementally rather than rejected solely by
/// a fixed ceiling. Codec declarations are still checked for platform
/// representation and implausible expansion ratios before eager allocation.
/// Returns an SSTable error for malformed input or decompressor failure.
pub fn decompress_block(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => {
            if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(data) {
                let size = usize::try_from(size).map_err(|_| Error::SSTable {
                    message: format!(
                        "Zstd declared block size {size} cannot be represented on this platform"
                    ),
                    source: None,
                })?;
                if bulk_zstd_size_if_safe(data.len(), size).is_some() {
                    return zstd::bulk::decompress(data, size).map_err(|e| Error::SSTable {
                        message: format!("Zstd decompression failed: {e}"),
                        source: None,
                    });
                }
            }
            // Unknown, very large, or unusually compressible declared sizes
            // use the incremental decoder. This avoids a hostile eager
            // allocation while retaining compatibility with valid old frames
            // of any decoded size or compression ratio.
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

fn bulk_zstd_size_if_safe(payload_size: usize, declared_size: usize) -> Option<usize> {
    let plausible_for_bulk = payload_size
        .saturating_mul(MAX_BULK_ZSTD_EXPANSION_RATIO)
        .saturating_add(64);
    (declared_size <= MAX_BULK_ZSTD_DECOMPRESSED_BYTES && declared_size <= plausible_for_bulk)
        .then_some(declared_size)
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
    use std::io::Write;

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
    fn zstd_bulk_frames_and_legacy_stream_frames_both_round_trip() {
        let data = (0..32 * 1024)
            .map(|offset| (offset * 31 + offset / 7) as u8)
            .collect::<Vec<_>>();

        let bulk = compress_block(&data, CompressionType::Zstd).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&bulk).unwrap(),
            Some(data.len() as u64)
        );
        assert_eq!(
            bulk_zstd_size_if_safe(bulk.len(), data.len()),
            Some(data.len())
        );
        assert_eq!(
            decompress_block(&bulk, CompressionType::Zstd).unwrap(),
            data
        );
        assert_eq!(
            zstd::decode_all(bulk.as_slice()).unwrap(),
            data,
            "legacy streaming readers must accept new bulk frames"
        );

        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.include_contentsize(false).unwrap();
        encoder.write_all(&data).unwrap();
        let legacy = encoder.finish().unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&legacy).unwrap(),
            None
        );
        assert_eq!(
            decompress_block(&legacy, CompressionType::Zstd).unwrap(),
            data
        );
    }

    #[test]
    fn zstd_high_ratio_and_hostile_declared_sizes_avoid_bulk_allocation() {
        let highly_compressible = vec![b'a'; 1024 * 1024];
        let compressed = compress_block(&highly_compressible, CompressionType::Zstd).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&compressed).unwrap(),
            Some(highly_compressible.len() as u64)
        );
        assert_eq!(
            bulk_zstd_size_if_safe(compressed.len(), highly_compressible.len()),
            None
        );
        assert_eq!(
            decompress_block(&compressed, CompressionType::Zstd).unwrap(),
            highly_compressible
        );

        let small = compress_block(b"small", CompressionType::Zstd).unwrap();
        assert_eq!(small[4] & 0x20, 0x20, "bulk frame is single-segment");
        let original_header_bytes = 6;
        let hostile_size = (MAX_BULK_ZSTD_DECOMPRESSED_BYTES as u64) + 1;
        let mut hostile = Vec::with_capacity(small.len() + 7);
        hostile.extend_from_slice(&small[..4]);
        // Eight-byte frame-content-size field plus single-segment flag.
        hostile.push(0xe0);
        hostile.extend_from_slice(&hostile_size.to_le_bytes());
        hostile.extend_from_slice(&small[original_header_bytes..]);
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&hostile).unwrap(),
            Some(hostile_size)
        );
        assert_eq!(
            bulk_zstd_size_if_safe(hostile.len(), hostile_size as usize),
            None
        );
        let result = std::panic::catch_unwind(|| decompress_block(&hostile, CompressionType::Zstd));
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());
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
