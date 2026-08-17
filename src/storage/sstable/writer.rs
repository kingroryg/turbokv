//! SSTable writer implementation

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use tracing::info;

use super::codec::CountingWriter;
#[cfg(test)]
use super::types::SSTABLE_VERSION_V2;
use super::{
    compress_block, max_compressed_block_size, BlockBuilder, BloomFilter, CompressionType,
    IndexBuilder, IndexEntry, ProjectedBlockSizes, ProjectedIndexEntry, SSTableConfig, SSTableInfo,
    SSTABLE_MAGIC, SSTABLE_VERSION,
};
use crate::core::error::{Error, Result};

struct EncodedBlock {
    compressed: Vec<u8>,
    compression: CompressionType,
    checksum: u32,
}

impl EncodedBlock {
    fn from_builder(builder: &mut BlockBuilder, compression: CompressionType) -> Result<Self> {
        let compressed = compress_block(&builder.finish(), compression)?;
        let checksum = crc32fast::hash(&compressed);
        Ok(Self {
            compressed,
            compression,
            checksum,
        })
    }

    fn write_to<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        output.write_all(&self.compressed)?;
        write_block_footer_to(output, self.compression, self.checksum)
    }

    fn serialized_size(&self) -> u64 {
        let mut counter = CountingWriter::default();
        self.write_to(&mut counter)
            .expect("counting an encoded block cannot fail");
        counter.bytes_written()
    }

    fn serialized_size_upper_bound(uncompressed_size: usize, compression: CompressionType) -> u64 {
        let compressed_size = max_compressed_block_size(uncompressed_size, compression);
        let mut counter = CountingWriter::with_bytes_written(compressed_size as u64);
        write_block_footer_to(&mut counter, compression, 0)
            .expect("counting an encoded block upper bound cannot fail");
        counter.bytes_written()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputAppendDecision {
    Append,
    AppendAndSeal,
    SplitBefore,
}

struct EncodedFooter {
    index_offset: u64,
    index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    format_version: u32,
}

impl EncodedFooter {
    fn write_to<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        output.write_u64::<LittleEndian>(self.index_offset)?;
        output.write_u32::<LittleEndian>(self.index_size)?;
        output.write_u64::<LittleEndian>(self.bloom_offset)?;
        output.write_u32::<LittleEndian>(self.bloom_size)?;
        output.write_all(SSTABLE_MAGIC)?;
        output.write_u32::<LittleEndian>(self.format_version)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.index_offset.to_le_bytes());
        hasher.update(&self.index_size.to_le_bytes());
        hasher.update(&self.bloom_offset.to_le_bytes());
        hasher.update(&self.bloom_size.to_le_bytes());
        hasher.update(SSTABLE_MAGIC);
        hasher.update(&self.format_version.to_le_bytes());
        output.write_u32::<LittleEndian>(hasher.finalize())
    }

    fn serialized_size(&self) -> u64 {
        let mut counter = CountingWriter::default();
        self.write_to(&mut counter)
            .expect("counting an SSTable footer cannot fail");
        counter.bytes_written()
    }
}

/// SSTable writer
pub struct SSTableWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    config: SSTableConfig,
    current_block: BlockBuilder,
    index_builder: IndexBuilder,
    bloom_filter: BloomFilter,
    entry_count: u64,
    tombstone_count: u64,
    file_offset: u64,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
    min_sequence: Option<u64>,
    max_sequence: Option<u64>,
    format_version: u32,
    bloom_serialized_size: u64,
    footer_serialized_size: u64,
    #[cfg(test)]
    exact_projection_counter: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl SSTableWriter {
    /// Create new SSTable writer
    pub fn new(path: impl AsRef<Path>, config: SSTableConfig) -> Result<Self> {
        Self::new_with_format(path, config, SSTABLE_VERSION)
    }

    fn new_with_format(
        path: impl AsRef<Path>,
        config: SSTableConfig,
        format_version: u32,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let writer = BufWriter::new(file);
        let bloom_filter = BloomFilter::with_rate(0.01, 10000); // 1% false positive rate
        let bloom_serialized_size = measure_bloom_filter(&bloom_filter, config.bloom_bits_per_key);
        let footer_serialized_size = EncodedFooter {
            index_offset: 0,
            index_size: 0,
            bloom_offset: 0,
            bloom_size: 0,
            format_version,
        }
        .serialized_size();

        Ok(Self {
            path,
            writer,
            config: config.clone(),
            current_block: BlockBuilder::new(config.block_size),
            index_builder: IndexBuilder::new(),
            bloom_filter,
            entry_count: 0,
            tombstone_count: 0,
            file_offset: 0,
            min_key: None,
            max_key: None,
            min_sequence: None,
            max_sequence: None,
            format_version,
            bloom_serialized_size,
            footer_serialized_size,
            #[cfg(test)]
            exact_projection_counter: None,
        })
    }

    /// Create a v2 table for on-disk compatibility tests.
    #[cfg(test)]
    pub(crate) fn new_legacy_v2(path: impl AsRef<Path>, config: SSTableConfig) -> Result<Self> {
        Self::new_with_format(path, config, SSTABLE_VERSION_V2)
    }

    /// Add key-value pair (`None` represents a tombstone/deletion).
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        self.add_versioned(key, value, 0)
    }

    /// Add a key-value version with its engine-wide sequence number.
    pub(crate) fn add_versioned(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
    ) -> Result<()> {
        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(Bytes::copy_from_slice(key));
        }
        self.max_key = Some(Bytes::copy_from_slice(key));
        self.min_sequence = Some(self.min_sequence.map_or(sequence, |min| min.min(sequence)));
        self.max_sequence = Some(self.max_sequence.map_or(sequence, |max| max.max(sequence)));

        // Add to bloom filter
        self.bloom_filter.insert(key);

        // Add to current block
        if !self.add_to_current_block(key, value, sequence) {
            // Block is full, flush it
            self.flush_block()?;

            // Try again with new block
            if !self.add_to_current_block(key, value, sequence) {
                return Err(Error::SSTable {
                    message: "Entry too large for block".to_string(),
                    source: None,
                });
            }
        }

        self.entry_count += 1;
        if value.is_none() {
            self.tombstone_count += 1;
        }
        Ok(())
    }

    /// Decide whether a compaction winner belongs in this output.
    ///
    /// A compressor-provided upper bound handles the common case without
    /// cloning or recompressing the current block. The first conservative
    /// crossing performs one exact projection: an entry that really fits is
    /// appended and seals the output, while an entry that does not starts the
    /// next output. Exact recompression is therefore bounded to once per
    /// output.
    pub(crate) fn decide_target_size(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        target_size: u64,
    ) -> Result<OutputAppendDecision> {
        let upper_bound = self.projected_size_upper_bound(key, value)?;
        if upper_bound <= target_size {
            return Ok(OutputAppendDecision::Append);
        }
        if self.config.compression == CompressionType::None {
            return Ok(OutputAppendDecision::SplitBefore);
        }

        #[cfg(test)]
        if let Some(counter) = &self.exact_projection_counter {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if self.projected_size_after(key, value, sequence)? > target_size {
            Ok(OutputAppendDecision::SplitBefore)
        } else {
            Ok(OutputAppendDecision::AppendAndSeal)
        }
    }

    /// Return the exact final file size if `key` were appended next.
    pub(crate) fn projected_size_after(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
    ) -> Result<u64> {
        let mut block = self.current_block.clone();
        let mut projected_index_entries = Vec::with_capacity(2);
        let mut file_offset = self.file_offset;

        if !block.add_versioned(key, value, sequence) {
            Self::project_block(
                &mut block,
                &mut projected_index_entries,
                &mut file_offset,
                &self.config,
            )?;
            if !block.add_versioned(key, value, sequence) {
                return Err(Error::SSTable {
                    message: "Entry too large for block".to_string(),
                    source: None,
                });
            }
        }
        Self::project_block(
            &mut block,
            &mut projected_index_entries,
            &mut file_offset,
            &self.config,
        )?;

        let projected_index_entries = projected_index_entries
            .iter()
            .map(|entry| ProjectedIndexEntry {
                last_key: &entry.last_key,
                block_offset: entry.block_offset,
                block_size: entry.block_size,
            })
            .collect::<Vec<_>>();
        let index_size = encoded_component_size(
            self.index_builder
                .serialized_size_with(&projected_index_entries),
            "index",
        )?;
        Ok(file_offset
            .saturating_add(u64::from(index_size))
            .saturating_add(self.bloom_serialized_size)
            .saturating_add(self.footer_serialized_size))
    }

    fn projected_size_upper_bound(&self, key: &[u8], value: Option<&[u8]>) -> Result<u64> {
        match self.current_block.projected_versioned_sizes(key, value) {
            ProjectedBlockSizes::Current { serialized_size } => {
                let block_size = EncodedBlock::serialized_size_upper_bound(
                    serialized_size,
                    self.config.compression,
                );
                let projected_index = [ProjectedIndexEntry {
                    last_key: key,
                    block_offset: self.file_offset,
                    block_size: encoded_component_size(block_size, "data block upper bound")?,
                }];
                self.complete_projected_size_upper_bound(
                    self.file_offset.saturating_add(block_size),
                    &projected_index,
                )
            }
            ProjectedBlockSizes::CurrentAndNext {
                current_serialized_size,
                next_serialized_size,
            } => {
                let current_last_key = self
                    .current_block
                    .last_key()
                    .expect("a split projection has a nonempty current block");
                let current_block_size = EncodedBlock::serialized_size_upper_bound(
                    current_serialized_size,
                    self.config.compression,
                );
                let next_block_offset = self.file_offset.saturating_add(current_block_size);
                let next_block_size = EncodedBlock::serialized_size_upper_bound(
                    next_serialized_size,
                    self.config.compression,
                );
                let projected_index = [
                    ProjectedIndexEntry {
                        last_key: &current_last_key,
                        block_offset: self.file_offset,
                        block_size: encoded_component_size(
                            current_block_size,
                            "data block upper bound",
                        )?,
                    },
                    ProjectedIndexEntry {
                        last_key: key,
                        block_offset: next_block_offset,
                        block_size: encoded_component_size(
                            next_block_size,
                            "data block upper bound",
                        )?,
                    },
                ];
                self.complete_projected_size_upper_bound(
                    next_block_offset.saturating_add(next_block_size),
                    &projected_index,
                )
            }
        }
    }

    fn complete_projected_size_upper_bound(
        &self,
        data_end: u64,
        projected_index: &[ProjectedIndexEntry<'_>],
    ) -> Result<u64> {
        let index_size = encoded_component_size(
            self.index_builder.serialized_size_with(projected_index),
            "index upper bound",
        )?;
        Ok(data_end
            .saturating_add(u64::from(index_size))
            .saturating_add(self.bloom_serialized_size)
            .saturating_add(self.footer_serialized_size))
    }

    #[cfg(test)]
    pub(crate) fn set_exact_projection_counter(
        &mut self,
        counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        self.exact_projection_counter = Some(counter);
    }

    /// Whether no entries have been appended to this writer.
    pub(crate) fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    fn project_block(
        block: &mut BlockBuilder,
        projected_index_entries: &mut Vec<IndexEntry>,
        file_offset: &mut u64,
        config: &SSTableConfig,
    ) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        let last_key = block
            .last_key()
            .expect("a nonempty projected block has a last key");
        let encoded = EncodedBlock::from_builder(block, config.compression)?;
        let block_size = encoded.serialized_size();
        projected_index_entries.push(IndexEntry {
            last_key,
            block_offset: *file_offset,
            block_size: encoded_component_size(block_size, "data block")?,
        });
        *file_offset = (*file_offset).saturating_add(block_size);
        Ok(())
    }

    fn add_to_current_block(&mut self, key: &[u8], value: Option<&[u8]>, sequence: u64) -> bool {
        #[cfg(test)]
        if self.format_version == SSTABLE_VERSION_V2 {
            return self.current_block.add_legacy_v2(key, value);
        }

        self.current_block.add_versioned(key, value, sequence)
    }

    /// Flush current block to disk
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Save the last key before finishing the block
        let last_key = self.current_block.last_key();

        let encoded = EncodedBlock::from_builder(&mut self.current_block, self.config.compression)?;
        let block_offset = self.file_offset;
        let block_size = encoded.serialized_size();
        let indexed_block_size = encoded_component_size(block_size, "data block")?;
        encoded.write_to(&mut self.writer)?;

        self.file_offset = self.file_offset.saturating_add(block_size);

        // Always add the last key of the block to the index
        if let Some(key) = last_key {
            self.index_builder
                .add(&key, block_offset, indexed_block_size)?;
        }

        // Reset block
        self.current_block = BlockBuilder::new(self.config.block_size);

        Ok(())
    }

    /// Finish writing SSTable
    pub fn finish(mut self) -> Result<SSTableInfo> {
        // Flush any remaining data
        self.flush_block()?;

        // Ensure we have at least one index entry
        if self.index_builder.entries().is_empty() && self.entry_count > 0 {
            // This shouldn't happen, but if it does, we have a problem
            return Err(Error::SSTable {
                message: "No index entries created for non-empty SSTable".to_string(),
                source: None,
            });
        }

        // Write index block
        let index_offset = self.file_offset;
        let index_data = self.index_builder.finish();
        self.writer.write_all(&index_data)?;
        let index_size = encoded_component_size(index_data.len() as u64, "index")?;
        self.file_offset = self.file_offset.saturating_add(u64::from(index_size));

        // Write bloom filter
        let bloom_offset = self.file_offset;
        let bloom_size = encoded_component_size(self.bloom_serialized_size, "bloom")?;
        write_bloom_filter_to(
            &self.bloom_filter,
            self.config.bloom_bits_per_key,
            &mut self.writer,
        )?;
        self.file_offset = self.file_offset.saturating_add(u64::from(bloom_size));

        // Write footer
        let footer = EncodedFooter {
            index_offset,
            index_size,
            bloom_offset,
            bloom_size,
            format_version: self.format_version,
        };
        footer.write_to(&mut self.writer)?;
        debug_assert_eq!(footer.serialized_size(), self.footer_serialized_size);
        let file_size = self.file_offset.saturating_add(self.footer_serialized_size);

        self.writer.flush()?;
        // Fsync to ensure SSTable data is on disk before manifest references it
        self.writer.get_ref().sync_all()?;

        info!(
            "Finished writing SSTable: {} entries, {} bytes",
            self.entry_count, file_size
        );

        Ok(SSTableInfo {
            id: 0, // Caller should set the proper id
            path: self.path,
            file_size,
            entry_count: self.entry_count,
            tombstone_count: self.tombstone_count,
            min_key: self.min_key.unwrap_or_default().to_vec(),
            max_key: self.max_key.unwrap_or_default().to_vec(),
            creation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            level: 0,
            min_sequence: self.min_sequence.unwrap_or(0),
            max_sequence: self.max_sequence.unwrap_or(0),
        })
    }
}

fn write_block_footer_to<W: Write>(
    output: &mut W,
    compression: CompressionType,
    checksum: u32,
) -> std::io::Result<()> {
    output.write_u8(compression as u8)?;
    output.write_u32::<LittleEndian>(checksum)
}

fn measure_bloom_filter(bloom_filter: &BloomFilter, bloom_bits_per_key: usize) -> u64 {
    let mut counter = CountingWriter::default();
    write_bloom_filter_to(bloom_filter, bloom_bits_per_key, &mut counter)
        .expect("counting a bloom filter cannot fail");
    counter.bytes_written()
}

fn write_bloom_filter_to<W: Write>(
    bloom_filter: &BloomFilter,
    bloom_bits_per_key: usize,
    output: &mut W,
) -> std::io::Result<()> {
    output.write_all(bloom_filter.as_bytes())?;
    let (num_hash_functions, num_bits) = bloom_filter.metadata();
    output.write_u32::<LittleEndian>(num_hash_functions as u32)?;
    output.write_u32::<LittleEndian>(num_bits as u32)?;
    output.write_u32::<LittleEndian>(bloom_bits_per_key as u32)
}

fn encoded_component_size(size: u64, component: &str) -> Result<u32> {
    u32::try_from(size).map_err(|_| Error::SSTable {
        message: format!("SSTable {component} exceeds the on-disk u32 size limit"),
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::storage::sstable::CompressionType;

    fn entry(index: usize) -> (Vec<u8>, Option<Vec<u8>>, u64) {
        let key = vec![index as u8, 0, 0xff, (index * 17) as u8];
        let value = (index % 5 != 0).then(|| {
            (0..73)
                .map(|offset| (index.wrapping_mul(31).wrapping_add(offset)) as u8)
                .collect()
        });
        (key, value, index as u64 + 1)
    }

    #[test]
    fn projected_size_matches_finished_writer_for_all_compression_modes() {
        let directory = TempDir::new().unwrap();
        for compression in [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let config = SSTableConfig {
                block_size: 96,
                compression,
                ..SSTableConfig::default()
            };
            for prefix_len in 1..=16 {
                let path = directory
                    .path()
                    .join(format!("{compression:?}-{prefix_len}.sst"));
                let mut writer = SSTableWriter::new(path, config.clone()).unwrap();
                for index in 0..prefix_len - 1 {
                    let (key, value, sequence) = entry(index);
                    writer
                        .add_versioned(&key, value.as_deref(), sequence)
                        .unwrap();
                }
                let (key, value, sequence) = entry(prefix_len - 1);
                let projected = writer
                    .projected_size_after(&key, value.as_deref(), sequence)
                    .unwrap();
                writer
                    .add_versioned(&key, value.as_deref(), sequence)
                    .unwrap();
                assert_eq!(writer.finish().unwrap().file_size, projected);
            }
        }
    }
}
