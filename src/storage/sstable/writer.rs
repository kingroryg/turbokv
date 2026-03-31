//! SSTable writer implementation

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use tracing::info;

use super::{
    compress_block, BlockBuilder, BloomFilter, IndexBuilder, SSTableConfig, SSTableInfo,
    FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION,
};
use crate::core::error::{Error, Result};

/// SSTable writer
pub struct SSTableWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    config: SSTableConfig,
    current_block: BlockBuilder,
    index_builder: IndexBuilder,
    bloom_filter: BloomFilter,
    entry_count: u64,
    file_offset: u64,
    min_key: Option<Bytes>,
    max_key: Option<Bytes>,
}

impl SSTableWriter {
    /// Create new SSTable writer
    pub fn new(path: impl AsRef<Path>, config: SSTableConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        let writer = BufWriter::new(file);
        let bloom_filter = BloomFilter::with_rate(0.01, 10000); // 1% false positive rate

        Ok(Self {
            path,
            writer,
            config: config.clone(),
            current_block: BlockBuilder::new(config.block_size),
            index_builder: IndexBuilder::new(),
            bloom_filter,
            entry_count: 0,
            file_offset: 0,
            min_key: None,
            max_key: None,
        })
    }

    /// Add key-value pair (None value represents a tombstone/deletion)
    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(Bytes::copy_from_slice(key));
        }
        self.max_key = Some(Bytes::copy_from_slice(key));

        // Add to bloom filter
        self.bloom_filter.insert(key);

        // For tombstones, use empty value with a marker in the block
        // We use an empty slice to represent tombstones - reader distinguishes via length
        let value_bytes = value.unwrap_or(&[]);

        // Add to current block
        if !self.current_block.add(key, value_bytes) {
            // Block is full, flush it
            self.flush_block()?;

            // Try again with new block
            if !self.current_block.add(key, value_bytes) {
                return Err(Error::SSTable {
                    message: "Entry too large for block".to_string(),
                    source: None,
                });
            }
        }

        self.entry_count += 1;
        Ok(())
    }

    /// Flush current block to disk
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Save the last key before finishing the block
        let last_key = self.current_block.last_key();

        let block_data = self.current_block.finish();
        let compressed = compress_block(&block_data, self.config.compression)?;

        // Write block
        let block_offset = self.file_offset;
        let block_size = compressed.len() + 5; // +5 for footer

        self.writer.write_all(&compressed)?;

        // Write block footer
        self.writer.write_u8(self.config.compression as u8)?;
        self.writer
            .write_u32::<LittleEndian>(crc32fast::hash(&compressed))?;

        self.file_offset += block_size as u64;

        // Always add the last key of the block to the index
        if let Some(key) = last_key {
            self.index_builder
                .add(&key, block_offset, block_size as u32)?;
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
        let index_size = index_data.len() as u32;
        self.file_offset += index_size as u64;

        // Write bloom filter
        let bloom_offset = self.file_offset;
        let bloom_data = self.serialize_bloom_filter()?;
        self.writer.write_all(&bloom_data)?;
        let bloom_size = bloom_data.len() as u32;
        self.file_offset += bloom_size as u64;

        // Write footer
        self.writer.write_u64::<LittleEndian>(index_offset)?;
        self.writer.write_u32::<LittleEndian>(index_size)?;
        self.writer.write_u64::<LittleEndian>(bloom_offset)?;
        self.writer.write_u32::<LittleEndian>(bloom_size)?;
        self.writer.write_all(SSTABLE_MAGIC)?;
        self.writer.write_u32::<LittleEndian>(SSTABLE_VERSION)?;

        // Compute CRC32 over the footer fields (excluding the checksum itself)
        let mut footer_hasher = crc32fast::Hasher::new();
        footer_hasher.update(&index_offset.to_le_bytes());
        footer_hasher.update(&index_size.to_le_bytes());
        footer_hasher.update(&bloom_offset.to_le_bytes());
        footer_hasher.update(&bloom_size.to_le_bytes());
        footer_hasher.update(SSTABLE_MAGIC);
        footer_hasher.update(&SSTABLE_VERSION.to_le_bytes());
        let checksum = footer_hasher.finalize();
        self.writer.write_u32::<LittleEndian>(checksum)?;

        let file_size = self.file_offset + FOOTER_SIZE as u64;

        self.writer.flush()?;

        info!(
            "Finished writing SSTable: {} entries, {} bytes",
            self.entry_count, file_size
        );

        Ok(SSTableInfo {
            id: 0, // Caller should set the proper id
            path: self.path,
            file_size,
            entry_count: self.entry_count,
            min_key: self.min_key.unwrap_or_default().to_vec(),
            max_key: self.max_key.unwrap_or_default().to_vec(),
            creation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            level: 0,
        })
    }

    /// Serialize bloom filter
    fn serialize_bloom_filter(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Write bloom filter data
        buffer.extend_from_slice(self.bloom_filter.as_bytes());

        // Write metadata
        let (num_hash_functions, num_bits) = self.bloom_filter.metadata();
        buffer.write_u32::<LittleEndian>(num_hash_functions as u32)?;
        buffer.write_u32::<LittleEndian>(num_bits as u32)?;
        buffer.write_u32::<LittleEndian>(self.config.bloom_bits_per_key as u32)?;

        Ok(buffer)
    }
}
