use crate::core::error::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::{BufMut, Bytes, BytesMut};

pub struct BlockBuilder {
    buffer: BytesMut,
    offsets: Vec<u32>,
    last_key: Option<Bytes>,
    max_size: usize,
}

impl BlockBuilder {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(max_size),
            offsets: Vec::new(),
            last_key: None,
            max_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> bool {
        let value_len = value.map_or(0, <[u8]>::len);
        // key_len + key + value_marker + value_len + value
        let entry_size = 4 + key.len() + 1 + 4 + value_len;

        // Check if adding this entry would exceed max size
        // Always allow at least one entry
        if !self.is_empty() && self.buffer.len() + entry_size > self.max_size {
            return false;
        }

        // Record offset
        self.offsets.push(self.buffer.len() as u32);

        // Write key length and key
        self.buffer.put_u32_le(key.len() as u32);
        self.buffer.put_slice(key);

        // Write value marker: 0 = tombstone, 1 = present value.
        self.buffer.put_u8(u8::from(value.is_some()));

        // Write value length and value
        self.buffer.put_u32_le(value_len as u32);
        if let Some(value) = value {
            self.buffer.put_slice(value);
        }

        // Update last key
        self.last_key = Some(Bytes::copy_from_slice(key));

        true
    }

    /// Check if the block is empty
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Get the last key in the block
    pub fn last_key(&self) -> Option<Bytes> {
        self.last_key.clone()
    }

    /// Get current size of the block
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Finish building the block and return the data
    pub fn finish(&mut self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.buffer.len() + self.offsets.len() * 4 + 4);

        // Write data
        result.extend_from_slice(&self.buffer);

        // Write offsets
        for offset in &self.offsets {
            result.write_u32::<LittleEndian>(*offset).unwrap();
        }

        // Write number of entries
        result
            .write_u32::<LittleEndian>(self.offsets.len() as u32)
            .unwrap();

        // Reset for reuse
        self.buffer.clear();
        self.offsets.clear();
        self.last_key = None;

        result
    }
}

/// Builder for SSTable index
pub struct IndexBuilder {
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub(crate) last_key: Bytes,
    pub(crate) block_offset: u64,
    pub(crate) block_size: u32,
}

impl IndexBuilder {
    /// Create a new index builder
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add an index entry
    pub fn add(&mut self, last_key: &[u8], block_offset: u64, block_size: u32) -> Result<()> {
        self.entries.push(IndexEntry {
            last_key: Bytes::copy_from_slice(last_key),
            block_offset,
            block_size,
        });
        Ok(())
    }

    /// Finish building the index and return the data
    pub fn finish(&mut self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write each index entry
        for entry in &self.entries {
            // Write key length and key
            buffer
                .write_u32::<LittleEndian>(entry.last_key.len() as u32)
                .unwrap();
            buffer.extend_from_slice(&entry.last_key);

            // Write offset and size
            buffer
                .write_u64::<LittleEndian>(entry.block_offset)
                .unwrap();
            buffer.write_u32::<LittleEndian>(entry.block_size).unwrap();
        }

        // Write number of entries
        buffer
            .write_u32::<LittleEndian>(self.entries.len() as u32)
            .unwrap();

        buffer
    }

    /// Get the index entries (for reading)
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }
}
