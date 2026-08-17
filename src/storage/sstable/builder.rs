use std::io::Write;

use crate::core::error::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::{BufMut, Bytes, BytesMut};

use super::codec::CountingWriter;

trait EntryEncodingSink {
    fn put_u32_le(&mut self, value: u32);
    fn put_u64_le(&mut self, value: u64);
    fn put_u8(&mut self, value: u8);
    fn put_slice(&mut self, value: &[u8]);
}

impl EntryEncodingSink for BytesMut {
    fn put_u32_le(&mut self, value: u32) {
        BufMut::put_u32_le(self, value);
    }

    fn put_u64_le(&mut self, value: u64) {
        BufMut::put_u64_le(self, value);
    }

    fn put_u8(&mut self, value: u8) {
        BufMut::put_u8(self, value);
    }

    fn put_slice(&mut self, value: &[u8]) {
        BufMut::put_slice(self, value);
    }
}

#[derive(Default)]
struct CountingEntrySink {
    bytes_written: usize,
}

impl EntryEncodingSink for CountingEntrySink {
    fn put_u32_le(&mut self, _value: u32) {
        self.bytes_written = self
            .bytes_written
            .saturating_add(std::mem::size_of::<u32>());
    }

    fn put_u64_le(&mut self, _value: u64) {
        self.bytes_written = self
            .bytes_written
            .saturating_add(std::mem::size_of::<u64>());
    }

    fn put_u8(&mut self, _value: u8) {
        self.bytes_written = self.bytes_written.saturating_add(std::mem::size_of::<u8>());
    }

    fn put_slice(&mut self, value: &[u8]) {
        self.bytes_written = self.bytes_written.saturating_add(value.len());
    }
}

fn write_versioned_entry<S: EntryEncodingSink>(
    output: &mut S,
    key: &[u8],
    value: Option<&[u8]>,
    sequence: u64,
) {
    output.put_u32_le(key.len() as u32);
    output.put_slice(key);
    output.put_u64_le(sequence);
    output.put_u8(u8::from(value.is_some()));
    output.put_u32_le(value.map_or(0, <[u8]>::len) as u32);
    if let Some(value) = value {
        output.put_slice(value);
    }
}

fn measure_versioned_entry(key: &[u8], value: Option<&[u8]>) -> usize {
    let mut counter = CountingEntrySink::default();
    write_versioned_entry(&mut counter, key, value, 0);
    counter.bytes_written
}

#[derive(Clone)]
pub struct BlockBuilder {
    buffer: BytesMut,
    offsets: Vec<u32>,
    last_key: Option<Bytes>,
    max_size: usize,
}

pub(crate) enum ProjectedBlockSizes {
    Current {
        serialized_size: usize,
    },
    CurrentAndNext {
        current_serialized_size: usize,
        next_serialized_size: usize,
    },
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
        self.add_versioned(key, value, 0)
    }

    /// Add an entry using the current block format.
    pub fn add_versioned(&mut self, key: &[u8], value: Option<&[u8]>, sequence: u64) -> bool {
        let entry_size = measure_versioned_entry(key, value);

        // Check if adding this entry would exceed max size
        // Always allow at least one entry
        if !self.is_empty() && self.buffer.len() + entry_size > self.max_size {
            return false;
        }

        // Record offset
        self.offsets.push(self.buffer.len() as u32);

        write_versioned_entry(&mut self.buffer, key, value, sequence);

        // Update last key
        self.last_key = Some(Bytes::copy_from_slice(key));

        true
    }

    /// Serialized uncompressed block sizes if one current-format entry were
    /// appended, without cloning or encoding the block payload.
    pub(crate) fn projected_versioned_sizes(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> ProjectedBlockSizes {
        let entry_size = measure_versioned_entry(key, value);
        let appended_data_size = self.buffer.len().saturating_add(entry_size);
        if self.is_empty() || appended_data_size <= self.max_size {
            ProjectedBlockSizes::Current {
                serialized_size: Self::finished_size(
                    appended_data_size,
                    self.offsets.len().saturating_add(1),
                ),
            }
        } else {
            ProjectedBlockSizes::CurrentAndNext {
                current_serialized_size: Self::finished_size(self.buffer.len(), self.offsets.len()),
                next_serialized_size: Self::finished_size(entry_size, 1),
            }
        }
    }

    fn finished_size(data_size: usize, entry_count: usize) -> usize {
        data_size
            .saturating_add(entry_count.saturating_mul(std::mem::size_of::<u32>()))
            .saturating_add(std::mem::size_of::<u32>())
    }

    /// Add an entry using the v2 layout, which predates per-entry sequences.
    #[cfg(test)]
    pub(crate) fn add_legacy_v2(&mut self, key: &[u8], value: Option<&[u8]>) -> bool {
        let value_len = value.map_or(0, <[u8]>::len);
        let entry_size = 4 + key.len() + 1 + 4 + value_len;

        if !self.is_empty() && self.buffer.len() + entry_size > self.max_size {
            return false;
        }

        self.offsets.push(self.buffer.len() as u32);
        BufMut::put_u32_le(&mut self.buffer, key.len() as u32);
        BufMut::put_slice(&mut self.buffer, key);
        BufMut::put_u8(&mut self.buffer, u8::from(value.is_some()));
        BufMut::put_u32_le(&mut self.buffer, value_len as u32);
        if let Some(value) = value {
            BufMut::put_slice(&mut self.buffer, value);
        }
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
        let mut result =
            Vec::with_capacity(Self::finished_size(self.buffer.len(), self.offsets.len()));

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
    encoded_entries_size: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub(crate) last_key: Bytes,
    pub(crate) block_offset: u64,
    pub(crate) block_size: u32,
}

pub(crate) struct ProjectedIndexEntry<'a> {
    pub(crate) last_key: &'a [u8],
    pub(crate) block_offset: u64,
    pub(crate) block_size: u32,
}

impl IndexBuilder {
    /// Create a new index builder
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            encoded_entries_size: 0,
        }
    }

    /// Add an index entry
    pub fn add(&mut self, last_key: &[u8], block_offset: u64, block_size: u32) -> Result<()> {
        let entry = IndexEntry {
            last_key: Bytes::copy_from_slice(last_key),
            block_offset,
            block_size,
        };
        let mut counter = CountingWriter::with_bytes_written(self.encoded_entries_size);
        Self::write_entry(&mut counter, &entry)?;
        self.encoded_entries_size = counter.bytes_written();
        self.entries.push(entry);
        Ok(())
    }

    /// Finish building the index and return the data
    pub fn finish(&mut self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.serialized_size() as usize);
        self.write_to(&mut buffer)
            .expect("writing an index to memory cannot fail");
        buffer
    }

    /// Get the index entries (for reading)
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Serialized size including the trailing entry count.
    pub(crate) fn serialized_size(&self) -> u64 {
        self.serialized_size_with(&[])
    }

    /// Exact encoded size after appending projected block-index entries.
    pub(crate) fn serialized_size_with(&self, additional: &[ProjectedIndexEntry<'_>]) -> u64 {
        let mut counter = CountingWriter::with_bytes_written(self.encoded_entries_size);
        for entry in additional {
            Self::write_entry_fields(
                &mut counter,
                entry.last_key,
                entry.block_offset,
                entry.block_size,
            )
            .expect("counting an index entry cannot fail");
        }
        Self::write_entry_count(
            &mut counter,
            self.entries.len().saturating_add(additional.len()),
        )
        .expect("counting an index entry count cannot fail");
        counter.bytes_written()
    }

    fn write_to<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        for entry in &self.entries {
            Self::write_entry(output, entry)?;
        }
        Self::write_entry_count(output, self.entries.len())
    }

    fn write_entry<W: Write>(output: &mut W, entry: &IndexEntry) -> std::io::Result<()> {
        Self::write_entry_fields(
            output,
            &entry.last_key,
            entry.block_offset,
            entry.block_size,
        )
    }

    fn write_entry_fields<W: Write>(
        output: &mut W,
        last_key: &[u8],
        block_offset: u64,
        block_size: u32,
    ) -> std::io::Result<()> {
        output.write_u32::<LittleEndian>(last_key.len() as u32)?;
        output.write_all(last_key)?;
        output.write_u64::<LittleEndian>(block_offset)?;
        output.write_u32::<LittleEndian>(block_size)
    }

    fn write_entry_count<W: Write>(output: &mut W, entry_count: usize) -> std::io::Result<()> {
        output.write_u32::<LittleEndian>(entry_count as u32)
    }
}
