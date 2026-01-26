//! SSTable iterator implementation

use std::io::{Cursor, Read, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;

use super::SSTableReader;
use crate::core::error::Result;

/// Iterator over SSTable entries
pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    current_block_idx: usize,
    current_block_data: Option<Vec<u8>>,
    current_block_position: usize,
    current_block_entries: Vec<u32>, // key_offset
    current_entry_idx: usize,
}

impl<'a> SSTableIterator<'a> {
    /// Create new iterator
    pub(crate) fn new(reader: &'a SSTableReader) -> Self {
        Self {
            reader,
            current_block_idx: 0,
            current_block_data: None,
            current_block_position: 0,
            current_block_entries: Vec::new(),
            current_entry_idx: 0,
        }
    }

    /// Load next block
    fn load_next_block(&mut self) -> Result<bool> {
        let index_entries = self.reader.index().entries();

        if self.current_block_idx >= index_entries.len() {
            return Ok(false);
        }

        let entry = &index_entries[self.current_block_idx];
        let block_data = self
            .reader
            .read_block(entry.block_offset, entry.block_size)?;

        // Parse block structure to get entry offsets
        self.parse_block_structure(&block_data)?;

        self.current_block_data = Some(block_data);
        self.current_block_position = 0;
        self.current_entry_idx = 0;
        self.current_block_idx += 1;

        Ok(true)
    }

    /// Parse block structure to extract entry offsets
    fn parse_block_structure(&mut self, block_data: &[u8]) -> Result<()> {
        self.current_block_entries.clear();

        let data_len = block_data.len();
        if data_len < 4 {
            return Ok(());
        }

        // Read number of entries from the end
        let mut cursor = Cursor::new(block_data);
        cursor.seek(SeekFrom::End(-4))?;
        let entry_count = cursor.read_u32::<LittleEndian>()? as usize;

        // Read offsets
        let offsets_start = data_len - 4 - (entry_count * 4);
        cursor.seek(SeekFrom::Start(offsets_start as u64))?;

        let mut offsets = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            offsets.push(cursor.read_u32::<LittleEndian>()?);
        }

        // Store key offsets
        for i in 0..entry_count {
            self.current_block_entries.push(offsets[i]);
        }

        Ok(())
    }

    /// Read next key-value pair from current block
    fn read_next_entry(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        if self.current_entry_idx >= self.current_block_entries.len() {
            return Ok(None);
        }

        let block_data = self.current_block_data.as_ref().unwrap();
        let key_offset = self.current_block_entries[self.current_entry_idx];

        let mut cursor = Cursor::new(block_data);

        // Read key
        cursor.seek(SeekFrom::Start(key_offset as u64))?;
        let key_len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut key = vec![0u8; key_len];
        cursor.read_exact(&mut key)?;

        // Read value - no need to seek, we're already positioned after the key
        let value_len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut value = vec![0u8; value_len];
        cursor.read_exact(&mut value)?;

        self.current_entry_idx += 1;

        Ok(Some((Bytes::from(key), Bytes::from(value))))
    }
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to read from current block
            if self.current_block_data.is_some() {
                match self.read_next_entry() {
                    Ok(Some(entry)) => return Some(Ok(entry)),
                    Ok(None) => {
                        // Current block exhausted, load next
                        self.current_block_data = None;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Load next block
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
