//! SSTable iterator implementation

use std::sync::Arc;

use bytes::Bytes;

use super::super::cache::CachedBlock;
#[cfg(test)]
use super::codec::parse_block_offsets;
use super::codec::{decode_entry_ref, SSTableEntryRef};
use super::reader::SSTableEntry;
use super::SSTableReader;
use crate::core::error::Result;

/// Iterator over SSTable entries
pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    cursor: SSTableCursorState,
}

/// Block traversal state shared by borrowed and reader-owning cursors.
struct SSTableCursorState {
    current_block_idx: usize,
    current_block: Option<CachedBlock>,
    current_entry_idx: usize,
    failed: bool,
}

impl<'a> SSTableIterator<'a> {
    /// Create new iterator
    pub(crate) fn new(reader: &'a SSTableReader) -> Self {
        Self {
            reader,
            cursor: SSTableCursorState::new(0),
        }
    }

    /// Advance while preserving tombstones and sequence metadata.
    pub(crate) fn next_versioned(&mut self) -> Option<Result<(Bytes, SSTableEntry)>> {
        self.cursor
            .next_versioned_ref(self.reader)
            .map(|entry| entry.map(|(key, entry)| (key, entry.into_entry())))
    }
}

impl SSTableCursorState {
    fn new(current_block_idx: usize) -> Self {
        Self {
            current_block_idx,
            current_block: None,
            current_entry_idx: 0,
            failed: false,
        }
    }

    fn load_next_block(&mut self, reader: &SSTableReader) -> Result<bool> {
        let index_entries = reader.index().entries();
        if self.current_block_idx >= index_entries.len() {
            return Ok(false);
        }

        let entry = &index_entries[self.current_block_idx];
        self.current_block = Some(reader.read_block_shared(entry.block_offset, entry.block_size)?);
        self.current_entry_idx = 0;
        self.current_block_idx += 1;
        Ok(true)
    }

    fn read_next_entry(&mut self, format_version: u32) -> Result<Option<(Bytes, SSTableEntryRef)>> {
        let Some(block) = self.current_block.as_ref() else {
            return Ok(None);
        };
        let layout = block
            .layout()
            .expect("reader blocks always have a validated layout");
        if self.current_entry_idx >= layout.entry_count() {
            return Ok(None);
        }
        let entry_range = layout.entry_range(block.data(), self.current_entry_idx);
        // An error belongs to this physical entry. Advance first so callers can
        // never observe the same error forever.
        self.current_entry_idx += 1;

        decode_entry_ref(
            format_version,
            block.data().clone(),
            entry_range.start,
            entry_range.end,
        )
        .map(Some)
    }

    fn next_versioned_ref(
        &mut self,
        reader: &SSTableReader,
    ) -> Option<Result<(Bytes, SSTableEntryRef)>> {
        if self.failed {
            return None;
        }
        loop {
            if self.current_block.is_some() {
                match self.read_next_entry(reader.format_version()) {
                    Ok(Some(entry)) => return Some(Ok(entry)),
                    Ok(None) => self.current_block = None,
                    Err(error) => {
                        self.failed = true;
                        self.current_block = None;
                        return Some(Err(reader.file_error("data block entry", error)));
                    }
                }
            }

            match self.load_next_block(reader) {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(error) => {
                    self.failed = true;
                    self.current_block = None;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl std::iter::FusedIterator for SSTableIterator<'_> {}

/// An owned, seekable SSTable cursor used by database scans.
///
/// The cursor retains its reader independently of the live SSTable list, so a
/// compaction may unlink an input after source capture without invalidating an
/// in-progress scan.
pub(crate) struct SSTableRangeCursor {
    reader: Arc<SSTableReader>,
    cursor: SSTableCursorState,
}

impl SSTableRangeCursor {
    pub(crate) fn new(reader: Arc<SSTableReader>, start: &[u8]) -> Self {
        let current_block_idx = reader
            .index()
            .entries()
            .partition_point(|entry| entry.last_key.as_ref() < start);
        Self {
            reader,
            cursor: SSTableCursorState::new(current_block_idx),
        }
    }

    pub(crate) fn next_versioned_ref(&mut self) -> Option<Result<(Bytes, SSTableEntryRef)>> {
        self.cursor.next_versioned_ref(&self.reader)
    }

    #[cfg(test)]
    pub(crate) fn retained_block(&self) -> Option<&Bytes> {
        self.cursor.current_block.as_ref().map(CachedBlock::data)
    }
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<(Bytes, Option<Bytes>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_versioned()
            .map(|entry| entry.map(|(key, entry)| (key, entry.value.into_option())))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use tempfile::TempDir;

    use super::*;
    use crate::storage::sstable::{BlockBuilder, CompressionType, SSTableConfig, SSTableWriter};

    #[test]
    fn hostile_entry_count_is_rejected_without_arithmetic_overflow() {
        let mut block = vec![0_u8; 8];
        block[4..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(parse_block_offsets(&block).is_err());
    }

    #[test]
    fn entry_length_cannot_consume_the_following_entry() {
        let mut builder = BlockBuilder::new(1024);
        assert!(builder.add_versioned(b"a", Some(b"one"), 1));
        assert!(builder.add_versioned(b"b", Some(b"two"), 2));
        let mut block = builder.finish();
        let offsets = parse_block_offsets(&block).unwrap();
        // [key length][a][sequence][marker] precede the first value length.
        block[14..18].copy_from_slice(&100_u32.to_le_bytes());
        let block = Bytes::from(block);

        let error = decode_entry_ref(
            super::super::types::SSTABLE_VERSION,
            block,
            offsets[0] as usize,
            offsets[1] as usize,
        )
        .err()
        .expect("cross-entry value length must be rejected");
        assert!(error.to_string().contains("value length"));
    }

    #[test]
    fn shortened_value_length_cannot_hide_trailing_entry_bytes() {
        let mut builder = BlockBuilder::new(1024);
        assert!(builder.add_versioned(b"a", Some(b"one"), 1));
        assert!(builder.add_versioned(b"b", Some(b"two"), 2));
        let mut block = builder.finish();
        let offsets = parse_block_offsets(&block).unwrap();
        block[14..18].copy_from_slice(&1_u32.to_le_bytes());
        let block = Bytes::from(block);

        let error = decode_entry_ref(
            super::super::types::SSTABLE_VERSION,
            block,
            offsets[0] as usize,
            offsets[1] as usize,
        )
        .err()
        .expect("trailing bytes must be rejected");
        assert!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn borrowed_iterator_reports_a_late_block_error_once_then_fuses() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("corrupt.sst");
        let mut writer = SSTableWriter::new(
            &path,
            SSTableConfig {
                compression: CompressionType::None,
                ..SSTableConfig::default()
            },
        )
        .unwrap();
        writer.add_versioned(b"key", Some(b"value"), 1).unwrap();
        writer.finish().unwrap();

        let reader = SSTableReader::open(&path).unwrap();
        let offset = reader.index().entries()[0].block_offset;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        let mut iterator = reader.iter();
        assert!(iterator.next().unwrap().is_err());
        assert!(iterator.next().is_none());
        assert!(iterator.next().is_none());
    }
}
