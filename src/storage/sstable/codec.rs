use std::io::{self, Write};
use std::ops::Range;

use bytes::Bytes;

use super::super::cache::BlockLayout;
use super::reader::{SSTableEntry, SSTableValue};
use crate::core::error::{Error, Result};

/// A zero-copy sink for measuring the exact byte count produced by an encoder.
///
/// Encoders pass their existing slices through [`Write::write`], so measuring
/// large keys, bloom filters, or compressed blocks never copies their payloads.
#[derive(Default)]
pub(crate) struct CountingWriter {
    bytes_written: u64,
}

impl CountingWriter {
    pub(crate) fn with_bytes_written(bytes_written: u64) -> Self {
        Self { bytes_written }
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let added = u64::try_from(buffer.len())
            .map_err(|_| io::Error::other("encoded slice length exceeds u64"))?;
        self.bytes_written = self
            .bytes_written
            .checked_add(added)
            .ok_or_else(|| io::Error::other("encoded byte count exceeds u64"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A decoded value reference backed by one verified, decompressed block.
pub(crate) struct SSTableEntryRef {
    pub(crate) sequence: Option<u64>,
    pub(crate) block: Bytes,
    pub(crate) value_range: Option<Range<usize>>,
}

impl SSTableEntryRef {
    pub(crate) fn into_entry(self) -> SSTableEntry {
        let value = self.value_range.map_or(SSTableValue::Tombstone, |range| {
            SSTableValue::Value(self.block.slice(range))
        });
        SSTableEntry {
            sequence: self.sequence,
            value,
        }
    }
}

#[cfg(test)]
pub(crate) fn parse_block_offsets(block_data: &[u8]) -> Result<Vec<u32>> {
    let layout = parse_block_layout(block_data)?;
    Ok((0..layout.entry_count())
        .map(|index| layout.entry_offset(block_data, index) as u32)
        .collect())
}

/// Validate a block's encoded offset table without materializing it.
///
/// The returned layout is safe to reuse only with the same immutable block.
/// This function is the single validation authority for point reads, scans,
/// and cache insertion.
pub(crate) fn parse_block_layout(block_data: &[u8]) -> Result<BlockLayout> {
    let data_len = block_data.len();
    if data_len < 4 {
        return Err(invalid_block("block is missing its entry count"));
    }

    let entry_count = u32::from_le_bytes(
        block_data[data_len - 4..]
            .try_into()
            .expect("four-byte slice"),
    ) as usize;
    let offsets_bytes = entry_count
        .checked_mul(4)
        .ok_or_else(|| invalid_block("entry offset table length overflow"))?;
    let footer_bytes = 4_usize
        .checked_add(offsets_bytes)
        .ok_or_else(|| invalid_block("entry offset table length overflow"))?;
    let offsets_start = data_len
        .checked_sub(footer_bytes)
        .ok_or_else(|| invalid_block("entry offset table exceeds block"))?;

    let mut previous = None;
    for bytes in block_data[offsets_start..data_len - 4].chunks_exact(4) {
        let offset = u32::from_le_bytes(bytes.try_into().expect("four-byte chunk"));
        if offset as usize >= offsets_start {
            return Err(invalid_block("entry offset points outside entry data"));
        }
        if previous.is_some_and(|previous| previous >= offset) {
            return Err(invalid_block("entry offsets are not strictly increasing"));
        }
        previous = Some(offset);
    }
    if entry_count > 0
        && u32::from_le_bytes(
            block_data[offsets_start..offsets_start + 4]
                .try_into()
                .expect("four-byte first offset"),
        ) != 0
    {
        return Err(invalid_block("first entry offset is not zero"));
    }
    Ok(BlockLayout::new(offsets_start, entry_count))
}

pub(crate) fn decode_entry_ref(
    format_version: u32,
    block: Bytes,
    entry_offset: usize,
    entry_end: usize,
) -> Result<(Bytes, SSTableEntryRef)> {
    let mut position = entry_offset;
    let key_len = read_u32(&block, &mut position, entry_end)? as usize;
    let key_end = position
        .checked_add(key_len)
        .filter(|end| *end <= entry_end)
        .ok_or_else(|| invalid_block("key length exceeds entry data"))?;
    let key = block.slice(position..key_end);
    position = key_end;

    if format_version == super::types::SSTABLE_VERSION_V1 {
        let value_len = read_u32(&block, &mut position, entry_end)? as usize;
        let value_end = checked_value_end(position, value_len, entry_end)?;
        if value_end != entry_end {
            return Err(invalid_block("entry has trailing bytes after its value"));
        }
        // Version 1 predates tombstones. A zero-length value is therefore an
        // empty stored value, not a deletion.
        let value_range = Some(position..value_end);
        return Ok((
            key,
            SSTableEntryRef {
                sequence: None,
                block,
                value_range,
            },
        ));
    }

    let sequence = if format_version >= super::types::SSTABLE_VERSION {
        Some(read_u64(&block, &mut position, entry_end)?)
    } else {
        None
    };
    let marker = read_u8(&block, &mut position, entry_end)?;
    let value_len = read_u32(&block, &mut position, entry_end)? as usize;
    let value_end = checked_value_end(position, value_len, entry_end)?;
    if value_end != entry_end {
        return Err(invalid_block("entry has trailing bytes after its value"));
    }
    let value_range = match marker {
        0 if value_len == 0 => None,
        0 => return Err(invalid_block("tombstone has a nonzero value length")),
        1 => Some(position..value_end),
        other => return Err(invalid_block(&format!("invalid value marker: {other}"))),
    };
    Ok((
        key,
        SSTableEntryRef {
            sequence,
            block,
            value_range,
        },
    ))
}

fn read_u8(data: &[u8], position: &mut usize, end: usize) -> Result<u8> {
    let value = *data
        .get(*position)
        .filter(|_| *position < end)
        .ok_or_else(|| invalid_block("entry is truncated"))?;
    *position += 1;
    Ok(value)
}

fn read_u32(data: &[u8], position: &mut usize, end: usize) -> Result<u32> {
    let value_end = position
        .checked_add(4)
        .filter(|value_end| *value_end <= end)
        .ok_or_else(|| invalid_block("entry is truncated"))?;
    let value = u32::from_le_bytes(
        data[*position..value_end]
            .try_into()
            .expect("four-byte slice"),
    );
    *position = value_end;
    Ok(value)
}

fn read_u64(data: &[u8], position: &mut usize, end: usize) -> Result<u64> {
    let value_end = position
        .checked_add(8)
        .filter(|value_end| *value_end <= end)
        .ok_or_else(|| invalid_block("entry is truncated"))?;
    let value = u64::from_le_bytes(
        data[*position..value_end]
            .try_into()
            .expect("eight-byte slice"),
    );
    *position = value_end;
    Ok(value)
}

fn checked_value_end(position: usize, value_len: usize, entry_end: usize) -> Result<usize> {
    position
        .checked_add(value_len)
        .filter(|end| *end <= entry_end)
        .ok_or_else(|| invalid_block("value length exceeds entry data"))
}

fn invalid_block(message: &str) -> Error {
    Error::SSTable {
        message: message.to_string(),
        source: None,
    }
}
