//! SSTable reader implementation

use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use super::super::cache::{BlockCache, CacheKey};
use super::codec::{data_end, decode_entry_ref, parse_block_offsets};
use super::types::{SSTABLE_VERSION_V1, SSTABLE_VERSION_V2};
use super::{
    decompress_block, BloomFilter, CompressionType, SSTableIterator, FOOTER_SIZE, SSTABLE_MAGIC,
    SSTABLE_VERSION,
};
use crate::core::error::{Error, Result};

/// SSTable reader
pub struct SSTableReader {
    file_id: u64,
    mmap: Mmap,
    format_version: u32,
    metadata_offset: u64,
    index: SSTableIndex,
    bloom_filter: Option<BloomFilter>,
    cache: Option<Arc<BlockCache>>,
}

/// SSTable index for fast lookups
pub(crate) struct SSTableIndex {
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub(crate) last_key: Bytes,
    pub(crate) block_offset: u64,
    pub(crate) block_size: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockInfo {
    pub offset: u64,
    pub size: u32,
}

#[derive(Debug, Clone)]
pub(crate) enum SSTableValue {
    Value(Bytes),
    Tombstone,
}

impl SSTableValue {
    pub(crate) fn into_option(self) -> Option<Bytes> {
        match self {
            Self::Value(value) => Some(value),
            Self::Tombstone => None,
        }
    }
}

/// A persisted value state and its engine-wide mutation sequence.
#[derive(Debug, Clone)]
pub(crate) struct SSTableEntry {
    /// Legacy v1/v2 tables do not contain per-entry sequence numbers.
    pub(crate) sequence: Option<u64>,
    pub(crate) value: SSTableValue,
}

impl SSTableReader {
    /// Open SSTable for reading
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(sstable_corruption("file is shorter than its footer"));
        }

        // Memory-map the file
        let mmap = unsafe {
            MmapOptions::new().map(&file).map_err(|e| Error::Io {
                message: "Failed to mmap SSTable".to_string(),
                source: e,
            })?
        };

        // Read footer
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer_start = usize::try_from(footer_offset)
            .map_err(|_| sstable_corruption("footer offset cannot be represented in memory"))?;
        let mut cursor = Cursor::new(&mmap[footer_start..]);

        let index_offset = cursor.read_u64::<LittleEndian>()?;
        let index_size = cursor.read_u32::<LittleEndian>()?;
        let bloom_offset = cursor.read_u64::<LittleEndian>()?;
        let bloom_size = cursor.read_u32::<LittleEndian>()?;

        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;
        if &magic != SSTABLE_MAGIC {
            return Err(Error::SSTable {
                message: "Invalid SSTable magic number".to_string(),
                source: None,
            });
        }

        let version = cursor.read_u32::<LittleEndian>()?;
        if version != SSTABLE_VERSION
            && version != SSTABLE_VERSION_V2
            && version != SSTABLE_VERSION_V1
        {
            return Err(Error::SSTable {
                message: format!("Unsupported SSTable version: {}", version),
                source: None,
            });
        }

        // Verify footer checksum
        let stored_checksum = cursor.read_u32::<LittleEndian>()?;
        let mut footer_hasher = crc32fast::Hasher::new();
        footer_hasher.update(&index_offset.to_le_bytes());
        footer_hasher.update(&index_size.to_le_bytes());
        footer_hasher.update(&bloom_offset.to_le_bytes());
        footer_hasher.update(&bloom_size.to_le_bytes());
        footer_hasher.update(&magic);
        footer_hasher.update(&version.to_le_bytes());
        let computed_checksum = footer_hasher.finalize();
        // v0.2.0 and v0.2.1 wrote a zero footer-checksum placeholder under
        // the v1 identifier. Later v1 files and every newer format require the
        // checksum to match. Version 1 data-block checksums remain mandatory;
        // database preflight also cross-checks its footer/index/bloom metadata
        // against every decoded block.
        let released_v1_placeholder = version == SSTABLE_VERSION_V1 && stored_checksum == 0;
        if !released_v1_placeholder && stored_checksum != computed_checksum {
            return Err(Error::SSTable {
                message: format!(
                    "Footer checksum mismatch: stored={:#010x}, computed={:#010x}",
                    stored_checksum, computed_checksum
                ),
                source: None,
            });
        }

        // Load index
        let index_range =
            component_range("index", index_offset, u64::from(index_size), footer_offset)?;
        if index_size < 4 {
            return Err(sstable_corruption("index is missing its entry count"));
        }
        let index_data = &mmap[index_range];
        let index = SSTableIndex::load(index_data)?;
        let index_end = index_offset
            .checked_add(u64::from(index_size))
            .ok_or_else(|| sstable_corruption("index range overflows"))?;

        // Load bloom filter
        let bloom_filter = if bloom_size > 0 {
            let bloom_range = component_range(
                "bloom filter",
                bloom_offset,
                u64::from(bloom_size),
                footer_offset,
            )?;
            if bloom_offset != index_end {
                return Err(sstable_corruption(
                    "bloom filter does not immediately follow the index",
                ));
            }
            let bloom_data = &mmap[bloom_range];
            Some(Self::deserialize_bloom_filter(bloom_data)?)
        } else {
            if bloom_offset != footer_offset || index_end != footer_offset {
                return Err(sstable_corruption(
                    "empty bloom filter does not immediately follow the index",
                ));
            }
            None
        };

        // Compute file_id from path hash
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let file_id = hasher.finish();

        Ok(Self {
            file_id,
            mmap,
            format_version: version,
            metadata_offset: index_offset,
            index,
            bloom_filter,
            cache: None,
        })
    }

    /// Open and eagerly validate every data block and acceleration structure.
    pub(crate) fn open_validated(path: impl AsRef<Path>) -> Result<Self> {
        let reader = Self::open(path)?;
        reader.validate_contents()?;
        Ok(reader)
    }

    /// Open with block cache
    pub fn open_with_cache(path: impl AsRef<Path>, cache: Arc<BlockCache>) -> Result<Self> {
        let mut reader = Self::open(path)?;
        reader.cache = Some(cache);
        Ok(reader)
    }

    /// Set cache after opening
    pub fn set_cache(&mut self, cache: Arc<BlockCache>) {
        self.cache = Some(cache);
    }

    /// Get value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self.get_entry(key)? {
            Some(SSTableEntry {
                value: SSTableValue::Value(value),
                ..
            }) => Some(value),
            Some(SSTableEntry {
                value: SSTableValue::Tombstone,
                ..
            })
            | None => None,
        })
    }

    /// Get raw SSTable entry by key, preserving tombstones.
    pub(crate) fn get_entry(&self, key: &[u8]) -> Result<Option<SSTableEntry>> {
        // Check bloom filter first
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.contains(key) {
                return Ok(None);
            }
        }

        // Find block that might contain the key
        let block_info = match self.index.find_block(key) {
            Some(info) => info,
            None => return Ok(None),
        };

        // Read and decompress block
        let block_data = self.read_block_shared(block_info.offset, block_info.size)?;

        // Search within block
        self.search_block_entry(&block_data, key)
    }

    /// Read and decompress a block into shared storage.
    ///
    /// Cache hits clone only a `Bytes` handle. On a miss, the decompressor's
    /// output allocation becomes the cached block without another full copy.
    pub(crate) fn read_block_shared(&self, offset: u64, size: u32) -> Result<Bytes> {
        if size < 5 {
            return Err(Error::SSTable {
                message: "Block size is smaller than its footer".to_string(),
                source: None,
            });
        }
        let cache_key = CacheKey::new(self.file_id, offset);

        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached);
            }
        }

        // Cache miss - read from mmap
        let footer_end_u64 = offset
            .checked_add(u64::from(size))
            .ok_or_else(|| Error::SSTable {
                message: "Block offset/size overflow".to_string(),
                source: None,
            })?;
        let block_end = footer_end_u64 - 5;
        let footer_end = usize::try_from(footer_end_u64).map_err(|_| Error::SSTable {
            message: "Block offset/size cannot be represented in memory".to_string(),
            source: None,
        })?;
        if footer_end > self.mmap.len() {
            return Err(Error::SSTable {
                message: format!(
                    "Block offset/size exceeds file: end={}, file_len={}",
                    footer_end,
                    self.mmap.len()
                ),
                source: None,
            });
        }
        let block_data = &self.mmap[offset as usize..block_end as usize];

        // Read footer
        let compression = CompressionType::try_from(self.mmap[block_end as usize])?;
        let crc = (&self.mmap[(block_end + 1) as usize..(block_end + 5) as usize])
            .read_u32::<LittleEndian>()?;

        // Verify CRC
        if crc32fast::hash(block_data) != crc {
            return Err(Error::SSTable {
                message: "Block CRC mismatch".to_string(),
                source: None,
            });
        }

        // Decompress
        let decompressed = Bytes::from(decompress_block(block_data, compression)?);

        // Store in cache
        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, decompressed.clone());
        }

        Ok(decompressed)
    }

    /// Validate every checksummed block and cross-check the unchecksummed index
    /// and bloom-filter acceleration data against the authoritative entries.
    fn validate_contents(&self) -> Result<()> {
        let mut expected_block_offset = 0_u64;
        let mut previous_key: Option<Bytes> = None;

        for index_entry in self.index.entries() {
            if index_entry.block_offset != expected_block_offset {
                return Err(sstable_corruption(
                    "data blocks are not contiguous from the start of the file",
                ));
            }
            let block_end = index_entry
                .block_offset
                .checked_add(u64::from(index_entry.block_size))
                .ok_or_else(|| sstable_corruption("data block range overflows"))?;
            if block_end > self.metadata_offset {
                return Err(sstable_corruption("data block overlaps the index"));
            }

            let block = self.read_block_shared(index_entry.block_offset, index_entry.block_size)?;
            let offsets = parse_block_offsets(&block)?;
            if offsets.is_empty() {
                return Err(sstable_corruption("index references an empty data block"));
            }
            let entries_end = data_end(block.len(), offsets.len())?;
            let mut block_last_key = None;
            for (entry_index, entry_offset) in offsets.iter().enumerate() {
                let entry_end = offsets
                    .get(entry_index + 1)
                    .map_or(entries_end, |offset| *offset as usize);
                let (key, _) = decode_entry_ref(
                    self.format_version,
                    block.clone(),
                    *entry_offset as usize,
                    entry_end,
                )?;
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous.as_ref() >= key.as_ref())
                {
                    return Err(sstable_corruption(
                        "keys are not strictly increasing across data blocks",
                    ));
                }
                if self
                    .bloom_filter
                    .as_ref()
                    .is_some_and(|filter| !filter.contains(&key))
                {
                    return Err(sstable_corruption(
                        "bloom filter excludes a key stored in the table",
                    ));
                }
                previous_key = Some(key.clone());
                block_last_key = Some(key);
            }
            if block_last_key.as_deref() != Some(index_entry.last_key.as_ref()) {
                return Err(sstable_corruption(
                    "index key does not match its data block's final key",
                ));
            }
            expected_block_offset = block_end;
        }

        if expected_block_offset != self.metadata_offset {
            return Err(sstable_corruption(
                "data blocks do not end at the index offset",
            ));
        }
        Ok(())
    }

    /// Search for key within a block
    fn search_block_entry(
        &self,
        block_data: &Bytes,
        target_key: &[u8],
    ) -> Result<Option<SSTableEntry>> {
        let offsets = parse_block_offsets(block_data)?;
        let block_data_end = data_end(block_data.len(), offsets.len())?;

        // Binary search through entries
        let mut left = 0;
        let mut right = offsets.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let entry_end = offsets
                .get(mid + 1)
                .map_or(block_data_end, |offset| *offset as usize);
            let (key, entry) = decode_entry_ref(
                self.format_version,
                block_data.clone(),
                offsets[mid] as usize,
                entry_end,
            )?;

            match key.as_ref().cmp(target_key) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(entry.into_entry()));
                }
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
            }
        }

        Ok(None)
    }

    /// Create iterator over all entries
    pub fn iter(&self) -> SSTableIterator<'_> {
        SSTableIterator::new(self)
    }

    /// Get reference to index
    pub(crate) fn index(&self) -> &SSTableIndex {
        &self.index
    }

    pub(crate) fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Deserialize bloom filter from raw data
    fn deserialize_bloom_filter(data: &[u8]) -> Result<BloomFilter> {
        if data.len() < 12 {
            return Err(sstable_corruption("bloom filter metadata is truncated"));
        }

        let mut cursor = Cursor::new(&data[data.len() - 12..]);
        let num_hash_functions = cursor.read_u32::<LittleEndian>()? as usize;
        let num_bits = cursor.read_u32::<LittleEndian>()? as usize;
        let _bits_per_key = cursor.read_u32::<LittleEndian>()? as usize;

        let bits_data = data[..data.len() - 12].to_vec();
        if bits_data.is_empty() {
            return Err(sstable_corruption("bloom filter bit payload is empty"));
        }
        let capacity = bits_data
            .len()
            .checked_mul(8)
            .ok_or_else(|| sstable_corruption("bloom filter bit capacity overflows"))?;
        if num_bits == 0 || num_bits > capacity || capacity - num_bits >= 8 {
            return Err(sstable_corruption(
                "bloom filter bit count does not match its byte payload",
            ));
        }
        if num_hash_functions == 0 || num_hash_functions > 64 {
            return Err(sstable_corruption("bloom filter hash count is invalid"));
        }
        Ok(BloomFilter::from_serialized_parts(
            bits_data,
            num_bits,
            num_hash_functions,
        ))
    }
}

impl SSTableIndex {
    /// Load index from raw data
    pub(crate) fn load(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(sstable_corruption("index is missing its entry count"));
        }
        let entries_end = data.len() - 4;
        let entry_count = u32::from_le_bytes(
            data[entries_end..]
                .try_into()
                .expect("four-byte index count"),
        ) as usize;
        if entry_count > entries_end / 16 {
            return Err(sstable_corruption(
                "index entry count exceeds its encoded payload",
            ));
        }
        let mut cursor = Cursor::new(&data[..entries_end]);
        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            // Read key length and key
            let key_len = read_index_u32(&mut cursor, "key length")? as usize;
            if key_len > remaining_index_bytes(&cursor) {
                return Err(sstable_corruption(
                    "index key length exceeds its encoded payload",
                ));
            }
            let mut key = vec![0u8; key_len];
            cursor
                .read_exact(&mut key)
                .map_err(|_| sstable_corruption("index key is truncated"))?;

            // Read offset and size
            let block_offset = read_index_u64(&mut cursor, "block offset")?;
            let block_size = read_index_u32(&mut cursor, "block size")?;

            if entries
                .last()
                .is_some_and(|previous: &IndexEntry| previous.last_key.as_ref() >= key.as_slice())
            {
                return Err(sstable_corruption("index keys are not strictly increasing"));
            }

            entries.push(IndexEntry {
                last_key: Bytes::from(key),
                block_offset,
                block_size,
            });
        }

        if remaining_index_bytes(&cursor) != 0 {
            return Err(sstable_corruption(
                "index contains trailing bytes before its entry count",
            ));
        }

        Ok(Self { entries })
    }

    /// Find block that might contain the given key
    pub(crate) fn find_block(&self, key: &[u8]) -> Option<BlockInfo> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search: find first block whose last_key >= key
        let idx = self
            .entries
            .partition_point(|entry| entry.last_key.as_ref() < key);

        if idx < self.entries.len() {
            let entry = &self.entries[idx];
            Some(BlockInfo {
                offset: entry.block_offset,
                size: entry.block_size,
            })
        } else {
            None
        }
    }

    /// Get all entries (for iterator)
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }
}

fn component_range(
    component: &str,
    offset: u64,
    size: u64,
    footer_offset: u64,
) -> Result<std::ops::Range<usize>> {
    let end = offset
        .checked_add(size)
        .ok_or_else(|| sstable_corruption(&format!("{component} range overflows")))?;
    if end > footer_offset {
        return Err(sstable_corruption(&format!(
            "{component} range exceeds the data area"
        )));
    }
    let start = usize::try_from(offset).map_err(|_| {
        sstable_corruption(&format!(
            "{component} offset cannot be represented in memory"
        ))
    })?;
    let end = usize::try_from(end).map_err(|_| {
        sstable_corruption(&format!("{component} end cannot be represented in memory"))
    })?;
    Ok(start..end)
}

fn sstable_corruption(message: &str) -> Error {
    Error::SSTable {
        message: format!("SSTable corruption: {message}"),
        source: None,
    }
}

fn remaining_index_bytes(reader: &Cursor<&[u8]>) -> usize {
    reader
        .get_ref()
        .len()
        .saturating_sub(reader.position() as usize)
}

fn read_index_u32(reader: &mut Cursor<&[u8]>, field: &str) -> Result<u32> {
    if remaining_index_bytes(reader) < std::mem::size_of::<u32>() {
        return Err(sstable_corruption(&format!("index {field} is truncated")));
    }
    reader
        .read_u32::<LittleEndian>()
        .map_err(|_| sstable_corruption(&format!("index {field} is truncated")))
}

fn read_index_u64(reader: &mut Cursor<&[u8]>, field: &str) -> Result<u64> {
    if remaining_index_bytes(reader) < std::mem::size_of::<u64>() {
        return Err(sstable_corruption(&format!("index {field} is truncated")));
    }
    reader
        .read_u64::<LittleEndian>()
        .map_err(|_| sstable_corruption(&format!("index {field} is truncated")))
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use super::*;
    use crate::storage::cache::BlockCache;
    use crate::storage::sstable::{CompressionType, SSTableConfig, SSTableWriter};
    use tempfile::TempDir;

    fn uncompressed_config() -> SSTableConfig {
        SSTableConfig {
            compression: CompressionType::None,
            ..SSTableConfig::default()
        }
    }

    #[test]
    fn current_format_round_trips_sequences_values_and_tombstones() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("versioned.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"empty", Some(b""), 41).unwrap();
        writer.add_versioned(b"removed", None, 42).unwrap();
        let info = writer.finish().unwrap();

        assert_eq!((info.min_sequence, info.max_sequence), (41, 42));
        let reader = SSTableReader::open(path).unwrap();

        let empty = reader.get_entry(b"empty").unwrap().unwrap();
        assert_eq!(empty.sequence, Some(41));
        assert!(matches!(empty.value, SSTableValue::Value(ref value) if value.is_empty()));

        let removed = reader.get_entry(b"removed").unwrap().unwrap();
        assert_eq!(removed.sequence, Some(42));
        assert!(matches!(removed.value, SSTableValue::Tombstone));
    }

    #[test]
    fn legacy_v2_tables_remain_readable_without_inventing_exact_sequences() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("legacy-v2.sst");
        let mut writer = SSTableWriter::new_legacy_v2(&path, uncompressed_config()).unwrap();
        writer.add(b"empty", Some(b"")).unwrap();
        writer.add(b"removed", None).unwrap();
        writer.finish().unwrap();

        let reader = SSTableReader::open(path).unwrap();
        let empty = reader.get_entry(b"empty").unwrap().unwrap();
        assert_eq!(empty.sequence, None);
        assert!(matches!(empty.value, SSTableValue::Value(ref value) if value.is_empty()));

        let removed = reader.get_entry(b"removed").unwrap().unwrap();
        assert_eq!(removed.sequence, None);
        assert!(matches!(removed.value, SSTableValue::Tombstone));
    }

    #[test]
    fn shared_block_cache_hits_reuse_the_decompressed_allocation() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("cached.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"key", Some(b"value"), 1).unwrap();
        writer.finish().unwrap();

        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let reader = SSTableReader::open_with_cache(path, Arc::clone(&cache)).unwrap();
        let index = &reader.index().entries()[0];
        let first = reader
            .read_block_shared(index.block_offset, index.block_size)
            .unwrap();
        let second = reader
            .read_block_shared(index.block_offset, index.block_size)
            .unwrap();

        assert_eq!(first.as_ptr(), second.as_ptr());
        assert_eq!(cache.stats().hits, 1);
    }

    fn mutate_first_uncompressed_block(path: &Path, mutate: impl FnOnce(&mut [u8])) {
        let reader = SSTableReader::open(path).unwrap();
        let index = reader.index().entries()[0].clone();
        drop(reader);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(index.block_offset)).unwrap();
        let mut bytes = vec![0_u8; index.block_size as usize];
        file.read_exact(&mut bytes).unwrap();
        let data_end = bytes.len() - 5;
        mutate(&mut bytes[..data_end]);
        let checksum = crc32fast::hash(&bytes[..data_end]);
        bytes[data_end + 1..data_end + 5].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(index.block_offset)).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn point_and_iterator_reject_the_same_cross_entry_value_length() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("cross-entry.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"a", Some(b"one"), 1).unwrap();
        writer.add_versioned(b"b", Some(b"two"), 2).unwrap();
        writer.finish().unwrap();

        mutate_first_uncompressed_block(&path, |block| {
            block[14..18].copy_from_slice(&100_u32.to_le_bytes());
        });
        let reader = SSTableReader::open(path).unwrap();
        let point_error = reader.get_entry(b"a").unwrap_err().to_string();
        let iteration_error = reader.iter().next().unwrap().unwrap_err().to_string();

        assert_eq!(point_error, iteration_error);
        assert!(point_error.contains("value length"));
    }

    #[test]
    fn point_and_iterator_reject_the_same_nonzero_tombstone_length() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("invalid-tombstone.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"a", Some(b"one"), 1).unwrap();
        writer.finish().unwrap();

        mutate_first_uncompressed_block(&path, |block| block[13] = 0);
        let reader = SSTableReader::open(path).unwrap();
        let point_error = reader.get_entry(b"a").unwrap_err().to_string();
        let iteration_error = reader.iter().next().unwrap().unwrap_err().to_string();

        assert_eq!(point_error, iteration_error);
        assert!(point_error.contains("tombstone"));
    }
}
