//! SSTable reader implementation

use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use super::super::cache::{BlockCache, CacheKey, CachedBlock};
use super::codec::{decode_entry_ref, parse_block_layout};
use super::types::{SSTABLE_VERSION_V1, SSTABLE_VERSION_V2};
use super::{
    decompress_block, BloomFilter, CompressionType, SSTableIterator, FOOTER_SIZE, SSTABLE_MAGIC,
    SSTABLE_VERSION,
};
use crate::core::error::{Error, Result};

static NEXT_CACHE_NAMESPACE: AtomicU64 = AtomicU64::new(1);

/// Memory-mapped reader for one immutable SSTable file.
///
/// Opening validates the footer, index, and Bloom-filter encoding. Data blocks
/// are checksummed and decoded lazily by [`get`](Self::get) and
/// [`iter`](Self::iter), unless the engine uses its internal eager-validation
/// path during database open. The mapping and any attached cache remain valid
/// until the reader and all iterators borrowing it are dropped.
pub struct SSTableReader {
    path: std::path::PathBuf,
    cache_namespace: u64,
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
    /// Opens an SSTable for reading.
    ///
    /// This synchronous operation opens and memory-maps the complete file, then
    /// validates its footer, index, and Bloom filter. It does not eagerly read
    /// or decompress every data block. The mapping consumes virtual address
    /// space but does not copy the full file into heap memory.
    ///
    /// Returns an I/O or SSTable-corruption error if the file cannot be opened,
    /// mapped, or its eagerly inspected metadata is invalid.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Self::open_inner(&path).map_err(|error| sstable_file_error(&path, "open", error))
    }

    fn open_inner(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();

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

        // Each opened reader owns a process-unique cache namespace. Compaction
        // can retire a path while an older pinned reader remains alive; a
        // replacement reader must never consume that reader's blocks, even on
        // filesystems where identity metadata is unavailable or too coarse.
        let cache_namespace = NEXT_CACHE_NAMESPACE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::ResourceExhausted {
                resource: "SSTable block-cache namespace space".to_string(),
            })?;

        Ok(Self {
            path: path.to_path_buf(),
            cache_namespace,
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
        let path = path.as_ref().to_path_buf();
        let reader = Self::open(&path)?;
        reader
            .validate_contents()
            .map_err(|error| sstable_file_error(&path, "validated contents", error))?;
        Ok(reader)
    }

    /// Opens an SSTable and attaches a shared decompressed-block cache.
    ///
    /// Opening has the same synchronous mapping and validation behavior as
    /// [`open`](Self::open). Cached blocks are namespace-isolated per reader so
    /// a replacement file at the same path cannot reuse stale cached contents.
    pub fn open_with_cache(path: impl AsRef<Path>, cache: Arc<BlockCache>) -> Result<Self> {
        let mut reader = Self::open(path)?;
        reader.cache = Some(cache);
        Ok(reader)
    }

    /// Attaches or replaces the shared decompressed-block cache.
    ///
    /// Existing blocks in either cache are not migrated or cleared. Later
    /// cache misses allocate decompression output and may evict other entries.
    pub fn set_cache(&mut self, cache: Arc<BlockCache>) {
        self.cache = Some(cache);
    }

    /// Returns the value for `key`, treating a persisted tombstone as absent.
    ///
    /// The lookup is synchronous. It may fault mapped pages, read and verify a
    /// block, decompress it, and allocate a returned [`Bytes`] handle. Cache
    /// hits share immutable block storage. An I/O, checksum, compression, or
    /// encoding error is returned if the selected block cannot be decoded.
    ///
    /// This low-level method observes only this table; it does not reconcile
    /// newer memtable entries or other SSTables.
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
        self.get_entry_inner(key)
            .map_err(|error| self.file_error("data block entry", error))
    }

    fn get_entry_inner(&self, key: &[u8]) -> Result<Option<SSTableEntry>> {
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
        let block = self.read_block_shared_inner(block_info.offset, block_info.size)?;

        // Search within block
        self.search_block_entry(&block, key)
    }

    /// Read and decompress a block into shared storage.
    ///
    /// Cache hits clone only a `Bytes` handle and its inline parsed layout. On
    /// a miss, the decompressor's output allocation becomes the cached block
    /// without another full copy.
    pub(crate) fn read_block_shared(&self, offset: u64, size: u32) -> Result<CachedBlock> {
        self.read_block_shared_inner(offset, size)
            .map_err(|error| self.file_error("data block", error))
    }

    fn read_block_shared_inner(&self, offset: u64, size: u32) -> Result<CachedBlock> {
        if size < 5 {
            return Err(Error::SSTable {
                message: "Block size is smaller than its footer".to_string(),
                source: None,
            });
        }
        let cache_key = CacheKey::new(self.cache_namespace, offset);

        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get_block(&cache_key) {
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
        let layout = parse_block_layout(&decompressed)?;
        let block = CachedBlock::validated(decompressed, layout);

        // Store in cache
        if let Some(ref cache) = self.cache {
            cache.insert_block(cache_key, block.clone());
        }

        Ok(block)
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
            let layout = block
                .layout()
                .expect("reader blocks always have a validated layout");
            if layout.entry_count() == 0 {
                return Err(sstable_corruption("index references an empty data block"));
            }
            let mut block_last_key = None;
            for entry_index in 0..layout.entry_count() {
                let entry_range = layout.entry_range(block.data(), entry_index);
                let (key, _) = decode_entry_ref(
                    self.format_version,
                    block.data().clone(),
                    entry_range.start,
                    entry_range.end,
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
        block: &CachedBlock,
        target_key: &[u8],
    ) -> Result<Option<SSTableEntry>> {
        let layout = block
            .layout()
            .expect("reader blocks always have a validated layout");

        // Binary search through entries
        let mut left = 0;
        let mut right = layout.entry_count();

        while left < right {
            let mid = left + (right - left) / 2;
            let entry_range = layout.entry_range(block.data(), mid);
            let (key, entry) = decode_entry_ref(
                self.format_version,
                block.data().clone(),
                entry_range.start,
                entry_range.end,
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

    /// Creates a sorted iterator over all entries, including tombstones.
    ///
    /// Construction is allocation-free apart from small cursor state. Blocks
    /// are loaded synchronously and lazily as iteration advances. Each item can
    /// return an I/O, checksum, compression, or encoding error; after its first
    /// error the iterator is fused. Keys are copied, while values share
    /// immutable block storage when the format permits.
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

    pub(crate) fn file_error(&self, context: &str, error: Error) -> Error {
        sstable_file_error(&self.path, context, error)
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

fn sstable_file_error(path: &Path, context: &str, error: Error) -> Error {
    match error {
        Error::SSTable { message, source } => Error::SSTable {
            message: format!("{} [{context}]: {message}", path.display()),
            source,
        },
        Error::Io { message, source } => Error::Io {
            message: format!("SSTable {} [{context}]: {message}", path.display()),
            source,
        },
        Error::ResourceExhausted { resource } => Error::ResourceExhausted {
            resource: format!("{resource} while opening {} [{context}]", path.display()),
        },
        Error::Internal { message } => Error::SSTable {
            message: format!("{} [{context}]: Internal error: {message}", path.display()),
            source: None,
        },
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
    use std::collections::BTreeMap;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use super::*;
    use crate::storage::cache::BlockCache;
    use crate::storage::sstable::{CompressionType, SSTableConfig, SSTableWriter};
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn uncompressed_config() -> SSTableConfig {
        SSTableConfig {
            compression: CompressionType::None,
            ..SSTableConfig::default()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn arbitrary_sorted_entries_round_trip_through_points_bloom_index_and_streaming(
            entries in prop::collection::btree_map(
                prop::collection::vec(any::<u8>(), 0..32),
                prop::option::of(prop::collection::vec(any::<u8>(), 0..512)),
                1..64,
            ),
            compression in super::super::compression::compression_type_strategy(),
        ) {
            let directory = TempDir::new().unwrap();
            let path = directory.path().join(format!("property-{compression:?}.sst"));
            let mut writer = SSTableWriter::new(
                &path,
                SSTableConfig {
                    block_size: 128,
                    compression,
                    ..SSTableConfig::default()
                },
            )
            .unwrap();
            for (index, (key, value)) in entries.iter().enumerate() {
                writer
                    .add_versioned(key, value.as_deref(), index as u64 + 1)
                    .unwrap();
            }
            let info = writer.finish().unwrap();
            prop_assert_eq!(info.entry_count as usize, entries.len());

            let reader = SSTableReader::open_validated(&path).unwrap();
            let bloom = reader.bloom_filter.as_ref().unwrap();
            for (index, (key, expected)) in entries.iter().enumerate() {
                prop_assert!(bloom.contains(key), "compression={compression:?}, key={key:?}");
                let actual = reader.get_entry(key).unwrap().unwrap();
                prop_assert_eq!(actual.sequence, Some(index as u64 + 1));
                match (&actual.value, expected) {
                    (SSTableValue::Value(actual), Some(expected)) => {
                        prop_assert_eq!(actual.as_ref(), expected.as_slice());
                    }
                    (SSTableValue::Tombstone, None) => {}
                    _ => prop_assert!(false, "compression={compression:?}, key={key:?}"),
                }
            }

            let actual = reader
                .iter()
                .map(|entry| entry.map(|(key, value)| (key.to_vec(), value.map(|value| value.to_vec()))))
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let expected = entries
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            prop_assert_eq!(actual, expected, "compression={:?}", compression);

            let definite_negative = (0..4_096_u64)
                .map(u64::to_le_bytes)
                .find(|candidate| !entries.contains_key(candidate.as_slice()) && !bloom.contains(candidate));
            prop_assert!(definite_negative.is_some(), "compression={compression:?}");
            prop_assert!(reader.get(&definite_negative.unwrap()).unwrap().is_none());
        }
    }

    #[test]
    fn large_binary_values_cross_the_configured_block_size_for_every_codec() {
        let directory = TempDir::new().unwrap();
        let mut value = (0..1024 * 1024)
            .map(|offset| (offset * 131 + offset / 17) as u8)
            .collect::<Vec<_>>();
        value[0] = 0;
        *value.last_mut().unwrap() = 0xff;

        for compression in [
            CompressionType::None,
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let path = directory.path().join(format!("large-{compression:?}.sst"));
            let mut writer = SSTableWriter::new(
                &path,
                SSTableConfig {
                    block_size: 64,
                    compression,
                    ..SSTableConfig::default()
                },
            )
            .unwrap();
            writer
                .add_versioned(b"large\0\xff", Some(&value), 9)
                .unwrap();
            writer.finish().unwrap();

            let reader = SSTableReader::open_validated(&path).unwrap();
            assert_eq!(
                reader.get(b"large\0\xff").unwrap().unwrap().as_ref(),
                value,
                "compression={compression:?}"
            );
        }
    }

    #[test]
    fn multi_block_index_seeks_exact_keys_and_gaps() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("multi-block.sst");
        let mut writer = SSTableWriter::new(
            &path,
            SSTableConfig {
                block_size: 96,
                compression: CompressionType::None,
                ..SSTableConfig::default()
            },
        )
        .unwrap();
        let entries = (0..128)
            .map(|index| {
                (
                    format!("{:04}", index * 2).into_bytes(),
                    vec![index as u8; 37],
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (index, (key, value)) in entries.iter().enumerate() {
            writer
                .add_versioned(key, Some(value), index as u64 + 1)
                .unwrap();
        }
        writer.finish().unwrap();

        let reader = SSTableReader::open_validated(&path).unwrap();
        assert!(reader.index.entries().len() > 32);
        for (index, (key, value)) in entries.iter().enumerate() {
            assert_eq!(reader.get(key).unwrap().unwrap().as_ref(), value);
            let gap = format!("{:04}", index * 2 + 1);
            assert!(reader.get(gap.as_bytes()).unwrap().is_none(), "gap={gap}");
            assert!(reader.index.find_block(key).is_some(), "key={key:?}");
        }
        assert!(reader.get(b"9999").unwrap().is_none());
        assert!(reader.index.find_block(b"9999").is_none());
        assert_eq!(reader.iter().count(), entries.len());
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

        assert_eq!(first.data().as_ptr(), second.data().as_ptr());
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn malformed_offset_layout_never_enters_the_block_cache() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("bad-offsets.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"a", Some(b"one"), 1).unwrap();
        writer.add_versioned(b"b", Some(b"two"), 2).unwrap();
        writer.finish().unwrap();

        mutate_first_uncompressed_block(&path, |block| {
            let offsets_start = block.len() - 4 - 2 * 4;
            block[offsets_start + 4..offsets_start + 8].copy_from_slice(&0_u32.to_le_bytes());
        });

        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let reader = SSTableReader::open_with_cache(path, Arc::clone(&cache)).unwrap();
        let index = &reader.index().entries()[0];
        let error = reader
            .read_block_shared(index.block_offset, index.block_size)
            .unwrap_err();
        assert!(error.to_string().contains("strictly increasing"));
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[test]
    fn cached_layout_does_not_hide_late_entry_corruption_and_iterator_fuses() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("cached-bad-entry.sst");
        let mut writer = SSTableWriter::new(&path, uncompressed_config()).unwrap();
        writer.add_versioned(b"a", Some(b"one"), 1).unwrap();
        writer.add_versioned(b"b", Some(b"two"), 2).unwrap();
        writer.finish().unwrap();

        mutate_first_uncompressed_block(&path, |block| {
            // [key length][a][sequence][marker] precede the value length.
            block[14..18].copy_from_slice(&100_u32.to_le_bytes());
        });

        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let reader = SSTableReader::open_with_cache(path, Arc::clone(&cache)).unwrap();
        let index = &reader.index().entries()[0];
        reader
            .read_block_shared(index.block_offset, index.block_size)
            .expect("the offset layout is valid and is cached before entry decoding");

        let mut iterator = reader.iter();
        let error = iterator.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("value length"));
        assert!(iterator.next().is_none());
        assert!(iterator.next().is_none());
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
