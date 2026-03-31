//! SSTable reader implementation

use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use super::super::cache::{BlockCache, CacheKey};
use super::{
    decompress_block, BloomFilter, CompressionType, SSTableConfig, SSTableInfo, SSTableIterator,
    FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION,
};
use crate::core::error::{Error, Result};

/// SSTable reader
pub struct SSTableReader {
    #[allow(dead_code)]
    path: PathBuf,
    file_id: u64,
    mmap: Mmap,
    #[allow(dead_code)]
    info: SSTableInfo,
    index: SSTableIndex,
    bloom_filter: Option<BloomFilter>,
    #[allow(dead_code)]
    config: SSTableConfig,
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

impl SSTableReader {
    /// Open SSTable for reading
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        // Memory-map the file
        let mmap = unsafe {
            MmapOptions::new().map(&file).map_err(|e| Error::Io {
                message: "Failed to mmap SSTable".to_string(),
                source: e,
            })?
        };

        // Read footer
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::SSTable {
                message: "SSTable file too small".to_string(),
                source: None,
            });
        }

        let footer_offset = file_size - FOOTER_SIZE as u64;
        let mut cursor = Cursor::new(&mmap[footer_offset as usize..]);

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
        if version != SSTABLE_VERSION {
            return Err(Error::SSTable {
                message: format!("Unsupported SSTable version: {}", version),
                source: None,
            });
        }

        let _checksum = cursor.read_u32::<LittleEndian>()?;

        // Load index
        let index_data = &mmap[index_offset as usize..(index_offset + index_size as u64) as usize];
        let index = SSTableIndex::load(index_data)?;

        // Load bloom filter
        let bloom_filter = if bloom_size > 0 {
            let bloom_data =
                &mmap[bloom_offset as usize..(bloom_offset + bloom_size as u64) as usize];
            Some(Self::deserialize_bloom_filter(bloom_data)?)
        } else {
            None
        };

        // Extract min/max keys from index
        let (min_key, max_key) = if index.entries.is_empty() {
            (vec![], vec![])
        } else {
            // For min key, we need to read the first entry of the first block
            let first_key = Self::read_first_key(&mmap, &index)?;
            let last_key = index.entries.last().unwrap().last_key.to_vec();
            (first_key.to_vec(), last_key)
        };

        // Create info
        let info = SSTableInfo {
            id: 0, // Will be set by caller
            path: path.clone(),
            file_size,
            entry_count: 0, // Could be stored in footer or calculated
            min_key,
            max_key,
            creation_time: 0, // Could be stored in footer
            level: 0,
        };

        // Compute file_id from path hash
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let file_id = hasher.finish();

        Ok(Self {
            path,
            file_id,
            mmap,
            info,
            index,
            bloom_filter,
            config: SSTableConfig::default(),
            cache: None,
        })
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
        let block_data = self.read_block(block_info.offset, block_info.size)?;

        // Search within block
        self.search_block(&block_data, key)
    }

    /// Read and decompress a block (with cache)
    pub(crate) fn read_block(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let cache_key = CacheKey::new(self.file_id, offset);

        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.to_vec());
            }
        }

        // Cache miss - read from mmap
        let block_end = offset + size as u64 - 5; // -5 for footer
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
        let decompressed = decompress_block(block_data, compression)?;

        // Store in cache
        if let Some(ref cache) = self.cache {
            cache.insert(cache_key, Bytes::copy_from_slice(&decompressed));
        }

        Ok(decompressed)
    }

    /// Search for key within a block
    fn search_block(&self, block_data: &[u8], target_key: &[u8]) -> Result<Option<Bytes>> {
        let mut cursor = Cursor::new(block_data);

        // First, read the footer to get entry count and offsets
        let data_len = block_data.len();
        if data_len < 4 {
            return Ok(None);
        }

        // Read number of entries from the end
        cursor.seek(SeekFrom::End(-4))?;
        let entry_count = cursor.read_u32::<LittleEndian>()? as usize;

        // Read offsets
        let offsets_start = data_len - 4 - (entry_count * 4);
        cursor.seek(SeekFrom::Start(offsets_start as u64))?;

        let mut offsets = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            offsets.push(cursor.read_u32::<LittleEndian>()?);
        }

        // Binary search through entries
        let mut left = 0;
        let mut right = entry_count;

        while left < right {
            let mid = left + (right - left) / 2;
            cursor.seek(SeekFrom::Start(offsets[mid] as u64))?;

            // Read key at mid position
            let key_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            cursor.read_exact(&mut key)?;

            match key.as_slice().cmp(target_key) {
                std::cmp::Ordering::Equal => {
                    // Found it, read value
                    let value_len = cursor.read_u32::<LittleEndian>()? as usize;
                    let mut value = vec![0u8; value_len];
                    cursor.read_exact(&mut value)?;
                    return Ok(Some(Bytes::from(value)));
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

    /// Get reference to internal mmap
    #[allow(dead_code)]
    pub(crate) fn mmap(&self) -> &Mmap {
        &self.mmap
    }

    /// Get reference to index
    pub(crate) fn index(&self) -> &SSTableIndex {
        &self.index
    }

    /// Read first key from the first block
    fn read_first_key(_mmap: &Mmap, index: &SSTableIndex) -> Result<Bytes> {
        if index.entries.is_empty() {
            return Ok(Bytes::new());
        }

        // For now, just return empty bytes - we'll read it properly after initialization
        // Reading the first key requires decompressing the block which needs the compression type
        Ok(Bytes::new())
    }

    /// Deserialize bloom filter from raw data
    fn deserialize_bloom_filter(data: &[u8]) -> Result<BloomFilter> {
        if data.len() < 12 {
            return Err(Error::SSTable {
                message: "Invalid bloom filter data".to_string(),
                source: None,
            });
        }

        let mut cursor = Cursor::new(&data[data.len() - 12..]);
        let _num_hash_functions = cursor.read_u32::<LittleEndian>()? as usize;
        let _num_bits = cursor.read_u32::<LittleEndian>()? as usize;
        let bits_per_key = cursor.read_u32::<LittleEndian>()? as usize;

        let bits_data = data[..data.len() - 12].to_vec();
        Ok(BloomFilter::from_bytes(bits_data, bits_per_key))
    }
}

impl SSTableIndex {
    /// Load index from raw data
    pub(crate) fn load(data: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);
        let mut entries = Vec::new();

        // Read number of entries from the end
        cursor.seek(SeekFrom::End(-4))?;
        let entry_count = cursor.read_u32::<LittleEndian>()? as usize;

        // Reset to beginning
        cursor.seek(SeekFrom::Start(0))?;

        for _ in 0..entry_count {
            // Read key length and key
            let key_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            cursor.read_exact(&mut key)?;

            // Read offset and size
            let block_offset = cursor.read_u64::<LittleEndian>()?;
            let block_size = cursor.read_u32::<LittleEndian>()?;

            entries.push(IndexEntry {
                last_key: Bytes::from(key),
                block_offset,
                block_size,
            });
        }

        Ok(Self { entries })
    }

    /// Find block that might contain the given key
    pub(crate) fn find_block(&self, key: &[u8]) -> Option<BlockInfo> {
        if self.entries.is_empty() {
            return None;
        }

        // Binary search: find first block whose last_key >= key
        let idx = self.entries.partition_point(|entry| entry.last_key.as_ref() < key);

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
