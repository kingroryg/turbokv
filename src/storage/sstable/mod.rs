//! SSTables are immutable, sorted files that store data on disk.
//! They are the primary storage format for TurboKV.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SSTable File Structure                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                    Data Blocks                      │    │
//! │  │  ┌──────────────────────────────────────────────┐   │    │
//! │  │  │ Block 1 (Default: 16KB)                      │   │    │
//! │  │  │ Entries: [key_len][key][seq][tag][len][val]   │   │    │
//! │  │  │ Offsets: [entry_offset ...][entry_count]     │   │    │
//! │  │  │ Block Footer: [compression][crc32]           │   │    │
//! │  │  └──────────────────────────────────────────────┘   │    │
//! │  │  Block 2...                                         │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                    Index Block                      │    │
//! │  │  ┌──────────────────────────────────────────────┐   │    │
//! │  │  │ Index Entry 1: [last_key][offset][size]      │   │    │
//! │  │  │ Index Entry 2: [last_key][offset][size]      │   │    │
//! │  │  │ ...                                          │   │    │
//! │  │  └──────────────────────────────────────────────┘   │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                   Bloom Filter                      │    │
//! │  │  [filter_data][num_probes][bits_per_key]            │    │
//! │  └─────────────────────────────────────────────────────┘    │
//! │                                                             │
//! │  ┌──────────────────────────────────────────────────────┐   │
//! │  │                      Footer                          │   │
//! │  │  [index_offset][index_size][bloom_offset][bloom_size]│   │
//! │  │  [magic_number][version][checksum]                   │   │
//! │  └──────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────┘
//! ```

mod bloom;
mod builder;
mod codec;
mod compression;
mod iterator;
mod reader;
mod types;
mod writer;

pub(crate) use bloom::BloomFilter;
pub(crate) use builder::{BlockBuilder, IndexBuilder};
pub(crate) use builder::{IndexEntry, ProjectedBlockSizes, ProjectedIndexEntry};
pub(crate) use codec::SSTableEntryRef;
pub(crate) use compression::max_compressed_block_size;
pub use compression::CompressionType;
pub(crate) use compression::{compress_block, decompress_block};
pub use iterator::SSTableIterator;
pub(crate) use iterator::SSTableRangeCursor;
pub(crate) use reader::SSTableEntry;
pub use reader::SSTableReader;
pub use types::{
    SSTableConfig, SSTableInfo, FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION, SSTABLE_VERSION_V1,
    SSTABLE_VERSION_V2,
};
pub(crate) use writer::OutputAppendDecision;
pub use writer::SSTableWriter;
