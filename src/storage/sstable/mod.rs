//! SSTables are immutable, sorted files that store data on disk.
//! They are the primary storage format for HanshiroDB.
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    SSTable File Structure                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌─────────────────────────────────────────────────────┐    │
//! │  │                    Data Blocks                      │    │
//! │  │  ┌──────────────────────────────────────────────┐   │    │
//! │  │  │ Block 1 (Default: 16KB)                      │   │    │
//! │  │  │ ┌─────────────────────────────────────────┐  │   │    │
//! │  │  │ │ Entry 1: [key_len][key][value_len][val] │  │   │    │
//! │  │  │ │ Entry 2: [key_len][key][value_len][val] │  │   │    │
//! │  │  │ │ ...                                     │  │   │    │
//! │  │  │ │ Entry N: [key_len][key][value_len][val] │  │   │    │
//! │  │  │ └─────────────────────────────────────────┘  │   │    │
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

mod bloom;
mod builder;
mod compression;
mod iterator;
mod reader;
mod types;
mod writer;

pub use bloom::BloomFilter;
pub use builder::{BlockBuilder, IndexBuilder};
pub use compression::{compress_block, decompress_block, CompressionType};
pub use iterator::SSTableIterator;
pub use reader::SSTableReader;
pub use types::{SSTableConfig, SSTableInfo, FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION};
pub use writer::SSTableWriter;