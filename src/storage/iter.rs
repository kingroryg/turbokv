//! Streaming guard iterators for range and prefix scans.
//!
//! A scan retains a coherent set of read-only memtables and pinned SSTable
//! readers, then performs an incremental k-way merge. The iterator itself keeps
//! one head and, for each SSTable source, at most one decompressed block. Its
//! working set is therefore O(source count * block size), plus O(source count)
//! merge metadata. The independently configured shared block cache is bounded
//! by decompressed payload and parsed-layout bytes. Collection helpers
//! necessarily retain their output too.
//!
//! [`EntryGuard`] defers the copy of an in-memory value until it is requested.
//! SSTable keys and values share a pinned decompressed block, so inspecting a
//! key does not copy its value. The on-disk format interleaves keys and values
//! inside compressed blocks; CRC verification and whole-block decompression are
//! therefore required before any key in that block can be yielded.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
#[cfg(test)]
use std::collections::HashSet;
use std::fmt;
use std::ops::{Bound, Range};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use super::directory_lock::DirectoryLock;
use super::engine::StorageError;
use super::memtable::MemTable;
use super::prefix_upper_bound;
use super::sstable::{SSTableEntryRef, SSTableInfo, SSTableRangeCursor, SSTableReader};
use super::version::VersionOrder;

/// The result produced by one step of a streaming scan.
pub type ScanResult = std::result::Result<EntryGuard, ScanError>;
/// An owned key-value pair returned by collection helpers.
pub type ScanEntry = (Vec<u8>, Vec<u8>);

/// An error discovered while advancing a streaming scan.
///
/// Iterator creation still uses the database or storage API's normal error
/// type. This purpose-specific boundary represents failures discovered later,
/// while blocks are read and decoded.
#[derive(Debug, thiserror::Error)]
#[error("scan failed: {inner}")]
pub struct ScanError {
    #[source]
    inner: StorageError,
}

impl From<StorageError> for ScanError {
    fn from(inner: StorageError) -> Self {
        Self { inner }
    }
}

impl ScanError {
    pub(crate) fn into_storage_error(self) -> StorageError {
        self.inner
    }
}

/// A guard that owns a key and retains its value source.
///
/// Calling [`Self::key`] never copies or materializes the value. For frozen
/// memtables, [`Self::value`] performs the first value clone and caches it. For
/// SSTables, the guard returns a slice of the already decompressed block. The
/// guard retains its source allocation until dropped.
pub struct EntryGuard {
    key: Vec<u8>,
    value: GuardValue,
}

enum GuardValue {
    Memory {
        table: Arc<MemTable>,
        key: Vec<u8>,
        cached: OnceLock<Vec<u8>>,
    },
    Block {
        block: Bytes,
        range: Range<usize>,
    },
}

impl Clone for EntryGuard {
    fn clone(&self) -> Self {
        let value = match &self.value {
            GuardValue::Memory { table, key, cached } => GuardValue::Memory {
                table: Arc::clone(table),
                key: key.clone(),
                cached: cached.clone(),
            },
            GuardValue::Block { block, range } => GuardValue::Block {
                block: block.clone(),
                range: range.clone(),
            },
        };
        Self {
            key: self.key.clone(),
            value,
        }
    }
}

impl fmt::Debug for EntryGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntryGuard")
            .field("key", &self.key)
            .field("value_loaded", &self.value_loaded())
            .finish_non_exhaustive()
    }
}

impl EntryGuard {
    fn from_candidate(key: Bytes, value: CandidateValue) -> Self {
        let key = key.to_vec();
        let value = match value {
            CandidateValue::Memory(table) => GuardValue::Memory {
                key: key.clone(),
                table,
                cached: OnceLock::new(),
            },
            CandidateValue::Block { block, range } => GuardValue::Block { block, range },
            CandidateValue::Tombstone => {
                unreachable!("only live source-backed values become guarded candidates")
            }
        };
        Self { key, value }
    }

    /// Borrow the key allocated while the iterator advanced.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Borrow the value, materializing and caching one copy for memtable sources.
    #[inline]
    pub fn value(&self) -> &[u8] {
        match &self.value {
            GuardValue::Memory { table, key, cached } => cached
                .get_or_init(|| {
                    table
                        .get(key)
                        .expect("a frozen scan winner must retain its value")
                })
                .as_slice(),
            GuardValue::Block { block, range } => &block[range.clone()],
        }
    }

    /// Get the value length without materializing an in-memory value.
    #[inline]
    pub fn value_len(&self) -> usize {
        match &self.value {
            GuardValue::Memory { table, key, cached } => cached.get().map_or_else(
                || {
                    table
                        .data
                        .get(key.as_slice())
                        .and_then(|entry| entry.value().value.as_ref().map(Vec::len))
                        .expect("a frozen scan winner must retain its value")
                },
                Vec::len,
            ),
            GuardValue::Block { range, .. } => range.len(),
        }
    }

    /// Consume the guard and return its owned key and an owned value.
    ///
    /// SSTable-backed values are copied from the retained decompressed block;
    /// a previously unloaded memtable value is cloned once.
    #[inline]
    pub fn into_pair(self) -> (Vec<u8>, Vec<u8>) {
        let Self { key, value } = self;
        (key, materialize_value(value))
    }

    /// Consume the guard and return just the key without loading its value.
    #[inline]
    pub fn into_key(self) -> Vec<u8> {
        self.key
    }

    /// Consume the guard and return an owned value, allocating when necessary.
    #[inline]
    pub fn into_value(self) -> Vec<u8> {
        materialize_value(self.value)
    }

    fn value_loaded(&self) -> bool {
        match &self.value {
            GuardValue::Block { .. } => true,
            GuardValue::Memory { cached, .. } => cached.get().is_some(),
        }
    }

    #[cfg(test)]
    pub(crate) fn value_loaded_for_test(&self) -> bool {
        self.value_loaded()
    }
}

fn materialize_value(value: GuardValue) -> Vec<u8> {
    match value {
        GuardValue::Memory { table, key, cached } => cached.into_inner().unwrap_or_else(|| {
            table
                .get(&key)
                .expect("a frozen scan winner must retain its value")
        }),
        GuardValue::Block { block, range } => block[range].to_vec(),
    }
}

/// Iterator over a coherent range of entries.
///
/// Late SSTable checksum or decode failures are returned once and then the
/// iterator is fused. This is intentionally a fallible item type: an infallible
/// iterator cannot stream disk blocks without silently hiding late corruption.
/// Advancing is synchronous and can block on cache locks, mmap page faults,
/// checksum validation, and decompression. Working memory is proportional to
/// source count plus at most one decompressed block per active SSTable source.
/// Dropping the iterator cancels no maintenance; it releases its snapshot
/// sources and directory-ownership guard.
pub struct RangeIter {
    sources: Vec<SourceCursor>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    equal: Vec<HeapEntry>,
    pending_error: Option<StorageError>,
    done: bool,
    _directory_lock: Option<Arc<DirectoryLock>>,
}

impl RangeIter {
    pub(crate) fn from_sources(
        bounds: ScanBounds,
        memtables: Vec<Arc<MemTable>>,
        sstables: Vec<ScanSstable>,
        directory_lock: Arc<DirectoryLock>,
    ) -> std::result::Result<Self, StorageError> {
        let bounds = Arc::new(bounds);
        let mut sources = Vec::with_capacity(memtables.len() + sstables.len());
        sources.extend(
            memtables
                .into_iter()
                .enumerate()
                .map(|(generation_rank, table)| {
                    SourceCursor::Memory(MemoryCursor::new(
                        table,
                        generation_rank as u64,
                        Arc::clone(&bounds),
                    ))
                }),
        );
        sources.extend(
            sstables.into_iter().map(|source| {
                SourceCursor::Sstable(SstableSource::new(source, Arc::clone(&bounds)))
            }),
        );

        let mut iterator = Self {
            equal: Vec::with_capacity(sources.len()),
            sources,
            heap: BinaryHeap::new(),
            pending_error: None,
            done: false,
            _directory_lock: Some(directory_lock),
        };
        for source in 0..iterator.sources.len() {
            if let Some(entry) = iterator.sources[source].next_entry()? {
                iterator.heap.push(Reverse(HeapEntry { source, entry }));
            }
        }
        Ok(iterator)
    }

    pub(crate) fn empty(directory_lock: Arc<DirectoryLock>) -> Self {
        Self {
            sources: Vec::new(),
            heap: BinaryHeap::new(),
            equal: Vec::new(),
            pending_error: None,
            done: false,
            _directory_lock: Some(directory_lock),
        }
    }

    /// Count entries synchronously while propagating any scan error.
    pub fn count(mut self) -> std::result::Result<usize, ScanError> {
        self.try_fold(0, |count, entry| entry.map(|_| count + 1))
    }

    /// Allocate and collect keys without materializing memtable values.
    pub fn keys(mut self) -> std::result::Result<Vec<Vec<u8>>, ScanError> {
        self.try_fold(Vec::new(), |mut keys, entry| {
            keys.push(entry?.into_key());
            Ok(keys)
        })
    }

    /// Allocate and collect all key-value pairs.
    pub fn collect_pairs(mut self) -> std::result::Result<Vec<ScanEntry>, ScanError> {
        self.try_fold(Vec::new(), |mut pairs, entry| {
            pairs.push(entry?.into_pair());
            Ok(pairs)
        })
    }

    /// Lazily skip entries and take a limited number while preserving scan errors.
    ///
    /// Skipped entries still perform the underlying key/block traversal, but
    /// their memtable values are not materialized.
    pub fn paginate(self, offset: usize, limit: usize) -> impl Iterator<Item = ScanResult> {
        Paginate {
            inner: self,
            remaining_offset: offset,
            remaining_limit: limit,
            done: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn working_set_for_test(&self) -> (usize, usize, usize, usize) {
        let mut blocks = HashSet::new();
        let mut block_bytes = 0;
        let mut retain = |block: &Bytes| {
            let pointer = block.as_ptr() as usize;
            if blocks.insert(pointer) {
                block_bytes += block.len();
            }
        };
        for source in &self.sources {
            if let SourceCursor::Sstable(source) = source {
                if let Some(block) = source.cursor.retained_block() {
                    retain(block);
                }
            }
        }
        for head in &self.heap {
            if let CandidateValue::Block { block, .. } = &head.0.entry.value {
                retain(block);
            }
        }
        (
            self.sources.len(),
            self.heap.len(),
            blocks.len(),
            block_bytes,
        )
    }
}

impl Iterator for RangeIter {
    type Item = ScanResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(error) = self.pending_error.take() {
            self.done = true;
            self.heap.clear();
            return Some(Err(error.into()));
        }

        loop {
            let Reverse(first) = self.heap.pop()?;
            let key = first.entry.key.clone();
            self.equal.clear();
            self.equal.push(first);
            while self
                .heap
                .peek()
                .is_some_and(|Reverse(head)| head.entry.key == key)
            {
                let Reverse(head) = self.heap.pop().expect("peeked heap entry");
                self.equal.push(head);
            }

            let mut winner_index = 0;
            for index in 1..self.equal.len() {
                if self.equal[index].entry.order > self.equal[winner_index].entry.order {
                    winner_index = index;
                }
            }
            let winner = self.equal.swap_remove(winner_index);
            let winner_key = winner.entry.key;
            let winner_value = winner.entry.value;

            for source in
                std::iter::once(winner.source).chain(self.equal.drain(..).map(|head| head.source))
            {
                match self.sources[source].next_entry() {
                    Ok(Some(entry)) => self.heap.push(Reverse(HeapEntry { source, entry })),
                    Ok(None) => {}
                    Err(error) => {
                        if self.pending_error.is_none() {
                            self.pending_error = Some(error);
                        }
                    }
                }
            }

            match winner_value {
                CandidateValue::Tombstone => {
                    if let Some(error) = self.pending_error.take() {
                        self.done = true;
                        self.heap.clear();
                        return Some(Err(error.into()));
                    }
                }
                value => return Some(Ok(EntryGuard::from_candidate(winner_key, value))),
            }
        }
    }
}

impl std::iter::FusedIterator for RangeIter {}

struct Paginate {
    inner: RangeIter,
    remaining_offset: usize,
    remaining_limit: usize,
    done: bool,
}

impl Iterator for Paginate {
    type Item = ScanResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.remaining_limit == 0 {
            return None;
        }
        loop {
            match self.inner.next()? {
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
                Ok(_) if self.remaining_offset > 0 => self.remaining_offset -= 1,
                Ok(entry) => {
                    self.remaining_limit -= 1;
                    return Some(Ok(entry));
                }
            }
        }
    }
}

impl std::iter::FusedIterator for Paginate {}

/// Prefix scans use the same streaming implementation as range scans.
pub type PrefixIter = RangeIter;

pub(crate) struct ScanSstable {
    pub(crate) info: SSTableInfo,
    pub(crate) reader: Arc<SSTableReader>,
}

#[derive(Clone)]
pub(crate) enum ScanBounds {
    Range {
        start: Vec<u8>,
        end: Vec<u8>,
    },
    Prefix {
        prefix: Vec<u8>,
        upper: Option<Vec<u8>>,
    },
}

impl ScanBounds {
    pub(crate) fn range(start: &[u8], end: &[u8]) -> Self {
        Self::Range {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    pub(crate) fn prefix(prefix: &[u8]) -> Self {
        Self::Prefix {
            prefix: prefix.to_vec(),
            upper: prefix_upper_bound(prefix),
        }
    }

    pub(crate) fn start(&self) -> &[u8] {
        match self {
            Self::Range { start, .. } => start,
            Self::Prefix { prefix, .. } => prefix,
        }
    }

    pub(crate) fn overlaps_table(&self, info: &SSTableInfo) -> bool {
        if info.min_key.is_empty() && info.max_key.is_empty() {
            return true;
        }
        match self {
            Self::Range { start, end } => {
                info.min_key.as_slice() < end.as_slice()
                    && info.max_key.as_slice() >= start.as_slice()
            }
            Self::Prefix { prefix, upper } => {
                info.max_key.as_slice() >= prefix.as_slice()
                    && upper
                        .as_ref()
                        .map_or(true, |upper| info.min_key.as_slice() < upper.as_slice())
            }
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
            Self::Prefix { prefix, .. } => key.starts_with(prefix),
        }
    }

    fn past_end(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { end, .. } => key >= end.as_slice(),
            Self::Prefix {
                prefix,
                upper: Some(upper),
            } => key >= upper.as_slice() || (key >= prefix.as_slice() && !key.starts_with(prefix)),
            Self::Prefix {
                prefix,
                upper: None,
            } => key >= prefix.as_slice() && !key.starts_with(prefix),
        }
    }
}

enum SourceCursor {
    Memory(MemoryCursor),
    Sstable(SstableSource),
}

impl SourceCursor {
    fn next_entry(&mut self) -> std::result::Result<Option<SourceEntry>, StorageError> {
        match self {
            Self::Memory(cursor) => Ok(cursor.next_entry()),
            Self::Sstable(cursor) => cursor.next_entry(),
        }
    }
}

struct MemoryCursor {
    table: Arc<MemTable>,
    generation_rank: u64,
    bounds: Arc<ScanBounds>,
    last_key: Option<Bytes>,
    done: bool,
}

impl MemoryCursor {
    fn new(table: Arc<MemTable>, generation_rank: u64, bounds: Arc<ScanBounds>) -> Self {
        Self {
            table,
            generation_rank,
            bounds,
            last_key: None,
            done: false,
        }
    }

    fn next_entry(&mut self) -> Option<SourceEntry> {
        if self.done {
            return None;
        }
        let entry = match &self.last_key {
            Some(last_key) => self
                .table
                .data
                .lower_bound::<[u8]>(Bound::Excluded(last_key.as_ref())),
            None => self
                .table
                .data
                .lower_bound::<[u8]>(Bound::Included(self.bounds.start())),
        }?;
        let key = Bytes::from(entry.key().clone());
        if self.bounds.past_end(&key) {
            self.done = true;
            return None;
        }
        self.last_key = Some(key.clone());
        if !self.bounds.contains(&key) {
            self.done = true;
            return None;
        }
        let table_entry = entry.value();
        Some(SourceEntry {
            key,
            order: VersionOrder::memory(table_entry.sequence, self.generation_rank),
            value: if table_entry.is_tombstone() {
                CandidateValue::Tombstone
            } else {
                CandidateValue::Memory(Arc::clone(&self.table))
            },
        })
    }
}

struct SstableSource {
    info: SSTableInfo,
    cursor: SSTableRangeCursor,
    bounds: Arc<ScanBounds>,
    done: bool,
}

impl SstableSource {
    fn new(source: ScanSstable, bounds: Arc<ScanBounds>) -> Self {
        let cursor = SSTableRangeCursor::new(source.reader, bounds.start());
        Self {
            info: source.info,
            cursor,
            bounds,
            done: false,
        }
    }

    fn next_entry(&mut self) -> std::result::Result<Option<SourceEntry>, StorageError> {
        if self.done {
            return Ok(None);
        }
        loop {
            let Some(entry) = self.cursor.next_versioned_ref() else {
                self.done = true;
                return Ok(None);
            };
            let (key, entry) = entry.map_err(|error| StorageError::SSTable(error.to_string()))?;
            if self.bounds.past_end(&key) {
                self.done = true;
                return Ok(None);
            }
            if !self.bounds.contains(&key) {
                continue;
            }
            return Ok(Some(SourceEntry::from_sstable(key, entry, &self.info)));
        }
    }
}

struct SourceEntry {
    key: Bytes,
    order: VersionOrder,
    value: CandidateValue,
}

impl SourceEntry {
    fn from_sstable(key: Bytes, entry: SSTableEntryRef, info: &SSTableInfo) -> Self {
        let order = VersionOrder::sstable(entry.sequence, info.id);
        let value = entry
            .value_range
            .map_or(CandidateValue::Tombstone, |range| CandidateValue::Block {
                block: entry.block,
                range,
            });
        Self { key, order, value }
    }
}

enum CandidateValue {
    Tombstone,
    Memory(Arc<MemTable>),
    Block { block: Bytes, range: Range<usize> },
}

struct HeapEntry {
    source: usize,
    entry: SourceEntry,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.key == other.entry.key && self.source == other.source
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entry
            .key
            .cmp(&other.entry.key)
            .then_with(|| self.source.cmp(&other.source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_prefix_bounds_do_not_overflow() {
        let all = ScanBounds::prefix(b"");
        assert!(all.contains(b""));
        assert!(all.contains(&[0xff, 0xff]));
        assert!(!all.past_end(&[0xff, 0xff]));

        let terminal = ScanBounds::prefix(&[0xff, 0xff]);
        assert!(terminal.contains(&[0xff, 0xff, 0x00]));
        assert!(!terminal.contains(&[0xff]));
        assert!(!terminal.past_end(&[0xff, 0xff, 0xff]));
    }

    #[test]
    fn pagination_reports_an_error_before_its_offset_and_fuses() {
        let iterator = RangeIter {
            sources: Vec::new(),
            heap: BinaryHeap::new(),
            equal: Vec::new(),
            pending_error: Some(StorageError::SSTable("corrupt".to_string())),
            done: false,
            _directory_lock: None,
        };
        let mut page = iterator.paginate(10, 2);
        assert!(page.next().unwrap().is_err());
        assert!(page.next().is_none());
    }
}
