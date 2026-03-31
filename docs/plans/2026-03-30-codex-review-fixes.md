# Codex Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all critical bugs, correctness/claims mismatches, and efficiency issues identified in the Codex review.

**Architecture:** Fixes are organized by severity. Critical bugs first, then correctness issues, then efficiency improvements. Each fix is isolated to minimize blast radius.

**Tech Stack:** Rust, Tokio, bytes crate, CRC32

---

### Task 1: Fix range/scan_prefix merge order (memtable must win over SSTables)

**Files:**
- Modify: `src/storage/engine.rs:403-470`

**Problem:** Memtable results are inserted first, then SSTables overwrite them. Memtable data is newer and should take priority. Also, SSTable read errors are silently dropped via `if let Ok(...)`.

**Step 1: Fix range() merge order**

In `range()`, swap the order: insert SSTable results first (oldest to newest), then memtable results last so they override:

```rust
pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    use std::collections::BTreeMap;

    if self.wal.is_none() {
        self.memtable_manager
            .flush_thread_local()
            .map_err(StorageError::MemTable)?;
    }

    let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    // Get from SSTables first (oldest first, newer overrides)
    let sstables = self.sstables.read().await;
    for sst in sstables.iter() {
        let entries = self.range_from_sstable(sst, start, end).await?;
        for (key, value) in entries {
            merged.insert(key, value);
        }
    }

    // Get from memtable LAST (newest data, overrides SSTable)
    for (key, value) in self.memtable_manager.range(start, end) {
        merged.insert(key, Some(value));
    }

    Ok(merged
        .into_iter()
        .filter_map(|(k, v)| v.map(|val| (k, val)))
        .collect())
}
```

**Step 2: Fix scan_prefix() merge order**

Same fix for `scan_prefix()`:

```rust
pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    use std::collections::BTreeMap;

    if self.wal.is_none() {
        self.memtable_manager
            .flush_thread_local()
            .map_err(StorageError::MemTable)?;
    }

    let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    // Get from SSTables first (oldest first, newer overrides)
    let sstables = self.sstables.read().await;
    for sst in sstables.iter() {
        let entries = self.prefix_from_sstable(sst, prefix).await?;
        for (key, value) in entries {
            merged.insert(key, value);
        }
    }

    // Get from memtable LAST (newest data, overrides SSTable)
    for (key, value) in self.memtable_manager.scan_prefix(prefix) {
        merged.insert(key, Some(value));
    }

    Ok(merged
        .into_iter()
        .filter_map(|(k, v)| v.map(|val| (k, val)))
        .collect())
}
```

Note: The SSTable error handling (`if let Ok(...)`) is also fixed here — we now propagate errors with `?` instead of silently dropping them.

**Step 3: Run tests**

Run: `cargo test`
Expected: All existing tests pass.

**Step 4: Commit**

```bash
git add src/storage/engine.rs
git commit -m "fix: correct range/scan_prefix merge order so memtable wins over SSTables"
```

---

### Task 2: Fix SSTable id:0 from writer

**Files:**
- Modify: `src/storage/sstable/writer.rs:177`
- Modify: `src/storage/engine.rs:768-798`

**Problem:** `SSTableWriter::finish()` returns `SSTableInfo { id: 0, ... }`. The engine sets the proper id when updating the manifest but pushes the `info.clone()` (with id:0) to the in-memory sstables list. This breaks `sstables.retain(|sst| !input_ids.contains(&sst.id))` during compaction since multiple SSTables share id:0.

**Step 1: Set the id on the SSTableInfo before pushing to the list**

In `flush_memtable_to_sstable`, after `writer.finish()`, set the id before using it:

```rust
let mut info = writer
    .finish()
    .map_err(|e| StorageError::SSTable(format!("Failed to finish SSTable: {}", e)))?;

// Set the proper id (writer returns id: 0 as placeholder)
info.id = id;
```

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/storage/sstable/writer.rs src/storage/engine.rs
git commit -m "fix: set SSTable id before adding to in-memory list"
```

---

### Task 3: Fix compaction duplicate-key resolution order

**Files:**
- Modify: `src/storage/compaction.rs:322-330`

**Problem:** The `Ord` impl for `MergeEntry` breaks ties by `source` index with `self.source.cmp(&other.source)`. Since this is used in a `BinaryHeap<Reverse<...>>`, the entry with the *lowest* source index pops first. The comment says "lower source = newer data" but this is not guaranteed — it depends on the order readers are opened, which comes from `job.input_sstables`. If older SSTables appear first in that list, their data wins incorrectly.

The fix: SSTables in the sstables list are appended in creation order (newer SSTables are later in the list), so *higher* source index = newer. We need to reverse the source comparison so that higher source index (newer) wins.

**Step 1: Fix Ord impl**

```rust
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by key first, then by source index (higher source = newer data)
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => other.source.cmp(&self.source),
            other_ord => other_ord,
        }
    }
}
```

With `Reverse<MergeEntry>` in the heap, when two entries have the same key, the one with the higher source index (newer) pops first. The first entry for a key is kept; duplicates are dropped. So newer data wins.

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/storage/compaction.rs
git commit -m "fix: compaction keeps newest entry for duplicate keys"
```

---

### Task 4: Fix AlignedBuffer unsound Vec::from_raw_parts

**Files:**
- Modify: `src/storage/direct_io.rs:149-168`

**Problem:** `Vec::from_raw_parts` requires the pointer to have been allocated by the same allocator (i.e., via `Vec`'s allocator, which is the global allocator). But here we use `std::alloc::alloc_zeroed` with a custom layout that may have different alignment/size than what `Vec` expects. When `Vec` is dropped, it calls `dealloc` with its own calculated layout (`size * capacity, align_of::<u8>() = 1`), not the original custom layout. This is UB.

**Fix:** Use a raw pointer + length/capacity manually, and implement `Drop` to dealloc with the correct layout. Or simpler: just allocate a `Vec<u8>` and check/adjust alignment.

The simplest safe approach: allocate an oversized `Vec<u8>`, find the aligned offset within it, and use a slice. But since `AlignedBuffer` wraps `Vec<u8>` and exposes `data` methods, we'll store the layout alongside and implement `Drop` manually instead of relying on `Vec`'s drop.

**Step 1: Rewrite AlignedBuffer to be sound**

```rust
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
    layout: std::alloc::Layout,
    alignment: usize,
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    pub fn new(capacity: usize, alignment: usize) -> Self {
        let aligned_capacity = (capacity + alignment - 1) & !(alignment - 1);
        let layout = std::alloc::Layout::from_size_align(aligned_capacity, alignment).unwrap();

        let ptr = unsafe {
            let p = std::alloc::alloc_zeroed(layout);
            if p.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            p
        };

        Self {
            ptr,
            len: 0,
            capacity: aligned_capacity,
            layout,
            alignment,
        }
    }

    pub fn alignment(&self) -> usize {
        self.alignment
    }

    pub fn is_aligned(&self) -> bool {
        self.ptr as usize % self.alignment == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        assert!(new_len <= self.capacity, "AlignedBuffer overflow");
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len = new_len;
    }

    pub fn pad_to_alignment(&mut self) {
        let aligned_len = (self.len + self.alignment - 1) & !(self.alignment - 1);
        if aligned_len > self.len && aligned_len <= self.capacity {
            unsafe {
                std::ptr::write_bytes(self.ptr.add(self.len), 0, aligned_len - self.len);
            }
            self.len = aligned_len;
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}
```

Note: Need to update all call sites that use `self.data` to use the new API (`as_slice()`, `extend_from_slice()`, etc.).

**Step 2: Update call sites in direct_io.rs**

Read the rest of `direct_io.rs` to find all usages of `self.data` on `AlignedBuffer` and update them to use the new methods.

**Step 3: Run tests**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/storage/direct_io.rs
git commit -m "fix: make AlignedBuffer sound by using proper alloc/dealloc layout"
```

---

### Task 5: Fix WAL recovery — propagate errors instead of silently stopping

**Files:**
- Modify: `src/storage/wal/file.rs:121-129`
- Modify: `src/storage/wal/iterator.rs:65`

**Problem 1:** In `recover_file()`, the loop breaks on *any* error, treating corruption the same as EOF. This means a single corrupted entry causes all subsequent valid entries to be lost.

**Problem 2:** In `WalEntryIterator::next()`, `Err(_)` moves to the next file, so mid-file corruption silently drops remaining entries.

**Step 1: Fix recover_file to distinguish EOF from corruption**

In `recover_file()`, change the error handling:

```rust
loop {
    match read_entry_versioned(&mut reader, has_merkle) {
        Ok(entry) => {
            last_sequence = entry.sequence;
        }
        Err(WalError::Eof) => break,
        Err(e) => {
            // Log the corruption but stop recovery at this point.
            // Truncate to last valid position on next write.
            tracing::warn!("WAL recovery stopped at corrupted entry: {}", e);
            break;
        }
    }
}
```

**Step 2: Fix WalEntryIterator to distinguish EOF from read errors**

```rust
match read_entry(reader) {
    Ok(entry) => {
        if entry.sequence < self.start_sequence && self.start_sequence > 0 {
            continue;
        }
        return Some(Ok(entry));
    }
    Err(super::types::WalError::Eof) => {
        // End of this file, try next
        if let Err(e) = self.open_next_file() {
            return Some(Err(e));
        }
        if self.reader.is_none() {
            return None;
        }
    }
    Err(e) => {
        // Corrupted entry — report error
        return Some(Err(e));
    }
}
```

**Step 3: Un-ignore WAL tests**

Remove the `#[ignore = ...]` attributes from the two WAL tests in `src/storage/wal/mod.rs:680` and `src/storage/wal/mod.rs:710`. If they fail, debug and fix the underlying issue.

**Step 4: Run tests**

Run: `cargo test`
Expected: PASS (including the previously-ignored WAL tests)

**Step 5: Commit**

```bash
git add src/storage/wal/file.rs src/storage/wal/iterator.rs src/storage/wal/mod.rs
git commit -m "fix: WAL recovery distinguishes EOF from corruption, un-ignore WAL tests"
```

---

### Task 6: Fix DbConfig::durable() vs DbOptions::durable() mismatch

**Files:**
- Modify: `src/core/types.rs:98-102`

**Problem:** `DbConfig::durable()` sets `sync_writes: true` but `DbOptions::durable()` sets `sync_writes: false`. The `DbOptions::durable()` behavior is correct (durable = WAL without fsync per write). `DbConfig::durable()` should match.

**Step 1: Fix DbConfig::durable()**

```rust
pub fn durable() -> Self {
    Self {
        wal_enabled: true,
        compression: Compression::Lz4,
        sync_writes: false,  // Changed from true - durable mode uses periodic sync, not per-write
        memtable_size: 64 * 1024 * 1024,
        block_cache_size: 64 * 1024 * 1024,
        max_open_files: 1000,
        compaction_style: CompactionStyle::SizeTiered,
        max_wal_size: 128 * 1024 * 1024,
        flush_interval: Duration::from_secs(60),
        compaction_interval: Duration::from_secs(300),
    }
}
```

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/core/types.rs
git commit -m "fix: DbConfig::durable() now correctly sets sync_writes=false"
```

---

### Task 7: Fix README claims to match actual defaults

**Files:**
- Modify: `README.md:220-223`

**Problem:** README says default `sync_writes` is `false` and default `compression` is `Lz4`, but `DbOptions::default()` has `sync_writes: true` and `compression: Snappy`.

**Step 1: Fix the DbOptions table in README**

Change the table to match actual code:

```markdown
| Field | Default | Description |
|-------|---------|-------------|
| `wal_enabled` | true | Enable write-ahead log |
| `sync_writes` | true | Sync writes to disk (false = durable mode, true = paranoid mode) |
| `memtable_size` | 64MB | MemTable size before flush |
| `block_cache_size` | 64MB | Block cache size (0 to disable) |
| `compression` | Snappy | Compression algorithm |
```

Also fix line 66 — `Db::open()` uses `DbOptions::default()` which is paranoid mode (sync_writes=true), not durable mode. Update the comment:

```rust
    // Open database with default options (paranoid mode - WAL + fsync)
```

Or alternatively, change `DbOptions::default()` to match the README's stated "durable mode" default by setting `sync_writes: false`. This is a design decision — check which behavior is preferred.

**Decision:** Change `DbOptions::default()` to use `sync_writes: false` (durable mode) since the README says "default options (durable mode)" and durable is the recommended mode. This makes the code match the docs.

**Step 2: Fix DbOptions::default()**

```rust
impl Default for DbOptions {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            sync_writes: false,  // Durable mode (periodic sync, not per-write)
            memtable_size: 64 * 1024 * 1024,
            block_cache_size: 64 * 1024 * 1024,
            compression: Compression::Lz4,  // Match README
        }
    }
}
```

And update the README table to match:

```markdown
| `sync_writes` | false | Sync writes to disk (true = paranoid mode) |
| `compression` | Lz4 | Compression algorithm |
```

**Step 3: Run tests**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add README.md src/storage/db.rs
git commit -m "fix: align DbOptions defaults with README (durable mode, Lz4)"
```

---

### Task 8: Fix batch write atomicity claim

**Files:**
- Modify: `README.md:103`

**Problem:** README says "Atomic batch write" but batch ops are applied one-by-one to the memtable, so readers can observe partial batch state. The WAL write is atomic (single append_batch call), but the memtable application is not.

**Step 1: Update README comment**

Change line 103 from:

```markdown
// Atomic batch write
```

To:

```markdown
// Batch write (atomic in WAL, applied sequentially to memtable)
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: clarify batch write atomicity semantics"
```

---

### Task 9: Implement manifest and SSTable checksums

**Files:**
- Modify: `src/storage/manifest.rs:190-191` (save)
- Modify: `src/storage/manifest.rs:132-133` (load)
- Modify: `src/storage/sstable/writer.rs:164-166`

**Problem:** Checksums are placeholder `0` values with `// TODO` comments.

**Step 1: Implement manifest checksum**

Use CRC32 over the manifest body (everything between magic and checksum). In `save()`, compute CRC32 of all written bytes and write it. In `load()`, verify it.

**Step 2: Implement SSTable file checksum**

In `SSTableWriter::finish()`, compute CRC32 over the file content before the footer checksum field and write it.

**Step 3: Run tests**

Run: `cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/storage/manifest.rs src/storage/sstable/writer.rs
git commit -m "feat: implement manifest and SSTable file checksums"
```

---

### Task 10: Fix iterator "lazy value loading" documentation

**Files:**
- Modify: `src/storage/iter.rs:1-24`

**Problem:** Module docs claim "lazy value loading" and "reduced I/O" but values are fully pre-loaded at construction time. The doc comment on `value()` at line 55-56 already says "Currently this is a cheap operation as values are pre-loaded" — the module header should be consistent.

**Step 1: Update module docs**

```rust
//! # Guard Iterators for TurboKV
//!
//! Provides guard-based iteration over scan results.
//!
//! ## Benefits
//!
//! - **Scan keys without consuming values**: Count keys, filter by key pattern
//! - **Selective value access**: Only access values you need
//! - **Efficient filtering**: Skip entries by key pattern without allocating
//!
//! Note: Values are currently pre-loaded. Future versions may implement
//! true lazy loading from disk for large value workloads.
```

**Step 2: Update README lines 208-209**

Change "lazy value loading" to "guard-based value access":

```markdown
| `range_iter(start, end)` | Range scan with guard-based value access |
| `scan_prefix_iter(prefix)` | Prefix scan with guard-based value access |
```

**Step 3: Commit**

```bash
git add src/storage/iter.rs README.md
git commit -m "docs: clarify iterator guard semantics (values are pre-loaded)"
```

---

### Task 11: Add index-assisted seek for range/prefix SSTable scans

**Files:**
- Modify: `src/storage/engine.rs:676-736`
- Modify: `src/storage/sstable/reader.rs` (may need a new method)

**Problem:** `range_from_sstable` and `prefix_from_sstable` iterate from block 0 of every SSTable, ignoring the index. This is O(n) per SSTable for scans.

**Step 1: Use min_key/max_key to skip irrelevant SSTables**

Before opening an SSTable reader, check if the SSTable's key range overlaps with the query range using the `SSTableInfo` min/max keys:

```rust
async fn range_from_sstable(
    &self,
    sst: &SSTableInfo,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    // Skip SSTable if its key range doesn't overlap with query range
    if !sst.min_key.is_empty() && sst.min_key.as_slice() >= end {
        return Ok(Vec::new());
    }
    if !sst.max_key.is_empty() && sst.max_key.as_slice() < start {
        return Ok(Vec::new());
    }

    // ... existing reader logic
}
```

Same for `prefix_from_sstable`.

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/storage/engine.rs
git commit -m "perf: skip SSTables whose key range doesn't overlap scan range"
```

---

### Task 12: Replace linear scan in SSTableIndex::find_block with binary search

**Files:**
- Modify: `src/storage/sstable/reader.rs:361-380`

**Problem:** `find_block` does a linear scan through index entries. Binary search is O(log n).

**Step 1: Implement binary search**

```rust
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
```

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/storage/sstable/reader.rs
git commit -m "perf: use binary search in SSTableIndex::find_block"
```

---

### Task 13: Invoke WAL truncation after flush

**Files:**
- Modify: `src/storage/engine.rs:787-794`

**Problem:** WAL checkpoint is recorded in the manifest after flush, but `wal.truncate()` is never called in the engine flow. Old WAL files accumulate indefinitely.

**Step 1: Call truncate after updating manifest checkpoint**

After the manifest save in `flush_memtable_to_sstable`, truncate old WAL files:

```rust
// After manifest.save() succeeds:
if let Some(ref wal) = self.wal {
    let checkpoint = wal.current_sequence();
    if let Err(e) = wal.truncate(checkpoint).await {
        tracing::warn!("Failed to truncate WAL: {}", e);
    }
}
```

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/storage/engine.rs
git commit -m "fix: truncate old WAL files after successful flush"
```

---

## Execution Order

1. Task 1 — Range/prefix merge order (critical bug)
2. Task 2 — SSTable id:0 fix (critical bug)
3. Task 3 — Compaction key resolution (critical bug)
4. Task 4 — AlignedBuffer soundness (critical bug)
5. Task 5 — WAL recovery error handling (critical bug)
6. Task 6 — DbConfig::durable() mismatch (correctness)
7. Task 7 — README/defaults alignment (correctness)
8. Task 8 — Batch atomicity docs (correctness)
9. Task 9 — Checksums implementation (correctness)
10. Task 10 — Iterator docs fix (correctness)
11. Task 11 — Index-assisted SSTable scans (efficiency)
12. Task 12 — Binary search in index (efficiency)
13. Task 13 — WAL truncation (efficiency)
