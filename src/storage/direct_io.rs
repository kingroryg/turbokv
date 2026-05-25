//! Direct I/O support for TurboKV
//!
//! Provides optional Direct I/O (O_DIRECT on Linux, F_NOCACHE on macOS)
//! for bypassing the OS page cache. This is beneficial when:
//!
//! - Working set is larger than available RAM
//! - Application manages its own cache (like our block cache)
//! - Predictable latency is more important than throughput
//!
//! ## Usage
//!
//! This module exposes the low-level building blocks. The public `DbOptions`
//! type does not currently wire direct I/O into the main engine.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Alignment requirement for direct I/O (typically 4KB)
pub const DIRECT_IO_ALIGNMENT: usize = 4096;

/// Configuration for direct I/O
#[derive(Debug, Clone, Copy)]
pub struct DirectIoConfig {
    /// Enable direct I/O for WAL
    pub wal_direct_io: bool,
    /// Enable direct I/O for SSTables
    pub sstable_direct_io: bool,
    /// Alignment size (default: 4KB)
    pub alignment: usize,
}

impl Default for DirectIoConfig {
    fn default() -> Self {
        Self {
            wal_direct_io: false,
            sstable_direct_io: false,
            alignment: DIRECT_IO_ALIGNMENT,
        }
    }
}

impl DirectIoConfig {
    /// Enable direct I/O for all components
    pub fn enabled() -> Self {
        Self {
            wal_direct_io: true,
            sstable_direct_io: true,
            alignment: DIRECT_IO_ALIGNMENT,
        }
    }
}

/// Open a file with optional direct I/O
#[cfg(target_os = "linux")]
pub fn open_with_direct_io<P: AsRef<Path>>(
    path: P,
    create: bool,
    write: bool,
    direct: bool,
) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut opts = OpenOptions::new();
    opts.read(true);

    if write {
        opts.write(true);
    }
    if create {
        opts.create(true);
    }

    if direct {
        // O_DIRECT on Linux
        opts.custom_flags(libc::O_DIRECT);
    }

    opts.open(path)
}

/// Open a file with optional direct I/O (macOS version)
#[cfg(target_os = "macos")]
pub fn open_with_direct_io<P: AsRef<Path>>(
    path: P,
    create: bool,
    write: bool,
    direct: bool,
) -> io::Result<File> {
    use std::os::unix::io::AsRawFd;

    let mut opts = OpenOptions::new();
    opts.read(true);

    if write {
        opts.write(true);
    }
    if create {
        opts.create(true);
    }

    let file = opts.open(path)?;

    if direct {
        // F_NOCACHE on macOS (not true direct I/O, but similar effect)
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
    }

    Ok(file)
}

/// Open a file with optional direct I/O (fallback for other platforms)
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn open_with_direct_io<P: AsRef<Path>>(
    path: P,
    create: bool,
    write: bool,
    _direct: bool, // Ignored on unsupported platforms
) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true);

    if write {
        opts.write(true);
    }
    if create {
        opts.create(true);
    }

    opts.open(path)
}

/// Aligned buffer for direct I/O writes
///
/// Direct I/O requires memory buffers to be aligned to the filesystem's
/// block size (typically 4KB). Memory is manually managed to ensure the
/// deallocation layout matches the allocation layout exactly.
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
    layout: std::alloc::Layout,
    alignment: usize,
}

// Safety: The buffer owns its allocation and access is governed by &/&mut references.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Create a new aligned buffer with the specified capacity
    pub fn new(capacity: usize, alignment: usize) -> Self {
        // Round up capacity to alignment
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

    /// Get the alignment
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Check if the buffer's data pointer is properly aligned
    pub fn is_aligned(&self) -> bool {
        self.ptr as usize % self.alignment == 0
    }

    /// Get the data as a slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Extend the buffer with data, growing if necessary
    pub fn extend_aligned(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        if new_len > self.capacity {
            self.grow(new_len);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len = new_len;
    }

    /// Pad buffer to alignment boundary with zeros
    pub fn pad_to_alignment(&mut self) {
        let remainder = self.len % self.alignment;
        if remainder != 0 {
            let padding = self.alignment - remainder;
            let new_len = self.len + padding;
            if new_len > self.capacity {
                self.grow(new_len);
            }
            unsafe {
                std::ptr::write_bytes(self.ptr.add(self.len), 0, padding);
            }
            self.len = new_len;
        }
    }

    /// Get the length (including any padding)
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Grow the buffer to accommodate at least `min_capacity` bytes.
    fn grow(&mut self, min_capacity: usize) {
        // Double the capacity or use min_capacity, whichever is larger
        let new_capacity = std::cmp::max(self.capacity * 2, min_capacity);
        // Round up to alignment
        let new_capacity = (new_capacity + self.alignment - 1) & !(self.alignment - 1);

        let new_layout = std::alloc::Layout::from_size_align(new_capacity, self.alignment).unwrap();

        let new_ptr = unsafe {
            let p = std::alloc::alloc_zeroed(new_layout);
            if p.is_null() {
                std::alloc::handle_alloc_error(new_layout);
            }
            // Copy existing data
            std::ptr::copy_nonoverlapping(self.ptr, p, self.len);
            // Free old allocation
            std::alloc::dealloc(self.ptr, self.layout);
            p
        };

        self.ptr = new_ptr;
        self.capacity = new_capacity;
        self.layout = new_layout;
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

/// A writer that ensures all writes are properly aligned for direct I/O
pub struct DirectIoWriter {
    file: File,
    buffer: AlignedBuffer,
    alignment: usize,
}

impl DirectIoWriter {
    /// Create a new direct I/O writer
    pub fn new(file: File, buffer_size: usize, alignment: usize) -> Self {
        Self {
            file,
            buffer: AlignedBuffer::new(buffer_size, alignment),
            alignment,
        }
    }

    /// Flush the internal buffer to disk
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            // Pad to alignment
            self.buffer.pad_to_alignment();

            // Write aligned data
            self.file.write_all(self.buffer.as_slice())?;
            self.buffer.clear();
        }
        self.file.sync_all()
    }
}

impl Write for DirectIoWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_aligned(buf);

        // If buffer exceeds threshold, flush
        if self.buffer.len() >= self.alignment * 16 {
            self.buffer.pad_to_alignment();
            self.file.write_all(self.buffer.as_slice())?;
            self.buffer.clear();
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_aligned_buffer() {
        let mut buf = AlignedBuffer::new(4096, DIRECT_IO_ALIGNMENT);
        assert!(buf.is_aligned());

        buf.extend_aligned(b"hello world");
        assert_eq!(buf.len(), 11);

        buf.pad_to_alignment();
        assert_eq!(buf.len() % DIRECT_IO_ALIGNMENT, 0);
    }

    #[test]
    fn test_open_direct_io() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.dat");

        let file = open_with_direct_io(&path, true, true, false);
        assert!(file.is_ok());
    }
}
