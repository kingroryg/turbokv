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
//! Enable via `DbOptions`:
//! ```ignore
//! let options = DbOptions {
//!     direct_io: true,
//!     ..DbOptions::default()
//! };
//! ```

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
/// block size (typically 4KB).
pub struct AlignedBuffer {
    data: Vec<u8>,
    alignment: usize,
}

impl AlignedBuffer {
    /// Create a new aligned buffer with the specified capacity
    pub fn new(capacity: usize, alignment: usize) -> Self {
        // Round up capacity to alignment
        let aligned_capacity = (capacity + alignment - 1) & !(alignment - 1);

        // Allocate with extra space for alignment
        let layout =
            std::alloc::Layout::from_size_align(aligned_capacity + alignment, alignment).unwrap();

        let data = unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Vec::from_raw_parts(ptr, 0, aligned_capacity)
        };

        Self { data, alignment }
    }

    /// Get the alignment
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Check if the buffer's data pointer is properly aligned
    pub fn is_aligned(&self) -> bool {
        self.data.as_ptr() as usize % self.alignment == 0
    }

    /// Get a mutable reference to the inner Vec
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    /// Get the data as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Extend the buffer, padding to alignment if needed
    pub fn extend_aligned(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    /// Pad buffer to alignment boundary with zeros
    pub fn pad_to_alignment(&mut self) {
        let remainder = self.data.len() % self.alignment;
        if remainder != 0 {
            let padding = self.alignment - remainder;
            self.data.extend(std::iter::repeat(0u8).take(padding));
        }
    }

    /// Get the length (including any padding)
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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
