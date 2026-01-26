//! Write Buffer Pool for TurboKV
//!
//! Provides reusable pre-allocated buffers to minimize allocation overhead
//! during high-throughput write operations.
//!
//! ## Optimization Technique
//!
//! Instead of allocating a new `Vec<u8>` for every write batch, we maintain
//! a pool of pre-allocated buffers that can be reused. This reduces:
//! - Memory allocation overhead
//! - GC pressure
//! - Memory fragmentation

use parking_lot::Mutex;
use std::sync::Arc;

/// Default buffer size (64KB)
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Default pool size (number of buffers)
const DEFAULT_POOL_SIZE: usize = 16;

/// A pooled buffer that automatically returns to the pool when dropped
pub struct PooledBuffer {
    buffer: Vec<u8>,
    pool: Arc<BufferPoolInner>,
}

impl PooledBuffer {
    /// Get the buffer as a mutable slice
    #[inline]
    pub fn as_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Get the buffer length
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear the buffer for reuse
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Extend from slice
    #[inline]
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.buffer.extend_from_slice(slice);
    }

    /// Get buffer as slice
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Reserve additional capacity
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        // Return buffer to pool if it hasn't grown too large
        if self.buffer.capacity() <= DEFAULT_BUFFER_SIZE * 4 {
            let mut taken = std::mem::take(&mut self.buffer);
            taken.clear();
            let mut pool = self.pool.buffers.lock();
            if pool.len() < self.pool.max_size {
                pool.push(taken);
            }
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

struct BufferPoolInner {
    buffers: Mutex<Vec<Vec<u8>>>,
    buffer_size: usize,
    max_size: usize,
}

/// A pool of reusable write buffers
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

impl BufferPool {
    /// Create a new buffer pool with default settings
    pub fn new() -> Self {
        Self::with_config(DEFAULT_BUFFER_SIZE, DEFAULT_POOL_SIZE)
    }

    /// Create a buffer pool with custom configuration
    pub fn with_config(buffer_size: usize, pool_size: usize) -> Self {
        // Pre-allocate buffers
        let mut buffers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            buffers.push(Vec::with_capacity(buffer_size));
        }

        Self {
            inner: Arc::new(BufferPoolInner {
                buffers: Mutex::new(buffers),
                buffer_size,
                max_size: pool_size,
            }),
        }
    }

    /// Get a buffer from the pool
    ///
    /// If the pool is empty, allocates a new buffer.
    #[inline]
    pub fn get(&self) -> PooledBuffer {
        let buffer = {
            let mut pool = self.inner.buffers.lock();
            pool.pop()
                .unwrap_or_else(|| Vec::with_capacity(self.inner.buffer_size))
        };

        PooledBuffer {
            buffer,
            pool: Arc::clone(&self.inner),
        }
    }

    /// Get a buffer with at least the specified capacity
    #[inline]
    pub fn get_with_capacity(&self, min_capacity: usize) -> PooledBuffer {
        let mut buffer = {
            let mut pool = self.inner.buffers.lock();
            pool.pop()
                .unwrap_or_else(|| Vec::with_capacity(min_capacity))
        };

        if buffer.capacity() < min_capacity {
            buffer.reserve(min_capacity - buffer.capacity());
        }

        PooledBuffer {
            buffer,
            pool: Arc::clone(&self.inner),
        }
    }

    /// Get current pool size (number of available buffers)
    pub fn available(&self) -> usize {
        self.inner.buffers.lock().len()
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new();
        assert_eq!(pool.available(), DEFAULT_POOL_SIZE);

        // Get a buffer
        let mut buf = pool.get();
        assert_eq!(pool.available(), DEFAULT_POOL_SIZE - 1);

        // Use the buffer
        buf.extend_from_slice(b"hello world");
        assert_eq!(buf.len(), 11);

        // Drop returns to pool
        drop(buf);
        assert_eq!(pool.available(), DEFAULT_POOL_SIZE);
    }

    #[test]
    fn test_buffer_reuse() {
        let pool = BufferPool::new();

        // Get and use a buffer
        {
            let mut buf = pool.get();
            buf.extend_from_slice(&[0u8; 1000]);
        }

        // Get another buffer - should reuse
        let buf = pool.get();
        assert!(buf.buffer.capacity() >= DEFAULT_BUFFER_SIZE);
    }

    #[test]
    fn test_buffer_pool_exhaustion() {
        let pool = BufferPool::with_config(1024, 2);

        let _buf1 = pool.get();
        let _buf2 = pool.get();
        assert_eq!(pool.available(), 0);

        // Should still work - allocates new buffer
        let _buf3 = pool.get();
    }
}
