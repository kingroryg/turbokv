//! Bloom filter implementation for SSTable using gxhash for speed
//!
//! Includes both standard bloom filters and prefix bloom filters for
//! optimized range/prefix scan operations.

pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hash_functions: usize,
}

/// Prefix bloom filter for optimized prefix lookups
///
/// Unlike a standard bloom filter that checks for exact key matches,
/// this filter indexes key prefixes at multiple lengths to quickly
/// determine if any keys with a given prefix exist.
pub struct PrefixBloomFilter {
    /// Bloom filter for prefix lookups
    filter: BloomFilter,
    /// Maximum prefix length to index
    max_prefix_len: usize,
}

impl PrefixBloomFilter {
    /// Create a new prefix bloom filter
    ///
    /// # Arguments
    /// * `bits_per_key` - Bits per key (higher = lower false positive rate)
    /// * `num_keys` - Expected number of keys
    /// * `max_prefix_len` - Maximum prefix length to index (e.g., 8 means prefixes of length 1-8 are indexed)
    pub fn new(bits_per_key: usize, num_keys: usize, max_prefix_len: usize) -> Self {
        // Account for multiple prefix insertions per key
        let effective_keys = num_keys * max_prefix_len;
        Self {
            filter: BloomFilter::new(bits_per_key, effective_keys),
            max_prefix_len,
        }
    }

    /// Create with a target false positive rate
    pub fn with_rate(false_positive_rate: f64, expected_items: usize, max_prefix_len: usize) -> Self {
        let effective_items = expected_items * max_prefix_len;
        Self {
            filter: BloomFilter::with_rate(false_positive_rate, effective_items),
            max_prefix_len,
        }
    }

    /// Insert a key, adding all its prefixes to the filter
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        // Insert prefixes of increasing length
        let max_len = std::cmp::min(key.len(), self.max_prefix_len);
        for len in 1..=max_len {
            self.filter.insert(&key[..len]);
        }
    }

    /// Check if any keys with the given prefix might exist
    ///
    /// Returns false if definitely no keys with this prefix exist.
    /// Returns true if keys might exist (may be false positive).
    #[inline]
    pub fn may_contain_prefix(&self, prefix: &[u8]) -> bool {
        if prefix.is_empty() {
            return true; // Empty prefix matches everything
        }

        // Check the prefix directly
        let check_len = std::cmp::min(prefix.len(), self.max_prefix_len);
        self.filter.contains(&prefix[..check_len])
    }

    /// Get the filter as bytes for serialization
    pub fn as_bytes(&self) -> &[u8] {
        self.filter.as_bytes()
    }

    /// Reconstruct from bytes
    pub fn from_bytes(bits: Vec<u8>, bits_per_key: usize, max_prefix_len: usize) -> Self {
        Self {
            filter: BloomFilter::from_bytes(bits, bits_per_key),
            max_prefix_len,
        }
    }

    /// Get metadata for serialization
    pub fn metadata(&self) -> (usize, usize, usize) {
        let (num_hash, num_bits) = self.filter.metadata();
        (num_hash, num_bits, self.max_prefix_len)
    }
}

impl BloomFilter {
    pub fn new(bits_per_key: usize, num_keys: usize) -> Self {
        let num_bits = std::cmp::max(64, bits_per_key * num_keys);
        let num_bytes = (num_bits + 7) / 8;
        let num_hash_functions = std::cmp::max(1, (bits_per_key as f64 * 0.69) as usize);
        
        Self {
            bits: vec![0; num_bytes],
            num_bits,
            num_hash_functions,
        }
    }
    
    pub fn with_rate(false_positive_rate: f64, expected_items: usize) -> Self {
        let bits_per_key = (-false_positive_rate.ln() / (2.0_f64.ln().powi(2))) * 1.44;
        Self::new(bits_per_key.ceil() as usize, expected_items)
    }
    
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        let h = gxhash::gxhash64(key, 0);
        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        
        for i in 0..self.num_hash_functions {
            let bit_pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }
    
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let h = gxhash::gxhash64(key, 0);
        let h1 = h as usize;
        let h2 = (h >> 32) as usize;
        
        for i in 0..self.num_hash_functions {
            let bit_pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            if (self.bits[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }
        true
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
    
    pub fn from_bytes(bits: Vec<u8>, bits_per_key: usize) -> Self {
        let num_bits = bits.len() * 8;
        let num_hash_functions = std::cmp::max(1, (bits_per_key as f64 * 0.69) as usize);
        Self { bits, num_bits, num_hash_functions }
    }
    
    pub fn metadata(&self) -> (usize, usize) {
        (self.num_hash_functions, self.num_bits)
    }
}