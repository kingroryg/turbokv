//! Bloom filter implementation for SSTable using gxhash for speed

pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hash_functions: usize,
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