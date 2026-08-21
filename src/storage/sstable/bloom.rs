//! Bloom filter implementation for SSTable using gxhash for speed
//!
//! Exact-key Bloom filters let point reads skip SSTables that cannot contain
//! the requested key.

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
        let h = bloom_hash(key);
        let h1 = h as usize;
        let h2 = (h >> 32) as usize;

        for i in 0..self.num_hash_functions {
            let bit_pos = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        let h = bloom_hash(key);
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

    /// Reconstruct the exact parameters persisted in an SSTable.
    pub(crate) fn from_serialized_parts(
        bits: Vec<u8>,
        num_bits: usize,
        num_hash_functions: usize,
    ) -> Self {
        Self {
            bits,
            num_bits,
            num_hash_functions,
        }
    }

    pub fn metadata(&self) -> (usize, usize) {
        (self.num_hash_functions, self.num_bits)
    }
}

#[inline]
fn bloom_hash(key: &[u8]) -> u64 {
    // gxhash deliberately performs a masked 16-byte SIMD load for short
    // inputs. Its page-boundary check prevents a fault but can still read past
    // the Rust allocation, which native sanitizers correctly reject. Keep the
    // exact released hash algorithm (Bloom bits are persisted) while giving
    // that load a fully initialized allocation to read.
    if key.len() <= 16 {
        let mut padded = [0_u8; 16];
        padded[..key.len()].copy_from_slice(key);
        gxhash::gxhash64(&padded[..key.len()], 0)
    } else {
        gxhash::gxhash64(key, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_hash_and_bit_vectors_cover_padded_and_direct_paths() {
        const EXPECTED_HASHES: [u64; 19] = [
            0xeed9_6c43_96ff_e83f,
            0x0b68_680a_fced_5be3,
            0x467d_c74c_9e91_12b3,
            0x6762_dc1e_d959_18d9,
            0xd73f_72be_76f8_ec31,
            0xdaac_8c58_478a_33d2,
            0xf40c_b800_42a0_fd1f,
            0xb136_8985_ebe1_cdfb,
            0x563c_d810_41fa_220e,
            0x5510_744c_80e5_9def,
            0x9ddc_83c0_e0a5_aa0f,
            0x6e4e_6e29_2d68_8ffa,
            0x9c55_7ef1_760f_6811,
            0x1f9f_0bd1_edc5_96ba,
            0x319c_c1f2_a6c3_9932,
            0xfaa0_2122_6a0c_c4e1,
            0x3ae0_af30_4681_87e5,
            0x8d6e_ba78_aec8_784b,
            0x33ac_a5e3_9374_b83c,
        ];
        const EXPECTED_BITS: [u8; 48] = [
            0x06, 0x8c, 0x20, 0xc2, 0x28, 0xa2, 0x84, 0x1c, 0x80, 0xc1, 0x0b, 0x52, 0x1c, 0x90,
            0x20, 0x44, 0xac, 0xc1, 0xda, 0xc0, 0x31, 0x6d, 0x2e, 0x08, 0x28, 0x88, 0x20, 0x44,
            0x80, 0x82, 0x20, 0x80, 0x2d, 0xec, 0x20, 0xca, 0x20, 0x22, 0x88, 0xc4, 0xe0, 0x98,
            0x26, 0x00, 0x2a, 0xe8, 0x8b, 0x08,
        ];
        const EXPECTED_POSITIONS: [(usize, [usize; 8]); 6] = [
            (0, [319, 130, 325, 136, 331, 142, 337, 148]),
            (1, [355, 365, 375, 1, 11, 21, 31, 41]),
            (15, [353, 131, 293, 71, 233, 11, 173, 335]),
            (16, [357, 277, 197, 117, 37, 341, 261, 181]),
            (17, [331, 195, 59, 307, 171, 35, 283, 147]),
            (18, [60, 159, 258, 357, 72, 171, 270, 369]),
        ];

        let mut filter = BloomFilter::new(12, 32);
        let keys = (0..=17)
            .map(|length| (0..length).map(|byte| byte as u8).collect::<Vec<_>>())
            .chain(std::iter::once((0..31).map(|byte| byte as u8).collect()))
            .collect::<Vec<_>>();

        for (key, expected_hash) in keys.iter().zip(EXPECTED_HASHES) {
            assert_eq!(
                bloom_hash(key),
                expected_hash,
                "fixed hash for key length {}",
                key.len()
            );
            filter.insert(key);
            assert!(filter.contains(key), "short key length {}", key.len());

            if key.len() <= 16 {
                let mut padded = [0_u8; 16];
                padded[..key.len()].copy_from_slice(key);
                // Passing the original logical length over the exact readable
                // SIMD width produces the released gxhash mapping.
                assert_eq!(bloom_hash(key), gxhash::gxhash64(&padded[..key.len()], 0));
            }
        }
        for (key_index, expected_positions) in EXPECTED_POSITIONS {
            let hash = EXPECTED_HASHES[key_index];
            let low_seed = hash as usize;
            let high_seed = (hash >> 32) as usize;
            let positions = std::array::from_fn(|index| {
                low_seed.wrapping_add(index.wrapping_mul(high_seed)) % 384
            });
            assert_eq!(
                positions,
                expected_positions,
                "fixed bit positions for key length {}",
                keys[key_index].len()
            );
        }
        // The fixed bytes exercise both double-hash components: the low half
        // supplies the first bit and the high half supplies every stride.
        assert_eq!(filter.as_bytes(), EXPECTED_BITS);
        assert!(keys.iter().all(|key| filter.contains(key)));
    }
}
