//! Shared diagnostics for deterministic storage stress tests.

use std::fmt::Display;

use rand::Rng;

pub(crate) fn stress_context(
    seed: u64,
    sequence: u64,
    generation: impl Display,
    file: impl Display,
) -> String {
    format!("seed={seed:#018x} sequence={sequence} generation={generation} file={file}")
}

pub(crate) fn stress_key_value(
    rng: &mut impl Rng,
    maximum_value_length: usize,
) -> (usize, Vec<u8>, Vec<u8>) {
    let key_index = rng.gen_range(0..24);
    let key = format!("stress:key:{key_index:02}").into_bytes();
    let mut value = vec![0; rng.gen_range(0..=maximum_value_length)];
    rng.fill(&mut value[..]);
    (key_index, key, value)
}
