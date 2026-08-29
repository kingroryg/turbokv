//! PROTOTYPE: Creusot proof harness for TurboKV WAL-v5 arithmetic.

#![allow(unused_variables)]

extern crate creusot_std;

use creusot_std::prelude::*;

#[path = "../../../src/storage/wal/verified_arithmetic.rs"]
mod verified_arithmetic;

/// Check that every successfully calculated record end is exact, monotonic,
/// and cannot lie before either part of its encoded prefix.
pub fn prove_successful_record_end_is_exact(offset: u64, header_length: u64, payload_length: u64) {
    if let Some(end) =
        verified_arithmetic::checked_record_end(offset, header_length, payload_length)
    {
        proof_assert!(end@ == offset@ + header_length@ + payload_length@);
        proof_assert!(end@ >= offset@);
        proof_assert!(end@ >= header_length@);
        proof_assert!(end@ >= payload_length@);
    }
}

/// Check that failure has only one cause: the mathematical end is not
/// representable as a `u64`.
pub fn prove_rejection_means_overflow(offset: u64, header_length: u64, payload_length: u64) {
    if verified_arithmetic::checked_record_end(offset, header_length, payload_length).is_none() {
        proof_assert!(offset@ + header_length@ + payload_length@ > u64::MAX@);
    }
}
