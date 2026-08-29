//! PROTOTYPE: pure WAL arithmetic selected for a Creusot feasibility proof.
//!
//! This module lives on the verification-spike branch only. It isolates an
//! actual production recovery calculation from filesystem, mmap, async, and
//! concurrency code that Creusot is not intended to model directly.

#![allow(unexpected_cfgs)]

#[cfg(creusot)]
use creusot_std::prelude::*;

/// Compute the exclusive end of a record without allowing machine-integer
/// overflow.
///
/// Recovery must never accept a wrapped end offset: doing so could turn a
/// malformed record length into an apparently in-bounds tail.
#[cfg_attr(
    creusot,
    ensures(match result {
        Some(end) => end@ == offset@ + header_length@ + payload_length@,
        None => offset@ + header_length@ + payload_length@ > u64::MAX@,
    })
)]
pub(super) fn checked_record_end(
    offset: u64,
    header_length: u64,
    payload_length: u64,
) -> Option<u64> {
    offset
        .checked_add(header_length)
        .and_then(|end| end.checked_add(payload_length))
}
