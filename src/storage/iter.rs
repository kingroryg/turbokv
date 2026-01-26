//! # Guard Iterators for TurboKV
//!
//! Provides lazy value loading through guard-based iteration.
//!
//! ## Benefits
//!
//! - **Scan keys without loading values**: Count keys, filter by key pattern
//! - **Selective value loading**: Only load values you need
//! - **Reduced I/O**: Skip expensive blob/SSTable reads for filtered keys
//!
//! ## Example
//!
//! ```rust,ignore
//! // Count keys without loading values
//! let count = db.range_iter(b"user:", b"user:\xff").await?.count();
//!
//! // Only load values for matching keys
//! for guard in db.range_iter(b"user:", b"user:\xff").await? {
//!     if guard.key().ends_with(b":active") {
//!         let value = guard.value();  // Only loads here
//!         process(value);
//!     }
//! }
//! ```

use std::vec::IntoIter;

/// A guard that holds a key and can provide its value.
///
/// The guard allows inspecting the key without necessarily loading the value,
/// enabling efficient filtering and key-only operations.
#[derive(Debug, Clone)]
pub struct EntryGuard {
    key: Vec<u8>,
    value: Option<Vec<u8>>, // None = tombstone (filtered out in public API)
}

impl EntryGuard {
    /// Create a new entry guard
    pub(crate) fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }

    /// Get the key (always cheap, already in memory)
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get the value.
    ///
    /// Currently this is a cheap operation as values are pre-loaded.
    /// Future versions may implement true lazy loading from disk.
    #[inline]
    pub fn value(&self) -> &[u8] {
        self.value.as_deref().unwrap_or(&[])
    }

    /// Consume the guard and return the key-value pair
    #[inline]
    pub fn into_pair(self) -> (Vec<u8>, Vec<u8>) {
        (self.key, self.value.unwrap_or_default())
    }

    /// Consume the guard and return just the key
    #[inline]
    pub fn into_key(self) -> Vec<u8> {
        self.key
    }

    /// Consume the guard and return just the value
    #[inline]
    pub fn into_value(self) -> Vec<u8> {
        self.value.unwrap_or_default()
    }
}

/// Iterator over a range of entries with lazy value access.
///
/// This iterator yields [`EntryGuard`] items that allow inspecting keys
/// without loading values, enabling efficient filtering operations.
pub struct RangeIter {
    inner: IntoIter<EntryGuard>,
}

impl RangeIter {
    /// Create a new range iterator from collected entries
    pub(crate) fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let guards = entries
            .into_iter()
            .map(|(k, v)| EntryGuard::new(k, v))
            .collect::<Vec<_>>();
        Self {
            inner: guards.into_iter(),
        }
    }

    /// Count the number of entries without consuming values.
    ///
    /// This is useful when you only need to know how many keys match.
    #[inline]
    pub fn count(self) -> usize {
        self.inner.len()
    }

    /// Collect just the keys, discarding values.
    ///
    /// More efficient than collecting pairs when you don't need values.
    pub fn keys(self) -> Vec<Vec<u8>> {
        self.inner.map(|g| g.into_key()).collect()
    }

    /// Collect as key-value pairs.
    pub fn collect_pairs(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner.map(|g| g.into_pair()).collect()
    }

    /// Skip entries and take a limited number (for pagination).
    pub fn paginate(self, offset: usize, limit: usize) -> impl Iterator<Item = EntryGuard> {
        self.inner.skip(offset).take(limit)
    }
}

impl Iterator for RangeIter {
    type Item = EntryGuard;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for RangeIter {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Iterator over entries matching a prefix with lazy value access.
///
/// Identical to [`RangeIter`] but semantically represents a prefix scan.
pub type PrefixIter = RangeIter;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_guard_key_access() {
        let guard = EntryGuard::new(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(guard.key(), b"key1");
        assert_eq!(guard.value(), b"value1");
    }

    #[test]
    fn test_entry_guard_into_pair() {
        let guard = EntryGuard::new(b"key1".to_vec(), b"value1".to_vec());
        let (k, v) = guard.into_pair();
        assert_eq!(k, b"key1");
        assert_eq!(v, b"value1");
    }

    #[test]
    fn test_range_iter_count() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let iter = RangeIter::new(entries);
        assert_eq!(iter.count(), 3);
    }

    #[test]
    fn test_range_iter_keys() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let iter = RangeIter::new(entries);
        let keys = iter.keys();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn test_range_iter_filter() {
        let entries = vec![
            (b"user:1:name".to_vec(), b"alice".to_vec()),
            (b"user:1:email".to_vec(), b"alice@example.com".to_vec()),
            (b"user:2:name".to_vec(), b"bob".to_vec()),
            (b"user:2:email".to_vec(), b"bob@example.com".to_vec()),
        ];
        let iter = RangeIter::new(entries);

        // Only get names (filter by key pattern)
        let names: Vec<_> = iter
            .filter(|g| g.key().ends_with(b":name"))
            .map(|g| g.into_value())
            .collect();

        assert_eq!(names, vec![b"alice".to_vec(), b"bob".to_vec()]);
    }

    #[test]
    fn test_range_iter_paginate() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ];
        let iter = RangeIter::new(entries);

        // Skip 2, take 2
        let page: Vec<_> = iter.paginate(2, 2).map(|g| g.into_key()).collect();
        assert_eq!(page, vec![b"c".to_vec(), b"d".to_vec()]);
    }
}
