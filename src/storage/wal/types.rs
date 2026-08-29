use bytes::Bytes;
use std::time::Duration;
use thiserror::Error;

/// Magic bytes at the start of every WAL segment.
pub const WAL_MAGIC: &[u8; 8] = b"TURBOKV\0";
/// Stable identifier for the first released WAL layout.
pub const WAL_VERSION_V1: u32 = 1;
/// Stable identifier for the second released WAL layout.
///
/// Versions 1 and 2 share the same physical entry representation.
pub const WAL_VERSION_V2: u32 = 2;
/// Stable identifier for entries after removal of the legacy extension.
pub const WAL_VERSION_V3: u32 = 3;
/// Stable identifier for the atomic-batch WAL layout.
pub const WAL_VERSION_V4: u32 = 4;
/// Stable identifier written by this release.
///
/// Version 5 adds a last-published record tag, a checksum covering record
/// framing metadata, and an acknowledged logical-end lower bound. Opening a
/// validated v1-v4 WAL starts a new v5 segment and retains older segments for
/// replay until their checkpoint permits reclamation.
pub const WAL_VERSION: u32 = 5;
/// Encoded segment-header size in bytes.
pub const WAL_HEADER_SIZE: usize = 64;
/// Encoded v3-v5 record-header size in bytes.
pub const ENTRY_HEADER_SIZE: usize = 32;
/// Largest supported paranoid group-commit collection window.
pub const MAX_GROUP_COMMIT_DELAY_US: u64 = 60_000_000;
pub(crate) const LEGACY_ENTRY_EXTENSION_SIZE: usize = 96;
pub(crate) const WAL_FIRST_SEQUENCE_OFFSET: u64 = 20;
pub(crate) const ENTRY_TYPE_OFFSET: usize = 20;
pub(crate) const ENTRY_FLAGS_OFFSET: usize = 21;
#[cfg(test)]
pub(crate) const ENTRY_CRC_OFFSET: u64 = 22;
pub(crate) const ENTRY_RESERVED_START: usize = 26;
pub(crate) const ENTRY_RESERVED_SIZE: usize = ENTRY_HEADER_SIZE - ENTRY_RESERVED_START;
pub(crate) const WAL_ACKNOWLEDGED_END_OFFSET: u64 = 48;
pub(crate) const WAL_ACKNOWLEDGED_END_CRC_OFFSET: u64 = 56;
pub(crate) const V5_COMMIT_TAG: [u8; 2] = [0xa5, 0x5a];
pub(crate) const V5_COMMIT_TAG_OFFSET: usize = ENTRY_HEADER_SIZE - V5_COMMIT_TAG.len();
const BATCH_HEADER_SIZE: usize = 4;
const BATCH_OPERATION_HEADER_SIZE: usize = 9;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SingleRecordLengths {
    pub key: u32,
    pub payload: u32,
}

#[derive(Debug, Error)]
/// Errors produced while validating, reading, writing, syncing, or reclaiming WALs.
pub enum WalError {
    /// A filesystem operation failed.
    #[error("I/O error: {message}")]
    Io {
        /// Operation-specific failure description.
        message: String,
        /// Optional underlying operating-system error.
        #[source]
        source: Option<std::io::Error>,
    },

    /// Header, record, sequence, or configuration bytes violate the WAL format.
    #[error("Invalid WAL format: {0}")]
    InvalidFormat(String),

    /// A record does not match its stored CRC32.
    ///
    /// Versions 1 through 4 cover the payload; version 5 covers the framing
    /// fields and payload together.
    #[error("CRC mismatch: data corrupted")]
    CrcMismatch,

    /// Bytes cannot be safely classified as a recoverable active-tail failure.
    #[error("WAL corruption: {0}")]
    Corruption(String),

    /// The paranoid group-commit writer is unavailable or poisoned.
    #[error("Channel closed")]
    ChannelClosed,

    /// An iterator or decoder reached the physical end of a segment.
    #[error("EOF reached")]
    Eof,
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io {
            message: e.to_string(),
            source: Some(e),
        }
    }
}

/// Result type for low-level WAL operations.
pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
/// Physical WAL record types.
pub enum EntryType {
    /// Normal key-value data
    Data = 1,
    /// Reserved legacy checkpoint record; current checkpoints live in the manifest.
    Checkpoint = 2,
    /// Reserved legacy truncation record; current reclamation unlinks whole segments.
    Truncate = 3,
    /// Tombstone (key deletion)
    Delete = 4,
    /// A checksummed envelope containing one atomic logical write batch.
    Batch = 5,
}

impl TryFrom<u8> for EntryType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(EntryType::Data),
            2 => Ok(EntryType::Checkpoint),
            3 => Ok(EntryType::Truncate),
            4 => Ok(EntryType::Delete),
            5 => Ok(EntryType::Batch),
            _ => Err(WalError::InvalidFormat(format!(
                "Invalid entry type: {}",
                value
            ))),
        }
    }
}

/// One logical mutation decoded from a validated WAL record.
///
/// The `data` field contains an encoded key-value pair:
/// - For Data: `[key_len: u32][key][value]`
/// - For Delete: `[key_len: u32][key]`
///
/// Physical batch envelopes are expanded by the public WAL iterator and read
/// methods, so callers normally receive `Data` and `Delete` entries.
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Engine-wide mutation sequence.
    pub sequence: u64,
    /// Milliseconds since the Unix epoch recorded when the physical record formed.
    pub timestamp: u64,
    /// Logical mutation kind.
    pub entry_type: EntryType,
    /// Encoded key and optional value payload.
    pub data: Bytes,
}

impl WalEntry {
    /// Borrow the encoded key, or return `None` for a malformed payload.
    pub fn decode_key(&self) -> Option<&[u8]> {
        if self.data.len() < 4 {
            return None;
        }
        let key_len =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;

        if self.data.len() < 4 + key_len {
            return None;
        }
        Some(&self.data[4..4 + key_len])
    }

    /// Borrow the encoded value.
    ///
    /// Returns `None` for deletes and malformed payloads. An empty stored value
    /// is returned as `Some(&[])`.
    pub fn decode_value(&self) -> Option<&[u8]> {
        if self.entry_type == EntryType::Delete {
            return None;
        }
        if self.data.len() < 4 {
            return None;
        }
        let key_len =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;

        if self.data.len() < 4 + key_len {
            return None;
        }
        Some(&self.data[4 + key_len..])
    }

    /// Decode a non-batch mutation into its borrowed key and optional value.
    ///
    /// Returns `None` for malformed payloads and physical batch envelopes.
    pub fn decode_kv(&self) -> Option<(&[u8], Option<&[u8]>)> {
        if self.entry_type == EntryType::Batch {
            return None;
        }
        let key = self.decode_key()?;
        let value = if self.entry_type == EntryType::Delete {
            None
        } else {
            self.decode_value()
        };
        Some((key, value))
    }

    /// Lowest and highest engine sequence represented by this physical record.
    pub(crate) fn sequence_bounds(&self) -> Result<(u64, u64)> {
        if self.entry_type != EntryType::Batch {
            return Ok((self.sequence, self.sequence));
        }

        let operation_count = batch_operation_count(&self.data)?;
        let last_sequence = self
            .sequence
            .checked_add(operation_count.saturating_sub(1) as u64)
            .ok_or_else(|| WalError::InvalidFormat("batch sequence range overflows".to_string()))?;
        Ok((self.sequence, last_sequence))
    }

    /// Expand an atomic physical batch record into its logical mutations.
    pub(crate) fn into_logical_entries(self) -> Result<Vec<Self>> {
        if self.entry_type != EntryType::Batch {
            return Ok(vec![self]);
        }

        let operation_count = batch_operation_count(&self.data)?;
        let mut entries = Vec::with_capacity(operation_count);
        let mut offset = BATCH_HEADER_SIZE;
        for index in 0..operation_count {
            let entry_type = EntryType::try_from(self.data[offset])?;
            let key_length = read_u32(&self.data, offset + 1)? as usize;
            let value_length = read_u32(&self.data, offset + 5)? as usize;
            offset += BATCH_OPERATION_HEADER_SIZE;
            let key_end = offset
                .checked_add(key_length)
                .ok_or_else(|| WalError::InvalidFormat("batch key length overflows".to_string()))?;
            let value_end = key_end.checked_add(value_length).ok_or_else(|| {
                WalError::InvalidFormat("batch value length overflows".to_string())
            })?;
            let key = &self.data[offset..key_end];
            let value = &self.data[key_end..value_end];
            let data = match entry_type {
                EntryType::Data => encode_kv(key, value),
                EntryType::Delete => encode_delete(key),
                _ => {
                    return Err(WalError::InvalidFormat(
                        "batch contains a non-mutation entry".to_string(),
                    ));
                }
            };
            let sequence = self.sequence.checked_add(index as u64).ok_or_else(|| {
                WalError::InvalidFormat("batch sequence range overflows".to_string())
            })?;
            entries.push(Self {
                sequence,
                timestamp: self.timestamp,
                entry_type,
                data: Bytes::from(data),
            });
            offset = value_end;
        }
        Ok(entries)
    }
}

impl AsRef<WalEntry> for WalEntry {
    fn as_ref(&self) -> &WalEntry {
        self
    }
}

#[derive(Debug, Clone)]
/// Low-level WAL rotation and acknowledgement configuration.
///
/// `sync_on_write = false` acknowledges after a committed record reaches the
/// operating-system page cache through a physically reserved shared mapping or
/// ordered-write fallback. `true` routes callers through ordered group commit
/// and acknowledges only after `File::sync_all` succeeds.
pub struct WalConfig {
    /// Maximum file size before rotation (bytes)
    pub max_file_size: u64,
    /// Sync to disk after each write
    pub sync_on_write: bool,
    /// Maximum time the paranoid writer waits to collect a commit group.
    ///
    /// Zero disables the intentional wait while still grouping requests that
    /// are already queued together.
    pub group_commit_delay_us: u64,
    /// Maximum number of callers in one paranoid commit group.
    ///
    /// This must be greater than zero. One caller may still contain an atomic
    /// [`WriteBatch`](crate::core::WriteBatch) with multiple logical mutations.
    /// The legacy field name is retained for source compatibility; use
    /// [`WalConfig::with_max_group_size`] when constructing new configurations.
    pub max_batch_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            group_commit_delay_us: 2000,
            max_batch_size: 512,
        }
    }
}

impl WalConfig {
    /// Configure unsynced WAL acknowledgement for throughput-oriented tools.
    pub fn fast() -> Self {
        Self {
            sync_on_write: false,
            group_commit_delay_us: 500,
            max_batch_size: 1024,
            ..Default::default()
        }
    }

    /// Configure process-crash-oriented WAL acknowledgement without per-write sync.
    ///
    /// Success means the committed record reached the operating-system page
    /// cache. Supported local filesystems use bounded physical reservation and
    /// a shared mapping; unsupported allocation APIs retain ordered file writes.
    /// It does not promise recent acknowledgements survive power loss.
    pub fn durable() -> Self {
        Self {
            sync_on_write: false,
            group_commit_delay_us: 100, // Low delay for throughput
            max_batch_size: 1024,
            ..Default::default()
        }
    }

    /// Configure group-committed `File::sync_all` acknowledgement.
    ///
    /// This is power-loss-oriented but still depends on the filesystem and
    /// device honoring the platform sync contract.
    pub fn paranoid() -> Self {
        Self {
            sync_on_write: true,
            group_commit_delay_us: 0, // No delay - fsync latency itself batches concurrent writes
            ..Default::default()
        }
    }

    /// Set the bounded collection window for paranoid group commit.
    ///
    /// Durations larger than [`u64::MAX`] microseconds are saturated and values
    /// above 60 seconds are rejected when the WAL is opened. A zero duration
    /// performs no intentional wait but still drains requests already queued
    /// behind the current writer.
    pub fn with_group_commit_delay(mut self, delay: Duration) -> Self {
        self.group_commit_delay_us = u64::try_from(delay.as_micros()).unwrap_or(u64::MAX);
        self
    }

    /// Set the maximum number of callers sharing one paranoid durability
    /// barrier.
    ///
    /// A value of zero is rejected when the WAL is opened.
    pub fn with_max_group_size(mut self, maximum: usize) -> Self {
        self.max_batch_size = maximum;
        self
    }
}

#[inline]
/// Allocate the version-independent payload for one put record.
///
/// # Panics
///
/// Panics when the key and value cannot fit in one WAL record. Database and
/// WAL append APIs report that condition as [`WalError::InvalidFormat`] before
/// allocating a sequence; this low-level encoder is intended for validated
/// format tooling and fixtures.
pub fn encode_kv(key: &[u8], value: &[u8]) -> Vec<u8> {
    let lengths = checked_single_record_lengths(key.len(), value.len())
        .expect("put payload exceeds the WAL record limit");
    let mut buf = Vec::with_capacity(lengths.payload as usize);
    buf.extend_from_slice(&lengths.key.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

#[inline]
/// Allocate the version-independent payload for one delete record.
///
/// # Panics
///
/// Panics when the key cannot fit in one WAL record. Database and WAL append
/// APIs report that condition as [`WalError::InvalidFormat`] before allocating
/// a sequence; this low-level encoder is intended for validated format tooling
/// and fixtures.
pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let lengths = checked_single_record_lengths(key.len(), 0)
        .expect("delete payload exceeds the WAL record limit");
    let mut buf = Vec::with_capacity(lengths.payload as usize);
    buf.extend_from_slice(&lengths.key.to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

pub(crate) fn checked_single_record_lengths(
    key_length: usize,
    value_length: usize,
) -> Result<SingleRecordLengths> {
    let payload_length = 4_usize
        .checked_add(key_length)
        .and_then(|length| length.checked_add(value_length))
        .ok_or_else(|| WalError::InvalidFormat("WAL record payload size overflows".to_string()))?;
    let key = u32::try_from(key_length)
        .map_err(|_| WalError::InvalidFormat("WAL record key is too large".to_string()))?;
    let payload = checked_record_payload_length(payload_length)?;
    Ok(SingleRecordLengths { key, payload })
}

pub(crate) fn checked_record_payload_length(payload_length: usize) -> Result<u32> {
    u32::try_from(payload_length)
        .map_err(|_| WalError::InvalidFormat("WAL record payload is too large".to_string()))
}

/// Encode a logical write batch into one checksummed WAL record payload.
pub(crate) fn encode_batch(entries: &[(&[u8], Option<&[u8]>)]) -> Result<Vec<u8>> {
    let operation_count = u32::try_from(entries.len())
        .map_err(|_| WalError::InvalidFormat("batch contains too many operations".to_string()))?;
    let total_size = entries
        .iter()
        .try_fold(BATCH_HEADER_SIZE, |size, (key, value)| {
            let _ = u32::try_from(key.len())
                .map_err(|_| WalError::InvalidFormat("batch key is too large".to_string()))?;
            let value_length = value.map_or(0, <[u8]>::len);
            let _ = u32::try_from(value_length)
                .map_err(|_| WalError::InvalidFormat("batch value is too large".to_string()))?;
            size.checked_add(BATCH_OPERATION_HEADER_SIZE)
                .and_then(|size| size.checked_add(key.len()))
                .and_then(|size| size.checked_add(value_length))
                .ok_or_else(|| WalError::InvalidFormat("batch payload is too large".to_string()))
        })?;
    let _ = u32::try_from(total_size)
        .map_err(|_| WalError::InvalidFormat("batch payload is too large".to_string()))?;

    let mut encoded = Vec::with_capacity(total_size);
    encoded.extend_from_slice(&operation_count.to_le_bytes());
    for (key, value) in entries {
        let entry_type = if value.is_some() {
            EntryType::Data
        } else {
            EntryType::Delete
        };
        encoded.push(entry_type as u8);
        encoded.extend_from_slice(&(key.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&(value.map_or(0, <[u8]>::len) as u32).to_le_bytes());
        encoded.extend_from_slice(key);
        if let Some(value) = value {
            encoded.extend_from_slice(value);
        }
    }
    Ok(encoded)
}

pub(crate) fn validate_batch(data: &[u8]) -> Result<()> {
    let operation_count = batch_operation_count(data)?;
    let mut offset = BATCH_HEADER_SIZE;
    for _ in 0..operation_count {
        let operation_header_end = offset
            .checked_add(BATCH_OPERATION_HEADER_SIZE)
            .ok_or_else(|| WalError::InvalidFormat("batch offset overflows".to_string()))?;
        if operation_header_end > data.len() {
            return Err(WalError::InvalidFormat(
                "batch operation header is truncated".to_string(),
            ));
        }
        let entry_type = EntryType::try_from(data[offset])?;
        if !matches!(entry_type, EntryType::Data | EntryType::Delete) {
            return Err(WalError::InvalidFormat(
                "batch contains a non-mutation entry".to_string(),
            ));
        }
        let key_length = read_u32(data, offset + 1)? as usize;
        let value_length = read_u32(data, offset + 5)? as usize;
        if entry_type == EntryType::Delete && value_length != 0 {
            return Err(WalError::InvalidFormat(
                "batch delete contains value bytes".to_string(),
            ));
        }
        offset = operation_header_end
            .checked_add(key_length)
            .and_then(|offset| offset.checked_add(value_length))
            .ok_or_else(|| {
                WalError::InvalidFormat("batch operation length overflows".to_string())
            })?;
        if offset > data.len() {
            return Err(WalError::InvalidFormat(
                "batch operation exceeds its payload".to_string(),
            ));
        }
    }
    if offset != data.len() {
        return Err(WalError::InvalidFormat(
            "batch contains trailing payload bytes".to_string(),
        ));
    }
    Ok(())
}

fn batch_operation_count(data: &[u8]) -> Result<usize> {
    let count = read_u32(data, 0)? as usize;
    if count == 0 {
        return Err(WalError::InvalidFormat(
            "batch contains no operations".to_string(),
        ));
    }
    Ok(count)
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32> {
    let bytes = data
        .get(offset..offset.saturating_add(4))
        .ok_or_else(|| WalError::InvalidFormat("batch payload is truncated".to_string()))?;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("four-byte batch field"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_record_format_limits_are_checked_without_allocating_payloads() {
        let maximum_payload = usize::try_from(u32::MAX).unwrap();

        let largest_value = checked_single_record_lengths(0, maximum_payload - 4).unwrap();
        assert_eq!(largest_value.key, 0);
        assert_eq!(largest_value.payload, u32::MAX);

        let largest_key = checked_single_record_lengths(maximum_payload - 4, 0).unwrap();
        assert_eq!(largest_key.key, u32::MAX - 4);
        assert_eq!(largest_key.payload, u32::MAX);

        assert!(matches!(
            checked_single_record_lengths(maximum_payload - 3, 0),
            Err(WalError::InvalidFormat(message)) if message.contains("payload is too large")
        ));
        assert!(matches!(
            checked_single_record_lengths(usize::MAX, 1),
            Err(WalError::InvalidFormat(message)) if message.contains("overflows")
        ));
    }
}
