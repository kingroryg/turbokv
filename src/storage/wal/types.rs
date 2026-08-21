use bytes::Bytes;
use std::time::Duration;
use thiserror::Error;

pub const WAL_MAGIC: &[u8; 8] = b"TURBOKV\0";
/// Stable identifiers for the Merkle-extension WAL layouts. Versions 1 and 2
/// share the same physical entry representation.
pub const WAL_VERSION_V1: u32 = 1;
pub const WAL_VERSION_V2: u32 = 2;
/// Stable identifier for entries after removal of the legacy extension.
pub const WAL_VERSION_V3: u32 = 3;
/// Stable identifier written by this release. Version 4 adds atomic batch
/// records; opening a validated v1-v3 WAL starts a new v4 segment and retains
/// the old segments for replay until their checkpoint permits reclamation.
pub const WAL_VERSION: u32 = 4;
pub const WAL_HEADER_SIZE: usize = 64;
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
const BATCH_HEADER_SIZE: usize = 4;
const BATCH_OPERATION_HEADER_SIZE: usize = 9;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: Option<std::io::Error>,
    },

    #[error("Invalid WAL format: {0}")]
    InvalidFormat(String),

    #[error("CRC mismatch: data corrupted")]
    CrcMismatch,

    #[error("WAL corruption: {0}")]
    Corruption(String),

    #[error("Channel closed")]
    ChannelClosed,

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

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    /// Normal key-value data
    Data = 1,
    /// Checkpoint marker (safe point for recovery)
    Checkpoint = 2,
    /// Truncation marker
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

/// The `data` field contains encoded key-value pair:
/// - For Data: `[key_len: u32][key][value]`
/// - For Delete: `[key_len: u32][key]`
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: u64,
    pub entry_type: EntryType,
    pub data: Bytes,
}

impl WalEntry {
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
    /// Fast WAL config - no sync, optimized for throughput
    pub fn fast() -> Self {
        Self {
            sync_on_write: false,
            group_commit_delay_us: 500,
            max_batch_size: 1024,
            ..Default::default()
        }
    }

    /// Durable WAL config - WAL enabled but no sync per write
    /// Data survives process crash (OS flushes buffers)
    pub fn durable() -> Self {
        Self {
            sync_on_write: false,
            group_commit_delay_us: 100, // Low delay for throughput
            max_batch_size: 1024,
            ..Default::default()
        }
    }

    /// Paranoid WAL config - sync on every write
    /// Data survives power loss
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
pub fn encode_kv(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len() + value.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

#[inline]
pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + key.len());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf
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
