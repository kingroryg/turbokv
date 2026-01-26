use bytes::Bytes;
use thiserror::Error;

pub const WAL_MAGIC: &[u8; 8] = b"TURBOKV\0";
pub const WAL_VERSION: u32 = 3; // v3: removed Merkle overhead
pub const WAL_HEADER_SIZE: usize = 64;
pub const ENTRY_HEADER_SIZE: usize = 32;

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
}

impl TryFrom<u8> for EntryType {
    type Error = WalError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(EntryType::Data),
            2 => Ok(EntryType::Checkpoint),
            3 => Ok(EntryType::Truncate),
            4 => Ok(EntryType::Delete),
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
        let key_len = u32::from_le_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
        ]) as usize;

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
        let key_len = u32::from_le_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
        ]) as usize;

        if self.data.len() < 4 + key_len {
            return None;
        }
        Some(&self.data[4 + key_len..])
    }

    pub fn decode_kv(&self) -> Option<(&[u8], Option<&[u8]>)> {
        let key = self.decode_key()?;
        let value = if self.entry_type == EntryType::Delete {
            None
        } else {
            self.decode_value()
        };
        Some((key, value))
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
    /// Enable compression (not yet implemented)
    pub compression: bool,
    /// Write buffer size
    pub buffer_size: usize,
    /// Group commit delay in microseconds
    pub group_commit_delay_us: u64,
    /// Maximum batch size for group commit
    pub max_batch_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            max_file_size: 1024 * 1024 * 1024, // 1GB
            sync_on_write: true,
            compression: false,
            buffer_size: 64 * 1024, // 64KB
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
