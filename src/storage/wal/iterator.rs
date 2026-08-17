//! WAL entry iterator for TurboKV

use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use super::file::{read_and_validate_header, read_entry_versioned, WalFormat};
use super::types::{Result, WalEntry, WAL_HEADER_SIZE};

/// Streaming iterator over WAL entries.
///
/// Iterates through multiple WAL files in sequence order.
pub struct WalEntryIterator {
    paths: Vec<PathBuf>,
    current_idx: usize,
    reader: Option<BufReader<File>>,
    format: WalFormat,
    current_file_end: u64,
    start_sequence: u64,
    failed: bool,
}

impl WalEntryIterator {
    pub(crate) fn new(paths: Vec<PathBuf>, start_sequence: u64) -> Result<Self> {
        let mut iter = Self {
            paths,
            current_idx: 0,
            reader: None,
            format: WalFormat::Current,
            current_file_end: 0,
            start_sequence,
            failed: false,
        };
        iter.open_next_file()?;
        Ok(iter)
    }

    fn open_next_file(&mut self) -> Result<bool> {
        if self.current_idx >= self.paths.len() {
            self.reader = None;
            return Ok(false);
        }

        let path = &self.paths[self.current_idx];
        self.current_idx += 1;
        let file = File::open(path)?;
        self.current_file_end = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let (format, _) = read_and_validate_header(&mut reader)?;
        reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        self.format = format;
        self.reader = Some(reader);
        Ok(true)
    }
}

impl Iterator for WalEntryIterator {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let reader = self.reader.as_mut()?;

            let position = match reader.stream_position() {
                Ok(position) => position,
                Err(error) => {
                    self.failed = true;
                    self.reader = None;
                    return Some(Err(error.into()));
                }
            };
            let remaining = self.current_file_end.saturating_sub(position);
            match read_entry_versioned(reader, self.format, remaining) {
                Ok(entry) => {
                    // Skip entries before start_sequence
                    if entry.sequence < self.start_sequence && self.start_sequence > 0 {
                        continue;
                    }
                    return Some(Ok(entry));
                }
                Err(ref e) if matches!(e, super::types::WalError::Eof) => {
                    // End of this file, try next
                    if let Err(e) = self.open_next_file() {
                        self.failed = true;
                        self.reader = None;
                        return Some(Err(e));
                    }
                    if self.reader.is_none() {
                        return None;
                    }
                }
                Err(e) => {
                    // Corrupted entry — report error to caller
                    self.failed = true;
                    self.reader = None;
                    return Some(Err(e));
                }
            }
        }
    }
}
