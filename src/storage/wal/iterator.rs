//! WAL entry iterator for TurboKV

use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use super::file::read_entry;
use super::types::{Result, WalEntry, WAL_HEADER_SIZE};

/// Streaming iterator over WAL entries.
///
/// Iterates through multiple WAL files in sequence order.
pub struct WalEntryIterator {
    paths: Vec<PathBuf>,
    current_idx: usize,
    reader: Option<BufReader<File>>,
    start_sequence: u64,
}

impl WalEntryIterator {
    pub(crate) fn new(paths: Vec<PathBuf>, start_sequence: u64) -> Result<Self> {
        let mut iter = Self {
            paths,
            current_idx: 0,
            reader: None,
            start_sequence,
        };
        iter.open_next_file()?;
        Ok(iter)
    }

    fn open_next_file(&mut self) -> Result<bool> {
        while self.current_idx < self.paths.len() {
            let path = &self.paths[self.current_idx];
            self.current_idx += 1;

            if let Ok(file) = File::open(path) {
                let mut reader = BufReader::new(file);
                if reader.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64)).is_ok() {
                    self.reader = Some(reader);
                    return Ok(true);
                }
            }
        }
        self.reader = None;
        Ok(false)
    }
}

impl Iterator for WalEntryIterator {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let reader = self.reader.as_mut()?;

            match read_entry(reader) {
                Ok(entry) => {
                    // Skip entries before start_sequence
                    if entry.sequence < self.start_sequence && self.start_sequence > 0 {
                        continue;
                    }
                    return Some(Ok(entry));
                }
                Err(_) => {
                    if let Err(e) = self.open_next_file() {
                        return Some(Err(e));
                    }
                    if self.reader.is_none() {
                        return None;
                    }
                }
            }
        }
    }
}
