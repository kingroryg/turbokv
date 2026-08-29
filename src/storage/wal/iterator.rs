//! WAL entry iterator for TurboKV

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use super::file::{inspect_segment, read_and_validate_header, read_entry_versioned, WalFormat};
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
    pending: VecDeque<WalEntry>,
    failed: bool,
    active_snapshot: Option<(PathBuf, u64)>,
}

impl WalEntryIterator {
    #[cfg(test)]
    pub(crate) fn new(paths: Vec<PathBuf>, start_sequence: u64) -> Result<Self> {
        Self::new_inner(paths, start_sequence, None)
    }

    pub(crate) fn new_with_active_end(
        paths: Vec<PathBuf>,
        start_sequence: u64,
        active_path: PathBuf,
        active_end: u64,
    ) -> Result<Self> {
        Self::new_inner(paths, start_sequence, Some((active_path, active_end)))
    }

    fn new_inner(
        paths: Vec<PathBuf>,
        start_sequence: u64,
        active_snapshot: Option<(PathBuf, u64)>,
    ) -> Result<Self> {
        let mut iter = Self {
            paths,
            current_idx: 0,
            reader: None,
            format: WalFormat::Current,
            current_file_end: 0,
            start_sequence,
            pending: VecDeque::new(),
            failed: false,
            active_snapshot,
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
        self.current_file_end = if self
            .active_snapshot
            .as_ref()
            .is_some_and(|(active_path, _)| active_path == path)
        {
            self.active_snapshot
                .as_ref()
                .expect("active snapshot was matched")
                .1
        } else {
            inspect_segment(path, false)?.valid_end
        };
        let file = File::open(path)?;
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
            if let Some(entry) = self.pending.pop_front() {
                if entry.sequence >= self.start_sequence || self.start_sequence == 0 {
                    return Some(Ok(entry));
                }
                continue;
            }

            let reader = self.reader.as_mut()?;

            let position = match reader.stream_position() {
                Ok(position) => position,
                Err(error) => {
                    self.failed = true;
                    self.reader = None;
                    return Some(Err(error.into()));
                }
            };
            if position >= self.current_file_end {
                if let Err(error) = self.open_next_file() {
                    self.failed = true;
                    self.reader = None;
                    return Some(Err(error));
                }
                if self.reader.is_none() {
                    return None;
                }
                continue;
            }
            let remaining = self.current_file_end.saturating_sub(position);
            match read_entry_versioned(reader, self.format, remaining) {
                Ok(entry) => match entry.into_logical_entries() {
                    Ok(entries) => self.pending.extend(entries),
                    Err(error) => {
                        self.failed = true;
                        self.reader = None;
                        return Some(Err(error));
                    }
                },
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
