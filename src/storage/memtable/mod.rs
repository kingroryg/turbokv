//! # MemTable Module
//!
//! In-memory version storage using concurrent skip lists.
//!
//! The manager assigns engine-wide sequences, optionally stages no-WAL inserts
//! in per-thread buffers, and keeps the active table plus a FIFO of frozen
//! generations. A frozen generation remains readable until the engine has
//! durably installed its SSTable and explicitly completes the flush.
//!
//! ```text
//! no-WAL insert ──> per-thread buffer ─┐
//! direct/bulk/WAL apply ─────────────┤
//!                                   ▼
//!                         active concurrent skip list
//!                                   │ size/entry limit or explicit freeze
//!                                   ▼
//!                     immutable generation FIFO (still readable)
//!                                   │ engine persists + publishes SSTable
//!                                   ▼
//!                            completed and dequeued
//! ```

mod manager;
mod table;
mod types;

pub use manager::MemTableManager;
pub use table::{MemTable, MemTableError, Result as MemTableResult};
pub use types::{MemTableConfig, MemTableEntry, MemTableManagerStats, MemTableStats};
