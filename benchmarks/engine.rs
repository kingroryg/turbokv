use crate::protocol::{AcknowledgementPolicy, Durability, MEMTABLE_BYTES};
use fjall::{
    CompressionType as FjallCompression, Config as FjallConfig, Keyspace, PartitionCreateOptions,
    PartitionHandle, PersistMode,
};
use rocksdb::statistics::Ticker;
use rocksdb::{
    BlockBasedOptions, DBCompressionType, Direction, FlushOptions, IteratorMode,
    Options as RocksOptions, WriteOptions, DB,
};
use serde::Serialize;
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use turbokv::{Engine, PhysicalStats, StorageConfig, StorageError, WalConfig};

pub type DynError = Box<dyn std::error::Error>;

const ROCKS_MAX_TOTAL_WAL_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineName {
    TurboKv,
    Fjall,
    RocksDb,
}

impl EngineName {
    pub const ALL: [Self; 3] = [Self::TurboKv, Self::Fjall, Self::RocksDb];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TurboKv => "turbokv",
            Self::Fjall => "fjall",
            Self::RocksDb => "rocksdb",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct StorageCounters {
    pub wal_bytes_written: Option<u64>,
    pub flush_bytes_written: Option<u64>,
    pub compaction_bytes_read: Option<u64>,
    pub compaction_bytes_written: Option<u64>,
}

impl StorageCounters {
    pub fn delta(self, before: Self) -> Self {
        Self {
            wal_bytes_written: difference(self.wal_bytes_written, before.wal_bytes_written),
            flush_bytes_written: difference(self.flush_bytes_written, before.flush_bytes_written),
            compaction_bytes_read: difference(
                self.compaction_bytes_read,
                before.compaction_bytes_read,
            ),
            compaction_bytes_written: difference(
                self.compaction_bytes_written,
                before.compaction_bytes_written,
            ),
        }
    }

    pub fn physical_write_bytes(self) -> Option<u64> {
        Some(self.wal_bytes_written? + self.flush_bytes_written? + self.compaction_bytes_written?)
    }
}

fn difference(after: Option<u64>, before: Option<u64>) -> Option<u64> {
    Some(after?.saturating_sub(before?))
}

struct FjallState {
    keyspace: Keyspace,
    partition: PartitionHandle,
    acknowledgement_persist_mode: PersistMode,
}

struct RocksState {
    db: DB,
    options: RocksOptions,
    write_options: WriteOptions,
    path: PathBuf,
    full_sync_after_put: bool,
    active_wal: RefCell<Option<PinnedWal>>,
}

struct PinnedWal {
    path: PathBuf,
    file: File,
    device: u64,
    inode: u64,
    synced_len: u64,
}

impl RocksState {
    fn sync_active_wal_with_full_barrier(&self) -> Result<(), DynError> {
        if self.active_wal.borrow().is_none() {
            let wal_path = fs::read_dir(&self.path)?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
                .max()
                .ok_or("RocksDB acknowledged a write without an active WAL file")?;
            let wal = OpenOptions::new().read(true).write(true).open(&wal_path)?;
            let metadata = wal.metadata()?;
            self.active_wal.replace(Some(PinnedWal {
                path: wal_path,
                file: wal,
                device: metadata.dev(),
                inode: metadata.ino(),
                synced_len: 0,
            }));
        }
        let mut active_wal = self.active_wal.borrow_mut();
        let wal = active_wal.as_mut().expect("active WAL was initialized");
        let handle_metadata = wal.file.metadata()?;
        let path_metadata = wal.path.metadata()?;
        if handle_metadata.dev() != wal.device
            || handle_metadata.ino() != wal.inode
            || path_metadata.dev() != wal.device
            || path_metadata.ino() != wal.inode
        {
            return Err("RocksDB rotated or replaced the pinned WAL during a write epoch".into());
        }
        if handle_metadata.len() <= wal.synced_len {
            return Err("RocksDB WAL did not grow after flush_wal(false); refusing to claim a full-sync acknowledgement".into());
        }
        wal.file.sync_all()?;
        wal.synced_len = handle_metadata.len();
        Ok(())
    }

    fn forget_active_wal(&self) {
        self.active_wal.replace(None);
    }

    fn full_sync_database_files(&self) -> Result<(), DynError> {
        for entry in fs::read_dir(&self.path)? {
            let path = entry?.path();
            if path.is_file() {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)?
                    .sync_all()?;
            }
        }
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
}

enum State {
    Turbo(Box<Engine>),
    Fjall(FjallState),
    Rocks(Box<RocksState>),
}

pub struct Database {
    name: EngineName,
    durability: Durability,
    path: PathBuf,
    state: Option<State>,
}

impl Database {
    pub async fn open(
        name: EngineName,
        durability: Durability,
        path: &Path,
    ) -> Result<Self, DynError> {
        Ok(Self {
            name,
            durability,
            path: path.to_path_buf(),
            state: Some(open_state(name, durability, path).await?),
        })
    }

    pub async fn reopen(&mut self) -> Result<(), DynError> {
        self.state.take();
        let handoff_started = Instant::now();
        loop {
            match open_state(self.name, self.durability, &self.path).await {
                Ok(state) => {
                    self.state = Some(state);
                    break;
                }
                Err(error)
                    if self.name == EngineName::TurboKv
                        && handoff_started.elapsed() < Duration::from_secs(5)
                        && matches!(
                            error.downcast_ref::<StorageError>(),
                            Some(StorageError::DirectoryLocked { .. })
                        ) =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub fn drop_without_settlement(&mut self) {
        self.state.take();
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DynError> {
        match self.state()? {
            State::Turbo(engine) => engine.insert(key, value).await?,
            State::Fjall(state) => {
                state.partition.insert(key, value)?;
                state.keyspace.persist(state.acknowledgement_persist_mode)?;
            }
            State::Rocks(state) => {
                state.db.put_opt(key, value, &state.write_options)?;
                state.db.flush_wal(false)?;
                if state.full_sync_after_put {
                    state.sync_active_wal_with_full_barrier()?;
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DynError> {
        let value = match self.state()? {
            State::Turbo(engine) => engine.get(key).await?,
            State::Fjall(state) => state.partition.get(key)?.map(|value| value.to_vec()),
            State::Rocks(state) => state.db.get(key)?,
        };
        Ok(value)
    }

    pub async fn scan_all(&self) -> Result<(u64, u64), DynError> {
        let mut count = 0_u64;
        let mut checksum = 0_u64;
        match self.state()? {
            State::Turbo(engine) => {
                let mut iterator = engine.scan_prefix_iter(b"").await?;
                for entry in &mut iterator {
                    let entry = entry?;
                    count += 1;
                    checksum ^= checksum_pair(entry.key(), entry.value());
                }
            }
            State::Fjall(state) => {
                for entry in state.partition.iter() {
                    let (key, value) = entry?;
                    count += 1;
                    checksum ^= checksum_pair(&key, &value);
                }
            }
            State::Rocks(state) => {
                for entry in state
                    .db
                    .iterator(IteratorMode::From(b"", Direction::Forward))
                {
                    let (key, value) = entry?;
                    count += 1;
                    checksum ^= checksum_pair(&key, &value);
                }
            }
        }
        Ok((count, checksum))
    }

    pub async fn flush(&self) -> Result<(), DynError> {
        match self.state()? {
            State::Turbo(engine) => engine.flush().await?,
            State::Fjall(state) => {
                state.keyspace.persist(PersistMode::SyncAll)?;
                state.partition.rotate_memtable_and_wait()?;
            }
            State::Rocks(state) => {
                state.db.flush_wal(true)?;
                let mut options = FlushOptions::default();
                options.set_wait(true);
                state.db.flush_opt(&options)?;
                state.forget_active_wal();
                state.full_sync_database_files()?;
            }
        }
        Ok(())
    }

    pub async fn compact(&self) -> Result<(), DynError> {
        match self.state()? {
            State::Turbo(engine) => {
                engine.compact().await?;
            }
            State::Fjall(state) => state.partition.major_compact()?,
            State::Rocks(state) => {
                state.db.compact_range::<&[u8], &[u8]>(None, None);
                state.full_sync_database_files()?;
            }
        }
        Ok(())
    }

    pub async fn settle(&self, expected_keys: u64) -> Result<Duration, DynError> {
        let started = std::time::Instant::now();
        self.flush().await?;
        self.compact().await?;
        self.compact().await?;
        if self.live_keys().await? != expected_keys {
            return Err(format!(
                "{} settlement did not retain {expected_keys} keys",
                self.name.as_str()
            )
            .into());
        }
        Ok(started.elapsed())
    }

    pub async fn live_keys(&self) -> Result<u64, DynError> {
        let keys = match self.state()? {
            State::Turbo(engine) => engine.logical_stats().await?.live_keys,
            State::Fjall(state) => u64::try_from(state.partition.len()?)?,
            State::Rocks(_) => self.scan_all().await?.0,
        };
        Ok(keys)
    }

    pub fn counters(&self) -> Result<StorageCounters, DynError> {
        let counters = match self.state()? {
            State::Turbo(engine) => turbo_counters(&engine.physical_stats()),
            State::Fjall(_) => StorageCounters::default(),
            State::Rocks(state) => StorageCounters {
                wal_bytes_written: Some(state.options.get_ticker_count(Ticker::WalFileBytes)),
                flush_bytes_written: Some(state.options.get_ticker_count(Ticker::FlushWriteBytes)),
                compaction_bytes_read: Some(
                    state.options.get_ticker_count(Ticker::CompactReadBytes),
                ),
                compaction_bytes_written: Some(
                    state.options.get_ticker_count(Ticker::CompactWriteBytes),
                ),
            },
        };
        Ok(counters)
    }

    pub async fn close(mut self) -> Result<(), DynError> {
        if let Some(State::Turbo(engine)) = self.state.take() {
            engine.shutdown().await?;
        }
        Ok(())
    }

    fn state(&self) -> Result<&State, DynError> {
        self.state
            .as_ref()
            .ok_or_else(|| format!("{} database is closed", self.name.as_str()).into())
    }
}

async fn open_state(
    name: EngineName,
    durability: Durability,
    path: &Path,
) -> Result<State, DynError> {
    match name {
        EngineName::TurboKv => {
            let mut config = match durability.acknowledgement_policy() {
                AcknowledgementPolicy::WalInOsCache => StorageConfig::durable(path.to_path_buf()),
                AcknowledgementPolicy::WalFsync => StorageConfig::paranoid(path.to_path_buf()),
            };
            config.wal_config = match durability.acknowledgement_policy() {
                AcknowledgementPolicy::WalInOsCache => WalConfig::durable(),
                AcknowledgementPolicy::WalFsync => WalConfig::paranoid()
                    .with_group_commit_delay(Duration::ZERO)
                    .with_max_group_size(1),
            };
            config.memtable_config.max_size = MEMTABLE_BYTES;
            config.block_cache_size = 0;
            config.sstable_config.compression = turbokv::storage::sstable::CompressionType::None;
            config.flush_interval = Duration::from_secs(3_600);
            config.compaction_interval = Duration::from_secs(3_600);
            Ok(State::Turbo(Box::new(Engine::open(config).await?)))
        }
        EngineName::Fjall => {
            let keyspace = FjallConfig::new(path)
                .manual_journal_persist(true)
                .flush_workers(1)
                .compaction_workers(0)
                .cache_size(0)
                .open()?;
            let partition = keyspace.open_partition(
                "bench",
                PartitionCreateOptions::default()
                    .compression(FjallCompression::None)
                    .manual_journal_persist(true)
                    .max_memtable_size(MEMTABLE_BYTES as u32),
            )?;
            let acknowledgement_persist_mode = match durability.acknowledgement_policy() {
                AcknowledgementPolicy::WalInOsCache => PersistMode::Buffer,
                AcknowledgementPolicy::WalFsync => PersistMode::SyncAll,
            };
            Ok(State::Fjall(FjallState {
                keyspace,
                partition,
                acknowledgement_persist_mode,
            }))
        }
        EngineName::RocksDb => {
            let mut options = RocksOptions::default();
            options.create_if_missing(true);
            options.set_compression_type(DBCompressionType::None);
            options.set_write_buffer_size(MEMTABLE_BYTES);
            options.set_max_total_wal_size(ROCKS_MAX_TOTAL_WAL_BYTES);
            options.set_max_background_jobs(1);
            options.set_disable_auto_compactions(true);
            options.set_use_fsync(true);
            options.enable_statistics();
            options.set_stats_dump_period_sec(0);
            let mut table = BlockBasedOptions::default();
            table.disable_cache();
            options.set_block_based_table_factory(&table);
            let db = DB::open(&options, path)?;
            let mut write_options = WriteOptions::default();
            let full_sync = durability.acknowledgement_policy() == AcknowledgementPolicy::WalFsync;
            write_options.set_sync(false);
            write_options.disable_wal(false);
            Ok(State::Rocks(Box::new(RocksState {
                db,
                options,
                write_options,
                path: path.to_path_buf(),
                full_sync_after_put: full_sync,
                active_wal: RefCell::new(None),
            })))
        }
    }
}

fn turbo_counters(stats: &PhysicalStats) -> StorageCounters {
    StorageCounters {
        wal_bytes_written: Some(stats.amplification.wal_bytes_written_since_open),
        flush_bytes_written: Some(stats.amplification.flush_bytes_written_since_open),
        compaction_bytes_read: Some(stats.amplification.compaction_input_bytes_since_open),
        compaction_bytes_written: Some(stats.amplification.compaction_output_bytes_since_open),
    }
}

fn checksum_pair(key: &[u8], value: &[u8]) -> u64 {
    key.iter()
        .chain(value)
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}
