use crate::protocol::{AcknowledgementBoundary, Durability, MEMTABLE_BYTES};
use fjall::{
    Batch as FjallBatch, CompressionType as FjallCompression, Config as FjallConfig, Keyspace,
    PartitionCreateOptions, PartitionHandle, PersistMode,
};
use redb::{
    Database as RedbDatabase, Durability as RedbDurability, ReadableTableMetadata, TableDefinition,
};
use serde::Serialize;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use turbokv::{
    Engine, PhysicalStats, StorageConfig, StorageError, WalConfig, WriteBatch as TurboWriteBatch,
};

pub type DynError = Box<dyn std::error::Error>;

const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bench");

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineName {
    TurboKv,
    Fjall,
    Redb,
}

impl EngineName {
    pub const ALL: [Self; 3] = [Self::TurboKv, Self::Fjall, Self::Redb];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TurboKv => "turbokv",
            Self::Fjall => "fjall",
            Self::Redb => "redb",
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

struct RedbState {
    db: Mutex<RedbDatabase>,
    path: PathBuf,
    commit_durability: RedbDurability,
    full_sync_after_commit: bool,
}

impl RedbState {
    fn lock(&self) -> Result<MutexGuard<'_, RedbDatabase>, DynError> {
        self.db
            .lock()
            .map_err(|_| "redb database mutex was poisoned".into())
    }

    fn sync_file(&self) -> Result<(), DynError> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), DynError> {
    File::open(path.parent().ok_or("redb path has no parent directory")?)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), DynError> {
    Ok(())
}

enum State {
    Turbo(Box<Engine>),
    Fjall(FjallState),
    Redb(RedbState),
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
        self.state = Some(open_state_after_handoff(self.name, self.durability, &self.path).await?);
        Ok(())
    }

    /// Open after this benchmark deliberately dropped an earlier handle.
    ///
    /// `TurboKV` background work may briefly retain directory ownership after
    /// `Engine` drop. Setup/recovery handoff waits for that same-process owner;
    /// ordinary benchmark opens retain the product's immediate contention
    /// error contract through [`Self::open`].
    pub async fn open_after_handoff(
        name: EngineName,
        durability: Durability,
        path: &Path,
    ) -> Result<Self, DynError> {
        Ok(Self {
            name,
            durability,
            path: path.to_path_buf(),
            state: Some(open_state_after_handoff(name, durability, path).await?),
        })
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
            State::Redb(state) => {
                let db = state.lock()?;
                let mut transaction = db.begin_write()?;
                transaction.set_durability(state.commit_durability);
                {
                    let mut table = transaction.open_table(REDB_TABLE)?;
                    table.insert(key, value)?;
                }
                transaction.commit()?;
                if state.full_sync_after_commit {
                    state.sync_file()?;
                }
            }
        }
        Ok(())
    }

    pub async fn put_batch(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), DynError> {
        match self.state()? {
            State::Turbo(engine) => {
                let mut batch = TurboWriteBatch::with_capacity(keys.len());
                for key in keys {
                    batch.put(key, value);
                }
                engine.write_batch(&batch).await?;
            }
            State::Fjall(state) => {
                let mut batch = FjallBatch::with_capacity(state.keyspace.clone(), keys.len())
                    .durability(Some(state.acknowledgement_persist_mode));
                for key in keys {
                    batch.insert(&state.partition, key.as_slice(), value);
                }
                batch.commit()?;
            }
            State::Redb(state) => {
                let db = state.lock()?;
                let mut transaction = db.begin_write()?;
                transaction.set_durability(state.commit_durability);
                {
                    let mut table = transaction.open_table(REDB_TABLE)?;
                    for key in keys {
                        table.insert(key.as_slice(), value)?;
                    }
                }
                transaction.commit()?;
                if state.full_sync_after_commit {
                    state.sync_file()?;
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DynError> {
        let value = match self.state()? {
            State::Turbo(engine) => engine.get(key).await?,
            State::Fjall(state) => state.partition.get(key)?.map(|value| value.to_vec()),
            State::Redb(state) => {
                let db = state.lock()?;
                let transaction = db.begin_read()?;
                let table = transaction.open_table(REDB_TABLE)?;
                table.get(key)?.map(|value| value.value().to_vec())
            }
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
            State::Redb(state) => {
                let db = state.lock()?;
                let transaction = db.begin_read()?;
                let table = transaction.open_table(REDB_TABLE)?;
                for entry in table.range::<&[u8]>(..)? {
                    let (key, value) = entry?;
                    count += 1;
                    checksum ^= checksum_pair(key.value(), value.value());
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
            State::Redb(state) => {
                let db = state.lock()?;
                let mut transaction = db.begin_write()?;
                transaction.set_durability(RedbDurability::Immediate);
                transaction.commit()?;
                state.sync_file()?;
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
            State::Redb(state) => {
                state.lock()?.compact()?;
                state.sync_file()?;
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
            State::Redb(state) => {
                let db = state.lock()?;
                let transaction = db.begin_read()?;
                transaction.open_table(REDB_TABLE)?.len()?
            }
        };
        Ok(keys)
    }

    pub fn counters(&self) -> Result<StorageCounters, DynError> {
        let counters = match self.state()? {
            State::Turbo(engine) => turbo_counters(&engine.physical_stats()),
            State::Fjall(_) | State::Redb(_) => StorageCounters::default(),
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
            let mut config = match durability {
                Durability::Fast => StorageConfig::fast(path.to_path_buf()),
                Durability::Durable => {
                    let mut config = StorageConfig::durable(path.to_path_buf());
                    config.wal_config = WalConfig::durable();
                    config
                }
                Durability::Paranoid => {
                    let mut config = StorageConfig::paranoid(path.to_path_buf());
                    config.wal_config = WalConfig::paranoid()
                        .with_group_commit_delay(Duration::ZERO)
                        .with_max_group_size(1);
                    config
                }
            };
            config.memtable_config.max_size = MEMTABLE_BYTES;
            config.block_cache_size = 0;
            config.sstable_config.compression = turbokv::storage::sstable::CompressionType::None;
            config.flush_interval = Duration::from_secs(3_600);
            config.compaction_interval = Duration::from_secs(3_600);
            Ok(State::Turbo(Box::new(Engine::open(config).await?)))
        }
        EngineName::Fjall => {
            let acknowledgement_persist_mode = match durability.acknowledgement_boundary() {
                AcknowledgementBoundary::InMemory => {
                    return Err("fast mode is TurboKV-only".into());
                }
                AcknowledgementBoundary::ProcessCrashRecoverable => PersistMode::Buffer,
                AcknowledgementBoundary::PowerLossDurable => PersistMode::SyncAll,
            };
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
            Ok(State::Fjall(FjallState {
                keyspace,
                partition,
                acknowledgement_persist_mode,
            }))
        }
        EngineName::Redb => {
            let (commit_durability, full_sync_after_commit) =
                match durability.acknowledgement_boundary() {
                    AcknowledgementBoundary::InMemory => {
                        return Err("fast mode is TurboKV-only".into());
                    }
                    AcknowledgementBoundary::ProcessCrashRecoverable => {
                        (RedbDurability::Eventual, false)
                    }
                    AcknowledgementBoundary::PowerLossDurable => (RedbDurability::Immediate, true),
                };
            fs::create_dir_all(path)?;
            let database_path = path.join("bench.redb");
            let initialize_table = !database_path.exists();
            let mut builder = RedbDatabase::builder();
            builder.set_cache_size(0);
            let db = builder.create(&database_path)?;
            if initialize_table {
                let mut transaction = db.begin_write()?;
                transaction.set_durability(RedbDurability::Immediate);
                transaction.open_table(REDB_TABLE)?;
                transaction.commit()?;
                File::open(&database_path)?.sync_all()?;
                sync_parent_directory(&database_path)?;
            }
            Ok(State::Redb(RedbState {
                db: Mutex::new(db),
                path: database_path,
                commit_durability,
                full_sync_after_commit,
            }))
        }
    }
}

async fn open_state_after_handoff(
    name: EngineName,
    durability: Durability,
    path: &Path,
) -> Result<State, DynError> {
    let handoff_started = Instant::now();
    loop {
        match open_state(name, durability, path).await {
            Ok(state) => return Ok(state),
            Err(error)
                if name == EngineName::TurboKv
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
