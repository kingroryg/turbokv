use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use turbokv::storage::manifest::Manifest;
use turbokv::storage::sstable::CompressionType;
use turbokv::{
    Compression, Db, DbOptions, Engine, MemTableConfig, SSTableConfig, StorageConfig,
    StorageResult, WriteBatch,
};

const MODEL_KEYSPACE: usize = 32;
const MODEL_STEPS: u64 = 72;
const CONCURRENCY_SEED: u64 = 0x5c17_ea11_21db_7a9e;
const MIN_CONCURRENCY_ROUNDS: usize = 16;

#[derive(Clone, Copy, Debug)]
enum ModelMode {
    Fast,
    Durable,
    Paranoid,
}

impl ModelMode {
    fn options(self) -> DbOptions {
        let (mut options, compression) = match self {
            Self::Fast => (DbOptions::fast(), Compression::None),
            Self::Durable => (DbOptions::durable(), Compression::Snappy),
            Self::Paranoid => (DbOptions::paranoid(), Compression::Lz4),
        };
        options.memtable_size = 512;
        options.block_cache_size = 4 * 1024;
        options.compression = compression;
        options
    }
}

fn model_context(mode: ModelMode, seed: u64, steps: u64, step: u64) -> String {
    format!(
        "seed={seed:#018x} config={{mode={mode:?},steps={steps},keyspace={MODEL_KEYSPACE},memtable_bytes=512,block_cache_bytes=4096}} step={step}"
    )
}

fn model_key(index: usize) -> Vec<u8> {
    format!("model:key:{index:02}").into_bytes()
}

fn generated_value(rng: &mut StdRng, step: u64) -> Vec<u8> {
    let length = rng.gen_range(0..=96);
    let mut value = vec![0; length];
    rng.fill_bytes(&mut value);
    value.extend_from_slice(&step.to_le_bytes());
    value
}

async fn assert_db_matches_model(db: &Db, expected: &BTreeMap<Vec<u8>, Vec<u8>>, context: &str) {
    let actual = db
        .scan_prefix(b"")
        .await
        .unwrap_or_else(|error| panic!("{context}: whole-database scan failed: {error}"));
    let expected_pairs = expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected_pairs, "{context}: ordered scan diverged");

    for index in 0..MODEL_KEYSPACE {
        let key = model_key(index);
        let value = db
            .get(&key)
            .await
            .unwrap_or_else(|error| panic!("{context}: point read for {key:?} failed: {error}"));
        assert_eq!(
            value.as_ref(),
            expected.get(&key),
            "{context}: point read for {key:?} diverged"
        );
    }

    let range_start = model_key(MODEL_KEYSPACE / 4);
    let range_end = model_key(MODEL_KEYSPACE * 3 / 4);
    let actual_range = db
        .range(&range_start, &range_end)
        .await
        .unwrap_or_else(|error| panic!("{context}: range read failed: {error}"));
    let expected_range = expected
        .range(range_start..range_end)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual_range, expected_range,
        "{context}: ordered range diverged"
    );
}

async fn run_generated_model(mode: ModelMode, seed: u64, steps: u64) {
    let initial_context = model_context(mode, seed, steps, 0);
    let directory = TempDir::new()
        .unwrap_or_else(|error| panic!("{initial_context}: temporary directory failed: {error}"));
    let options = mode.options();
    let mut db = Db::open_with_options(directory.path(), options.clone())
        .await
        .unwrap_or_else(|error| panic!("{initial_context}: initial open failed: {error}"));
    let mut expected = BTreeMap::new();
    let mut rng = StdRng::seed_from_u64(seed);

    for step in 0..steps {
        let context = model_context(mode, seed, steps, step);
        let key_index = rng.gen_range(0..MODEL_KEYSPACE);
        let key = model_key(key_index);
        match rng.gen_range(0..11) {
            0..=2 => {
                let value = generated_value(&mut rng, step);
                db.insert(&key, &value)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: insert failed: {error}"));
                expected.insert(key, value);
            }
            3 => {
                db.remove(&key)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: remove failed: {error}"));
                expected.remove(&key);
            }
            4 => {
                let other_key = model_key((key_index + 11) % MODEL_KEYSPACE);
                let first = generated_value(&mut rng, step);
                let second = generated_value(&mut rng, step ^ seed);
                db.insert_many([
                    (key.clone(), first.clone()),
                    (other_key.clone(), second.clone()),
                ])
                .await
                .unwrap_or_else(|error| panic!("{context}: insert_many failed: {error}"));
                expected.insert(key, first);
                expected.insert(other_key, second);
            }
            5 => {
                let deleted_key = model_key((key_index + 7) % MODEL_KEYSPACE);
                let value = generated_value(&mut rng, step);
                let final_value = generated_value(&mut rng, step.wrapping_add(1));
                let mut batch = WriteBatch::new();
                batch.put(&key, &value);
                batch.delete(&key);
                batch.put(&key, &final_value);
                batch.delete(&deleted_key);
                db.write_batch(&batch)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: write batch failed: {error}"));
                expected.insert(key, final_value);
                expected.remove(&deleted_key);
            }
            6 => {
                let actual = db.get(&key).await.unwrap_or_else(|error| {
                    panic!("{context}: generated point read failed: {error}")
                });
                assert_eq!(
                    actual.as_ref(),
                    expected.get(&key),
                    "{context}: generated point read diverged"
                );
            }
            7 => assert_db_matches_model(&db, &expected, &context).await,
            8 => db
                .flush()
                .await
                .unwrap_or_else(|error| panic!("{context}: flush failed: {error}")),
            9 => {
                db.compact()
                    .await
                    .unwrap_or_else(|error| panic!("{context}: compaction failed: {error}"));
            }
            _ => {
                let modeled = expected.remove(&key);
                let actual = db
                    .take(&key)
                    .await
                    .unwrap_or_else(|error| panic!("{context}: take failed: {error}"));
                assert_eq!(actual, modeled, "{context}: take result diverged");
            }
        }

        if step % 13 == 12 {
            assert_db_matches_model(&db, &expected, &context).await;
            db.close()
                .await
                .unwrap_or_else(|error| panic!("{context}: close failed: {error}"));
            db = Db::open_with_options(directory.path(), options.clone())
                .await
                .unwrap_or_else(|error| panic!("{context}: reopen failed: {error}"));
            assert_db_matches_model(&db, &expected, &context).await;
        }
    }

    let final_context = model_context(mode, seed, steps, steps);
    db.flush()
        .await
        .unwrap_or_else(|error| panic!("{final_context}: final flush failed: {error}"));
    db.compact()
        .await
        .unwrap_or_else(|error| panic!("{final_context}: final compaction failed: {error}"));
    assert_db_matches_model(&db, &expected, &final_context).await;
    db.close()
        .await
        .unwrap_or_else(|error| panic!("{final_context}: final close failed: {error}"));

    let reopened = Db::open_with_options(directory.path(), options)
        .await
        .unwrap_or_else(|error| panic!("{final_context}: final reopen failed: {error}"));
    assert_db_matches_model(&reopened, &expected, &final_context).await;
    reopened
        .close()
        .await
        .unwrap_or_else(|error| panic!("{final_context}: reopened close failed: {error}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_whole_database_operations_match_ordered_model() {
    for (mode, seed) in [
        (ModelMode::Fast, 0x0d61_77f4_83a9_02bc),
        (ModelMode::Durable, 0x31ee_c85a_b47d_6092),
        (ModelMode::Paranoid, 0xa63b_4e91_05fd_c278),
    ] {
        run_generated_model(mode, seed, MODEL_STEPS).await;
    }
}

#[derive(Clone, Copy)]
struct ConcurrencyRun {
    seed: u64,
    writers: usize,
    writes_per_writer: usize,
}

#[derive(Clone, Copy)]
enum MaintenanceAction {
    Flush,
    Compact,
}

impl MaintenanceAction {
    fn trace_code(self) -> char {
        match self {
            Self::Flush => 'F',
            Self::Compact => 'C',
        }
    }
}

impl ConcurrencyRun {
    fn cancellation_round(self) -> usize {
        7.min(self.writes_per_writer.saturating_sub(1))
    }

    fn maintenance_action(self, round: usize) -> MaintenanceAction {
        if (round + (self.seed as usize & 1)) % 2 == 0 {
            MaintenanceAction::Flush
        } else {
            MaintenanceAction::Compact
        }
    }

    fn context(self) -> String {
        let trace = (0..self.writes_per_writer)
            .map(|round| self.maintenance_action(round).trace_code())
            .collect::<String>();
        format!(
            "seed={:#018x} config={{mode=paranoid,worker_threads=4,writers={},writes_per_writer={},wal_max_bytes=384,memtable_entries=6,l0_trigger=2,background_intervals_secs=3600,schedule=round-barrier-v1,cancellation_after_round={},maintenance_trace={trace}}}",
            self.seed,
            self.writers,
            self.writes_per_writer,
            self.cancellation_round()
        )
    }
}

fn aggressive_storage_config(path: &Path) -> StorageConfig {
    let mut config = StorageConfig::paranoid(path.to_path_buf());
    config.wal_config.max_file_size = 384;
    config.wal_config.group_commit_delay_us = 5_000;
    config.memtable_config = MemTableConfig {
        max_size: 1_024,
        max_entries: 6,
        max_age: Duration::from_secs(3_600),
    };
    config.sstable_config = SSTableConfig {
        block_size: 256,
        compression: CompressionType::None,
        ..SSTableConfig::default()
    };
    config.compaction_config.l0_compaction_trigger = 2;
    config.compaction_config.max_levels = 4;
    config.compaction_config.target_file_size = 4 * 1024;
    // Manual, seed-derived maintenance rounds provide the replay schedule.
    // Long background intervals keep the gate independent of wall-clock timing.
    config.flush_interval = Duration::from_secs(3_600);
    config.compaction_interval = Duration::from_secs(3_600);
    config.max_immutable_memtables_before_stall = usize::MAX;
    config.max_l0_files_before_stall = u64::MAX;
    config
}

fn acknowledged_key(writer: usize, operation: usize) -> Vec<u8> {
    format!("ack:{writer:02}:{operation:04}").into_bytes()
}

fn acknowledged_value(seed: u64, writer: usize, operation: usize) -> Vec<u8> {
    format!(
        "seed={seed:016x};writer={writer:02};operation={operation:04};payload={:016x}",
        seed.rotate_left((writer as u32) & 63) ^ operation as u64
    )
    .into_bytes()
}

fn expected_acknowledged_model(run: ConcurrencyRun) -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..run.writers)
        .flat_map(|writer| {
            (0..run.writes_per_writer).map(move |operation| {
                (
                    acknowledged_key(writer, operation),
                    acknowledged_value(run.seed, writer, operation),
                )
            })
        })
        .collect()
}

fn parse_acknowledged_key(key: &[u8]) -> Option<(usize, usize)> {
    let text = std::str::from_utf8(key).ok()?;
    let mut parts = text.split(':');
    (parts.next()? == "ack").then_some(())?;
    let writer = parts.next()?.parse().ok()?;
    let operation = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((writer, operation))
}

fn wal_segment_sequences(path: &Path, context: &str) -> Vec<u64> {
    let mut sequences = std::fs::read_dir(path.join("wal"))
        .unwrap_or_else(|error| panic!("{context}: failed to inspect WAL directory: {error}"))
        .filter_map(|entry| {
            let entry = entry
                .unwrap_or_else(|error| panic!("{context}: failed to inspect WAL entry: {error}"));
            entry
                .path()
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
        })
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    sequences
}

#[derive(Default)]
struct MaintenanceProgress {
    state: Mutex<MaintenanceProgressState>,
}

#[derive(Clone, Copy, Default)]
struct MaintenanceProgressState {
    rotation: bool,
    multiple_segments: bool,
    oldest_segment_at_peak: Option<u64>,
    older_segment_reclaimed: bool,
    checkpoint: bool,
    flush_output: bool,
    compaction_input: bool,
    compaction_output: bool,
}

impl MaintenanceProgress {
    fn observe(&self, engine: &Engine, path: &Path, context: &str) {
        let sequences = wal_segment_sequences(path, context);
        let manifest = Manifest::load_or_create(path)
            .unwrap_or_else(|error| panic!("{context}: failed to inspect manifest: {error}"));
        let stats = engine.physical_stats();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|error| panic!("{context}: progress lock poisoned: {error}"));

        state.rotation |= sequences.iter().any(|sequence| *sequence != 0);
        if sequences.len() > 1 {
            state.multiple_segments = true;
            let oldest = sequences[0];
            state.oldest_segment_at_peak = Some(
                state
                    .oldest_segment_at_peak
                    .map_or(oldest, |previous| previous.min(oldest)),
            );
        }
        if let (Some(oldest), Some(current)) = (state.oldest_segment_at_peak, sequences.first()) {
            state.older_segment_reclaimed |= *current > oldest;
        }
        state.checkpoint |= manifest.wal_checkpoint > 0;
        state.flush_output |= stats.amplification.flush_bytes_written_since_open > 0;
        state.compaction_input |= stats.amplification.compaction_input_bytes_since_open > 0;
        state.compaction_output |= stats.amplification.compaction_output_bytes_since_open > 0;
    }
}

async fn validate_concurrent_scan(engine: &Engine, seed: u64, context: &str) {
    let rows = engine
        .scan_prefix(b"ack:")
        .await
        .unwrap_or_else(|error| panic!("{context}: concurrent scan failed: {error}"));
    assert!(
        rows.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "{context}: concurrent scan was not strictly ordered"
    );
    for (key, value) in rows {
        let (writer, operation) = parse_acknowledged_key(&key)
            .unwrap_or_else(|| panic!("{context}: malformed acknowledged key {key:?}"));
        assert_eq!(
            value,
            acknowledged_value(seed, writer, operation),
            "{context}: acknowledged value was torn or corrupted for {key:?}"
        );
    }
}

async fn join_with_context(handle: JoinHandle<()>, context: &str, actor: &str) {
    handle
        .await
        .unwrap_or_else(|error| panic!("{context}: {actor} task failed: {error}"));
}

async fn run_concurrency_interleaving(run: ConcurrencyRun) {
    let context = run.context();
    assert!(
        run.writes_per_writer >= MIN_CONCURRENCY_ROUNDS,
        "{context}: concurrency replay requires at least {MIN_CONCURRENCY_ROUNDS} rounds"
    );
    let directory = TempDir::new()
        .unwrap_or_else(|error| panic!("{context}: temporary directory failed: {error}"));
    let config = aggressive_storage_config(directory.path());
    let engine = Arc::new(
        Engine::open(config.clone())
            .await
            .unwrap_or_else(|error| panic!("{context}: initial open failed: {error}")),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(MaintenanceProgress::default());
    let round_barrier = Arc::new(tokio::sync::Barrier::new(run.writers + 1));
    let cancellation_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let (cancellation_phase_sender, mut cancellation_phase_receiver) =
        tokio::sync::mpsc::channel(1);
    let shutdown_barrier = Arc::new(tokio::sync::Barrier::new(2));

    let scan_engine = Arc::clone(&engine);
    let scan_stop = Arc::clone(&stop);
    let scan_context = context.clone();
    let scanner = tokio::spawn(async move {
        while !scan_stop.load(Ordering::Acquire) {
            validate_concurrent_scan(&scan_engine, run.seed, &scan_context).await;
            tokio::task::yield_now().await;
        }
    });

    let maintenance_engine = Arc::clone(&engine);
    let maintenance_progress = Arc::clone(&progress);
    let maintenance_round_barrier = Arc::clone(&round_barrier);
    let maintenance_cancellation_barrier = Arc::clone(&cancellation_barrier);
    let maintenance_shutdown_barrier = Arc::clone(&shutdown_barrier);
    let maintenance_path = directory.path().to_path_buf();
    let maintenance_context = context.clone();
    let maintenance = tokio::spawn(async move {
        for round in 0..run.writes_per_writer {
            maintenance_round_barrier.wait().await;
            maintenance_progress.observe(
                &maintenance_engine,
                &maintenance_path,
                &maintenance_context,
            );
            match run.maintenance_action(round) {
                MaintenanceAction::Flush => {
                    maintenance_engine.flush().await.unwrap_or_else(|error| {
                        panic!(
                            "{maintenance_context}: maintenance flush round {round} failed: {error}"
                        )
                    });
                }
                MaintenanceAction::Compact => {
                    maintenance_engine.compact().await.unwrap_or_else(|error| {
                        panic!(
                            "{maintenance_context}: maintenance compaction round {round} failed: {error}"
                        )
                    });
                }
            }
            maintenance_progress.observe(
                &maintenance_engine,
                &maintenance_path,
                &maintenance_context,
            );
            maintenance_round_barrier.wait().await;
            if round == run.cancellation_round() {
                cancellation_phase_sender
                    .send(())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{maintenance_context}: cancellation coordinator dropped unexpectedly"
                        )
                    });
                maintenance_cancellation_barrier.wait().await;
            }
        }

        // Start one final flush in the same explicit phase as shutdown. Its
        // result must remain sound whichever side acquires the flush lane first.
        maintenance_shutdown_barrier.wait().await;
        maintenance_engine.flush().await.unwrap_or_else(|error| {
            panic!("{maintenance_context}: shutdown-race flush failed: {error}")
        });
    });

    let mut writer_handles = Vec::with_capacity(run.writers);
    for writer in 0..run.writers {
        let writer_engine = Arc::clone(&engine);
        let writer_round_barrier = Arc::clone(&round_barrier);
        let writer_context = context.clone();
        writer_handles.push(tokio::spawn(async move {
            let mut jitter =
                StdRng::seed_from_u64(run.seed ^ (writer as u64).wrapping_mul(0x9e37_79b9));
            for operation in 0..run.writes_per_writer {
                writer_round_barrier.wait().await;
                for _ in 0..jitter.gen_range(0..=2) {
                    tokio::task::yield_now().await;
                }
                let key = acknowledged_key(writer, operation);
                let value = acknowledged_value(run.seed, writer, operation);
                if operation % 7 == 3 {
                    let scratch = format!("scratch:{writer:02}:{operation:04}").into_bytes();
                    let mut batch = WriteBatch::new();
                    batch.put(&scratch, b"discarded");
                    batch.put(&key, &value);
                    batch.delete(&scratch);
                    writer_engine.write_batch(&batch).await.unwrap_or_else(|error| {
                        panic!(
                            "{writer_context}: writer={writer} operation={operation} batch failed: {error}"
                        )
                    });
                } else {
                    writer_engine.insert(&key, &value).await.unwrap_or_else(|error| {
                        panic!(
                            "{writer_context}: writer={writer} operation={operation} insert failed: {error}"
                        )
                    });
                }
                writer_round_barrier.wait().await;
            }
        }));
    }

    tokio::time::timeout(Duration::from_secs(15), cancellation_phase_receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("{context}: timed out waiting for cancellation schedule phase"))
        .unwrap_or_else(|| panic!("{context}: cancellation schedule phase closed early"));

    let (ready_sender, mut ready_receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut cancelled_writes = Vec::new();
    for operation in 0..64_usize {
        let cancel_engine = Arc::clone(&engine);
        let key = format!("cancel:{operation:04}").into_bytes();
        let value = format!("seed={:016x};cancel={operation:04}", run.seed).into_bytes();
        let handle = if operation % 2 == 0 {
            let ready_sender = ready_sender.clone();
            let cancel_context = context.clone();
            let insert_key = key.clone();
            let insert_value = value.clone();
            tokio::spawn(async move {
                let mut insert = Box::pin(cancel_engine.insert(&insert_key, &insert_value));
                let first_poll =
                    std::future::poll_fn(|context| Poll::Ready(insert.as_mut().poll(context)))
                        .await;
                ready_sender
                    .send((operation, first_poll.is_pending()))
                    .unwrap_or_else(|_| {
                        panic!("{cancel_context}: cancellation coordinator dropped unexpectedly")
                    });
                std::future::poll_fn(|_| {
                    std::hint::black_box(&insert);
                    Poll::<StorageResult<()>>::Pending
                })
                .await
            })
        } else {
            let insert_key = key.clone();
            let insert_value = value.clone();
            tokio::spawn(async move { cancel_engine.insert(&insert_key, &insert_value).await })
        };
        cancelled_writes.push((key, value, handle));
    }
    drop(ready_sender);
    for _ in 0..32 {
        let (operation, was_pending) =
            tokio::time::timeout(Duration::from_secs(15), ready_receiver.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!("{context}: timed out waiting for cancellation poll barrier")
                })
                .unwrap_or_else(|| panic!("{context}: cancellation poll barrier closed early"));
        assert!(
            was_pending,
            "{context}: cancellation write {operation} completed on its first poll"
        );
    }
    for (operation, (_, _, handle)) in cancelled_writes.iter().enumerate() {
        if operation % 2 == 0 {
            handle.abort();
        }
    }
    cancellation_barrier.wait().await;

    for (writer, handle) in writer_handles.into_iter().enumerate() {
        join_with_context(handle, &context, &format!("writer {writer}")).await;
    }

    let mut acknowledged_cancelled = BTreeMap::new();
    for (operation, (key, value, handle)) in cancelled_writes.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {
                acknowledged_cancelled.insert(key, value);
            }
            Ok(Err(error)) => {
                panic!("{context}: uncancelled write {operation} failed: {error}")
            }
            Err(error) if operation % 2 == 0 && error.is_cancelled() => {}
            Err(error) => panic!("{context}: cancellation task {operation} panicked: {error}"),
        }
    }

    shutdown_barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(15), engine.shutdown())
        .await
        .unwrap_or_else(|_| panic!("{context}: shutdown timed out during maintenance races"))
        .unwrap_or_else(|error| panic!("{context}: shutdown failed: {error}"));
    stop.store(true, Ordering::Release);
    scanner.abort();
    let scanner_result = scanner.await;
    assert!(
        scanner_result.is_ok() || scanner_result.is_err_and(|error| error.is_cancelled()),
        "{context}: scanner failed while being cancelled"
    );
    join_with_context(maintenance, &context, "maintenance").await;

    let progress = *progress
        .state
        .lock()
        .unwrap_or_else(|error| panic!("{context}: progress lock poisoned: {error}"));
    assert!(
        progress.rotation,
        "{context}: concurrent workload never advanced beyond the initial WAL segment"
    );
    assert!(
        progress.multiple_segments,
        "{context}: concurrent workload never retained more than one WAL segment"
    );
    assert!(
        progress.older_segment_reclaimed,
        "{context}: checkpointing never removed an older WAL segment"
    );
    assert!(
        progress.checkpoint,
        "{context}: WAL checkpoint never advanced"
    );
    assert!(
        progress.flush_output,
        "{context}: no SSTable flush output was observed during writer rounds"
    );
    assert!(
        progress.compaction_input && progress.compaction_output,
        "{context}: no completed compaction input/output was observed during writer rounds"
    );
    let expected = expected_acknowledged_model(run);
    let acknowledged = engine
        .scan_prefix(b"ack:")
        .await
        .unwrap_or_else(|error| panic!("{context}: final acknowledged scan failed: {error}"));
    assert_eq!(
        acknowledged,
        expected
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>(),
        "{context}: an acknowledged mutation vanished before reopen"
    );
    assert!(
        engine
            .scan_prefix(b"scratch:")
            .await
            .unwrap_or_else(|error| panic!("{context}: scratch scan failed: {error}"))
            .is_empty(),
        "{context}: atomic batch exposed a deleted scratch value"
    );
    let before_reopen = engine
        .scan_prefix(b"")
        .await
        .unwrap_or_else(|error| panic!("{context}: pre-reopen snapshot failed: {error}"));
    let before_map = before_reopen.iter().cloned().collect::<BTreeMap<_, _>>();
    for (key, value) in &acknowledged_cancelled {
        assert_eq!(
            before_map.get(key),
            Some(value),
            "{context}: successful cancellation-path write vanished before reopen for {key:?}"
        );
    }

    drop(engine);
    let reopened = Engine::open(config)
        .await
        .unwrap_or_else(|error| panic!("{context}: reopen failed: {error}"));
    let after_reopen = reopened
        .scan_prefix(b"")
        .await
        .unwrap_or_else(|error| panic!("{context}: post-reopen snapshot failed: {error}"));
    let after_map = after_reopen.into_iter().collect::<BTreeMap<_, _>>();
    for (key, value) in &before_reopen {
        assert_eq!(
            after_map.get(key),
            Some(value),
            "{context}: state visible before reopen vanished or changed for {key:?}"
        );
    }
    for (key, value) in &expected {
        assert_eq!(
            after_map.get(key),
            Some(value),
            "{context}: acknowledged state vanished or changed on reopen for {key:?}"
        );
    }
    for (key, value) in &acknowledged_cancelled {
        assert_eq!(
            after_map.get(key),
            Some(value),
            "{context}: successful cancellation-path write vanished on reopen for {key:?}"
        );
    }
    let allowed_cancelled = (0..64_usize)
        .map(|operation| {
            (
                format!("cancel:{operation:04}").into_bytes(),
                format!("seed={:016x};cancel={operation:04}", run.seed).into_bytes(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (key, value) in &after_map {
        let valid = expected
            .get(key)
            .or_else(|| allowed_cancelled.get(key))
            .is_some_and(|expected_value| expected_value == value);
        assert!(
            valid,
            "{context}: reopen exposed corrupt or partially applied state for {key:?}"
        );
    }
    reopened
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{context}: reopened shutdown failed: {error}"));
}

/// Inspired by SQLite's historically delicate WAL-reset races, but exercises
/// TurboKV's own segment rotation, checkpoint, and reclamation protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_wal_reset_inspired_rotation_checkpoint_reclamation_interleaving_is_sound() {
    run_concurrency_interleaving(ConcurrencyRun {
        seed: CONCURRENCY_SEED,
        writers: 4,
        writes_per_writer: 36,
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit whole-database stress repetitions; TURBOKV_SOUNDNESS_SEED and TURBOKV_SOUNDNESS_STEPS (minimum 16) select a replay"]
async fn repeated_seeded_database_soundness_stress() {
    let selected_seed = std::env::var("TURBOKV_SOUNDNESS_SEED").ok().map(|seed| {
        let seed = seed.trim_start_matches("0x");
        u64::from_str_radix(seed, 16)
            .unwrap_or_else(|error| panic!("invalid TURBOKV_SOUNDNESS_SEED={seed}: {error}"))
    });
    let steps = std::env::var("TURBOKV_SOUNDNESS_STEPS")
        .ok()
        .map(|steps| {
            steps
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("invalid TURBOKV_SOUNDNESS_STEPS={steps}: {error}"))
        })
        .unwrap_or(256);
    assert!(
        steps >= MIN_CONCURRENCY_ROUNDS as u64,
        "invalid TURBOKV_SOUNDNESS_STEPS={steps}: minimum is {MIN_CONCURRENCY_ROUNDS}"
    );
    let seeds = selected_seed.map_or_else(
        || {
            vec![
                0x0a3f_9021_77bc_e465,
                0x784c_1de9_b506_23af,
                0xe21b_6a85_40df_9137,
            ]
        },
        |seed| vec![seed],
    );

    for seed in seeds {
        for mode in [ModelMode::Fast, ModelMode::Durable, ModelMode::Paranoid] {
            run_generated_model(mode, seed, steps).await;
        }
        run_concurrency_interleaving(ConcurrencyRun {
            seed,
            writers: 6,
            writes_per_writer: steps as usize,
        })
        .await;
    }
}
