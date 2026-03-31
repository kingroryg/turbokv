//! Large-scale benchmark for TurboKV vs RocksDB vs fjall
//!
//! Tests with 10M keys, 400-byte values (4GB total)
//! Comparable to RocksDB benchmark methodology
//!
//! Run with: cargo bench --bench large_scale_bench

use fjall::{Config, PersistMode};
use std::time::Instant;
use tempfile::TempDir;
use turbokv::{Db, DbOptions};

const KEY_COUNT: usize = 10_000_000; // 10M keys
const VALUE_SIZE: usize = 400; // 400 bytes per value (RocksDB default)
const KEY_SIZE: usize = 20; // 20-byte keys (RocksDB default)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Large-Scale Benchmark: TurboKV vs RocksDB vs fjall");
    println!("===================================================");
    println!(
        "Keys: {} ({:.1}M)",
        KEY_COUNT,
        KEY_COUNT as f64 / 1_000_000.0
    );
    println!("Key size: {} bytes", KEY_SIZE);
    println!("Value size: {} bytes", VALUE_SIZE);
    println!(
        "Total data: {:.2} GB",
        (KEY_COUNT * (KEY_SIZE + VALUE_SIZE)) as f64 / 1_000_000_000.0
    );
    println!();

    // Pre-generate values (reuse same value to reduce memory pressure)
    let value = vec![0xABu8; VALUE_SIZE];

    // =====================
    // TurboKV Fast Mode (optimized - auto sync path + thread-local buffers)
    // =====================
    println!("Testing TURBOKV fast mode (optimized)...");
    {
        let temp = TempDir::new()?;
        let db = Db::open_with_options(temp.path(), DbOptions::fast()).await?;

        let start = Instant::now();
        for i in 0..KEY_COUNT {
            let key = format!("{:0>width$}", i, width = KEY_SIZE);
            db.insert(key.as_bytes(), &value).await?;

            if i > 0 && i % 1_000_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = i as f64 / elapsed;
                println!(
                    "  Progress: {}M keys, {:.0}K ops/sec",
                    i / 1_000_000,
                    rate / 1000.0
                );
            }
        }
        db.flush().await?;

        let elapsed = start.elapsed();
        let ops_per_sec = KEY_COUNT as f64 / elapsed.as_secs_f64();
        println!(
            "TURBOKV fast: {:.2}s, {:.0}K ops/sec",
            elapsed.as_secs_f64(),
            ops_per_sec / 1000.0
        );
    }
    println!();

    // =====================
    // TurboKV Durable Mode
    // =====================
    println!("Testing TURBOKV durable mode (WAL, periodic sync)...");
    {
        let temp = TempDir::new()?;
        let db = Db::open_with_options(temp.path(), DbOptions::durable()).await?;

        let start = Instant::now();
        for i in 0..KEY_COUNT {
            let key = format!("{:0>width$}", i, width = KEY_SIZE);
            db.insert(key.as_bytes(), &value).await?;

            if i > 0 && i % 1_000_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = i as f64 / elapsed;
                println!(
                    "  Progress: {}M keys, {:.0}K ops/sec",
                    i / 1_000_000,
                    rate / 1000.0
                );
            }
        }
        let write_elapsed = start.elapsed();
        println!(
            "  Writes done in {:.2}s, now flushing...",
            write_elapsed.as_secs_f64()
        );
        db.flush().await?;

        let elapsed = start.elapsed();
        let flush_time = elapsed - write_elapsed;
        let ops_per_sec = KEY_COUNT as f64 / elapsed.as_secs_f64();
        println!("  Flush took {:.2}s", flush_time.as_secs_f64());
        println!(
            "TURBOKV durable: {:.2}s total, {:.0}K ops/sec",
            elapsed.as_secs_f64(),
            ops_per_sec / 1000.0
        );
    }
    println!();

    // =====================
    // fjall (LSM-tree, Rust native)
    // =====================
    println!("Testing fjall (default config)...");
    {
        let temp = TempDir::new()?;
        let keyspace = Config::new(temp.path()).open()?;
        let db = keyspace.open_partition("bench", Default::default())?;

        let start = Instant::now();
        for i in 0..KEY_COUNT {
            let key = format!("{:0>width$}", i, width = KEY_SIZE);
            db.insert(key.as_bytes(), &value)?;

            if i > 0 && i % 1_000_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = i as f64 / elapsed;
                println!(
                    "  Progress: {}M keys, {:.0}K ops/sec",
                    i / 1_000_000,
                    rate / 1000.0
                );
            }
        }
        keyspace.persist(PersistMode::SyncAll)?;

        let elapsed = start.elapsed();
        let ops_per_sec = KEY_COUNT as f64 / elapsed.as_secs_f64();
        println!(
            "fjall default: {:.2}s, {:.0}K ops/sec",
            elapsed.as_secs_f64(),
            ops_per_sec / 1000.0
        );
    }
    println!();

    // =====================
    // RocksDB (for reference)
    // =====================
    println!("Testing RocksDB (default config with WAL)...");
    {
        let temp = TempDir::new()?;
        let db = rocksdb::DB::open_default(temp.path())?;

        let start = Instant::now();
        for i in 0..KEY_COUNT {
            let key = format!("{:0>width$}", i, width = KEY_SIZE);
            db.put(key.as_bytes(), &value)?;

            if i > 0 && i % 1_000_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = i as f64 / elapsed;
                println!(
                    "  Progress: {}M keys, {:.0}K ops/sec",
                    i / 1_000_000,
                    rate / 1000.0
                );
            }
        }
        db.flush()?;

        let elapsed = start.elapsed();
        let ops_per_sec = KEY_COUNT as f64 / elapsed.as_secs_f64();
        println!(
            "RocksDB: {:.2}s, {:.0}K ops/sec",
            elapsed.as_secs_f64(),
            ops_per_sec / 1000.0
        );
    }
    println!();

    println!("Benchmark complete!");
    Ok(())
}
