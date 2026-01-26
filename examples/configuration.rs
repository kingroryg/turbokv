#![allow(clippy::all, clippy::pedantic)]
//! Configuration options for TurboKV.
//!
//! TurboKV provides preset configurations optimized for different
//! use cases, plus fine-grained control over all options.
//!
//! Run with: `cargo run --example configuration`

use turbokv::{Compression, Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    // ============================================
    // FAST MODE - Maximum performance
    // ============================================
    println!("=== Fast Mode ===");
    println!("Best for: caches, temporary data, benchmarks");
    println!("Trade-off: No durability - data lost on crash\n");

    let fast_options = DbOptions::fast();
    println!("Configuration:");
    println!("  WAL enabled: {}", fast_options.wal_enabled);
    println!("  Sync writes: {}", fast_options.sync_writes);
    println!("  Compression: {:?}", fast_options.compression);

    let fast_path = temp_dir.path().join("fast_db");
    let db = Db::open_with_options(&fast_path, DbOptions::fast()).await?;

    // Fast mode is great for high-throughput scenarios
    for i in 0..1000 {
        let key = format!("key:{:05}", i);
        let value = format!("value:{}", i);
        db.insert(key.as_bytes(), value.as_bytes()).await?;
    }
    db.flush().await?;
    println!("Wrote 1000 keys in fast mode");

    drop(db);

    // ============================================
    // DURABLE MODE - Crash-safe
    // ============================================
    println!("\n=== Durable Mode ===");
    println!("Best for: most production workloads");
    println!("Trade-off: Slightly slower, but survives process crashes\n");

    let durable_options = DbOptions::durable();
    println!("Configuration:");
    println!("  WAL enabled: {}", durable_options.wal_enabled);
    println!("  Sync writes: {}", durable_options.sync_writes);
    println!("  Compression: {:?}", durable_options.compression);

    let durable_path = temp_dir.path().join("durable_db");
    let db = Db::open_with_options(&durable_path, DbOptions::durable()).await?;

    db.insert(b"important:data", b"this survives crashes")
        .await?;
    println!("Wrote important data with durability guarantee");

    drop(db);

    // ============================================
    // PARANOID MODE - Power-loss safe
    // ============================================
    println!("\n=== Paranoid Mode ===");
    println!("Best for: financial transactions, critical records");
    println!("Trade-off: Slowest, but survives power loss\n");

    let paranoid_options = DbOptions::paranoid();
    println!("Configuration:");
    println!("  WAL enabled: {}", paranoid_options.wal_enabled);
    println!("  Sync writes: {}", paranoid_options.sync_writes);
    println!("  Compression: {:?}", paranoid_options.compression);

    let paranoid_path = temp_dir.path().join("paranoid_db");
    let db = Db::open_with_options(&paranoid_path, DbOptions::paranoid()).await?;

    db.insert(b"transaction:001", b"$1000.00 transferred")
        .await?;
    println!("Wrote transaction with fsync guarantee");

    drop(db);

    // ============================================
    // CUSTOM CONFIGURATION
    // ============================================
    println!("\n=== Custom Configuration ===");
    println!("Fine-grained control over all options\n");

    let custom = DbOptions {
        wal_enabled: true,
        sync_writes: false,
        memtable_size: 128 * 1024 * 1024,    // 128MB memtable
        block_cache_size: 256 * 1024 * 1024, // 256MB cache
        compression: Compression::Zstd,      // Maximum compression
    };

    println!("Configuration:");
    println!("  WAL enabled: {}", custom.wal_enabled);
    println!("  Sync writes: {}", custom.sync_writes);
    println!(
        "  MemTable size: {} MB",
        custom.memtable_size / (1024 * 1024)
    );
    println!(
        "  Block cache: {} MB",
        custom.block_cache_size / (1024 * 1024)
    );
    println!("  Compression: {:?}", custom.compression);

    let custom_path = temp_dir.path().join("custom_db");
    let db = Db::open_with_options(&custom_path, custom).await?;

    db.insert(b"custom:key", b"custom configuration").await?;
    println!("Created database with custom configuration");

    drop(db);

    // ============================================
    // BUILDER PATTERN - Fluent configuration
    // ============================================
    println!("\n=== Builder Pattern ===");

    // Start with a preset and customize
    let tuned = DbOptions::durable().with_compression(Compression::Lz4);

    println!("Durable mode with LZ4 compression:");
    println!("  Compression: {:?}", tuned.compression);

    // ============================================
    // COMPRESSION COMPARISON
    // ============================================
    println!("\n=== Compression Options ===");
    println!("┌──────────┬───────────┬─────────────────────────────┐");
    println!("│ Algorithm│ Speed     │ Use Case                    │");
    println!("├──────────┼───────────┼─────────────────────────────┤");
    println!("│ None     │ Fastest   │ Already compressed data     │");
    println!("│ Lz4      │ Very fast │ Default - best balance      │");
    println!("│ Snappy   │ Fast      │ Alternative to LZ4          │");
    println!("│ Zstd     │ Slower    │ Maximum compression ratio   │");
    println!("└──────────┴───────────┴─────────────────────────────┘");

    // ============================================
    // DURABILITY COMPARISON
    // ============================================
    println!("\n=== Durability Levels ===");
    println!("┌──────────┬─────────┬──────────┬─────────────────────┐");
    println!("│ Mode     │ WAL     │ Sync     │ Survives            │");
    println!("├──────────┼─────────┼──────────┼─────────────────────┤");
    println!("│ Fast     │ No      │ No       │ Nothing (ephemeral) │");
    println!("│ Durable  │ Yes     │ No       │ Process crash       │");
    println!("│ Paranoid │ Yes     │ Yes      │ Power loss          │");
    println!("└──────────┴─────────┴──────────┴─────────────────────┘");

    println!("\nConfiguration example completed successfully!");
    Ok(())
}
