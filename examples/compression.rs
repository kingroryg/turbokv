#![allow(clippy::all, clippy::pedantic)]
//! Compression options in TurboKV.
//!
//! TurboKV supports multiple compression algorithms for SSTable blocks.
//! Choose based on your performance and storage requirements.
//!
//! Run with: `cargo run --example compression`

use std::time::Instant;
use turbokv::{Compression, Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    // Generate test data - compressible text
    let test_value: Vec<u8> = (0..1000)
        .map(|i| format!("This is test data line {} with some repetitive content. ", i))
        .collect::<String>()
        .into_bytes();

    println!("Test value size: {} bytes ({}KB)", test_value.len(), test_value.len() / 1024);
    println!("Writing 100 entries with each compression type...\n");

    let compressions = [
        (Compression::None, "None"),
        (Compression::Lz4, "LZ4"),
        (Compression::Snappy, "Snappy"),
        (Compression::Zstd, "Zstd"),
    ];

    for (compression, name) in &compressions {
        let db_path = temp_dir.path().join(format!("db_{}", name.to_lowercase()));

        let options = DbOptions::fast().with_compression(*compression);
        let db = Db::open_with_options(&db_path, options).await?;

        // Time the writes
        let start = Instant::now();
        for i in 0..100 {
            let key = format!("key:{:05}", i);
            db.insert(key.as_bytes(), &test_value).await?;
        }
        db.flush().await?;
        let write_time = start.elapsed();

        // Time the reads
        let start = Instant::now();
        for i in 0..100 {
            let key = format!("key:{:05}", i);
            let _ = db.get(key.as_bytes()).await?;
        }
        let read_time = start.elapsed();

        // Get disk usage
        let disk_size = get_dir_size(&db_path)?;

        println!("=== {} ===", name);
        println!("  Write time: {:?}", write_time);
        println!("  Read time: {:?}", read_time);
        println!("  Disk usage: {} KB", disk_size / 1024);
        println!("  Compression ratio: {:.2}x",
            (test_value.len() * 100) as f64 / disk_size as f64);
        println!();

        drop(db);
    }

    // ============================================
    // Choosing the Right Compression
    // ============================================
    println!("=== Recommendations ===\n");

    println!("Use Compression::None when:");
    println!("  - Data is already compressed (images, videos, encrypted)");
    println!("  - Maximum write speed is critical");
    println!("  - Disk space is not a concern");
    println!();

    println!("Use Compression::Lz4 (default) when:");
    println!("  - You want the best balance of speed and compression");
    println!("  - General-purpose workloads");
    println!("  - Most production use cases");
    println!();

    println!("Use Compression::Snappy when:");
    println!("  - Similar to LZ4, slight differences in ratio/speed");
    println!("  - Compatibility with other systems using Snappy");
    println!();

    println!("Use Compression::Zstd when:");
    println!("  - Maximum compression ratio is important");
    println!("  - Disk space is expensive (cloud storage)");
    println!("  - Willing to trade some CPU for smaller files");
    println!("  - Archival or cold storage");
    println!();

    // ============================================
    // Mixed workload example
    // ============================================
    println!("=== Mixed Workload Example ===\n");

    let db_path = temp_dir.path().join("mixed_db");

    // For mixed workloads, LZ4 is usually the best choice
    let options = DbOptions::durable().with_compression(Compression::Lz4);
    let db = Db::open_with_options(&db_path, options).await?;

    // JSON-like data (highly compressible)
    let json_data = r#"{"user_id": 12345, "name": "Alice", "email": "alice@example.com", "preferences": {"theme": "dark", "notifications": true}}"#;
    db.insert(b"user:12345", json_data.as_bytes()).await?;

    // Binary data (may or may not compress well)
    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    db.insert(b"binary:sample", &binary_data).await?;

    // Repeated data (compresses very well)
    let repeated = "AAAAAAAAAA".repeat(100);
    db.insert(b"repeated:sample", repeated.as_bytes()).await?;

    db.flush().await?;

    println!("Stored mixed data types with LZ4 compression");
    println!("LZ4 handles all types well with minimal overhead");

    println!("\nCompression example completed successfully!");
    Ok(())
}

fn get_dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                size += metadata.len();
            } else if metadata.is_dir() {
                size += get_dir_size(&entry.path())?;
            }
        }
    }
    Ok(size)
}
