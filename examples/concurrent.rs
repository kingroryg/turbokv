#![allow(clippy::all, clippy::pedantic)]
//! Concurrent access with TurboKV.
//!
//! TurboKV is designed for high concurrency using lock-free
//! data structures. Multiple tasks can read and write simultaneously.
//!
//! Run with: `cargo run --example concurrent`

use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    // ============================================
    // Shared Database Access
    // ============================================
    println!("=== Shared Database Access ===\n");

    let db = Arc::new(
        Db::open_with_options(temp_dir.path().join("concurrent"), DbOptions::fast()).await?,
    );

    println!("Database opened and wrapped in Arc for sharing\n");

    // ============================================
    // Concurrent Writers
    // ============================================
    println!("=== Concurrent Writers ===");

    let num_writers = 4;
    let writes_per_task = 1000;

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for writer_id in 0..num_writers {
        let db = Arc::clone(&db);

        handles.spawn(async move {
            for i in 0..writes_per_task {
                let key = format!("writer:{}:key:{:05}", writer_id, i);
                let value = format!("value from writer {} at {}", writer_id, i);
                db.insert(key.as_bytes(), value.as_bytes()).await.unwrap();
            }
            writer_id
        });
    }

    // Wait for all writers
    while let Some(result) = handles.join_next().await {
        let writer_id = result?;
        println!("  Writer {} completed", writer_id);
    }

    // Flush to ensure all writes are visible
    db.flush().await?;

    let elapsed = start.elapsed();
    let total_writes = num_writers * writes_per_task;

    println!("\nResults:");
    println!(
        "  {} writers x {} writes = {} total",
        num_writers, writes_per_task, total_writes
    );
    println!("  Time: {:?}", elapsed);
    println!(
        "  Throughput: {:.0} writes/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );

    // Verify all data was written
    let stats = db.stats();
    println!("  Keys in database: {}", stats.total_keys);

    // ============================================
    // Concurrent Readers
    // ============================================
    println!("\n=== Concurrent Readers ===");

    let num_readers = 4;
    let reads_per_task = 1000;

    let start = Instant::now();

    let mut handles = JoinSet::new();

    for reader_id in 0..num_readers {
        let db = Arc::clone(&db);

        handles.spawn(async move {
            let mut found = 0;
            for i in 0..reads_per_task {
                // Read from random writer's data
                let writer = i % num_writers;
                let key = format!("writer:{}:key:{:05}", writer, i % writes_per_task);
                if db.get(key.as_bytes()).await.unwrap().is_some() {
                    found += 1;
                }
            }
            (reader_id, found)
        });
    }

    while let Some(result) = handles.join_next().await {
        let (reader_id, found) = result?;
        println!(
            "  Reader {} found {}/{} keys",
            reader_id, found, reads_per_task
        );
    }

    let elapsed = start.elapsed();
    let total_reads = num_readers * reads_per_task;

    println!("\nResults:");
    println!(
        "  {} readers x {} reads = {} total",
        num_readers, reads_per_task, total_reads
    );
    println!("  Time: {:?}", elapsed);
    println!(
        "  Throughput: {:.0} reads/sec",
        total_reads as f64 / elapsed.as_secs_f64()
    );

    // ============================================
    // Mixed Read/Write Workload
    // ============================================
    println!("\n=== Mixed Read/Write Workload ===");

    let start = Instant::now();
    let mut handles = JoinSet::new();

    // Writers
    for writer_id in 0..2 {
        let db = Arc::clone(&db);
        handles.spawn(async move {
            for i in 0..500 {
                let key = format!("mixed:{}:{:05}", writer_id, i);
                db.insert(key.as_bytes(), b"mixed value").await.unwrap();
            }
            ("writer", writer_id)
        });
    }

    // Readers
    for reader_id in 0..2 {
        let db = Arc::clone(&db);
        handles.spawn(async move {
            for i in 0..500 {
                let key = format!("writer:0:key:{:05}", i % writes_per_task);
                let _ = db.get(key.as_bytes()).await.unwrap();
            }
            ("reader", reader_id)
        });
    }

    while let Some(result) = handles.join_next().await {
        let (role, id) = result?;
        println!("  {} {} completed", role, id);
    }

    let elapsed = start.elapsed();
    println!("\nMixed workload completed in {:?}", elapsed);

    // ============================================
    // Producer-Consumer Pattern
    // ============================================
    println!("\n=== Producer-Consumer Pattern ===");

    let db = Arc::new(
        Db::open_with_options(temp_dir.path().join("producer_consumer"), DbOptions::fast()).await?,
    );

    // Counter for produced items
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = JoinSet::new();

    // Producer
    let db_producer = Arc::clone(&db);
    let counter_producer = Arc::clone(&counter);
    handles.spawn(async move {
        for i in 0..100 {
            let key = format!("queue:{:05}", i);
            db_producer
                .insert(key.as_bytes(), b"task data")
                .await
                .unwrap();
            counter_producer.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        db_producer.flush().await.unwrap();
        "producer"
    });

    // Consumer (waits a bit then reads)
    let db_consumer = Arc::clone(&db);
    handles.spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let items = db_consumer.scan_prefix(b"queue:").await.unwrap();
        println!("  Consumer found {} items in queue", items.len());
        "consumer"
    });

    while let Some(result) = handles.join_next().await {
        let role = result?;
        println!("  {} completed", role);
    }

    // ============================================
    // Best Practices
    // ============================================
    println!("\n=== Concurrency Best Practices ===\n");

    println!("1. Wrap Db in Arc for sharing across tasks");
    println!("2. TurboKV uses lock-free structures internally");
    println!("3. Reads never block writes and vice versa");
    println!("4. Use WriteBatch for atomic multi-key updates");
    println!("5. Call flush() to ensure visibility across all readers");
    println!("6. For maximum throughput, use fast mode with periodic flush");

    println!("\nConcurrent access example completed successfully!");
    Ok(())
}
