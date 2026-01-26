#![allow(clippy::all, clippy::pedantic)]
//! Data persistence and recovery with TurboKV.
//!
//! This example demonstrates how TurboKV persists data and
//! recovers from restarts.
//!
//! Run with: `cargo run --example persistence`

use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a fixed path so we can demonstrate persistence
    let db_path = std::env::temp_dir().join("turbokv_persistence_example");

    // Clean up from previous runs
    if db_path.exists() {
        std::fs::remove_dir_all(&db_path)?;
    }

    println!("=== Phase 1: Initial Data Write ===\n");

    // Open database with durable options (WAL enabled)
    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;

        // Write some data
        db.insert(b"config:app_name", b"MyApp").await?;
        db.insert(b"config:version", b"1.0.0").await?;
        db.insert(b"config:debug", b"false").await?;

        // User data
        for i in 1..=5 {
            let key = format!("user:{}", i);
            let value = format!("User {}", i);
            db.insert(key.as_bytes(), value.as_bytes()).await?;
        }

        println!("Wrote initial data:");
        println!("  - 3 config entries");
        println!("  - 5 user entries");

        let stats = db.stats();
        println!("  - Total: {} keys, {} bytes\n", stats.total_keys, stats.total_bytes);

        // Simulate a clean shutdown
        db.flush().await?;
        println!("Flushed and closed database (clean shutdown)");
    }
    // Database is dropped here

    println!("\n=== Phase 2: Reopen and Verify ===\n");

    // Reopen the database
    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;

        println!("Reopened database, verifying data...");

        // Verify config
        let app_name = db.get(b"config:app_name").await?;
        assert_eq!(app_name, Some(b"MyApp".to_vec()));
        println!("  config:app_name = {:?}", app_name.map(|v| String::from_utf8_lossy(&v).to_string()));

        // Verify users
        let users = db.scan_prefix(b"user:").await?;
        println!("  Found {} users", users.len());
        assert_eq!(users.len(), 5);

        println!("\nData successfully recovered!");
    }

    println!("\n=== Phase 3: Simulate Crash Recovery ===\n");

    // Write data and "crash" (drop without flush)
    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;

        // Write new data
        db.insert(b"important:data", b"this must survive").await?;
        db.insert(b"transaction:001", b"pending").await?;

        println!("Wrote new data without calling flush()...");
        println!("Simulating crash (dropping without clean shutdown)");

        // Database drops here WITHOUT flush
        // In durable mode, WAL ensures data is safe
    }

    // "Recovery" - reopen after crash
    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;

        println!("\nReopened after simulated crash...");

        // With WAL enabled (durable mode), data should be there
        let important = db.get(b"important:data").await?;
        let transaction = db.get(b"transaction:001").await?;

        if important.is_some() && transaction.is_some() {
            println!("SUCCESS: WAL recovery worked!");
            println!("  important:data = {}", String::from_utf8_lossy(&important.unwrap()));
            println!("  transaction:001 = {}", String::from_utf8_lossy(&transaction.unwrap()));
        } else {
            println!("Note: Data may not be recovered depending on timing");
            println!("  (OS may not have flushed buffers yet)");
        }
    }

    println!("\n=== Phase 4: Durability Comparison ===\n");

    // Clean up for comparison
    std::fs::remove_dir_all(&db_path)?;

    println!("Comparing Fast vs Durable modes:\n");

    // FAST MODE (no durability)
    println!("Fast mode (wal_enabled: false):");
    {
        let db = Db::open_with_options(&db_path, DbOptions::fast()).await?;
        db.insert(b"fast:data", b"may be lost").await?;
        println!("  Wrote data, dropping without flush...");
        // Intentionally not flushing
    }

    {
        let db = Db::open_with_options(&db_path, DbOptions::fast()).await?;
        let data = db.get(b"fast:data").await?;
        if data.is_none() {
            println!("  Result: Data was LOST (expected in fast mode)");
        } else {
            println!("  Result: Data survived (got lucky with OS buffers)");
        }
    }

    // Clean up
    std::fs::remove_dir_all(&db_path)?;

    // DURABLE MODE (WAL enabled)
    println!("\nDurable mode (wal_enabled: true):");
    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;
        db.insert(b"durable:data", b"will survive").await?;
        println!("  Wrote data, dropping without flush...");
    }

    {
        let db = Db::open_with_options(&db_path, DbOptions::durable()).await?;
        let data = db.get(b"durable:data").await?;
        if data.is_some() {
            println!("  Result: Data SURVIVED (WAL recovery)");
        } else {
            println!("  Result: Data may still be in OS buffers");
        }
    }

    // ============================================
    // Best Practices
    // ============================================
    println!("\n=== Best Practices ===\n");

    println!("1. Use DbOptions::durable() for important data");
    println!("2. Use DbOptions::paranoid() for critical transactions");
    println!("3. Call flush() before clean shutdown");
    println!("4. Use WriteBatch for atomic multi-key operations");
    println!("5. Keep database open for duration of application");

    // Clean up example data
    std::fs::remove_dir_all(&db_path)?;

    println!("\nPersistence example completed successfully!");
    Ok(())
}
