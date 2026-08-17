#![allow(clippy::all, clippy::pedantic)]
//! Basic CRUD operations with TurboKV.
//!
//! Run with: `cargo run --example basic`

use turbokv::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("basic_example");

    // Open database with default options (durable, WAL enabled)
    let db = Db::open(&db_path).await?;
    println!("Opened database at {:?}", db_path);

    // ============================================
    // INSERT - Add key-value pairs
    // ============================================
    db.insert(b"name", b"TurboKV").await?;
    db.insert(b"version", b"0.4.0").await?;
    db.insert(b"language", b"Rust").await?;
    println!("Inserted 3 key-value pairs");

    // ============================================
    // GET - Retrieve values by key
    // ============================================
    if let Some(value) = db.get(b"name").await? {
        println!("name = {}", String::from_utf8_lossy(&value));
    }

    // Non-existent keys return None
    match db.get(b"nonexistent").await? {
        Some(_) => println!("Found nonexistent key (unexpected!)"),
        None => println!("Key 'nonexistent' not found (as expected)"),
    }

    // ============================================
    // CONTAINS_KEY - Check if key exists
    // ============================================
    if db.contains_key(b"version").await? {
        println!("Key 'version' exists");
    }

    // ============================================
    // UPDATE - Overwrite existing values
    // ============================================
    db.insert(b"version", b"0.4.1").await?;
    if let Some(value) = db.get(b"version").await? {
        println!("Updated version = {}", String::from_utf8_lossy(&value));
    }

    // ============================================
    // REMOVE - Delete keys
    // ============================================
    db.remove(b"language").await?;
    println!("Removed key 'language'");

    // Verify it's gone
    if !db.contains_key(b"language").await? {
        println!("Key 'language' successfully removed");
    }

    // ============================================
    // STATS - View database statistics
    // ============================================
    let logical = db.logical_stats().await?;
    let physical = db.physical_stats();
    println!("\nDatabase Statistics:");
    println!("  Live keys: {}", logical.live_keys);
    println!("  Logical bytes: {}", logical.total_bytes);
    println!(
        "  Active MemTable size: {} bytes",
        physical.memtables.active_bytes
    );

    // ============================================
    // FLUSH - Persist data to disk
    // ============================================
    db.flush().await?;
    println!("\nFlushed data to disk");

    println!("\nBasic example completed successfully!");
    Ok(())
}
