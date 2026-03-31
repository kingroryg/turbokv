#![allow(clippy::all, clippy::pedantic)]
//! Batch writes with TurboKV.
//!
//! WriteBatch groups multiple operations into a single write.
//! The batch is written atomically to the WAL, but operations
//! are applied sequentially to the memtable.
//!
//! Run with: `cargo run --example batch_writes`

use turbokv::{Db, DbOptions, WriteBatch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db = Db::open_with_options(temp_dir.path(), DbOptions::durable()).await?;

    // ============================================
    // Basic Batch Write
    // ============================================
    println!("=== Basic Batch Write ===");

    let mut batch = WriteBatch::new();
    batch.put(b"user:1:name", b"Alice");
    batch.put(b"user:1:email", b"alice@example.com");
    batch.put(b"user:1:role", b"admin");

    // All three writes are applied as a group
    db.write_batch(&batch).await?;
    println!("Wrote user:1 data in a single batch ({} operations)", batch.len());

    // Verify
    for key in &[
        &b"user:1:name"[..],
        &b"user:1:email"[..],
        &b"user:1:role"[..],
    ] {
        if let Some(value) = db.get(*key).await? {
            println!(
                "  {} = {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(&value)
            );
        }
    }

    // ============================================
    // Mixed Put and Delete Operations
    // ============================================
    println!("\n=== Mixed Put and Delete ===");

    let mut batch = WriteBatch::new();

    // Update some fields
    batch.put(b"user:1:role", b"superadmin");
    batch.put(b"user:1:verified", b"true");

    // Delete a field
    batch.delete(b"user:1:email");

    db.write_batch(&batch).await?;
    println!("Updated and deleted fields in a single batch");

    // Verify role was updated
    if let Some(value) = db.get(b"user:1:role").await? {
        println!("  user:1:role = {}", String::from_utf8_lossy(&value));
    }

    // Verify email was deleted
    if !db.contains_key(b"user:1:email").await? {
        println!("  user:1:email was deleted");
    }

    // ============================================
    // Pre-allocated Batch (for performance)
    // ============================================
    println!("\n=== Pre-allocated Batch ===");

    // When you know how many operations you'll have,
    // pre-allocate to avoid reallocations
    let mut batch = WriteBatch::with_capacity(100);

    for i in 0..100 {
        let key = format!("item:{:04}", i);
        let value = format!("value_{}", i);
        batch.put(key.as_bytes(), value.as_bytes());
    }

    db.write_batch(&batch).await?;
    println!("Wrote 100 items in a single batch");

    // ============================================
    // Batch Reuse (clear and reuse)
    // ============================================
    println!("\n=== Batch Reuse ===");

    let mut batch = WriteBatch::new();

    // First batch
    batch.put(b"counter:a", b"1");
    batch.put(b"counter:b", b"2");
    db.write_batch(&batch).await?;
    println!("First batch: wrote 2 counters");

    // Clear and reuse
    batch.clear();
    assert!(batch.is_empty());

    // Second batch with same object
    batch.put(b"counter:c", b"3");
    batch.put(b"counter:d", b"4");
    db.write_batch(&batch).await?;
    println!("Second batch: wrote 2 more counters (reused batch object)");

    // ============================================
    // Use Case: Transfer (Debit + Credit)
    // ============================================
    println!("\n=== Use Case: Batched Transfer ===");

    // Initial balances
    db.insert(b"balance:alice", b"1000").await?;
    db.insert(b"balance:bob", b"500").await?;

    // Transfer $200 from Alice to Bob
    // Batch ensures all writes go to the WAL together
    let mut transfer = WriteBatch::new();
    transfer.put(b"balance:alice", b"800"); // 1000 - 200
    transfer.put(b"balance:bob", b"700"); // 500 + 200
    transfer.put(b"tx:001", b"alice->bob:200"); // Audit trail

    db.write_batch(&transfer).await?;
    println!("Transferred $200 from Alice to Bob in a single batch");

    // Verify
    if let Some(alice) = db.get(b"balance:alice").await? {
        println!("  Alice's balance: ${}", String::from_utf8_lossy(&alice));
    }
    if let Some(bob) = db.get(b"balance:bob").await? {
        println!("  Bob's balance: ${}", String::from_utf8_lossy(&bob));
    }

    let stats = db.stats();
    println!(
        "\nFinal stats: {} keys, {} bytes",
        stats.total_keys, stats.total_bytes
    );

    println!("\nBatch writes example completed successfully!");
    Ok(())
}
