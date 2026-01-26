#![allow(clippy::all, clippy::pedantic)]
//! Range queries and prefix scans with TurboKV.
//!
//! TurboKV stores keys in sorted order, enabling efficient
//! range scans and prefix-based queries.
//!
//! Run with: `cargo run --example range_queries`

use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db = Db::open_with_options(temp_dir.path(), DbOptions::fast()).await?;

    // ============================================
    // Setup: Insert sample data
    // ============================================
    println!("=== Setting up sample data ===");

    // Users
    db.insert(b"user:001:name", b"Alice").await?;
    db.insert(b"user:001:email", b"alice@example.com").await?;
    db.insert(b"user:002:name", b"Bob").await?;
    db.insert(b"user:002:email", b"bob@example.com").await?;
    db.insert(b"user:003:name", b"Charlie").await?;
    db.insert(b"user:003:email", b"charlie@example.com").await?;

    // Posts
    db.insert(b"post:001:title", b"Hello World").await?;
    db.insert(b"post:001:author", b"user:001").await?;
    db.insert(b"post:002:title", b"TurboKV Guide").await?;
    db.insert(b"post:002:author", b"user:002").await?;

    // Time-series data (timestamps as keys)
    db.insert(b"metric:2024-01-01:cpu", b"45").await?;
    db.insert(b"metric:2024-01-02:cpu", b"52").await?;
    db.insert(b"metric:2024-01-03:cpu", b"48").await?;
    db.insert(b"metric:2024-01-04:cpu", b"61").await?;
    db.insert(b"metric:2024-01-05:cpu", b"55").await?;

    println!("Inserted sample data\n");

    // ============================================
    // PREFIX SCAN - Get all keys with a prefix
    // ============================================
    println!("=== Prefix Scan: All users ===");

    let users = db.scan_prefix(b"user:").await?;
    println!("Found {} user entries:", users.len());
    for (key, value) in &users {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    // ============================================
    // PREFIX SCAN - Specific user's data
    // ============================================
    println!("\n=== Prefix Scan: User 001's data ===");

    let user_001 = db.scan_prefix(b"user:001:").await?;
    for (key, value) in &user_001 {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    // ============================================
    // PREFIX SCAN - All posts
    // ============================================
    println!("\n=== Prefix Scan: All posts ===");

    let posts = db.scan_prefix(b"post:").await?;
    println!("Found {} post entries:", posts.len());
    for (key, value) in &posts {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    // ============================================
    // RANGE SCAN - Date range query
    // ============================================
    println!("\n=== Range Scan: Metrics from Jan 2-4 ===");

    // Range is [start, end) - inclusive start, exclusive end
    let metrics = db.range(b"metric:2024-01-02", b"metric:2024-01-05").await?;
    println!("Found {} metrics in range:", metrics.len());
    for (key, value) in &metrics {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    // ============================================
    // RANGE SCAN - Alphabetical range
    // ============================================
    println!("\n=== Range Scan: Users 001-002 ===");

    let user_range = db.range(b"user:001", b"user:003").await?;
    for (key, value) in &user_range {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    // ============================================
    // Pattern: Hierarchical keys
    // ============================================
    println!("\n=== Pattern: Hierarchical Keys ===");

    // Store hierarchical data
    db.insert(b"org:acme:team:eng:member:alice", b"engineer")
        .await?;
    db.insert(b"org:acme:team:eng:member:bob", b"senior engineer")
        .await?;
    db.insert(b"org:acme:team:sales:member:carol", b"sales rep")
        .await?;
    db.insert(b"org:acme:team:sales:member:dan", b"sales lead")
        .await?;

    // Query all of engineering team
    let eng_team = db.scan_prefix(b"org:acme:team:eng:").await?;
    println!("Engineering team ({} members):", eng_team.len());
    for (key, value) in &eng_team {
        // Extract member name from key
        let key_str = String::from_utf8_lossy(key);
        let member = key_str.split(':').last().unwrap_or("unknown");
        println!("  {} - {}", member, String::from_utf8_lossy(value));
    }

    // Query all teams
    let all_teams = db.scan_prefix(b"org:acme:team:").await?;
    println!("\nAll team members ({} total):", all_teams.len());

    // ============================================
    // Pattern: Secondary indexes
    // ============================================
    println!("\n=== Pattern: Secondary Indexes ===");

    // Primary data
    db.insert(b"product:SKU001:name", b"Widget").await?;
    db.insert(b"product:SKU001:price", b"9.99").await?;
    db.insert(b"product:SKU002:name", b"Gadget").await?;
    db.insert(b"product:SKU002:price", b"19.99").await?;

    // Secondary index by price range (store SKU as value)
    db.insert(b"idx:price:00999:SKU001", b"").await?; // $9.99 -> SKU001
    db.insert(b"idx:price:01999:SKU002", b"").await?; // $19.99 -> SKU002

    // Find products under $15 (price < 01500)
    let cheap = db.range(b"idx:price:00000", b"idx:price:01500").await?;
    println!("Products under $15:");
    for (key, _) in &cheap {
        let key_str = String::from_utf8_lossy(key);
        let sku = key_str.split(':').last().unwrap_or("unknown");
        if let Some(name) = db.get(format!("product:{}:name", sku).as_bytes()).await? {
            println!("  {} - {}", sku, String::from_utf8_lossy(&name));
        }
    }

    println!("\nRange queries example completed successfully!");
    Ok(())
}
