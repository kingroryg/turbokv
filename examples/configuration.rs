//! Supported database configuration.
//!
//! Run with: `cargo run --example configuration`

use turbokv::{Compression, Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;

    let mut options = DbOptions::durable().with_compression(Compression::Zstd);
    options.memtable_size = 8 * 1024 * 1024;
    options.block_cache_size = 16 * 1024 * 1024;

    let db = Db::open_with_options(directory.path(), options).await?;
    db.insert(b"configured", b"yes").await?;
    assert_eq!(db.get(b"configured").await?, Some(b"yes".to_vec()));

    db.close().await?;
    println!("supported configuration passed");
    Ok(())
}
