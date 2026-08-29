//! Basic key-value operations.
//!
//! Run with: `cargo run --example basic`

use turbokv::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = Db::open(directory.path()).await?;

    db.insert(b"language", b"Rust").await?;
    assert_eq!(db.get(b"language").await?, Some(b"Rust".to_vec()));
    assert!(db.contains_key(b"language").await?);

    db.insert(b"language", b"rust").await?;
    assert_eq!(db.get(b"language").await?, Some(b"rust".to_vec()));

    assert_eq!(db.take(b"language").await?, Some(b"rust".to_vec()));
    assert_eq!(db.get(b"language").await?, None);

    db.close().await?;
    println!("basic operations passed");
    Ok(())
}
