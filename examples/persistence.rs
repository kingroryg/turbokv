//! Synced write-ahead-log recovery after an unclean drop.
//!
//! Run with: `cargo run --example persistence`

use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let database_path = directory.path().join("database");

    let db = Db::open_with_options(&database_path, DbOptions::paranoid()).await?;
    db.insert(b"acknowledged", b"durable").await?;
    drop(db); // Deliberately skip flush and clean close.

    let recovered = Db::open_with_options(&database_path, DbOptions::paranoid()).await?;
    assert_eq!(
        recovered.get(b"acknowledged").await?,
        Some(b"durable".to_vec())
    );

    recovered.close().await?;
    println!("WAL recovery passed");
    Ok(())
}
