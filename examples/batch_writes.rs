//! Atomic batch writes.
//!
//! Run with: `cargo run --example batch_writes`

use turbokv::{Db, WriteBatch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = Db::open(directory.path()).await?;

    db.insert(b"cart:old", b"remove me").await?;

    let mut batch = WriteBatch::new();
    batch.put(b"cart:apple", b"2");
    batch.put(b"cart:pear", b"1");
    batch.delete(b"cart:old");
    db.write_batch(&batch).await?;

    assert_eq!(db.get(b"cart:apple").await?, Some(b"2".to_vec()));
    assert_eq!(db.get(b"cart:pear").await?, Some(b"1".to_vec()));
    assert_eq!(db.get(b"cart:old").await?, None);

    db.close().await?;
    println!("atomic batch passed");
    Ok(())
}
