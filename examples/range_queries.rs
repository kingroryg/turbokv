//! Prefix and range scans.
//!
//! Run with: `cargo run --example range_queries`

use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = Db::open_with_options(directory.path(), DbOptions::fast()).await?;

    for (key, value) in [
        (b"order:001".as_slice(), b"new".as_slice()),
        (b"order:002".as_slice(), b"paid".as_slice()),
        (b"order:003".as_slice(), b"sent".as_slice()),
        (b"user:001".as_slice(), b"Ada".as_slice()),
    ] {
        db.insert(key, value).await?;
    }

    let orders = db.scan_prefix(b"order:").await?;
    assert_eq!(orders.len(), 3);
    assert_eq!(orders[0].0, b"order:001");
    assert_eq!(orders[2].0, b"order:003");

    let middle = db.range(b"order:002", b"order:004").await?;
    assert_eq!(
        middle,
        vec![
            (b"order:002".to_vec(), b"paid".to_vec()),
            (b"order:003".to_vec(), b"sent".to_vec()),
        ]
    );

    db.close().await?;
    println!("ordered scans passed");
    Ok(())
}
