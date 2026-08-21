//! Concurrent access through a shared database handle.
//!
//! Run with: `cargo run --example concurrent`

use std::sync::Arc;

use tokio::task::JoinSet;
use turbokv::{Db, DbError, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const TASKS: usize = 4;
    const KEYS_PER_TASK: usize = 16;

    let directory = tempfile::tempdir()?;
    let db = Arc::new(Db::open_with_options(directory.path(), DbOptions::fast()).await?);
    let mut tasks = JoinSet::new();

    for task_id in 0..TASKS {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            for key_id in 0..KEYS_PER_TASK {
                let key = format!("task:{task_id}:{key_id:02}");
                let value = format!("value:{task_id}:{key_id:02}");
                db.insert(key.as_bytes(), value.as_bytes()).await?;
                assert_eq!(db.get(key.as_bytes()).await?, Some(value.into_bytes()));
            }
            Ok::<(), DbError>(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    assert_eq!(db.scan_prefix(b"task:").await?.len(), TASKS * KEYS_PER_TASK);

    let db = Arc::try_unwrap(db)
        .map_err(|_| std::io::Error::other("concurrent tasks retained the database"))?;
    db.close().await?;
    println!("concurrent access passed");
    Ok(())
}
