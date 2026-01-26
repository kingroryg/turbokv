#![allow(clippy::all, clippy::pedantic)]
//! Multithreaded benchmark to test buffer registry contention
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use turbokv::{Db, DbOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;

    let thread_counts = vec![1, 2, 4];
    let ops_per_thread = 250_000;

    println!("Multithreaded Write Benchmark");
    println!("==============================");
    println!("Ops per thread: {}", ops_per_thread);
    println!("Note: TurboKV scales well up to 4 concurrent writers\n");

    for threads in thread_counts {
        let db = Arc::new(
            Db::open_with_options(
                temp_dir.path().join(format!("db_{}", threads)),
                DbOptions::fast(),
            )
            .await?,
        );

        let start = Instant::now();
        let mut handles = JoinSet::new();

        for t in 0..threads {
            let db = Arc::clone(&db);
            handles.spawn(async move {
                for i in 0..ops_per_thread {
                    let key = format!("t{}:key:{:08}", t, i);
                    db.insert(key.as_bytes(), b"value").await.unwrap();
                }
            });
        }

        while handles.join_next().await.is_some() {}
        db.flush().await?;

        let elapsed = start.elapsed();
        let total_ops = threads * ops_per_thread;
        let ops_sec = total_ops as f64 / elapsed.as_secs_f64();

        println!(
            "{:2} threads: {:>8.0} ops/sec  ({:.2}s for {}K ops)",
            threads,
            ops_sec,
            elapsed.as_secs_f64(),
            total_ops / 1000
        );
    }

    Ok(())
}
