#![allow(dead_code)]

#[path = "engine.rs"]
mod engine;
#[path = "protocol.rs"]
mod protocol;
#[path = "workloads.rs"]
mod workloads;

use protocol::Durability;

#[test]
fn recovery_logical_bytes_include_every_crash_marker() {
    let config = protocol::Profile::Quick.defaults();
    assert_eq!(
        workloads::logical_live_bytes(workloads::Workload::Recovery, &config),
        config.keys * (protocol::KEY_BYTES + config.value_bytes) as u64
            + u64::from(config.recovery_cycles)
                * (protocol::KEY_BYTES + b"acknowledged-before-exit".len()) as u64
    );
}

#[tokio::test]
async fn redb_adapter_recovers_scans_settles_and_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = engine::Database::open(
        engine::EngineName::Redb,
        Durability::Durable,
        directory.path(),
    )
    .await
    .unwrap();

    database.put(b"alpha", b"one").await.unwrap();
    database.put(b"beta", b"two").await.unwrap();
    assert_eq!(database.get(b"alpha").await.unwrap(), Some(b"one".to_vec()));
    assert_eq!(database.scan_all().await.unwrap().0, 2);

    database.drop_without_settlement();
    database.reopen().await.unwrap();
    assert_eq!(database.get(b"beta").await.unwrap(), Some(b"two".to_vec()));
    database.flush().await.unwrap();
    database.compact().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn every_adapter_commits_atomic_insert_batches() {
    let keys = vec![
        b"batch-a".to_vec(),
        b"batch-b".to_vec(),
        b"batch-c".to_vec(),
    ];
    for engine in engine::EngineName::ALL {
        let directory = tempfile::tempdir().unwrap();
        let database = engine::Database::open(engine, Durability::Durable, directory.path())
            .await
            .unwrap();
        database.put_batch(&keys, b"value").await.unwrap();
        for key in &keys {
            assert_eq!(
                database.get(key).await.unwrap(),
                Some(b"value".to_vec()),
                "{} lost a committed batch key",
                engine.as_str()
            );
        }
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn turbo_paranoid_drop_hands_directory_to_the_next_opener() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = engine::Database::open(
        engine::EngineName::TurboKv,
        Durability::Paranoid,
        directory.path(),
    )
    .await
    .unwrap();

    database.put(b"alpha", b"one").await.unwrap();
    let opener = engine::Database::open_after_handoff(
        engine::EngineName::TurboKv,
        Durability::Paranoid,
        directory.path(),
    );
    let release = async {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        database.drop_without_settlement();
    };
    let (reopened, ()) = tokio::join!(opener, release);
    let reopened = reopened.unwrap();
    assert_eq!(reopened.get(b"alpha").await.unwrap(), Some(b"one".to_vec()));
    reopened.close().await.unwrap();
}
