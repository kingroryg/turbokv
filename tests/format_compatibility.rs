use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use turbokv::storage::manifest::{
    Manifest, SSTableManifestEntry, MANIFEST_VERSION, MANIFEST_VERSION_V1, MANIFEST_VERSION_V2,
};
use turbokv::storage::sstable::{
    CompressionType, SSTableConfig, SSTableReader, SSTableWriter, SSTABLE_VERSION,
    SSTABLE_VERSION_V1, SSTABLE_VERSION_V2,
};
use turbokv::storage::wal::{
    WalConfig, WriteAheadLog, WAL_VERSION, WAL_VERSION_V1, WAL_VERSION_V2, WAL_VERSION_V3,
    WAL_VERSION_V4,
};
use turbokv::{Db, DbOptions};

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/storage_formats"
);
const WAL_NAME: &str = "00000000000000000000.wal";
const BINARY_KEY: &[u8] = b"\x00k\xff";
const DELETED_KEY: &[u8] = b"\xffdeleted\x00";

fn fixture(name: &str) -> PathBuf {
    Path::new(FIXTURES).join(name)
}

fn copy_fixture(name: &str, destination: &Path) {
    fs::copy(fixture(name), destination).unwrap();
}

fn snapshot(directory: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let mut entries = fs::read_dir(directory)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        entries.sort_by_key(fs::DirEntry::path);
        for entry in entries {
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(directory, directory, &mut files);
    files
}

fn fixture_directory_crc(manifest_name: &str, wal_name: &str, sstable_name: &str) -> u32 {
    let files = [
        ("MANIFEST", fs::read(fixture(manifest_name)).unwrap()),
        (
            "sstables/L0/0000000001.sst",
            fs::read(fixture(sstable_name)).unwrap(),
        ),
        (
            "wal/00000000000000000000.wal",
            fs::read(fixture(wal_name)).unwrap(),
        ),
    ];
    let mut hasher = crc32fast::Hasher::new();
    for (path, bytes) in files {
        hasher.update(path.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    hasher.finalize()
}

fn assemble_release_database(manifest_name: &str, wal_name: &str, sstable_name: &str) -> TempDir {
    let directory = assemble_wal_database(manifest_name, wal_name);
    let table_directory = directory.path().join("sstables/L0");
    fs::create_dir_all(&table_directory).unwrap();
    copy_fixture(sstable_name, &table_directory.join("0000000001.sst"));
    directory
}

fn assemble_format_database(
    manifest_version: u32,
    zero_manifest_checksum: bool,
    wal_name: &str,
    sstable_name: &str,
    sstable_version: u32,
) -> TempDir {
    let directory = TempDir::new().unwrap();
    let wal_directory = directory.path().join("wal");
    let table_directory = directory.path().join("sstables/L0");
    fs::create_dir_all(&wal_directory).unwrap();
    fs::create_dir_all(&table_directory).unwrap();
    copy_fixture(wal_name, &wal_directory.join(WAL_NAME));
    let table = table_directory.join("0000000001.sst");
    copy_fixture(sstable_name, &table);

    fn push_u32(bytes: &mut Vec<u8>, value: u32) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn push_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }

    let relative_path = b"sstables/L0/0000000001.sst";
    let mut manifest = b"HNSHMNFT".to_vec();
    push_u32(&mut manifest, manifest_version);
    push_u64(&mut manifest, 2);
    if manifest_version == MANIFEST_VERSION_V1 {
        push_u32(&mut manifest, 0);
    }
    push_u32(&mut manifest, 1);
    push_u64(&mut manifest, 1);
    push_u32(&mut manifest, 0);
    push_u32(&mut manifest, relative_path.len() as u32);
    manifest.extend_from_slice(relative_path);
    push_u64(&mut manifest, table.metadata().unwrap().len());
    push_u64(&mut manifest, 2);
    if manifest_version == MANIFEST_VERSION {
        push_u64(
            &mut manifest,
            u64::from(sstable_version != SSTABLE_VERSION_V1),
        );
    }
    for key in [BINARY_KEY, &b"\xffk\x00"[..]] {
        push_u32(&mut manifest, key.len() as u32);
        manifest.extend_from_slice(key);
    }
    push_u64(&mut manifest, 41);
    push_u64(&mut manifest, 42);
    push_u64(&mut manifest, 1_700_000_000);
    let checksum = if zero_manifest_checksum {
        0
    } else {
        crc32fast::hash(&manifest)
    };
    push_u32(&mut manifest, checksum);
    fs::write(directory.path().join("MANIFEST"), manifest).unwrap();
    directory
}

fn assemble_wal_database(manifest_name: &str, wal_name: &str) -> TempDir {
    let directory = TempDir::new().unwrap();
    let wal_directory = directory.path().join("wal");
    fs::create_dir(&wal_directory).unwrap();
    copy_fixture(manifest_name, &directory.path().join("MANIFEST"));
    copy_fixture(wal_name, &wal_directory.join(WAL_NAME));
    directory
}

fn create_manifest_for_sstable(database: &Path, table: &Path) {
    let size = table.metadata().unwrap().len();
    let mut manifest = Manifest::new();
    manifest.add_sstable(SSTableManifestEntry {
        id: 1,
        level: 0,
        path: table.to_path_buf(),
        size,
        entry_count: 2,
        tombstone_count: 1,
        min_key: BINARY_KEY.to_vec(),
        max_key: b"\xffk\x00".to_vec(),
        min_sequence: 41,
        max_sequence: 42,
        creation_time: 1_700_000_000,
    });
    manifest.save(database).unwrap();
}

#[test]
fn stable_format_identifiers_do_not_drift() {
    assert_eq!(
        (MANIFEST_VERSION_V1, MANIFEST_VERSION_V2, MANIFEST_VERSION),
        (1, 2, 3)
    );
    assert_eq!(
        (
            WAL_VERSION_V1,
            WAL_VERSION_V2,
            WAL_VERSION_V3,
            WAL_VERSION_V4,
            WAL_VERSION,
        ),
        (1, 2, 3, 4, 5)
    );
    assert_eq!(
        (SSTABLE_VERSION_V1, SSTABLE_VERSION_V2, SSTABLE_VERSION),
        (1, 2, 3)
    );
}

#[test]
fn released_fixture_identities_and_directory_hashes_are_fixed() {
    assert_eq!(
        fixture_directory_crc(
            "release_v0_2_x_manifest.bin",
            "wal_v3.wal",
            "sst_v1_release_zero_crc.sst",
        ),
        0xe278_b2f1,
        "v0.2.0/v0.2.1 fixture identity changed"
    );
    assert_eq!(
        fixture_directory_crc("release_v0_5_0_manifest.bin", "wal_v3.wal", "sst_v2.sst",),
        0x8962_b4d0,
        "v0.5.0 fixture identity changed"
    );
}

#[tokio::test]
async fn released_databases_read_ranges_upgrade_reopen_and_stabilize() {
    for (release, manifest_name, sstable_name, second_value) in [
        (
            "v0.2.0/v0.2.1",
            "release_v0_2_x_manifest.bin",
            "sst_v1_release_zero_crc.sst",
            Some(&b""[..]),
        ),
        ("v0.5.0", "release_v0_5_0_manifest.bin", "sst_v2.sst", None),
    ] {
        let directory = assemble_release_database(manifest_name, "wal_v3.wal", sstable_name);
        let db = Db::open_with_options(directory.path(), DbOptions::durable())
            .await
            .unwrap();
        let binary = db.get(BINARY_KEY).await.unwrap().unwrap();
        assert!(binary.contains(&0x00), "{release}");
        assert!(binary.contains(&0xff), "{release}");
        assert_eq!(
            db.get(b"\xffk\x00").await.unwrap().as_deref(),
            second_value,
            "{release}"
        );
        let ranged = db.range(&b"\x00"[..], &b"\xff\xff"[..]).await.unwrap();
        assert!(ranged.iter().any(|(key, _)| key == BINARY_KEY), "{release}");
        assert_eq!(
            ranged.iter().any(|(key, _)| key == b"\xffk\x00"),
            second_value.is_some(),
            "{release}"
        );
        db.close().await.unwrap();

        let manifest_bytes = fs::read(directory.path().join("MANIFEST")).unwrap();
        assert_eq!(
            u32::from_le_bytes(manifest_bytes[8..12].try_into().unwrap()),
            MANIFEST_VERSION,
            "{release}"
        );
        let upgraded = snapshot(directory.path());
        let reopened = Db::open_with_options(directory.path(), DbOptions::durable())
            .await
            .unwrap();
        assert!(
            reopened.get(BINARY_KEY).await.unwrap().is_some(),
            "{release}"
        );
        assert_eq!(
            reopened.get(b"\xffk\x00").await.unwrap().as_deref(),
            second_value,
            "{release}"
        );
        reopened.close().await.unwrap();
        assert_eq!(snapshot(directory.path()), upgraded, "{release}");
    }
}

#[test]
fn every_manifest_fixture_loads_through_the_public_api() {
    for (name, version) in [
        ("manifest_v1_release_zero_crc.bin", 1),
        ("manifest_v1_crc.bin", 1),
        ("manifest_v2.bin", 2),
        ("manifest_v3.bin", 3),
    ] {
        let manifest = Manifest::load(&fixture(name)).unwrap();
        assert_eq!(manifest.loaded_format_version, version, "{name}");
        assert_eq!(manifest.wal_checkpoint, 0, "{name}");
        assert!(manifest.sstables.is_empty(), "{name}");
    }
}

#[test]
fn zero_checksum_compatibility_is_confined_to_released_v1_spellings() {
    let manifest_directory = TempDir::new().unwrap();
    let mut current_manifest = fs::read(fixture("manifest_v3.bin")).unwrap();
    let checksum_offset = current_manifest.len() - 4;
    current_manifest[checksum_offset..].fill(0);
    let manifest_path = manifest_directory.path().join("MANIFEST");
    fs::write(&manifest_path, current_manifest).unwrap();
    assert!(Manifest::load(&manifest_path)
        .unwrap_err()
        .to_string()
        .contains("Manifest checksum mismatch"));

    let sstable_directory = TempDir::new().unwrap();
    let mut current_sstable = fs::read(fixture("sst_v3.sst")).unwrap();
    let checksum_offset = current_sstable.len() - 4;
    current_sstable[checksum_offset..].fill(0);
    let sstable_path = sstable_directory.path().join("table.sst");
    fs::write(&sstable_path, current_sstable).unwrap();
    assert!(SSTableReader::open(&sstable_path)
        .err()
        .expect("zero current footer checksum must fail")
        .to_string()
        .contains("Footer checksum mismatch"));
}

#[test]
fn truncated_fixed_headers_have_component_specific_errors() {
    let directory = TempDir::new().unwrap();
    let manifest_path = directory.path().join("MANIFEST");
    fs::write(&manifest_path, &b"HNSHMNFT\x03\x00\x00"[..]).unwrap();
    assert_eq!(
        Manifest::load(&manifest_path).unwrap_err().to_string(),
        "Internal error: Manifest corruption: file is shorter than its header"
    );

    let sstable_path = directory.path().join("table.sst");
    fs::write(&sstable_path, vec![0_u8; 39]).unwrap();
    let error = SSTableReader::open(&sstable_path)
        .err()
        .expect("truncated SSTable must fail")
        .to_string();
    assert!(error.contains(&sstable_path.display().to_string()));
    assert!(error.contains("[open]"));
    assert!(error.contains("file is shorter than its footer"));
}

#[test]
fn every_sstable_fixture_reads_binary_empty_boundary_and_tombstone_records() {
    for (name, version) in [
        ("sst_v1_release_zero_crc.sst", 1),
        ("sst_v1_crc.sst", 1),
        ("sst_v2.sst", 2),
        ("sst_v3.sst", 3),
    ] {
        let fixture_bytes = fs::read(fixture(name)).unwrap();
        assert_eq!(fixture_bytes[64], 0, "{name}: block footer compression");

        let reader = SSTableReader::open(fixture(name)).unwrap();
        let binary = reader.get(BINARY_KEY).unwrap().unwrap();
        assert_eq!(binary.len(), [0, 45, 44, 36][version as usize], "{name}");
        assert!(binary.contains(&0x00), "{name}");
        assert!(binary.contains(&0xff), "{name}");

        let records = reader.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2, "{name}");
        assert_eq!(records[0].0.as_ref(), BINARY_KEY, "{name}");
        if version == 1 {
            assert_eq!(records[1].1.as_deref(), Some(&[][..]), "{name}");
        } else {
            assert!(records[1].1.is_none(), "{name}");
        }
    }
}

#[tokio::test]
async fn every_wal_fixture_reads_through_the_public_api() {
    for (name, expected_size, expected_entries) in [
        ("wal_v1.wal", 512, 2),
        ("wal_v2.wal", 512, 2),
        ("wal_v3.wal", 256, 2),
        ("wal_v4.wal", 269, 3),
        ("wal_v5.wal", 269, 3),
    ] {
        let original = fs::read(fixture(name)).unwrap();
        assert_eq!(original.len(), expected_size, "{name}");
        if name == "wal_v5.wal" {
            assert_eq!(crc32fast::hash(&original), 0x5d93_d7a6);
        }
        let directory = TempDir::new().unwrap();
        copy_fixture(name, &directory.path().join(WAL_NAME));

        let wal = WriteAheadLog::new(directory.path(), WalConfig::durable())
            .await
            .unwrap();
        let entries = wal.read_from(0).await.unwrap();
        assert_eq!(entries.len(), expected_entries, "{name}");
        assert_eq!(entries[0].decode_key(), Some(BINARY_KEY), "{name}");
        assert!(entries[0].decode_value().unwrap().contains(&0xff), "{name}");
        assert_eq!(entries[1].decode_key(), Some(DELETED_KEY), "{name}");
        assert!(entries[1].decode_value().is_none(), "{name}");
        if matches!(name, "wal_v4.wal" | "wal_v5.wal") {
            assert_eq!(entries[2].decode_key(), Some(&b"empty\x00"[..]));
            assert_eq!(entries[2].decode_value(), Some(&[][..]));
        }
        assert_eq!(fs::read(fixture(name)).unwrap(), original, "{name}");
    }
}

#[test]
fn configured_max_block_boundary_round_trips_through_public_apis() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("boundary.sst");
    let mut writer = SSTableWriter::new(
        &path,
        SSTableConfig {
            block_size: 64,
            compression: CompressionType::None,
            ..SSTableConfig::default()
        },
    )
    .unwrap();
    let mut boundary_value = (0..36).map(|index| (index * 13) as u8).collect::<Vec<_>>();
    boundary_value[0] = 0;
    boundary_value[35] = 0xff;
    writer.add(BINARY_KEY, Some(&boundary_value)).unwrap();
    writer.add(b"\xffk\x00", Some(b"neighbor")).unwrap();
    writer.finish().unwrap();

    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes[64], 0, "the exact 64-byte block is uncompressed");
    let footer_offset = bytes.len() - 40;
    let index_offset =
        u64::from_le_bytes(bytes[footer_offset..footer_offset + 8].try_into().unwrap()) as usize;
    let first_block_size_offset = index_offset + 4 + BINARY_KEY.len() + 8;
    assert_eq!(
        u32::from_le_bytes(
            bytes[first_block_size_offset..first_block_size_offset + 4]
                .try_into()
                .unwrap()
        ),
        69,
        "64 data bytes plus compression marker and CRC"
    );
    let reader = SSTableReader::open(&path).unwrap();
    assert_eq!(
        reader.get(BINARY_KEY).unwrap().unwrap().as_ref(),
        boundary_value
    );
}

#[tokio::test]
async fn configured_max_segment_boundary_rotates_after_current_fixture() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join(WAL_NAME);
    copy_fixture("wal_v5.wal", &path);
    let original = fs::read(&path).unwrap();
    let wal = WriteAheadLog::new(
        directory.path(),
        WalConfig {
            max_file_size: original.len() as u64,
            ..WalConfig::durable()
        },
    )
    .await
    .unwrap();
    wal.append(b"boundary\x00", b"after\xff").await.unwrap();
    wal.flush().await.unwrap();

    assert_eq!(fs::read(&path).unwrap(), original);
    let wal_files = fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
        .count();
    assert_eq!(wal_files, 2, "append beyond the exact maximum must rotate");
}

#[tokio::test]
async fn every_manifest_wal_sstable_combination_upgrades_reopens_and_is_idempotent() {
    let manifests = [
        ("manifest-v1-zero", MANIFEST_VERSION_V1, true),
        ("manifest-v1-crc", MANIFEST_VERSION_V1, false),
        ("manifest-v2", MANIFEST_VERSION_V2, false),
        ("manifest-v3", MANIFEST_VERSION, false),
    ];
    let wals = [
        "wal_v1.wal",
        "wal_v2.wal",
        "wal_v3.wal",
        "wal_v4.wal",
        "wal_v5.wal",
    ];
    let sstables = [
        ("sst_v1_release_zero_crc.sst", SSTABLE_VERSION_V1),
        ("sst_v1_crc.sst", SSTABLE_VERSION_V1),
        ("sst_v2.sst", SSTABLE_VERSION_V2),
        ("sst_v3.sst", SSTABLE_VERSION),
    ];

    for (manifest_name, manifest_version, zero_manifest_checksum) in manifests {
        for wal_name in wals {
            for (sstable_name, sstable_version) in sstables {
                let case = format!("{manifest_name}/{wal_name}/{sstable_name}");
                let directory = assemble_format_database(
                    manifest_version,
                    zero_manifest_checksum,
                    wal_name,
                    sstable_name,
                    sstable_version,
                );
                let db = Db::open_with_options(directory.path(), DbOptions::durable())
                    .await
                    .unwrap();
                assert!(db.get(BINARY_KEY).await.unwrap().is_some(), "{case}");
                assert_eq!(db.get(DELETED_KEY).await.unwrap(), None, "{case}");
                let second = db.get(b"\xffk\x00").await.unwrap();
                assert_eq!(
                    second.as_deref(),
                    (sstable_version == SSTABLE_VERSION_V1).then_some(&b""[..]),
                    "{case}"
                );
                let visible = db.scan_prefix(b"").await.unwrap();
                assert!(visible.iter().any(|(key, _)| key == BINARY_KEY), "{case}");
                let ranged = db.range(&b"\x00"[..], &b"\xff\xff"[..]).await.unwrap();
                assert!(ranged.iter().any(|(key, _)| key == BINARY_KEY), "{case}");
                db.close().await.unwrap();

                let manifest_bytes = fs::read(directory.path().join("MANIFEST")).unwrap();
                assert_eq!(
                    u32::from_le_bytes(manifest_bytes[8..12].try_into().unwrap()),
                    MANIFEST_VERSION,
                    "{case}"
                );
                let upgraded = snapshot(directory.path());

                let reopened = Db::open_with_options(directory.path(), DbOptions::durable())
                    .await
                    .unwrap();
                assert!(reopened.get(BINARY_KEY).await.unwrap().is_some(), "{case}");
                assert_eq!(
                    reopened.get(b"\xffk\x00").await.unwrap().as_deref(),
                    (sstable_version == SSTABLE_VERSION_V1).then_some(&b""[..]),
                    "{case}"
                );
                let ranged = reopened
                    .range(&b"\x00"[..], &b"\xff\xff"[..])
                    .await
                    .unwrap();
                assert!(ranged.iter().any(|(key, _)| key == BINARY_KEY), "{case}");
                reopened.close().await.unwrap();
                assert_eq!(snapshot(directory.path()), upgraded, "{case}");
            }
        }
    }
}

#[tokio::test]
async fn unsupported_manifest_fails_repeatably_without_mutating_existing_bytes() {
    let directory = TempDir::new().unwrap();
    let mut bytes = fs::read(fixture("manifest_v3.bin")).unwrap();
    bytes[8..12].copy_from_slice(&(MANIFEST_VERSION + 1).to_le_bytes());
    fs::write(directory.path().join("MANIFEST"), bytes).unwrap();
    fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
    let expected = snapshot(directory.path());

    for _ in 0..5 {
        let error = Db::open_with_options(directory.path(), DbOptions::durable())
            .await
            .err()
            .expect("future manifest must fail");
        assert!(error
            .to_string()
            .contains("Unsupported manifest version: 4"));
        assert_eq!(snapshot(directory.path()), expected);
    }
}

#[tokio::test]
async fn an_absent_lock_file_is_the_only_unsupported_open_side_effect() {
    let directory = TempDir::new().unwrap();
    let mut bytes = fs::read(fixture("manifest_v3.bin")).unwrap();
    bytes[8..12].copy_from_slice(&(MANIFEST_VERSION + 1).to_le_bytes());
    fs::write(directory.path().join("MANIFEST"), bytes.clone()).unwrap();

    let error = Db::open_with_options(directory.path(), DbOptions::durable())
        .await
        .err()
        .expect("future manifest must fail");
    assert!(error
        .to_string()
        .contains("Unsupported manifest version: 4"));
    assert_eq!(fs::read(directory.path().join("MANIFEST")).unwrap(), bytes);
    assert_eq!(
        fs::read(directory.path().join(".turbokv.lock")).unwrap(),
        b""
    );
    assert_eq!(snapshot(directory.path()).len(), 2);
}

#[tokio::test]
async fn unsupported_wal_fails_before_header_repair_or_directory_creation() {
    let directory = assemble_wal_database("manifest_v3.bin", "wal_v5.wal");
    let path = directory.path().join("wal").join(WAL_NAME);
    let mut bytes = fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&(WAL_VERSION + 1).to_le_bytes());
    fs::write(&path, bytes).unwrap();
    fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
    let expected = snapshot(directory.path());

    let error = Db::open_with_options(directory.path(), DbOptions::fast())
        .await
        .err()
        .expect("future WAL must fail even when WAL writes are disabled");
    assert!(error.to_string().contains("Unsupported WAL version: 6"));
    assert_eq!(snapshot(directory.path()), expected);
    assert!(!directory.path().join("sstables").exists());
}

#[tokio::test]
async fn unsupported_sstable_fails_before_cleanup_manifest_or_wal_mutation() {
    let directory = TempDir::new().unwrap();
    let table_directory = directory.path().join("sstables/L0");
    fs::create_dir_all(&table_directory).unwrap();
    let table = table_directory.join("future.sst");
    copy_fixture("sst_v3.sst", &table);
    let mut bytes = fs::read(&table).unwrap();
    let version_offset = bytes.len() - 8;
    bytes[version_offset..version_offset + 4].copy_from_slice(&(SSTABLE_VERSION + 1).to_le_bytes());
    fs::write(&table, bytes).unwrap();
    create_manifest_for_sstable(directory.path(), &table);
    fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
    let expected = snapshot(directory.path());

    let error = Db::open_with_options(directory.path(), DbOptions::durable())
        .await
        .err()
        .expect("future SSTable must fail");
    assert!(error.to_string().contains("Unsupported SSTable version: 4"));
    assert_eq!(snapshot(directory.path()), expected);
    assert!(!directory.path().join("wal").exists());
}

#[tokio::test]
async fn corrupt_or_future_orphan_sstables_fail_before_cleanup_mutates_bytes() {
    for kind in ["corrupt", "future"] {
        let directory = TempDir::new().unwrap();
        let table_directory = directory.path().join("sstables/L0");
        fs::create_dir_all(&table_directory).unwrap();
        let table = table_directory.join(format!("{kind}.sst"));
        let mut bytes = fs::read(fixture("sst_v3.sst")).unwrap();
        if kind == "future" {
            let version_offset = bytes.len() - 8;
            bytes[version_offset..version_offset + 4]
                .copy_from_slice(&(SSTABLE_VERSION + 1).to_le_bytes());
        } else {
            bytes[0] ^= 0x80;
        }
        fs::write(&table, bytes).unwrap();
        fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
        let expected = snapshot(directory.path());

        for _ in 0..2 {
            let error = Db::open_with_options(directory.path(), DbOptions::fast())
                .await
                .err()
                .expect("invalid orphan must fail rather than be deleted");
            let message = error.to_string();
            if kind == "future" {
                assert!(message.contains("Unsupported SSTable version: 4"));
            } else {
                assert!(message.contains("Block CRC mismatch"));
            }
            assert_eq!(snapshot(directory.path()), expected, "{kind}");
        }
    }
}

#[tokio::test]
async fn manifest_sstable_paths_cannot_escape_the_locked_database() {
    let external_directory = TempDir::new().unwrap();
    let external_table = external_directory.path().join("external.sst");
    copy_fixture("sst_v3.sst", &external_table);
    let external_bytes = fs::read(&external_table).unwrap();

    let traversal_directory = TempDir::new().unwrap();
    let mut traversal_manifest = fs::read(fixture("release_v0_2_x_manifest.bin")).unwrap();
    let original = b"sstables/L0/0000000001.sst";
    let traversal = b"../out/000000000000000.sst";
    assert_eq!(original.len(), traversal.len());
    let path_offset = traversal_manifest
        .windows(original.len())
        .position(|window| window == original)
        .unwrap();
    traversal_manifest[path_offset..path_offset + original.len()].copy_from_slice(traversal);
    fs::write(
        traversal_directory.path().join("MANIFEST"),
        traversal_manifest,
    )
    .unwrap();
    fs::write(
        traversal_directory.path().join(".turbokv.lock"),
        b"lock-sentinel",
    )
    .unwrap();
    let expected = snapshot(traversal_directory.path());
    let error = Db::open_with_options(traversal_directory.path(), DbOptions::fast())
        .await
        .err()
        .expect("parent traversal must fail");
    assert!(error.to_string().contains("contains parent traversal"));
    assert_eq!(snapshot(traversal_directory.path()), expected);
    assert_eq!(fs::read(&external_table).unwrap(), external_bytes);

    let absolute_directory = TempDir::new().unwrap();
    create_manifest_for_sstable(absolute_directory.path(), &external_table);
    fs::write(
        absolute_directory.path().join(".turbokv.lock"),
        b"lock-sentinel",
    )
    .unwrap();
    let expected = snapshot(absolute_directory.path());
    let error = Db::open_with_options(absolute_directory.path(), DbOptions::fast())
        .await
        .err()
        .expect("absolute escape must fail");
    assert!(error.to_string().contains("escapes database directory"));
    assert_eq!(snapshot(absolute_directory.path()), expected);
    assert_eq!(fs::read(&external_table).unwrap(), external_bytes);

    #[cfg(unix)]
    {
        let symlink_directory = TempDir::new().unwrap();
        let table_directory = symlink_directory.path().join("sstables/L0");
        fs::create_dir_all(&table_directory).unwrap();
        let link = table_directory.join("linked.sst");
        std::os::unix::fs::symlink(&external_table, &link).unwrap();
        create_manifest_for_sstable(symlink_directory.path(), &link);
        fs::write(
            symlink_directory.path().join(".turbokv.lock"),
            b"lock-sentinel",
        )
        .unwrap();
        let expected = snapshot(symlink_directory.path());
        let error = Db::open_with_options(symlink_directory.path(), DbOptions::fast())
            .await
            .err()
            .expect("symlink escape must fail");
        assert!(error.to_string().contains("escapes database directory"));
        assert_eq!(snapshot(symlink_directory.path()), expected);
        assert_eq!(fs::read(&external_table).unwrap(), external_bytes);
    }
}

#[tokio::test]
async fn every_sstable_component_corruption_is_deterministic_and_nonmutating() {
    for component in ["data", "index", "bloom", "footer"] {
        let directory = TempDir::new().unwrap();
        let table_directory = directory.path().join("sstables/L0");
        fs::create_dir_all(&table_directory).unwrap();
        let table = table_directory.join("corrupt.sst");
        let mut bytes = fs::read(fixture("sst_v3.sst")).unwrap();
        let footer_offset = bytes.len() - 40;
        let index_offset =
            u64::from_le_bytes(bytes[footer_offset..footer_offset + 8].try_into().unwrap())
                as usize;
        let bloom_offset = u64::from_le_bytes(
            bytes[footer_offset + 12..footer_offset + 20]
                .try_into()
                .unwrap(),
        ) as usize;
        match component {
            "data" => bytes[0] ^= 0x80,
            "index" => bytes[index_offset + 4] ^= 0x01,
            "bloom" => bytes[bloom_offset..bloom_offset + 8].fill(0),
            "footer" => *bytes.last_mut().unwrap() ^= 0x80,
            _ => unreachable!(),
        }
        fs::write(&table, bytes).unwrap();
        create_manifest_for_sstable(directory.path(), &table);
        fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
        let expected = snapshot(directory.path());
        let relative_table = Path::new("sstables").join("L0").join("corrupt.sst");
        let mut first_message = None;

        for _ in 0..2 {
            let message = Db::open_with_options(directory.path(), DbOptions::durable())
                .await
                .err()
                .expect("component corruption must fail")
                .to_string();
            let expected_fragment = match component {
                "data" => "Block CRC mismatch",
                "index" => "index key does not match",
                "bloom" => "bloom filter excludes",
                "footer" => "Footer checksum mismatch",
                _ => unreachable!(),
            };
            assert!(
                message.contains(expected_fragment),
                "{component}: {message}"
            );
            assert!(
                message.contains(&relative_table.display().to_string()),
                "{component}: affected file missing from {message}"
            );
            if let Some(first_message) = &first_message {
                assert_eq!(&message, first_message, "{component}");
            } else {
                first_message = Some(message);
            }
            assert_eq!(snapshot(directory.path()), expected, "{component}");
        }
    }
}

#[tokio::test]
async fn interior_wal_corruption_is_precise_repeatable_and_nonmutating() {
    let directory = assemble_wal_database("manifest_v3.bin", "wal_v3.wal");
    let path = directory.path().join("wal").join(WAL_NAME);
    let mut bytes = fs::read(&path).unwrap();
    bytes[64 + 22] ^= 0x80;
    fs::write(&path, bytes).unwrap();
    fs::write(directory.path().join(".turbokv.lock"), b"lock-sentinel").unwrap();
    let expected = snapshot(directory.path());

    for _ in 0..5 {
        let error = Db::open_with_options(directory.path(), DbOptions::durable())
            .await
            .err()
            .expect("interior WAL corruption must fail");
        let message = error.to_string();
        assert!(message.contains("CRC mismatch: data corrupted at byte 64"));
        assert_eq!(snapshot(directory.path()), expected);
    }
}

#[tokio::test]
async fn active_wal_physical_tail_is_recovered_only_after_read_only_classification() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join(WAL_NAME);
    copy_fixture("wal_v5.wal", &path);
    let valid_bytes = fs::read(&path).unwrap();
    let mut damaged = valid_bytes.clone();
    damaged.extend_from_slice(&[0xff, 0x00, 0x7f]);
    fs::write(&path, damaged).unwrap();

    let wal = WriteAheadLog::new(directory.path(), WalConfig::durable())
        .await
        .unwrap();
    let mapped_bytes = fs::read(&path).unwrap();
    assert_eq!(&mapped_bytes[..valid_bytes.len()], valid_bytes);
    assert_eq!(wal.current_size(), valid_bytes.len() as u64);
    assert_eq!(wal.read_from(0).await.unwrap().len(), 3);
    wal.flush().await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), valid_bytes);
}

#[tokio::test]
async fn truncated_wal_header_is_rejected_without_becoming_tail_recovery() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join(WAL_NAME);
    fs::write(&path, vec![0_u8; 63]).unwrap();
    let expected = fs::read(&path).unwrap();
    let error = WriteAheadLog::new(directory.path(), WalConfig::durable())
        .await
        .err()
        .expect("truncated WAL header must fail");
    assert_eq!(
        error.to_string(),
        "Invalid WAL format: WAL header is truncated: expected 64 bytes"
    );
    assert_eq!(fs::read(path).unwrap(), expected);
}

#[tokio::test]
async fn manifest_and_sstable_corruption_have_stable_errors_and_do_not_mutate() {
    let manifest_directory = TempDir::new().unwrap();
    let mut manifest_bytes = fs::read(fixture("manifest_v3.bin")).unwrap();
    *manifest_bytes.last_mut().unwrap() ^= 0x80;
    fs::write(manifest_directory.path().join("MANIFEST"), manifest_bytes).unwrap();
    fs::write(
        manifest_directory.path().join(".turbokv.lock"),
        b"lock-sentinel",
    )
    .unwrap();
    let expected = snapshot(manifest_directory.path());
    let error = Db::open_with_options(manifest_directory.path(), DbOptions::durable())
        .await
        .err()
        .expect("bad manifest checksum must fail");
    assert!(error.to_string().contains("Manifest checksum mismatch"));
    assert_eq!(snapshot(manifest_directory.path()), expected);

    let sstable_directory = TempDir::new().unwrap();
    let table_directory = sstable_directory.path().join("sstables/L0");
    fs::create_dir_all(&table_directory).unwrap();
    let table = table_directory.join("corrupt.sst");
    copy_fixture("sst_v3.sst", &table);
    let mut table_bytes = fs::read(&table).unwrap();
    table_bytes[0] ^= 0x80;
    fs::write(&table, table_bytes).unwrap();
    create_manifest_for_sstable(sstable_directory.path(), &table);
    fs::write(
        sstable_directory.path().join(".turbokv.lock"),
        b"lock-sentinel",
    )
    .unwrap();
    let expected = snapshot(sstable_directory.path());
    let error = Db::open_with_options(sstable_directory.path(), DbOptions::durable())
        .await
        .err()
        .expect("bad SSTable block checksum must fail");
    assert!(error.to_string().contains("Block CRC mismatch"));
    assert_eq!(snapshot(sstable_directory.path()), expected);
}
