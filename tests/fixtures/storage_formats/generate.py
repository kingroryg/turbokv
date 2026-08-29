#!/usr/bin/env python3
"""Generate or verify TurboKV's immutable storage-format fixtures."""

from __future__ import annotations

import argparse
import binascii
import hashlib
import struct
from pathlib import Path


ROOT = Path(__file__).resolve().parent
MANIFEST_MAGIC = b"HNSHMNFT"
WAL_MAGIC = b"TURBOKV\0"
SST_MAGIC = b"HANSHIRO"
CURSOR_CRC64_POLYNOMIAL = 0x42F0E1EBA9EA3693
V5_COMMIT_TAG = b"\xA5\x5A"


def u8(value: int) -> bytes:
    return struct.pack("<B", value)


def u32(value: int) -> bytes:
    return struct.pack("<I", value)


def u64(value: int) -> bytes:
    return struct.pack("<Q", value)


def crc32(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF


def cursor_crc64(version: int, segment_sequence: int, acknowledged_end: int) -> int:
    crc = 0xFFFFFFFFFFFFFFFF
    for byte in u32(version) + u64(segment_sequence) + u64(acknowledged_end):
        crc ^= byte << 56
        for _ in range(8):
            crc = (
                ((crc << 1) ^ CURSOR_CRC64_POLYNOMIAL)
                if crc & (1 << 63)
                else (crc << 1)
            ) & 0xFFFFFFFFFFFFFFFF
    return crc ^ 0xFFFFFFFFFFFFFFFF


def manifest_entry(version: int, path: bytes, table_size: int) -> bytes:
    encoded = u64(1) + u32(0) + u32(len(path)) + path
    encoded += u64(table_size) + u64(2)
    if version == 3:
        encoded += u64(1)
    encoded += u32(3) + b"\x00k\xff" + u32(3) + b"\xffk\x00"
    encoded += u64(41) + u64(42) + u64(1_700_000_000)
    return encoded


def manifest(
    version: int,
    legacy_zero_checksum: bool = False,
    checkpoint: int = 0,
    entries: tuple[bytes, ...] = (),
) -> bytes:
    payload = MANIFEST_MAGIC + u32(version) + u64(checkpoint)
    if version == 1:
        payload += u32(0)
    payload += u32(len(entries)) + b"".join(entries)
    checksum = 0 if legacy_zero_checksum else crc32(payload)
    return payload + u32(checksum)


def wal_payload(key: bytes, value: bytes | None) -> tuple[int, bytes]:
    if value is None:
        return 4, u32(len(key)) + key
    return 1, u32(len(key)) + key + value


def wal_record(
    version: int, sequence: int, entry_type: int, payload: bytes
) -> bytes:
    timestamp = 1_700_000_000_000 + sequence
    framing = (
        u32(len(payload))
        + u64(sequence)
        + u64(timestamp)
        + u8(entry_type)
        + u8(0)
    )
    checksum_input = framing + payload if version == 5 else payload
    reserved = bytes(4) + V5_COMMIT_TAG if version == 5 else bytes(6)
    header = (
        framing
        + u32(crc32(checksum_input))
        + reserved
    )
    extension = bytes(96) if version in (1, 2) else b""
    return header + extension + payload


def wal(version: int) -> bytes:
    first_key = b"\x00k\xff"
    deleted_key = b"\xffdeleted\x00"
    if version in (4, 5):
        padded_value = bytearray((index * 29) & 0xFF for index in range(124))
        padded_value[0] = 0x00
        padded_value[-1] = 0xFF
        operations = [
            (1, first_key, bytes(padded_value)),
            (4, deleted_key, b""),
            (1, b"empty\x00", b""),
        ]
        payload = u32(len(operations))
        for entry_type, key, value in operations:
            payload += u8(entry_type) + u32(len(key)) + u32(len(value)) + key + value
        records = wal_record(version, 0, 5, payload)
        logical_count = len(operations)
        last_sequence = logical_count - 1
    else:
        header_bytes = 128 if version in (1, 2) else 32
        target_size = 512 if version in (1, 2) else 256
        delete_type, delete_payload = wal_payload(deleted_key, None)
        first_value_size = (
            target_size
            - 64
            - (2 * header_bytes)
            - 4
            - len(first_key)
            - len(delete_payload)
        )
        first_value = bytearray((index * 17) & 0xFF for index in range(first_value_size))
        first_value[0] = 0x00
        first_value[-1] = 0xFF
        data_type, data_payload = wal_payload(first_key, first_value)
        records = wal_record(version, 0, data_type, data_payload)
        records += wal_record(version, 1, delete_type, delete_payload)
        logical_count = 2
        last_sequence = 1

    magic = b"HANSHIRO" if version == 1 else WAL_MAGIC
    header_prefix = (
        magic
        + u32(version)
        + u64(1_700_000_000)
        + u64(0)
        + u64(last_sequence)
        + u64(logical_count)
        + u32(0)
    )
    if version == 5:
        acknowledged_end = 64 + len(records)
        header = header_prefix + u64(acknowledged_end) + u64(
            cursor_crc64(version, 0, acknowledged_end)
        )
    else:
        header = header_prefix + bytes(16)
    return header + records


def sst_entry(version: int, key: bytes, value: bytes | None, sequence: int) -> bytes:
    encoded = u32(len(key)) + key
    if version == 1:
        assert value is not None
        return encoded + u32(len(value)) + value
    if version == 3:
        encoded += u64(sequence)
    encoded += u8(1 if value is not None else 0)
    encoded += u32(0 if value is None else len(value))
    return encoded + (b"" if value is None else value)


def sst_block(entries: list[bytes]) -> bytes:
    offsets: list[int] = []
    data = b""
    for entry in entries:
        offsets.append(len(data))
        data += entry
    data += b"".join(u32(offset) for offset in offsets) + u32(len(offsets))
    return data + u8(0) + u32(crc32(data))


def sstable(version: int, legacy_zero_checksum: bool = False) -> bytes:
    first_key = b"\x00k\xff"
    last_key = b"\xffk\x00"
    # The first uncompressed block is exactly 64 bytes before its five-byte
    # checksum footer, exercising the configured block-boundary spelling.
    first_value_size = {1: 45, 2: 44, 3: 36}[version]
    first_value = bytearray((index * 13) & 0xFF for index in range(first_value_size))
    first_value[0] = 0x00
    first_value[-1] = 0xFF
    first = sst_block([sst_entry(version, first_key, first_value, 41)])
    second_value = b"" if version == 1 else None
    second = sst_block([sst_entry(version, last_key, second_value, 42)])
    blocks = first + second

    index = b""
    offset = 0
    for last, block in ((first_key, first), (last_key, second)):
        index += u32(len(last)) + last + u64(offset) + u32(len(block))
        offset += len(block)
    index += u32(2)
    index_offset = len(blocks)

    # All-one bits are a valid (high false-positive) filter and make the
    # fixture independent of the hash implementation while preserving the
    # released metadata layout.
    bloom = bytes([0xFF] * 8) + u32(1) + u32(64) + u32(1)
    bloom_offset = index_offset + len(index)
    footer_payload = (
        u64(index_offset)
        + u32(len(index))
        + u64(bloom_offset)
        + u32(len(bloom))
        + SST_MAGIC
        + u32(version)
    )
    footer_checksum = 0 if legacy_zero_checksum else crc32(footer_payload)
    return blocks + index + bloom + footer_payload + u32(footer_checksum)


PROVENANCE = """TurboKV immutable storage-format fixtures

Generator: tests/fixtures/storage_formats/generate.py
Byte order: little endian. Compression: none.

Released format provenance:
- v0.2.0 (tag commit b577353a43c245e5830b3b16e06be1566763a3bc):
  manifest v1 with zero checksum placeholder,
  WAL v3, SSTable v1 with zero footer checksum placeholder.
- v0.2.1 (tag commit ee9bf5021fafa38fb238c62db796c01a529bf118):
  same stable identifiers and placeholder behavior as v0.2.0.
- v0.5.0 (tag commit bbc82a9270767a9767027de7dc373668b277f838):
  manifest v1 with CRC32, WAL v3, SSTable v2 with CRC32.

The historical layouts were transcribed from src/storage/manifest.rs,
src/storage/wal/{file,types}.rs, and src/storage/sstable/{writer,types}.rs at
those tag commits. Release-directory manifests use a relative SSTable path so
the immutable fixtures remain portable; database open resolves it against the
locked database directory before validation and migration.

Supported migration fixtures:
- manifest v1 placeholder + v1 CRC32, v2, and current v3;
- WAL v1 and v2 legacy 96-byte extensions, v3, v4 batches, and current v5;
- SSTable v1 placeholder + v1 CRC32, v2, and current v3.

The fixtures contain 0x00/0xff keys and values, an empty value, tombstones in
formats that can represent them, v4/v5 atomic batches, exact 64-byte uncompressed
SSTable block data, and exact 256/512-byte legacy WAL segment boundaries. Tests
exercise all 4 manifest x 5 WAL x 4 SSTable combinations through Db, configure
the writer's block maximum to that 64-byte spelling, and configure the WAL
maximum to the exact v5 fixture length before proving the next append rotates.

WAL v5 migration is one-way for older TurboKV binaries: once a v5 segment is
created, v1-v4 readers reject it. Back up the database before upgrading if a
downgrade may be required; downgrade only from a pre-v5 backup.

Normal tests never regenerate these files. Run `python3
tests/fixtures/storage_formats/generate.py --verify` to verify bytes and hashes.
"""


def artifacts() -> dict[str, bytes]:
    sst_v1_zero = sstable(1, True)
    sst_v2 = sstable(2)
    relative_table = b"sstables/L0/0000000001.sst"
    return {
        "manifest_v1_release_zero_crc.bin": manifest(1, True),
        "manifest_v1_crc.bin": manifest(1),
        "manifest_v2.bin": manifest(2),
        "manifest_v3.bin": manifest(3),
        "release_v0_2_x_manifest.bin": manifest(
            1,
            True,
            checkpoint=2,
            entries=(manifest_entry(1, relative_table, len(sst_v1_zero)),),
        ),
        "release_v0_5_0_manifest.bin": manifest(
            1,
            checkpoint=2,
            entries=(manifest_entry(1, relative_table, len(sst_v2)),),
        ),
        "wal_v1.wal": wal(1),
        "wal_v2.wal": wal(2),
        "wal_v3.wal": wal(3),
        "wal_v4.wal": wal(4),
        "wal_v5.wal": wal(5),
        "sst_v1_release_zero_crc.sst": sst_v1_zero,
        "sst_v1_crc.sst": sstable(1),
        "sst_v2.sst": sst_v2,
        "sst_v3.sst": sstable(3),
        "PROVENANCE.txt": PROVENANCE.encode(),
    }


def checksums(files: dict[str, bytes]) -> bytes:
    lines = []
    for name, data in sorted(files.items()):
        lines.append(f"{hashlib.sha256(data).hexdigest()}  {name}\n")
    return "".join(lines).encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()

    expected = artifacts()
    expected["SHA256SUMS"] = checksums(expected)
    if args.verify:
        unexpected = {
            path.name
            for path in ROOT.iterdir()
            if path.is_file() and path.name != Path(__file__).name
        } - expected.keys()
        failures = []
        for name, data in expected.items():
            path = ROOT / name
            if not path.is_file() or path.read_bytes() != data:
                failures.append(name)
        failures.extend(sorted(unexpected))
        if failures:
            print("fixture verification failed: " + ", ".join(failures))
            return 1
        print(f"verified {len(expected) - 2} binary fixtures and provenance")
        return 0

    for name, data in expected.items():
        (ROOT / name).write_bytes(data)
    print(f"wrote {len(expected) - 2} binary fixtures and provenance")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
