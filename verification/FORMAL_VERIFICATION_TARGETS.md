# Formal verification targets

This document defines what a formal-verification effort must prove before its
results can support TurboKV's production contract. It is intentionally written
before choosing proof annotations or adapting production code.

Creusot is a candidate tool, not the specification. If Creusot cannot express
or prove a required property, the property remains required and needs another
verification technique.

## Proof boundary

A proof counts only when it verifies the production function itself or a pure
function shared verbatim by production code. Proving a copied model is useful
for design exploration but does not establish correctness of the implementation.

The proof boundary must state every trusted assumption. Core invariants may not
be discharged by marking the implementation `trusted`, assuming the desired
postcondition, or replacing fallible operations with an idealized equivalent.

## Required guarantees

### 1. WAL representation and recovery

- Record and batch lengths, offsets, sequence ranges, and cursor arithmetic
  never wrap, panic, or index outside validated bytes.
- Encoding followed by decoding preserves arbitrary binary keys, values,
  deletes, operation order, and exact sequence numbers.
- A malformed encoding is rejected; it cannot decode into a different valid
  mutation or batch.
- A batch is recovered either completely and in order or not at all.
- The acknowledged cursor never advances beyond the contiguous prefix of
  fully published, checksummed records.
- Recovery includes every acknowledged record exactly once. It may include a
  complete unacknowledged record, but it never omits an acknowledged record.
- Tail repair never truncates before the acknowledged cursor or converts
  interior corruption into a recoverable tail.

### 2. Checkpoint and WAL reclamation safety

- The manifest checkpoint is the first sequence recovery may replay.
- The checkpoint is monotonic.
- Advancing the checkpoint proves that every lower sequence is represented by
  installed durable state; a frozen, active, cancelled, or unapplied mutation
  keeps the checkpoint at or below its sequence.
- A WAL segment is reclaimable only when its last valid sequence is strictly
  below the checkpoint. Batch boundaries cannot be split by checkpoint
  alignment or reclamation.

### 3. Version and tombstone resolution

- Resolution chooses exactly the maximum `VersionOrder` for a key, independent
  of source enumeration order.
- Missing, live value, and tombstone remain distinct until that winner is
  chosen.
- A winning tombstone suppresses every older value; an older tombstone cannot
  suppress a newer value.
- Point reads, range scans, and prefix scans use the same arbitration relation
  and therefore return the same winner for a key.

### 4. Compaction preservation

- Compaction preserves the observable key/value map produced by its inputs and
  unaffected tables.
- Each surviving key has exactly one winning output version; output splitting
  neither loses nor duplicates entries and preserves sorted key order.
- A tombstone is removed only when the captured strict frontier proves that no
  older value can later arrive from an input outside the compaction.
- Installing compaction outputs and removing inputs preserves level overlap and
  manifest identity invariants.

### 5. Ordered mutation and atomic visibility

- Engine sequence allocation is monotonic and cannot overlap, including batch
  ranges.
- Insert, delete, bulk insert, and batch operations publish in their allocated
  order.
- Readers cannot observe a strict subset of an atomic batch.
- Cancellation can leave an unacknowledged record recoverable, but it cannot
  cause an acknowledged mutation to disappear or an allocated sequence to be
  reused.

### 6. Streaming read equivalence

- A merged scan is strictly key-ordered, contains no duplicate key, and equals
  the reference result obtained by resolving every captured source and then
  applying the requested bounds or prefix.
- Tombstones are filtered only after winner selection.
- Seek and block-range arithmetic cannot skip an eligible first key or read
  outside a validated block.

## What Creusot cannot prove by itself

The following guarantees cross the operating-system or hardware boundary and
must remain explicit assumptions backed by crash tests, failpoints, platform
tests, and filesystem documentation:

- that mmap stores reach the operating-system page cache before process-crash
  acknowledgement;
- that `sync_all`, `F_FULLFSYNC`, `FlushFileBuffers`, atomic rename, and parent
  directory sync honor the documented power-loss contract;
- that successful physical reservation prevents later media faults or SIGBUS;
- Tokio scheduling, channel fairness, lock implementation, and cancellation;
- correctness of unsafe mmap and platform syscall wrappers; and
- device firmware, storage write-cache, and filesystem behavior.

Formal models can prove that TurboKV calls these boundaries in a safe order,
but not that the external system implements their promised effects.

## Evidence required for each accepted proof

- A pinned Creusot, Rust target, Why3, and solver version.
- One documented command that replays the proof from a clean checkout.
- No unlisted trusted functions or axioms in the proof dependency cone.
- A deliberate mutation that violates the invariant and makes the proof fail.
- Ordinary TurboKV tests, compatibility fixtures, crash tests, Miri, and
  sanitizers remain green; the proof supplements rather than replaces them.
- Architecture-dependent proofs are labelled with their verified target.

## First feasibility gate

The first spike verifies the production WAL record-end calculation. For every
possible machine-integer input it must prove that a successful end is the exact
mathematical sum, can never wrap behind the record, and failure occurs exactly
when the mathematical result is not representable.

This small obligation is not sufficient for WAL correctness. It is the first
gate because it is in the recovery dependency cone, has no external-system
assumptions, and tests whether Creusot can verify shared production code without
polluting TurboKV's normal build. If it succeeds, the next target is a pure WAL
publication/recovery state machine covering the cursor and truncation guarantees
above.
