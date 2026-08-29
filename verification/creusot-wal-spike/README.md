# TurboKV Creusot WAL spike

This is throwaway feasibility code on `codex/creusot-verification-spike`.
It verifies the actual pure function used by WAL-v5 recovery to calculate a
record's exclusive end offset.

The proof asks Creusot to establish, for every `u64` input triple:

- a returned end equals the mathematical sum of offset, header, and payload;
- a returned end cannot wrap behind any input component; and
- rejection occurs exactly when that mathematical sum exceeds `u64::MAX`.

Run it with one command:

```sh
./verification/creusot-wal-spike/verify.sh
```

This does not verify mmap publication, filesystem durability, async ordering,
or concurrency. Those remain covered by crash, compatibility, and stress tests.

## Recorded result

The feasibility gate passed on `aarch64-apple-darwin`:

- Creusot and `creusot-std` 0.13.0;
- Rust nightly 2026-06-22 (`1.98.0-nightly`);
- Why3 1.8.2 plus the Creusot-pinned revision;
- Alt-Ergo 2.6.2, Z3 4.15.3, CVC4 1.8, and CVC5 1.3.1; and
- three generated proof files, with every verification condition discharged.

There are no TurboKV `trusted` functions or custom axioms in this proof. The
Creusot standard-library contracts and integer model remain part of the tool's
trusted base.

The required falsification check also passed: changing the second checked
addition to checked subtraction left ordinary Rust compilation valid but made
Creusot fail `Coma.vc_checked_record_end`. Restoring the implementation made all
three proof files pass again.

Verdict: Creusot can verify small pure functions shared directly with TurboKV's
production path without affecting normal stable-Rust builds. This result does
not yet establish the higher-level WAL publication and recovery guarantees in
[`../FORMAL_VERIFICATION_TARGETS.md`](../FORMAL_VERIFICATION_TARGETS.md).
