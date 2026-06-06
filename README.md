<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <b>lock-db</b>
    <br>
    <sub><sup>LOCK MANAGER & DEADLOCK DETECTION</sup></sub>
</h1>

<div align="center">
    <a href="https://crates.io/crates/lock-db"><img alt="Crates.io" src="https://img.shields.io/crates/v/lock-db"></a>
    <a href="https://crates.io/crates/lock-db" alt="Download lock-db"><img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/lock-db?color=%230099ff"></a>
    <a href="https://docs.rs/lock-db" title="lock-db Documentation"><img alt="docs.rs" src="https://img.shields.io/docsrs/lock-db"></a>
    <a href="https://github.com/jamesgober/lock-db/actions"><img alt="GitHub CI" src="https://github.com/jamesgober/lock-db/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
</div>

<br>

<div align="left">
    <p>
        <strong>lock-db</strong> is the <b>lock manager</b> for a transactional database: the component that lets many transactions touch shared data at once without corrupting it, and that notices when they have deadlocked and breaks the tie.
    </p>
    <p>
        It provides <b>row and range locks</b> across <b>multiple granularities</b> (database, table, page, row) with the standard lock modes and a compatibility matrix, and it builds a <b>wait-for graph</b> to detect <b>deadlock cycles</b> and select a victim to abort.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). Row/range locks. Hierarchical granularity. Wait-for deadlock detection.
    </p>
    <blockquote>
        <strong>Status: feature-complete, API frozen, hardening.</strong> The feature set is complete and the public API is frozen until <code>2.0</code>: the five MGL modes, hierarchical and range locks, and wait-for deadlock detection. <code>v0.5.0</code> adds adversarial contention and deadlock-storm stress tests on top of the property and <code>loom</code> suites. The remaining road to <code>1.0.0</code> is integration and final benchmarks per <a href="./dev/ROADMAP.md"><code>dev/ROADMAP.md</code></a>.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

- **Lock modes** &mdash; the five standard multi-granularity modes &mdash; intention-shared (IS), intention-exclusive (IX), shared (S), shared-intention-exclusive (SIX), and exclusive (X) &mdash; with a `const` compatibility matrix at the core of every grant decision
- **Hierarchical granularities** &mdash; lock a database / table / page / row hierarchy correctly with intention locks; the manager enforces the matrix at every level
- **Range locks** &mdash; lock a contiguous span of keys (`KeyRange`) for predicate / phantom protection, with overlap-based conflict detection
- **Deadlock detection** &mdash; a wait-for graph with cycle detection and victim selection; the deadlock-aware `request` records waits and reports cycles, and `WaitForGraph` is reusable on its own
- **Sharded lock table** &mdash; the resource space is partitioned across independent shards so acquisitions on unrelated resources never contend on the same mutex
- **Acquire / release** &mdash; non-blocking `try_acquire`, single and bulk release, re-entrant acquisition, and lattice upgrades (e.g. S + IX &rarr; SIX)

<br>
<hr>
<br>

## Installation

```toml
[dependencies]
lock-db = "0.5"
```

<br>

## Quick Start

```rust
use lock_db::prelude::*;

// One manager, shared across all worker threads behind an `Arc`.
let lm = LockManager::new();
let row = ResourceId::new(1);
let (writer, reader) = (TxnId::new(1), TxnId::new(2));

// The writer takes the row exclusively.
lm.try_acquire(writer, row, LockMode::Exclusive).unwrap();

// A concurrent reader is refused while the write lock is held.
assert_eq!(lm.try_acquire(reader, row, LockMode::Shared), Err(LockError::Conflict));

// Once the writer commits and releases, the reader gets in.
lm.release(writer, row).unwrap();
lm.try_acquire(reader, row, LockMode::Shared).unwrap();
```

Range-lock a span of keys to keep another transaction from inserting into it
(phantom protection):

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let index = ResourceId::new(10); // the key space being protected

// Txn 1 read-locks keys [100, 200].
lm.try_acquire_range(TxnId::new(1), index, KeyRange::new(100, 200).unwrap(), LockMode::Shared).unwrap();

// Txn 2 cannot write key 150 inside that range, but a disjoint range is free.
assert!(lm.try_acquire_range(TxnId::new(2), index, KeyRange::point(150), LockMode::Exclusive).is_err());
lm.try_acquire_range(TxnId::new(2), index, KeyRange::new(201, 300).unwrap(), LockMode::Exclusive).unwrap();
```

A transaction drops its whole lock set — point and range — in one call at commit
or abort:

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let txn = TxnId::new(1);
for id in 0..3 {
    lm.try_acquire(txn, ResourceId::new(id), LockMode::Exclusive).unwrap();
}
assert_eq!(lm.release_all(txn), 3);
```

The deadlock-aware `request` records waits and reports cycles, naming a victim to
abort:

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let (a, b) = (ResourceId::new(1), ResourceId::new(2));
let (t1, t2) = (TxnId::new(1), TxnId::new(2));

lm.request(t1, a, LockMode::Exclusive); // T1 holds A
lm.request(t2, b, LockMode::Exclusive); // T2 holds B
lm.request(t1, b, LockMode::Exclusive); // T1 waits for T2

// T2 waiting for A closes the cycle; abort the victim to break it.
if let Acquisition::Deadlock(d) = lm.request(t2, a, LockMode::Exclusive) {
    lm.release_all(d.victim);
}
```

<br>

## API Overview

For the complete reference with method tables and examples, see [`docs/API.md`](./docs/API.md).

- [`LockMode`](./docs/API.md#lockmode) &mdash; the five MGL modes and the compatibility matrix
- [`LockManager`](./docs/API.md#lockmanager) &mdash; the sharded lock table (point locks, range locks, deadlock-aware `request`)
- [`WaitForGraph`](./docs/API.md#waitforgraph) &mdash; wait-for graph, cycle detection, and victim selection
- [`KeyRange`](./docs/API.md#keyrange) &mdash; an inclusive key interval for range locks
- [`TxnId` and `ResourceId`](./docs/API.md#identifiers) &mdash; opaque identifiers
- [`LockError`](./docs/API.md#lockerror) &mdash; failure modes

<br>

## Examples

Runnable examples live in [`examples/`](./examples). Run any of them with
`cargo run --example <name>`:

| Example | Shows |
|---------|-------|
| [`quick_start`](./examples/quick_start.rs) | Acquire, conflict, release on a single row. |
| [`two_phase_locking`](./examples/two_phase_locking.rs) | Growing-phase acquires, then `release_all` at commit. |
| [`shared_upgrade`](./examples/shared_upgrade.rs) | Read under a shared lock, then upgrade to exclusive. |
| [`hierarchy`](./examples/hierarchy.rs) | Intention locks over a database/table/page/row hierarchy. |
| [`range_locks`](./examples/range_locks.rs) | Range locking for phantom protection. |
| [`deadlock`](./examples/deadlock.rs) | Wait-for deadlock detection and victim abort. |
| [`concurrent`](./examples/concurrent.rs) | Many threads contending on one row, with a mutual-exclusion check. |

<br>
<hr>
<br>

## Where It Fits

`lock-db` is the concurrency-control layer. It is used by:

- [`txn-db`](https://github.com/jamesgober/txn-db) &mdash; transactions acquire and release locks here to enforce isolation
- [`page-db`](https://github.com/jamesgober/page-db) &mdash; page-granularity locks coordinate with the paged store
- [`index-db`](https://github.com/jamesgober/index-db) &mdash; range locks protect B+tree key ranges against phantoms
- storage engines &mdash; any engine needing pessimistic concurrency control

It has no first-party dependencies, so it builds and tests standalone today.

<br>

## Cross-Platform Support

Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon), and Windows (x86_64) are first-class and verified by the CI matrix.

<br>

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) and [`dev/DIRECTIVES.md`](./dev/DIRECTIVES.md). Before a PR: `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean.

<br>

<div id="license">
    <h2>License</h2>
    <p>Licensed under either of</p>
    <ul>
        <li><b>Apache License, Version 2.0</b> &mdash; <a href="./LICENSE-APACHE">LICENSE-APACHE</a></li>
        <li><b>MIT License</b> &mdash; <a href="./LICENSE-MIT">LICENSE-MIT</a></li>
    </ul>
    <p>at your option.</p>
</div>

<div align="center">
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>JAMES GOBER.</strong></sup>
</div>
