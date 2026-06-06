<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>lock-db</b><br>
    <sub><sup>API REFERENCE</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>API</span>
        <span>&nbsp;│&nbsp;</span>
        <a href="../CHANGELOG.md" title="Changelog"><b>CHANGELOG</b></a>
    </sup>
</div>
<br>

> Complete reference for every public item in `lock-db`, with examples.
>
> **Version: 0.2.0.** This release ships the lock-table core: shared/exclusive
> modes, the compatibility matrix, and a sharded, non-blocking lock table.
> Hierarchical and range locks, wait queues, and deadlock detection land across
> the rest of the 0.x series (see [`dev/ROADMAP.md`](../dev/ROADMAP.md)).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Concurrency model](#concurrency-model)
- [Public API](#public-api)
  - [`LockManager`](#lockmanager)
    - [`new`](#lockmanagernew)
    - [`with_shards`](#lockmanagerwith_shards)
    - [`try_acquire`](#lockmanagertry_acquire)
    - [`release`](#lockmanagerrelease)
    - [`release_all`](#lockmanagerrelease_all)
    - [`holder_count`](#lockmanagerholder_count)
    - [`mode_held`](#lockmanagermode_held)
    - [`shards`](#lockmanagershards)
  - [`LockMode`](#lockmode)
  - [Identifiers](#identifiers)
  - [`LockError`](#lockerror)
  - [Prelude](#prelude)
- [Feature flags](#feature-flags)
- [Usage patterns](#usage-patterns)

---

## Overview

A lock manager lets many transactions touch shared data at once without
corrupting it. Each transaction asks for a lock on a resource in a particular
[mode](#lockmode); the manager grants it only when that mode is *compatible*
with what every other transaction already holds on the resource. That single
rule — the compatibility matrix — is what keeps concurrent reads and writes
correct.

`lock-db` does not assign transaction or resource identities, persist anything,
or run a transaction lifecycle. It is the in-memory lock table that a higher
layer (`txn-db`, `page-db`, a storage engine) drives. The whole public surface
is four types:

| Type | Role |
|------|------|
| [`LockManager`](#lockmanager) | The sharded lock table. Acquire, release, query. |
| [`LockMode`](#lockmode) | Shared or exclusive, plus the compatibility rules. |
| [`TxnId`](#identifiers) / [`ResourceId`](#identifiers) | Opaque `u64` identifiers the caller assigns. |
| [`LockError`](#lockerror) | Why an operation failed. |

---

## Installation

```toml
[dependencies]
lock-db = "0.2"
```

To enable `serde` derives on the public types:

```toml
[dependencies]
lock-db = { version = "0.2", features = ["serde"] }
```

MSRV: Rust 1.85 (2024 edition).

---

## Quick Start

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let row = ResourceId::new(1);
let (writer, reader) = (TxnId::new(1), TxnId::new(2));

// The writer takes the row exclusively.
lm.try_acquire(writer, row, LockMode::Exclusive).unwrap();

// A concurrent reader is refused while the write lock is held.
assert_eq!(lm.try_acquire(reader, row, LockMode::Shared), Err(LockError::Conflict));

// Once the writer releases, the reader gets in.
lm.release(writer, row).unwrap();
lm.try_acquire(reader, row, LockMode::Shared).unwrap();
```

---

## Concurrency model

`LockManager` is `Send + Sync`. Every method takes `&self`, so the intended
deployment is a single manager shared across all worker threads behind an
[`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html); there is no outer
lock to manage.

```rust
use std::sync::Arc;
use std::thread;
use lock_db::prelude::*;

let lm = Arc::new(LockManager::new());
let mut handles = Vec::new();
for t in 0..4 {
    let lm = Arc::clone(&lm);
    handles.push(thread::spawn(move || {
        let txn = TxnId::new(t);
        let row = ResourceId::new(t); // each thread its own row: no contention
        lm.try_acquire(txn, row, LockMode::Exclusive).unwrap();
        lm.release(txn, row).unwrap();
    }));
}
for h in handles {
    h.join().unwrap();
}
```

Internally the table is split into a power-of-two number of **shards**, each a
mutex over its own slice of the resource space. The shard for a resource is
chosen by Fibonacci-hashing its id, which spreads sequential ids (the common
case for page and row numbers) evenly across shards. Two transactions touching
resources in different shards never block each other. Pick the shard count with
[`with_shards`](#lockmanagerwith_shards), or let [`new`](#lockmanagernew) scale
it to the machine.

Acquisition in this release is **non-blocking**: a request that cannot be
granted returns [`LockError::Conflict`](#lockerror) immediately rather than
parking the thread. The caller decides whether to retry, wait, or abort.

---

## Public API

### `LockManager`

The sharded lock table and the primary entry point of the crate. Available with
the default `std` feature.

```rust
use lock_db::LockManager;
```

| Method | Signature | Summary |
|--------|-----------|---------|
| [`new`](#lockmanagernew) | `fn new() -> Self` | Manager with a machine-scaled shard count. |
| [`with_shards`](#lockmanagerwith_shards) | `fn with_shards(shards: usize) -> Self` | Manager with an explicit shard count. |
| [`try_acquire`](#lockmanagertry_acquire) | `fn try_acquire(&self, txn: TxnId, res: ResourceId, mode: LockMode) -> Result<(), LockError>` | Take a lock, or fail without blocking. |
| [`release`](#lockmanagerrelease) | `fn release(&self, txn: TxnId, res: ResourceId) -> Result<(), LockError>` | Drop one lock. |
| [`release_all`](#lockmanagerrelease_all) | `fn release_all(&self, txn: TxnId) -> usize` | Drop every lock a transaction holds. |
| [`holder_count`](#lockmanagerholder_count) | `fn holder_count(&self, res: ResourceId) -> usize` | How many transactions hold a resource. |
| [`mode_held`](#lockmanagermode_held) | `fn mode_held(&self, txn: TxnId, res: ResourceId) -> Option<LockMode>` | The mode a transaction holds, if any. |
| [`shards`](#lockmanagershards) | `fn shards(&self) -> usize` | The shard count. |

`LockManager` also implements [`Default`](https://doc.rust-lang.org/std/default/trait.Default.html)
(equivalent to `new`).

---

#### `LockManager::new`

```rust
pub fn new() -> Self
```

Creates a lock manager with a shard count chosen for the current machine. The
count scales with the number of available CPUs (rounded up to a power of two and
clamped to a sensible range) so that contention on any single shard mutex stays
low on multi-core systems.

**Example**

```rust
use lock_db::LockManager;

let lm = LockManager::new();
assert!(lm.shards().is_power_of_two());
```

---

#### `LockManager::with_shards`

```rust
pub fn with_shards(shards: usize) -> Self
```

Creates a lock manager with an explicit shard count. `shards` is rounded up to
the next power of two (and `0` is treated as `1`), which lets the shard lookup
use a bit shift instead of a remainder. More shards reduce contention but cost a
mutex and two small maps each; fewer shards save memory at the cost of more
collisions.

**Parameters**

- `shards` — the desired number of shards. Rounded up to the next power of two.

**Examples**

```rust
use lock_db::LockManager;

assert_eq!(LockManager::with_shards(5).shards(), 8);
assert_eq!(LockManager::with_shards(0).shards(), 1);
assert_eq!(LockManager::with_shards(64).shards(), 64);
```

A single shard is useful for deterministic tests:

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::with_shards(1);
lm.try_acquire(TxnId::new(1), ResourceId::new(1), LockMode::Shared).unwrap();
assert_eq!(lm.shards(), 1);
```

---

#### `LockManager::try_acquire`

```rust
pub fn try_acquire(&self, txn: TxnId, res: ResourceId, mode: LockMode) -> Result<(), LockError>
```

Tries to acquire `mode` on `res` for `txn` without blocking. The request is
granted and `Ok(())` returned when:

- `txn` already holds a lock on `res` that **covers** `mode` (re-acquisition is
  idempotent, and asking for a weaker mode than you already hold is a no-op);
- `txn` already holds `res` shared, wants it exclusive, and is the **sole**
  holder (an in-place upgrade); or
- no other transaction holds `res` in a mode **incompatible** with `mode`.

Otherwise nothing changes and `Err(LockError::Conflict)` is returned.

**Parameters**

- `txn` — the transaction making the request.
- `res` — the resource to lock.
- `mode` — [`LockMode::Shared`](#lockmode) or [`LockMode::Exclusive`](#lockmode).

**Errors**

- [`LockError::Conflict`](#lockerror) — the lock cannot be granted right now.

**Examples**

Shared locks coexist; an exclusive lock excludes everyone:

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let row = ResourceId::new(10);

lm.try_acquire(TxnId::new(1), row, LockMode::Shared).unwrap();
lm.try_acquire(TxnId::new(2), row, LockMode::Shared).unwrap();
assert_eq!(lm.holder_count(row), 2);

assert!(lm.try_acquire(TxnId::new(3), row, LockMode::Exclusive).is_err());
```

Upgrade a shared lock to exclusive while you are the only holder:

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let key = ResourceId::new(7);
let t = TxnId::new(1);

lm.try_acquire(t, key, LockMode::Shared).unwrap();
lm.try_acquire(t, key, LockMode::Exclusive).unwrap(); // upgrade
assert_eq!(lm.mode_held(t, key), Some(LockMode::Exclusive));
```

A retry loop is the simplest way to wait for a contended lock:

```rust
use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};

fn acquire_spinning(lm: &LockManager, txn: TxnId, res: ResourceId, mode: LockMode) {
    while let Err(LockError::Conflict) = lm.try_acquire(txn, res, mode) {
        std::hint::spin_loop();
    }
}

let lm = LockManager::new();
acquire_spinning(&lm, TxnId::new(1), ResourceId::new(1), LockMode::Exclusive);
```

---

#### `LockManager::release`

```rust
pub fn release(&self, txn: TxnId, res: ResourceId) -> Result<(), LockError>
```

Releases the lock `txn` holds on `res`. When the last holder of a resource
releases, the resource's entry is removed from the table entirely.

**Errors**

- [`LockError::NotHeld`](#lockerror) — `txn` holds no lock on `res` (usually a
  double release or a bookkeeping mismatch in the caller).

**Examples**

```rust
use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let key = ResourceId::new(3);
let t = TxnId::new(1);

lm.try_acquire(t, key, LockMode::Exclusive).unwrap();
lm.release(t, key).unwrap();

// Releasing again is an error.
assert_eq!(lm.release(t, key), Err(LockError::NotHeld));
```

---

#### `LockManager::release_all`

```rust
pub fn release_all(&self, txn: TxnId) -> usize
```

Releases every lock held by `txn` across the whole table and returns how many
were released. This is the call a transaction layer makes at commit or abort.
It is proportional to the number of locks the transaction holds, not to the size
of the table, because each shard maintains a reverse index from transaction to
the resources it holds there.

**Examples**

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let t = TxnId::new(1);
for id in 0..5 {
    lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive).unwrap();
}

assert_eq!(lm.release_all(t), 5);
assert_eq!(lm.release_all(t), 0); // idempotent once empty
```

`release_all` touches only the named transaction; others are untouched:

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let row = ResourceId::new(1);
lm.try_acquire(TxnId::new(1), row, LockMode::Shared).unwrap();
lm.try_acquire(TxnId::new(2), row, LockMode::Shared).unwrap();

assert_eq!(lm.release_all(TxnId::new(1)), 1);
assert_eq!(lm.mode_held(TxnId::new(2), row), Some(LockMode::Shared));
```

---

#### `LockManager::holder_count`

```rust
pub fn holder_count(&self, res: ResourceId) -> usize
```

Returns the number of transactions currently holding `res`. In steady state
this is `0`, `1` for an exclusive lock, or the reader count for a shared lock.
Mostly useful for diagnostics and tests.

**Example**

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let key = ResourceId::new(1);
assert_eq!(lm.holder_count(key), 0);
lm.try_acquire(TxnId::new(1), key, LockMode::Shared).unwrap();
assert_eq!(lm.holder_count(key), 1);
```

---

#### `LockManager::mode_held`

```rust
pub fn mode_held(&self, txn: TxnId, res: ResourceId) -> Option<LockMode>
```

Returns the mode in which `txn` holds `res`, or `None` if it holds no lock on
it.

**Example**

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let key = ResourceId::new(1);
let t = TxnId::new(1);

assert_eq!(lm.mode_held(t, key), None);
lm.try_acquire(t, key, LockMode::Shared).unwrap();
assert_eq!(lm.mode_held(t, key), Some(LockMode::Shared));
```

---

#### `LockManager::shards`

```rust
pub fn shards(&self) -> usize
```

Returns the number of shards in the table — always a power of two.

**Example**

```rust
use lock_db::LockManager;

assert_eq!(LockManager::with_shards(10).shards(), 16);
```

---

### `LockMode`

The mode in which a transaction holds, or wants to hold, a lock. `no_std`.

```rust
pub enum LockMode {
    Shared,
    Exclusive,
}
```

| Variant | Meaning |
|---------|---------|
| `Shared` | A read lock. Any number of transactions may hold a resource `Shared` at once. |
| `Exclusive` | A write lock. Held by at most one transaction, and only when no one else holds the resource. |

| Method | Signature | Summary |
|--------|-----------|---------|
| `compatible_with` | `const fn compatible_with(self, other: LockMode) -> bool` | Whether two modes may be held on one resource at once. |
| `covers` | `const fn covers(self, other: LockMode) -> bool` | Whether holding `self` already grants everything `other` would. |
| `is_exclusive` | `const fn is_exclusive(self) -> bool` | Whether this is `Exclusive`. |

The compatibility matrix — the only compatible pair is shared/shared:

| held \ requested | `Shared` | `Exclusive` |
|------------------|:--------:|:-----------:|
| **`Shared`** | ✓ | ✗ |
| **`Exclusive`** | ✗ | ✗ |

**Examples**

```rust
use lock_db::LockMode;

// Two readers coexist; a writer excludes everyone.
assert!(LockMode::Shared.compatible_with(LockMode::Shared));
assert!(!LockMode::Shared.compatible_with(LockMode::Exclusive));
assert!(!LockMode::Exclusive.compatible_with(LockMode::Exclusive));

// Holding exclusive already covers a read; holding shared does not cover a write.
assert!(LockMode::Exclusive.covers(LockMode::Shared));
assert!(!LockMode::Shared.covers(LockMode::Exclusive));
```

Compatibility is symmetric:

```rust
use lock_db::LockMode;

for a in [LockMode::Shared, LockMode::Exclusive] {
    for b in [LockMode::Shared, LockMode::Exclusive] {
        assert_eq!(a.compatible_with(b), b.compatible_with(a));
    }
}
```

---

### Identifiers

`TxnId` and `ResourceId` are thin, `#[repr(transparent)]` newtypes over `u64`.
lock-db never interprets them: the caller decides what a transaction id means and
how to map a database, table, page, or row to a single `ResourceId`. Both are
`no_std`, `Copy`, `Ord`, and `Hash`.

```rust
pub struct TxnId(/* private */);
pub struct ResourceId(/* private */);
```

| Method | Applies to | Summary |
|--------|-----------|---------|
| `new(id: u64) -> Self` | both | Wrap a raw number. `const`. |
| `get(self) -> u64` | both | Read the underlying number. `const`. |
| `From<u64>` / `Into<u64>` | both | Conversions in both directions. |

**Examples**

```rust
use lock_db::{ResourceId, TxnId};

let t = TxnId::new(42);
assert_eq!(t.get(), 42);
assert_eq!(TxnId::from(42), t);

// A page number maps straight to a resource id.
let page = ResourceId::new(0xDEAD_BEEF);
assert_eq!(u64::from(page), 0xDEAD_BEEF);
```

> **Note:** Two distinct resources that map to the same `ResourceId` share a
> lock queue. Collision-free id assignment is the caller's responsibility.

---

### `LockError`

The set of ways a lock operation can fail. `#[non_exhaustive]`, so matching code
must keep a wildcard arm; later milestones add variants (for example a timeout
and a deadlock-victim signal). Implements `Display`, and `std::error::Error`
under the `std` feature. `no_std`.

```rust
#[non_exhaustive]
pub enum LockError {
    Conflict,
    NotHeld,
}
```

| Variant | Returned by | Meaning |
|---------|-------------|---------|
| `Conflict` | [`try_acquire`](#lockmanagertry_acquire) | The lock cannot be granted without blocking. |
| `NotHeld` | [`release`](#lockmanagerrelease) | No lock is held for this transaction and resource. |

**Example**

```rust
use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let row = ResourceId::new(1);

lm.try_acquire(TxnId::new(1), row, LockMode::Exclusive).unwrap();
assert_eq!(
    lm.try_acquire(TxnId::new(2), row, LockMode::Shared),
    Err(LockError::Conflict),
);
assert_eq!(lm.release(TxnId::new(9), row), Err(LockError::NotHeld));
```

---

### Prelude

`lock_db::prelude` re-exports the whole public surface for a single glob import.

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
lm.try_acquire(TxnId::new(1), ResourceId::new(1), LockMode::Shared).unwrap();
```

The prelude contains `LockMode`, `TxnId`, `ResourceId`, `LockError`, and (under
the `std` feature) `LockManager`.

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Enables `LockManager` and the `std::error::Error` impl. With `std` off, the crate is `no_std` and exposes only `LockMode`, `TxnId`, `ResourceId`, and `LockError`. |
| `serde` | no | Derives `serde::Serialize` / `Deserialize` on `LockMode`, `TxnId`, `ResourceId`, and `LockError`. |

---

## Usage patterns

**Two-phase locking (2PL).** Acquire every lock a transaction needs during its
growing phase; at commit or abort, drop them all at once with
[`release_all`](#lockmanagerrelease_all). Never release individually before the
transaction ends if you need strict 2PL.

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let txn = TxnId::new(1);

// Growing phase: take locks as the transaction reads and writes.
lm.try_acquire(txn, ResourceId::new(1), LockMode::Shared).unwrap();
lm.try_acquire(txn, ResourceId::new(2), LockMode::Exclusive).unwrap();

// Commit / abort: release everything in one call.
let released = lm.release_all(txn);
assert_eq!(released, 2);
```

**Read-then-write upgrade.** Take a resource shared, then upgrade to exclusive
once you decide to write it. The upgrade succeeds only while you are the sole
holder; otherwise it conflicts and you choose how to proceed.

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let row = ResourceId::new(1);
let txn = TxnId::new(1);

lm.try_acquire(txn, row, LockMode::Shared).unwrap();     // read
match lm.try_acquire(txn, row, LockMode::Exclusive) {    // upgrade to write
    Ok(()) => { /* now hold X */ }
    Err(LockError::Conflict) => { /* other readers present; retry or abort */ }
    Err(_) => unreachable!(),
}
```

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>
