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
> **Version: 0.3.0.** This release adds multi-granularity locking (the five MGL
> modes and the hierarchy protocol) and key-range locking on top of the sharded
> lock-table core. Wait queues and deadlock detection land in later 0.x releases
> (see [`dev/ROADMAP.md`](../dev/ROADMAP.md)).

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Concurrency model](#concurrency-model)
- [Hierarchical locking](#hierarchical-locking)
- [Public API](#public-api)
  - [`LockManager`](#lockmanager)
    - [`new`](#lockmanagernew)
    - [`with_shards`](#lockmanagerwith_shards)
    - [`try_acquire`](#lockmanagertry_acquire)
    - [`release`](#lockmanagerrelease)
    - [`release_all`](#lockmanagerrelease_all)
    - [`try_acquire_range`](#lockmanagertry_acquire_range)
    - [`release_range`](#lockmanagerrelease_range)
    - [`holder_count`](#lockmanagerholder_count)
    - [`mode_held`](#lockmanagermode_held)
    - [`range_count`](#lockmanagerrange_count)
    - [`shards`](#lockmanagershards)
  - [`LockMode`](#lockmode)
  - [`KeyRange`](#keyrange)
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
layer (`txn-db`, `page-db`, a storage engine) drives. The public surface is five
types:

| Type | Role |
|------|------|
| [`LockManager`](#lockmanager) | The sharded lock table. Point and range locks: acquire, release, query. |
| [`LockMode`](#lockmode) | The five MGL modes and their compatibility / lattice rules. |
| [`KeyRange`](#keyrange) | An inclusive key interval — the unit a range lock protects. |
| [`TxnId`](#identifiers) / [`ResourceId`](#identifiers) | Opaque `u64` identifiers the caller assigns. |
| [`LockError`](#lockerror) | Why an operation failed. |

---

## Installation

```toml
[dependencies]
lock-db = "0.3"
```

To enable `serde` derives on the public types:

```toml
[dependencies]
lock-db = { version = "0.3", features = ["serde"] }
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
resources in different shards never block each other. Range locks are sharded
the same way, by the key space they protect. Pick the shard count with
[`with_shards`](#lockmanagerwith_shards), or let [`new`](#lockmanagernew) scale
it to the machine.

Acquisition in this release is **non-blocking**: a request that cannot be
granted returns [`LockError::Conflict`](#lockerror) immediately rather than
parking the thread. The caller decides whether to retry, wait, or abort.

---

## Hierarchical locking

The intention modes (`IS`, `IX`, `SIX`) exist to lock a hierarchy —
database → table → page → row — correctly and cheaply. Without them, a
transaction wanting to lock a whole table would have to check every row lock
beneath it; an intention lock on the table summarises "someone is working
below" in one place.

The protocol is the standard one:

1. Map each node of the hierarchy to a [`ResourceId`](#identifiers).
2. Before locking a node in `S`, hold `IS` (or stronger) on every coarser node
   above it. Before locking a node in `X`, hold `IX` (or stronger) above it.
3. Acquire **coarse-to-fine**; release **fine-to-coarse** (or drop everything at
   once with [`release_all`](#lockmanagerrelease_all)).

lock-db enforces the compatibility matrix at each node; the caller follows the
protocol. To lock one row for writing:

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let (db, table, page, row) = (ResourceId::new(1), ResourceId::new(2), ResourceId::new(3), ResourceId::new(4));
let writer = TxnId::new(1);

// Coarse-to-fine: IX down the path, then X on the row.
for node in [db, table, page] {
    lm.try_acquire(writer, node, LockMode::IntentionExclusive).unwrap();
}
lm.try_acquire(writer, row, LockMode::Exclusive).unwrap();

// A reader can still descend elsewhere in the same table.
let reader = TxnId::new(2);
for node in [db, table] {
    lm.try_acquire(reader, node, LockMode::IntentionShared).unwrap();
}
// ...but not the row held exclusively.
assert!(lm.try_acquire(reader, row, LockMode::Shared).is_err());
```

---

## Public API

### `LockManager`

The sharded lock table and the primary entry point of the crate. Available with
the default `std` feature. Handles both **point locks** (on a
[`ResourceId`](#identifiers)) and **range locks** (on a
[`KeyRange`](#keyrange) within a key space).

```rust
use lock_db::LockManager;
```

| Method | Summary |
|--------|---------|
| [`new`](#lockmanagernew) | Manager with a machine-scaled shard count. |
| [`with_shards`](#lockmanagerwith_shards) | Manager with an explicit shard count. |
| [`try_acquire`](#lockmanagertry_acquire) | Take a point lock, or fail without blocking. |
| [`release`](#lockmanagerrelease) | Drop one point lock. |
| [`release_all`](#lockmanagerrelease_all) | Drop every lock (point and range) a transaction holds. |
| [`try_acquire_range`](#lockmanagertry_acquire_range) | Take a range lock, or fail without blocking. |
| [`release_range`](#lockmanagerrelease_range) | Drop one range lock. |
| [`holder_count`](#lockmanagerholder_count) | How many transactions hold a resource. |
| [`mode_held`](#lockmanagermode_held) | The mode a transaction holds, if any. |
| [`range_count`](#lockmanagerrange_count) | How many range locks are live in a space. |
| [`shards`](#lockmanagershards) | The shard count. |

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
mutex and a few small maps each; fewer shards save memory at the cost of more
collisions.

**Examples**

```rust
use lock_db::LockManager;

assert_eq!(LockManager::with_shards(5).shards(), 8);
assert_eq!(LockManager::with_shards(0).shards(), 1);
assert_eq!(LockManager::with_shards(64).shards(), 64);
```

---

#### `LockManager::try_acquire`

```rust
pub fn try_acquire(&self, txn: TxnId, res: ResourceId, mode: LockMode) -> Result<(), LockError>
```

Tries to acquire `mode` on `res` for `txn` without blocking. The request is
granted and `Ok(())` returned when:

- `txn` already holds a lock on `res` that [covers](#lockmode) `mode`
  (re-acquisition is idempotent, and asking for a weaker mode than you already
  hold is a no-op);
- `txn` already holds `res` in some mode and the [join](#lockmode) of that mode
  with `mode` is compatible with every **other** holder (an in-place upgrade —
  for example `Shared` to `Exclusive` when sole holder, or `Shared` + `IntentionExclusive`
  to `SharedIntentionExclusive`); or
- `txn` holds nothing on `res` and `mode` is compatible with every current
  holder.

Otherwise nothing changes and `Err(LockError::Conflict)` is returned.

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

Lattice upgrade — a reader that decides to write part of a subtree:

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let table = ResourceId::new(7);
let t = TxnId::new(1);

lm.try_acquire(t, table, LockMode::Shared).unwrap();            // read the table
lm.try_acquire(t, table, LockMode::IntentionExclusive).unwrap(); // intend to write a row
assert_eq!(lm.mode_held(t, table), Some(LockMode::SharedIntentionExclusive));
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

Releases the point lock `txn` holds on `res`. When the last holder of a resource
releases, the resource's entry is removed from the table entirely.

**Errors**

- [`LockError::NotHeld`](#lockerror) — `txn` holds no lock on `res` (usually a
  double release or a bookkeeping mismatch in the caller).

**Example**

```rust
use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let key = ResourceId::new(3);
let t = TxnId::new(1);

lm.try_acquire(t, key, LockMode::Exclusive).unwrap();
lm.release(t, key).unwrap();
assert_eq!(lm.release(t, key), Err(LockError::NotHeld));
```

---

#### `LockManager::release_all`

```rust
pub fn release_all(&self, txn: TxnId) -> usize
```

Releases every lock held by `txn` across the whole table — **both point locks
and range locks** — and returns how many were released. This is the call a
transaction layer makes at commit or abort. It is proportional to the number of
locks the transaction holds, not to the size of the table, because each shard
maintains a reverse index from transaction to the locks it holds there.

**Examples**

```rust
use lock_db::{KeyRange, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let t = TxnId::new(1);
for id in 0..5 {
    lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive).unwrap();
}
lm.try_acquire_range(t, ResourceId::new(99), KeyRange::point(1), LockMode::Shared).unwrap();

assert_eq!(lm.release_all(t), 6); // 5 point + 1 range
assert_eq!(lm.release_all(t), 0); // idempotent once empty
```

---

#### `LockManager::try_acquire_range`

```rust
pub fn try_acquire_range(&self, txn: TxnId, space: ResourceId, range: KeyRange, mode: LockMode) -> Result<(), LockError>
```

Tries to acquire `mode` over the key range `range` in key space `space`, for
`txn`, without blocking. A range lock protects a contiguous span of keys — use
it to stop another transaction from inserting into, or writing within, a range
you have read (phantom and predicate protection). `space` identifies the key
space the range lives in, typically an index; ranges in different spaces never
conflict.

The request is granted unless some **other** transaction already holds an
[overlapping](#keyrange) range in `space` in an [incompatible](#lockmode) mode.
The same transaction may hold several ranges in a space, including overlapping
ones; range locks are not merged or upgraded.

> **Performance.** Conflict detection scans the live ranges in `space` (overlap
> is not a key-equality lookup, so a hash map does not help). Cost is linear in
> the number of ranges held in that space. For spaces with very many concurrent
> range locks an interval tree would lower this; it is a candidate for a later
> release if profiling shows range contention dominates.

**Errors**

- [`LockError::Conflict`](#lockerror) — an overlapping, incompatible range is
  held by another transaction.

**Examples**

```rust
use lock_db::{KeyRange, LockError, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let index = ResourceId::new(1);

// A read lock over [100, 200].
lm.try_acquire_range(TxnId::new(1), index, KeyRange::new(100, 200).unwrap(), LockMode::Shared).unwrap();

// Another reader may share the overlapping range...
lm.try_acquire_range(TxnId::new(2), index, KeyRange::new(150, 250).unwrap(), LockMode::Shared).unwrap();

// ...but a writer inside it conflicts.
assert_eq!(
    lm.try_acquire_range(TxnId::new(3), index, KeyRange::point(150), LockMode::Exclusive),
    Err(LockError::Conflict),
);

// A disjoint range is free.
lm.try_acquire_range(TxnId::new(3), index, KeyRange::new(201, 300).unwrap(), LockMode::Exclusive).unwrap();
```

---

#### `LockManager::release_range`

```rust
pub fn release_range(&self, txn: TxnId, space: ResourceId, range: KeyRange) -> Result<(), LockError>
```

Releases a range lock `txn` holds over `range` in `space`. Matches on the
transaction and the exact range; if the transaction holds several locks on the
identical range (in different modes), one is released per call.

**Errors**

- [`LockError::NotHeld`](#lockerror) — `txn` holds no lock on that exact range
  in `space`.

**Example**

```rust
use lock_db::{KeyRange, LockError, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let index = ResourceId::new(1);
let r = KeyRange::new(1, 10).unwrap();
let t = TxnId::new(1);

lm.try_acquire_range(t, index, r, LockMode::Exclusive).unwrap();
lm.release_range(t, index, r).unwrap();
assert_eq!(lm.release_range(t, index, r), Err(LockError::NotHeld));
```

---

#### `LockManager::holder_count`

```rust
pub fn holder_count(&self, res: ResourceId) -> usize
```

Returns the number of transactions currently holding the point lock on `res`. In
steady state this is `0`, `1` for an exclusive lock, or the holder count for
shared / intention modes. Mostly useful for diagnostics and tests.

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

Returns the mode in which `txn` holds `res`, or `None` if it holds no point lock
on it.

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

#### `LockManager::range_count`

```rust
pub fn range_count(&self, space: ResourceId) -> usize
```

Returns the number of range locks currently held in `space`, across all
transactions and modes. Mostly useful for diagnostics and tests.

**Example**

```rust
use lock_db::{KeyRange, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let index = ResourceId::new(1);
assert_eq!(lm.range_count(index), 0);
lm.try_acquire_range(TxnId::new(1), index, KeyRange::point(1), LockMode::Shared).unwrap();
assert_eq!(lm.range_count(index), 1);
```

---

#### `LockManager::shards`

```rust
pub fn shards(&self) -> usize
```

Returns the number of shards in the table — always a power of two.

```rust
use lock_db::LockManager;

assert_eq!(LockManager::with_shards(10).shards(), 16);
```

---

### `LockMode`

The mode in which a transaction holds, or wants to hold, a lock. `no_std`. The
five modes are the standard multi-granularity locking (MGL) set:

```rust
pub enum LockMode {
    IntentionShared,           // IS
    IntentionExclusive,        // IX
    Shared,                    // S
    SharedIntentionExclusive,  // SIX
    Exclusive,                 // X
}
```

| Variant | Meaning |
|---------|---------|
| `IntentionShared` (IS) | Intent to take shared locks on finer resources below this one. |
| `IntentionExclusive` (IX) | Intent to take exclusive (or shared) locks on finer resources. |
| `Shared` (S) | A read lock. |
| `SharedIntentionExclusive` (SIX) | Read the subtree (S) and intend to write part of it (IX). |
| `Exclusive` (X) | A write lock; excludes all other holders. |

The compatibility matrix (`true` = the two modes may be held at once by
different transactions):

|       | IS | IX | S  | SIX | X  |
|-------|----|----|----|-----|----|
| **IS**  | ✓ | ✓ | ✓ | ✓  | ✗ |
| **IX**  | ✓ | ✓ | ✗ | ✗  | ✗ |
| **S**   | ✓ | ✗ | ✓ | ✗  | ✗ |
| **SIX** | ✓ | ✗ | ✗ | ✗  | ✗ |
| **X**   | ✗ | ✗ | ✗ | ✗  | ✗ |

| Method | Signature | Summary |
|--------|-----------|---------|
| `compatible_with` | `const fn compatible_with(self, other: LockMode) -> bool` | Whether two modes may be held on one resource at once (the matrix above). |
| `join` | `const fn join(self, other: LockMode) -> LockMode` | The least mode granting everything both grant — what an upgrade resolves to. |
| `covers` | `const fn covers(self, other: LockMode) -> bool` | Whether holding `self` already grants everything `other` would. |
| `is_exclusive` | `const fn is_exclusive(self) -> bool` | Whether this is `Exclusive`. |
| `is_intention` | `const fn is_intention(self) -> bool` | Whether this is an intention mode (IS, IX, SIX). |

**Examples**

```rust
use lock_db::LockMode;

// Compatibility.
assert!(LockMode::IntentionShared.compatible_with(LockMode::IntentionExclusive));
assert!(!LockMode::IntentionExclusive.compatible_with(LockMode::Shared));
assert!(!LockMode::Exclusive.compatible_with(LockMode::IntentionShared));

// The lattice: join is the least upper bound, covers is the order.
assert_eq!(LockMode::Shared.join(LockMode::IntentionExclusive), LockMode::SharedIntentionExclusive);
assert!(LockMode::SharedIntentionExclusive.covers(LockMode::Shared));
assert!(!LockMode::Shared.covers(LockMode::IntentionExclusive));
```

`compatible_with` and `join` are symmetric:

```rust
use lock_db::LockMode;

let all = [
    LockMode::IntentionShared,
    LockMode::IntentionExclusive,
    LockMode::Shared,
    LockMode::SharedIntentionExclusive,
    LockMode::Exclusive,
];
for a in all {
    for b in all {
        assert_eq!(a.compatible_with(b), b.compatible_with(a));
        assert_eq!(a.join(b), b.join(a));
    }
}
```

---

### `KeyRange`

An inclusive range of `u64` keys, `[start, end]` — the unit a range lock is
taken on. `no_std`. Inclusive bounds avoid the overflow corner that half-open
intervals hit at `u64::MAX` and make a single-key lock just `KeyRange::point(k)`.

```rust
pub struct KeyRange { /* private */ }
```

| Method | Signature | Summary |
|--------|-----------|---------|
| `new` | `const fn new(start: u64, end: u64) -> Option<KeyRange>` | The range `[start, end]`, or `None` if `start > end`. |
| `point` | `const fn point(key: u64) -> KeyRange` | The single-key range `[key, key]`. |
| `start` / `end` | `const fn start(self) -> u64` / `const fn end(self) -> u64` | The inclusive bounds. |
| `contains` | `const fn contains(self, key: u64) -> bool` | Whether `key` is within the range. |
| `overlaps` | `const fn overlaps(self, other: KeyRange) -> bool` | Whether two ranges share at least one key. |

**Examples**

```rust
use lock_db::KeyRange;

let r = KeyRange::new(100, 200).unwrap();
assert!(r.contains(150));
assert!(!r.contains(201));

// Inclusive bounds: [100, 200] and [200, 300] share key 200.
assert!(r.overlaps(KeyRange::new(200, 300).unwrap()));
assert!(!r.overlaps(KeyRange::new(201, 300).unwrap()));

// A single key, and an inverted range.
assert_eq!(KeyRange::point(42).start(), 42);
assert!(KeyRange::new(5, 4).is_none());
```

---

### Identifiers

`TxnId` and `ResourceId` are thin, `#[repr(transparent)]` newtypes over `u64`.
lock-db never interprets them: the caller decides what a transaction id means and
how to map a database, table, page, row, or key space to a single `ResourceId`.
Both are `no_std`, `Copy`, `Ord`, and `Hash`.

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
| `Conflict` | [`try_acquire`](#lockmanagertry_acquire), [`try_acquire_range`](#lockmanagertry_acquire_range) | The lock cannot be granted without blocking. |
| `NotHeld` | [`release`](#lockmanagerrelease), [`release_range`](#lockmanagerrelease_range) | No matching lock is held for this transaction. |

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
lm.try_acquire_range(TxnId::new(1), ResourceId::new(2), KeyRange::point(5), LockMode::Shared).unwrap();
```

The prelude contains `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, `LockError`,
and (under the `std` feature) `LockManager`.

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Enables `LockManager` and the `std::error::Error` impl. With `std` off, the crate is `no_std` and exposes only `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, and `LockError`. |
| `serde` | no | Derives `serde::Serialize` / `Deserialize` on `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, and `LockError`. |

---

## Usage patterns

**Two-phase locking (2PL).** Acquire every lock a transaction needs during its
growing phase; at commit or abort, drop them all at once with
[`release_all`](#lockmanagerrelease_all).

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let txn = TxnId::new(1);

lm.try_acquire(txn, ResourceId::new(1), LockMode::Shared).unwrap();
lm.try_acquire(txn, ResourceId::new(2), LockMode::Exclusive).unwrap();

let released = lm.release_all(txn);
assert_eq!(released, 2);
```

**Hierarchical write.** Hold intention locks coarse-to-fine, then the fine lock.
See [Hierarchical locking](#hierarchical-locking).

**Phantom protection.** Range-lock the span a predicate read covers, so no other
transaction can insert into it before you commit:

```rust
use lock_db::prelude::*;

let lm = LockManager::new();
let index = ResourceId::new(1);
let reader = TxnId::new(1);

// "SELECT ... WHERE id BETWEEN 100 AND 200" locks the whole range.
lm.try_acquire_range(reader, index, KeyRange::new(100, 200).unwrap(), LockMode::Shared).unwrap();

// An insert of id 150 by another transaction is now blocked.
assert!(lm.try_acquire_range(TxnId::new(2), index, KeyRange::point(150), LockMode::Exclusive).is_err());
```

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>
