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
> **Version: 1.0.0 — stable.** The public API is frozen until 2.0. The full
> surface is in place: the five MGL modes, hierarchical and range locks, and
> wait-for deadlock detection (the deadlock-aware [`request`](#lockmanagerrequest)
> and the standalone [`WaitForGraph`](#waitforgraph)), verified by property,
> `loom`, and adversarial stress suites.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Concurrency model](#concurrency-model)
- [Hierarchical locking](#hierarchical-locking)
- [Deadlock detection](#deadlock-detection)
- [Public API](#public-api)
  - [`LockManager`](#lockmanager)
    - [`new`](#lockmanagernew)
    - [`with_shards`](#lockmanagerwith_shards)
    - [`try_acquire`](#lockmanagertry_acquire)
    - [`release`](#lockmanagerrelease)
    - [`release_all`](#lockmanagerrelease_all)
    - [`try_acquire_range`](#lockmanagertry_acquire_range)
    - [`release_range`](#lockmanagerrelease_range)
    - [`request`](#lockmanagerrequest)
    - [`cancel_wait`](#lockmanagercancel_wait)
    - [`find_deadlock`](#lockmanagerfind_deadlock)
    - [`holder_count`](#lockmanagerholder_count)
    - [`mode_held`](#lockmanagermode_held)
    - [`range_count`](#lockmanagerrange_count)
    - [`waiting_count`](#lockmanagerwaiting_count)
    - [`shards`](#lockmanagershards)
  - [`Acquisition`](#acquisition)
  - [`WaitForGraph`](#waitforgraph)
  - [`VictimPolicy` and `Deadlock`](#victimpolicy-and-deadlock)
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
| [`LockManager`](#lockmanager) | The sharded lock table. Point and range locks, deadlock-aware `request`. |
| [`LockMode`](#lockmode) | The five MGL modes and their compatibility / lattice rules. |
| [`KeyRange`](#keyrange) | An inclusive key interval — the unit a range lock protects. |
| [`WaitForGraph`](#waitforgraph) | Wait-for graph: cycle detection and victim selection. |
| [`Acquisition`](#acquisition) | The outcome of a deadlock-aware `request`. |
| [`TxnId`](#identifiers) / [`ResourceId`](#identifiers) | Opaque `u64` identifiers the caller assigns. |
| [`LockError`](#lockerror) | Why an operation failed. |

---

## Installation

```toml
[dependencies]
lock-db = "1"
```

To enable `serde` derives on the public types:

```toml
[dependencies]
lock-db = { version = "1", features = ["serde"] }
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

## Deadlock detection

[`try_acquire`](#lockmanagertry_acquire) never blocks and never tracks anything:
on conflict the caller is free to retry, but the manager has no idea who is
waiting for whom, so it cannot tell a transient conflict from a genuine
deadlock. [`request`](#lockmanagerrequest) is the deadlock-aware path. On
conflict it records, in a **wait-for graph**, that the requesting transaction is
waiting for the current holders, then checks whether that closed a cycle.

- No cycle → [`Acquisition::Waiting`]. The caller suspends the transaction (with
  its own scheduler) and calls `request` again later to retry.
- Cycle → [`Acquisition::Deadlock`] carrying a [`Deadlock`](#victimpolicy-and-deadlock)
  with the cycle and a chosen `victim`. The caller aborts the victim with
  [`release_all`](#lockmanagerrelease_all), which frees its locks and clears its
  wait, breaking the cycle.

lock-db does not park threads; the transaction layer owns suspension and retry.
Detection is **exact**: the graph is rebuilt from the current lock table on every
check, so a wait left over from a since-released lock contributes no edge, and a
transaction is never reported as deadlocked unless it genuinely is.

Two detection modes share this machinery: [`request`](#lockmanagerrequest)
detects at the moment a wait is added (responsive), and
[`find_deadlock`](#lockmanagerfind_deadlock) scans the whole wait set on demand
(for a periodic background detector). The wait-for graph itself is exposed as
[`WaitForGraph`](#waitforgraph) for direct use and testing.

> Only transactions that wait through `request` appear in the graph. A
> transaction that spins on `try_acquire` is invisible to detection, and range
> locks are not tracked in it.

---

[`Acquisition::Waiting`]: #acquisition
[`Acquisition::Deadlock`]: #acquisition

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
| [`request`](#lockmanagerrequest) | Deadlock-aware acquire: grant, register a wait, or report a deadlock. |
| [`cancel_wait`](#lockmanagercancel_wait) | Remove a transaction's pending wait. |
| [`find_deadlock`](#lockmanagerfind_deadlock) | Scan the wait set for a deadlock (periodic detection). |
| [`holder_count`](#lockmanagerholder_count) | How many transactions hold a resource. |
| [`mode_held`](#lockmanagermode_held) | The mode a transaction holds, if any. |
| [`range_count`](#lockmanagerrange_count) | How many range locks are live in a space. |
| [`waiting_count`](#lockmanagerwaiting_count) | How many transactions are registered as waiting. |
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

#### `LockManager::request`

```rust
pub fn request(&self, txn: TxnId, res: ResourceId, mode: LockMode) -> Acquisition
```

The deadlock-aware counterpart to [`try_acquire`](#lockmanagertry_acquire).
Returns an [`Acquisition`](#acquisition):

- `Granted` — the lock was granted; proceed.
- `Waiting` — the lock is held incompatibly and `txn` is now recorded in the
  wait-for graph. Suspend the transaction and call `request` again later. No
  deadlock was found.
- `Deadlock(d)` — granting the wait would close a cycle. Abort `d.victim` with
  [`release_all`](#lockmanagerrelease_all). The victim may be `txn` or another
  transaction in the cycle.

The victim is chosen by the [`VictimPolicy::Youngest`](#victimpolicy-and-deadlock)
policy; for a different policy, apply
[`WaitForGraph::pick_victim`](#waitforgraph) to `d.cycle`. Only point locks taken
through `request` participate in detection (see [Deadlock detection](#deadlock-detection)).

> **Performance.** `request` serializes on a single wait-registry mutex, unlike
> the sharded `try_acquire`. Use `try_acquire` when you do not need deadlock
> detection, and `request` when you do.

**Examples**

```rust
use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let (a, b) = (ResourceId::new(1), ResourceId::new(2));
let (t1, t2) = (TxnId::new(1), TxnId::new(2));

assert_eq!(lm.request(t1, a, LockMode::Exclusive), Acquisition::Granted);
assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Granted);
assert_eq!(lm.request(t1, b, LockMode::Exclusive), Acquisition::Waiting);

match lm.request(t2, a, LockMode::Exclusive) {
    Acquisition::Deadlock(d) => {
        assert_eq!(d.victim, TxnId::new(2)); // youngest in the cycle
        lm.release_all(d.victim);            // break the deadlock
    }
    other => panic!("expected a deadlock, got {other:?}"),
}
```

A simple retry-on-wait loop a caller might build (a real one would suspend the
transaction and be woken on release rather than spin):

```rust
use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};

fn acquire_or_abort(lm: &LockManager, txn: TxnId, res: ResourceId, mode: LockMode) -> bool {
    loop {
        match lm.request(txn, res, mode) {
            Acquisition::Granted => return true,
            Acquisition::Waiting => std::hint::spin_loop(),
            Acquisition::Deadlock(d) => {
                lm.release_all(d.victim);
                if d.victim == txn {
                    return false; // we were chosen to abort
                }
            }
        }
    }
}

let lm = LockManager::new();
assert!(acquire_or_abort(&lm, TxnId::new(1), ResourceId::new(1), LockMode::Exclusive));
```

---

#### `LockManager::cancel_wait`

```rust
pub fn cancel_wait(&self, txn: TxnId)
```

Removes any pending wait for `txn` from the wait-for graph. Call this when a
transaction that previously got [`Acquisition::Waiting`](#acquisition) stops
waiting without acquiring the lock (for example a timeout).
[`release_all`](#lockmanagerrelease_all) already clears the wait, so this is only
needed when the transaction is releasing nothing.

**Example**

```rust
use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let res = ResourceId::new(1);
lm.request(TxnId::new(1), res, LockMode::Exclusive);
assert_eq!(lm.request(TxnId::new(2), res, LockMode::Exclusive), Acquisition::Waiting);
lm.cancel_wait(TxnId::new(2));
assert_eq!(lm.waiting_count(), 0);
```

---

#### `LockManager::find_deadlock`

```rust
pub fn find_deadlock(&self) -> Option<Deadlock>
```

Scans the current wait set for a deadlock, returning one if found. This is the
periodic-detection counterpart to the at-wait detection in
[`request`](#lockmanagerrequest): a background task can call it on an interval
instead of (or as well as) acting on `request`'s result. Like `request`, it
rebuilds the graph from the current lock table, so it reports only genuine
deadlocks.

**Example**

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let (a, b) = (ResourceId::new(1), ResourceId::new(2));
lm.request(TxnId::new(1), a, LockMode::Exclusive);
lm.request(TxnId::new(2), b, LockMode::Exclusive);
lm.request(TxnId::new(1), b, LockMode::Exclusive); // T1 waits for T2
assert!(lm.find_deadlock().is_none());
lm.request(TxnId::new(2), a, LockMode::Exclusive); // T2 waits for T1: cycle
let d = lm.find_deadlock().expect("a deadlock");
assert_eq!(d.cycle.len(), 2);
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

#### `LockManager::waiting_count`

```rust
pub fn waiting_count(&self) -> usize
```

Returns the number of transactions currently registered as waiting (via
[`request`](#lockmanagerrequest)). Mostly useful for diagnostics and tests.

```rust
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

let lm = LockManager::new();
let r = ResourceId::new(1);
lm.request(TxnId::new(1), r, LockMode::Exclusive);
lm.request(TxnId::new(2), r, LockMode::Exclusive); // waits
assert_eq!(lm.waiting_count(), 1);
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

### `Acquisition`

The outcome of a deadlock-aware [`request`](#lockmanagerrequest). `std`.

```rust
pub enum Acquisition {
    Granted,
    Waiting,
    Deadlock(Deadlock),
}
```

| Variant | Meaning | Caller should |
|---------|---------|---------------|
| `Granted` | The lock is held. | Proceed. |
| `Waiting` | Recorded as waiting; no cycle. | Suspend and retry `request` later. |
| `Deadlock(Deadlock)` | The wait closes a cycle. | Abort [`Deadlock::victim`](#victimpolicy-and-deadlock). |

`Acquisition` is `#[must_use]`: the whole point of the call is to act on which
variant comes back.

---

### `WaitForGraph`

A directed graph of which transactions are waiting for which, with cycle
detection. [`LockManager`](#lockmanager) builds one internally to detect
deadlocks; it is also public so callers can run their own detection (for example
over wait information they track elsewhere) and so the algorithm is testable in
isolation. `std`.

An edge `a -> b` means transaction `a` is blocked waiting for a lock held by
transaction `b`.

```rust
use lock_db::WaitForGraph;
```

| Method | Signature | Summary |
|--------|-----------|---------|
| `new` | `fn new() -> WaitForGraph` | An empty graph. |
| `add_wait` | `fn add_wait(&mut self, waiter: TxnId, holder: TxnId)` | Record `waiter` waits for `holder` (self-edges ignored). |
| `add_waits` | `fn add_waits(&mut self, waiter: TxnId, holders: &[TxnId])` | Record `waiter` waits for many holders. |
| `clear_waiter` | `fn clear_waiter(&mut self, waiter: TxnId)` | Remove every edge from `waiter`. |
| `remove_txn` | `fn remove_txn(&mut self, txn: TxnId)` | Remove `txn` as waiter and as holder. |
| `detect_cycle` | `fn detect_cycle(&self) -> Option<Vec<TxnId>>` | Any cycle in the graph. |
| `cycle_from` | `fn cycle_from(&self, start: TxnId) -> Option<Vec<TxnId>>` | A cycle reachable from `start`. |
| `pick_victim` | `fn pick_victim(cycle: &[TxnId], policy: VictimPolicy) -> Option<TxnId>` | Choose a victim from a cycle. |
| `is_empty` / `waiter_count` | — | Whether / how many transactions are waiting. |

Detection uses an iterative depth-first search (no recursion, so a long wait
chain cannot overflow the stack).

**Examples**

```rust
use lock_db::{TxnId, VictimPolicy, WaitForGraph};

let mut g = WaitForGraph::new();
// T1 -> T2 -> T3 -> T1 is a deadlock.
g.add_wait(TxnId::new(1), TxnId::new(2));
g.add_wait(TxnId::new(2), TxnId::new(3));
g.add_wait(TxnId::new(3), TxnId::new(1));

let cycle = g.detect_cycle().expect("a cycle");
assert_eq!(cycle.len(), 3);
assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Youngest), Some(TxnId::new(3)));
```

A chain with no back edge has no cycle:

```rust
use lock_db::{TxnId, WaitForGraph};

let mut g = WaitForGraph::new();
g.add_wait(TxnId::new(1), TxnId::new(2));
g.add_wait(TxnId::new(2), TxnId::new(3));
assert!(g.detect_cycle().is_none());
```

---

### `VictimPolicy` and `Deadlock`

`VictimPolicy` chooses which member of a cycle to abort; `Deadlock` is the
detected cycle plus the chosen victim. Both `std`; `VictimPolicy` derives `serde`
under the `serde` feature.

```rust
pub enum VictimPolicy {
    Youngest, // abort the largest TxnId (default)
    Oldest,   // abort the smallest TxnId
}

pub struct Deadlock {
    pub victim: TxnId,
    pub cycle: Vec<TxnId>,
}
```

Transaction ids are taken as a proxy for age — a larger id started later. Both
policies break the cycle correctly; they differ only in which transaction pays.
[`request`](#lockmanagerrequest) and [`find_deadlock`](#lockmanagerfind_deadlock)
use `Youngest`; apply [`WaitForGraph::pick_victim`](#waitforgraph) to
`Deadlock::cycle` for a different choice.

```rust
use lock_db::{TxnId, VictimPolicy, WaitForGraph};

let cycle = [TxnId::new(3), TxnId::new(7), TxnId::new(5)];
assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Youngest), Some(TxnId::new(7)));
assert_eq!(WaitForGraph::pick_victim(&cycle, VictimPolicy::Oldest), Some(TxnId::new(3)));
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
and (under the `std` feature) `LockManager`, `Acquisition`, `WaitForGraph`,
`VictimPolicy`, and `Deadlock`.

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Enables `LockManager`, `Acquisition`, `WaitForGraph`, `VictimPolicy`, `Deadlock`, and the `std::error::Error` impl. With `std` off, the crate is `no_std` and exposes only `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, and `LockError`. |
| `serde` | no | Derives `serde::Serialize` / `Deserialize` on `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, `LockError`, and `VictimPolicy`. |

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

**Deadlock handling.** Use [`request`](#lockmanagerrequest) instead of
`try_acquire` when a transaction is willing to wait; on
[`Acquisition::Deadlock`](#acquisition) abort the named victim. The victim's own
worker, on its next `request`, will get a clean grant or waiting result once the
cycle is gone:

```rust
use lock_db::prelude::*;

fn acquire_or_abort(lm: &LockManager, txn: TxnId, res: ResourceId, mode: LockMode) -> bool {
    loop {
        match lm.request(txn, res, mode) {
            Acquisition::Granted => return true,
            Acquisition::Waiting => {
                // A real caller suspends the transaction and is woken on a
                // release; this loop stands in for that.
                std::hint::spin_loop();
            }
            Acquisition::Deadlock(d) => {
                lm.release_all(d.victim);
                if d.victim == txn {
                    return false; // we were the victim
                }
            }
        }
    }
}
# let lm = LockManager::new();
# assert!(acquire_or_abort(&lm, TxnId::new(1), ResourceId::new(1), LockMode::Exclusive));
```

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>
