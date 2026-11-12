# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---


## [Unreleased]

## [1.1.0] - 2026-11-11

Fixes from an independent review: the deadlock-aware path no longer
serializes on one global mutex or rebuilds the wait-for graph on every call, a
deadlock victim is fenced until it has rolled back, and `request` grants in
first-in, first-out order. Additive to the 1.0 API; no MSRV change.

### Added

- `LockError::Aborted`, returned by `try_acquire` and `try_acquire_range` for a
  transaction chosen as a deadlock victim that has not been released since.
- `LockManager::is_aborted(txn)`, so a victim's own thread can ask whether it
  was chosen.

### Changed

- `request` keeps a first-in, first-out wait queue per resource. A request is
  granted only when it is compatible with every holder and with every
  transaction queued ahead of it, so a waiting writer is no longer overtaken by
  a stream of newer readers. Upgraders queue ahead of transactions that hold
  nothing on the resource. A `request` that 1.0 granted by overtaking a queued,
  conflicting waiter now returns `Waiting`. `try_acquire` is unchanged and does
  not consult the queue.
- A deadlock victim is marked when `request` or `find_deadlock` names it. Its
  next `request` returns `Acquisition::Deadlock` naming itself and its next
  `try_acquire` / `try_acquire_range` returns `LockError::Aborted`, until it
  calls `release_all` on itself after being told. Releasing it from another
  thread frees its locks but keeps the mark; a second `release_all` (the
  victim's own, if it gives up without asking) clears it too, so no mark is left
  behind. In 1.0 the victim's thread was
  never told and a later `request` handed it fresh locks after it had lost the
  earlier ones.
- Re-acquiring a range lock is idempotent: a request inside a range already
  held in a covering mode changes nothing, and a request for exactly a held
  range upgrades it in place to the join. In 1.0 each call added another
  holder, so a retry loop grew the space without bound.

### Performance

- Deadlock detection walks only the wait-for edges reachable from the new
  waiter, reading each edge from the lock table as it is reached, instead of
  rebuilding the whole graph under a global mutex on every conflicting call.
  The global wait mutex is gone: waits live in a table sharded by transaction
  id, and no global lock is held across a walk. A conflicting `request` with
  5,000 unrelated waiters went from 0.95 ms to 0.7 us.
- `find_deadlock` and `WaitForGraph::detect_cycle` share one visited set across
  every starting node and are linear in the size of the graph (was quadratic).
  A 2,000-waiter acyclic chain went from 234 ms to 0.74 ms.
- `release_all` visits only the shards the transaction holds locks in (tracked
  per transaction), not every shard. Acquire one lock plus `release_all` at
  1,024 shards went from 39 us to 0.39 us. Recording the shards costs about
  50 to 70 ns on a transaction's first lock in a shard and its last release
  there.

### Fixed

- Documentation: the module docs no longer say deadlock detection arrives in a
  later milestone; the "exact detection" claim now states its precondition
  (every abandoned wait is cleared with `cancel_wait` or `release_all`);
  `release_all` cost is described correctly; the README no longer says txn-db,
  page-db, and index-db use lock-db (none does); `LockMode` documents that its
  `Ord` is declaration order, not the privilege lattice; `VictimPolicy` notes
  that a restarted transaction with a new, larger id is the youngest again.

## [1.0.0] - 2026-06-06

First stable release. The public API is frozen until 2.0. No code changes from
v0.5.0 — this release captures the final benchmarks, confirms every quality gate
on Linux, macOS, and Windows (stable and MSRV 1.85), and promotes the crate to
1.0.

The complete, stable surface:

- Core (`no_std`): `LockMode`, `KeyRange`, `TxnId`, `ResourceId`, `LockError`.
- `std`: `LockManager`, `Acquisition`, `WaitForGraph`, `VictimPolicy`,
  `Deadlock`, and the `prelude`.

### Changed

- Version promoted to 1.0.0; the public API is stable and will not change before
  a 2.0 major release.

## [0.5.0] - 2026-06-05

Hardening and the formal API freeze. No new public surface — the feature set
frozen at v0.4.0 is now exercised under adversarial load, and the public API is
recorded as frozen until 2.0.

### Added

- Adversarial contention stress test: many threads hammering a shared resource
  pool with random shared/exclusive acquisitions, verifying mutual exclusion
  through an independent per-resource counter.
- Deadlock-storm stress test: many threads acquiring resource pairs in random
  order via `request`, asserting that detect-and-abort keeps every transaction
  making progress (a liveness check that fails loudly rather than hanging).

### Changed

- **Public API frozen.** The surface is stable until 2.0; remaining 0.x releases
  integrate against real consumers and capture final benchmarks. `cargo audit`
  and `cargo deny` are clean.

## [0.4.0] - 2026-06-05

Wait-for deadlock detection, and the feature freeze. This release completes the
1.0 feature set: a wait-for graph with cycle detection and victim selection, and
a deadlock-aware acquisition path on the lock manager. The public API is frozen
as of this release; remaining 0.x work is hardening only.

### Added

- `WaitForGraph` — a wait-for graph with iterative-DFS cycle detection
  (`detect_cycle`, `cycle_from`), edge maintenance (`add_wait`, `add_waits`,
  `clear_waiter`, `remove_txn`), and `pick_victim`.
- `VictimPolicy` (`Youngest` default, `Oldest`) and `Deadlock` (`victim` +
  `cycle`).
- `Acquisition` — the outcome of the new deadlock-aware acquisition
  (`Granted` / `Waiting` / `Deadlock`).
- `LockManager::request` — deadlock-aware acquire: grants, or registers a wait
  and reports `Waiting`, or reports `Deadlock` with a victim when the wait closes
  a cycle. Detection rebuilds the graph from the current lock table, so it never
  aborts a transaction that is not genuinely deadlocked.
- `LockManager::cancel_wait`, `find_deadlock` (on-demand/periodic detection), and
  `waiting_count`.
- Example `deadlock`; a `WaitForGraph` property test against Kahn's algorithm; a
  `loom` model check for the classic two-transaction deadlock; a `request`
  benchmark.

### Changed

- `LockManager::release_all` now also clears a transaction's pending wait.
- **API frozen.** As of v0.4.0 the public API is stable; no further additions are
  planned before 1.0.

## [0.3.0] - 2026-06-05

Multi-granularity and range locking. This release extends the lock-table core
with the full MGL mode set and lattice upgrades, and adds key-range locks for
phantom protection. Acquisition remains `try`-style; wait queues and deadlock
detection follow in later 0.x releases.

### Added

- Three new `LockMode` variants — `IntentionShared` (IS), `IntentionExclusive`
  (IX), and `SharedIntentionExclusive` (SIX) — completing the standard MGL mode
  set, with the full compatibility matrix.
- `LockMode::join` (lattice least upper bound) and `LockMode::is_intention`.
- `KeyRange` — an inclusive `[start, end]` key interval with `new`, `point`,
  `start`, `end`, `contains`, and `overlaps`.
- `LockManager::try_acquire_range`, `release_range`, and `range_count` for
  range locking with overlap-based conflict detection, sharded by key space.
- Examples `hierarchy` (multi-granularity protocol) and `range_locks` (phantom
  protection); property and `loom` coverage for range locks; a range benchmark.

### Changed

- `LockManager::try_acquire` upgrades now resolve to the lattice join of the
  held and requested modes (e.g. `Shared` + `IntentionExclusive` → `SIX`),
  granted when the joined mode is compatible with every other holder.
- `LockManager::release_all` now releases a transaction's range locks as well as
  its point locks, and counts both.

## [0.2.0] - 2026-06-05

The lock-table core. This release implements the compatibility matrix and a
sharded, non-blocking lock table on top of the v0.1.0 scaffold. Acquisition is
`try`-style: a request that cannot be granted returns an error rather than
blocking. Wait queues, hierarchical and range locks, and deadlock detection
follow in later 0.x releases.

### Added

- `LockMode` — shared and exclusive modes, with `compatible_with`, `covers`, and
  `is_exclusive`. The compatibility matrix is a single `const fn`.
- `TxnId` and `ResourceId` — opaque `u64` newtypes for transactions and
  lockable resources, with `new`/`get` and `From`/`Into` conversions.
- `LockError` — `Conflict` and `NotHeld`, `#[non_exhaustive]`, with `Display`
  and (under `std`) `std::error::Error`.
- `LockManager` — a sharded lock table with `new`, `with_shards`, `try_acquire`,
  `release`, `release_all`, `holder_count`, `mode_held`, and `shards`. Supports
  re-entrant acquisition, shared-to-exclusive upgrade of a sole holder, and
  bulk release proportional to the locks a transaction holds.
- `prelude` module re-exporting the public surface.
- Property tests cross-checking the manager against a reference model over
  arbitrary acquire/release sequences; `loom` model checks for the concurrent
  acquire/release paths; `criterion` benchmarks for the hot paths.

### Changed

- The core types (`LockMode`, `TxnId`, `ResourceId`, `LockError`) are
  `no_std`-compatible; `LockManager` is gated behind the `std` feature.

## [0.1.0] - 2026-06-05

Initial scaffold and repository bootstrap. No domain logic yet &mdash; this release establishes the structure, tooling, and quality gates the implementation will be built on.

### Added

- `Cargo.toml` with crate metadata, Rust 2024 edition, MSRV 1.85, dual `Apache-2.0 OR MIT` license.
- `README.md`, `docs/API.md`, `CONTRIBUTING.md`, and a documentation skeleton.
- `dev/DIRECTIVES.md` and `dev/ROADMAP.md` (committed engineering standards + plan).
- `REPS.md` compliance baseline; `deny.toml`, `clippy.toml`, `rustfmt.toml`.
- `.github/workflows/ci.yml` (Node 24 actions; fmt, clippy, test, doc, audit, deny) and `.github/FUNDING.yml`.

<!-- LINKS -->
[Unreleased]: https://github.com/jamesgober/lock-db/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/jamesgober/lock-db/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/jamesgober/lock-db/compare/v0.5.0...v1.0.0
[0.5.0]: https://github.com/jamesgober/lock-db/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/lock-db/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/lock-db/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/lock-db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/lock-db/releases/tag/v0.1.0
