# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---


## [Unreleased]

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
[Unreleased]: https://github.com/jamesgober/lock-db/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/jamesgober/lock-db/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/lock-db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/lock-db/releases/tag/v0.1.0
