<h1 align="center">
    <img width="90px" height="auto" src="https://raw.githubusercontent.com/jamesgober/jamesgober/main/media/icons/hexagon-3.svg" alt="Triple Hexagon">
    <br>
    <b>CHANGELOG</b>
</h1>
<p>
  All notable changes to <code>lock-db</code> will be documented in this file. The format is based on <a href="https://keepachangelog.com/en/1.1.0/">Keep a Changelog</a>,
  and this project adheres to <a href="https://semver.org/spec/v2.0.0.html/">Semantic Versioning</a>.
</p>

## [Unreleased]

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
[Unreleased]: https://github.com/jamesgober/lock-db/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jamesgober/lock-db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/lock-db/releases/tag/v0.1.0
