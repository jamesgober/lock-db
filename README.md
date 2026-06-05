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
        <strong>Status: pre-1.0, in active development.</strong> This is the <code>v0.1.0</code> scaffold &mdash; structure, tooling, and CI gates are in place; the implementation lands across the 0.x series per <a href="./dev/ROADMAP.md"><code>dev/ROADMAP.md</code></a>. The public API is frozen at <code>1.0.0</code>.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

- **Lock modes** &mdash; shared (S), exclusive (X), and intention locks (IS, IX, SIX) with a full compatibility matrix
- **Multiple granularities** &mdash; database, table, page, and row-level locks with intention-lock coupling
- **Row and range locks** &mdash; lock individual keys or key ranges (for predicate / phantom protection)
- **Lock table** &mdash; a sharded, contention-aware table mapping resources to their lock queues
- **Deadlock detection** &mdash; a wait-for graph with cycle detection and configurable victim selection
- **Wait / grant queues** &mdash; fair queuing with upgrade handling (S to X)

<br>
<hr>
<br>

## Installation

```toml
[dependencies]
lock-db = "0.1"
```

<br>

## API Overview

For the complete reference, see [`docs/API.md`](./docs/API.md).

- [`Lock modes`](./docs/API.md)
- [`Multiple granularities`](./docs/API.md)
- [`Row and range locks`](./docs/API.md)
- [`Lock table`](./docs/API.md)

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
