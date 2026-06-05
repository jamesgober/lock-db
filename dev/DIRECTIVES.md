# lock-db -- Engineering Directives

> Engineering standards and the definition of done for this project. Read alongside `REPS.md` (root, authoritative) and `dev/ROADMAP.md` (current phase). If anything here conflicts with `REPS.md`, `REPS.md` wins.

---

## 0. Philosophy

This library is built and maintained to a production standard and treated as a flagship piece of work. Plan the full path, then build one verified step at a time. "Good enough" is treated as a defect.

---

## 1. What this is

lock-db is the lock manager for a transactional database: the component that lets many transactions touch shared data at once without corrupting it, and that notices when they have deadlocked and breaks the tie. It provides row and range locks across multiple granularities (database, table, page, row) with the standard lock modes and a compatibility matrix, and it builds a wait-for graph to detect deadlock cycles and select a victim to abort.

---

## 2. Engineering law (non-negotiable)

- **Performance** -- peak is the baseline; borrow over clone; no steady-state hot-path allocation; no "faster" claim without `criterion` numbers.
- **Concurrency** -- correctness under contention is proven with `loom`, not assumed.
- **Correctness** -- the invariants in section 4 are covered by property tests.
- **Security** -- all untrusted input validated; every allocation bounded; library code never panics on hostile input; parse/recovery paths fuzzed.
- **Architecture** -- SOLID, KISS, YAGNI; one responsibility; trait seams are the extension points.
- **Cross-platform** -- Linux/macOS/Windows first-class, verified by CI.
- **Error handling** -- every fallible path returns `Result`; errors are never silently swallowed.
- **Production-ready** -- no commented-out code, no stray `println!`/`dbg!`; every public item has rustdoc with a runnable example.

---

## 3. Definition of done

1. Compiles clean on Linux/macOS/Windows, stable and MSRV.
2. `fmt`, `clippy -D warnings`, `test --all-features`, `cargo doc -D warnings` clean.
3. `cargo audit` + `cargo deny check` pass.
4. No `unwrap`/`expect`/`todo!`/`dbg!` in shipping code; `unsafe` only with `// SAFETY:`.
5. A Tier-1 API exists and headlines the docs.
6. Property tests cover every section-4 invariant; `loom` covers every concurrent path.
7. Hot-path changes carry benchmarks; no regression over 5%.
8. Docs and `CHANGELOG.md` updated.

---

## 4. Project-specific invariants

- The lock compatibility matrix is the correctness core: an incompatible grant must never be issued, property-tested across arbitrary request orderings.
- Deadlock detection must find every genuine cycle and never falsely abort a non-deadlocked transaction.
- Lock-table internals are concurrent; every lock-free or sharded path carries a `loom` model check.

Per-phase exit criteria in `dev/ROADMAP.md` encode these.

---

## 5. Integration points

- `txn-db`: transactions acquire and release locks here to enforce isolation
- `page-db`: page-granularity locks coordinate with the paged store
- `index-db`: range locks protect B+tree key ranges against phantoms
- storage engines: any engine needing pessimistic concurrency control

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>
