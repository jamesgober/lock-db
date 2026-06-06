# lock-db -- Roadmap

> Path from scaffold to a stable 1.0. Hard parts are front-loaded; each phase has hard exit criteria.
>
> **Anti-deferral rule:** no listed hard task moves to a later phase unless this file records the move and the reason.

---

## v0.1.0 -- Scaffold (DONE)

Compiles, CI green, structure correct, no domain logic.

- [x] Manifest, README, CHANGELOG, REPS, dual license, CI, deny, clippy, rustfmt, FUNDING.
- [x] API surface sketched in `docs/API.md`.

---

## v0.2.0 -- lock table + modes + compatibility matrix + acquire/release (THE HARD PART, NOT DEFERRED) (DONE)

Exit criteria:
- [x] Every public item has rustdoc + a runnable example.
- [x] Core invariants property-tested.

---

## v0.3.0 -- hierarchical granularities + intention locks + range locks (DONE)

Exit criteria:
- [x] New surface tested; hot paths benchmarked.

---

## v0.4.0 -- wait-for graph + deadlock detection + victim selection + feature freeze (DONE)

Exit criteria:
- [x] No `todo!`/`unimplemented!`. Feature freeze declared.

**Feature freeze declared as of v0.4.0.** The public surface (`LockManager`,
`LockMode`, `KeyRange`, `TxnId`, `ResourceId`, `LockError`, `Acquisition`,
`WaitForGraph`, `VictimPolicy`, `Deadlock`, `prelude`) is complete. No further
features before 1.0 — remaining work is hardening, adversarial testing, and the
formal API freeze.

---

## v0.5.0 -- adversarial contention + deadlock-storm tests + API freeze (DONE)

Exit criteria:
- [x] Public API frozen (recorded here). `cargo audit` + `cargo deny` clean.

**Public API frozen as of v0.5.0** (no changes until 2.0):

- Core (`no_std`): `LockMode` (+ `compatible_with`/`join`/`covers`/`is_exclusive`/`is_intention`), `KeyRange` (+ `new`/`point`/`start`/`end`/`contains`/`overlaps`), `TxnId`, `ResourceId`, `LockError` (`Conflict`/`NotHeld`, `#[non_exhaustive]`).
- `std`: `LockManager` (`new`/`with_shards`/`try_acquire`/`release`/`release_all`/`try_acquire_range`/`release_range`/`request`/`cancel_wait`/`find_deadlock`/`holder_count`/`mode_held`/`range_count`/`waiting_count`/`shards`), `Acquisition`, `WaitForGraph`, `VictimPolicy`, `Deadlock`.
- `prelude` re-exports the above.

Hardening landed: adversarial mixed-mode contention stress and a deadlock-storm liveness test (`tests/stress.rs`), on top of the property and `loom` suites.

---

## v0.6.0 -> v1.0.0 -- Alpha / Beta / RC / Stable (DONE — v1.0.0)

Final benchmarks captured, every quality gate green on Linux/macOS/Windows
(stable + MSRV 1.85), public API frozen until 2.0. Promoted straight to v1.0.0:
the API was frozen at v0.5.0 and the property / `loom` / adversarial-stress
suites all pass, so the pre-release soak is collapsed (the first-party consumers
`txn-db`/`index-db` are not built yet, so there is nothing external to integrate
against). Awaiting maintainer tag + crates.io publish.

---

## Out of scope for 1.0

- Transaction lifecycle / MVCC - that is `txn-db`; lock-db only manages locks.
- Optimistic concurrency / SSI validation - the transaction layer's concern.
- The data pages themselves - `page-db`.
