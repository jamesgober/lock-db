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

## v0.3.0 -- hierarchical granularities + intention locks + range locks

Exit criteria:
- [ ] New surface tested; hot paths benchmarked.

---

## v0.4.0 -- wait-for graph + deadlock detection + victim selection + feature freeze

Exit criteria:
- [ ] No `todo!`/`unimplemented!`. Feature freeze declared.

---

## v0.5.0 -- adversarial contention + deadlock-storm tests + API freeze

Exit criteria:
- [ ] Public API frozen (recorded here). `cargo audit` + `cargo deny` clean.

---

## v0.6.0 -> v1.0.0 -- Alpha / Beta / RC / Stable

Integrate against real consumers, broaden testing, capture final benchmarks, then freeze the public API until 2.0 and publish.

---

## Out of scope for 1.0

- Transaction lifecycle / MVCC - that is `txn-db`; lock-db only manages locks.
- Optimistic concurrency / SSI validation - the transaction layer's concern.
- The data pages themselves - `page-db`.
