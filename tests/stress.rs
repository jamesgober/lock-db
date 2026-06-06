//! Adversarial stress tests.
//!
//! The unit and property tests pin down correctness on small, controlled
//! inputs. These two tests instead pile on contention and provoke deadlocks on
//! purpose, then assert the invariants that must survive any schedule:
//!
//! - **`stress_mixed_shared_exclusive_mutual_exclusion`** runs many threads
//!   hammering a shared pool of resources with random shared and exclusive
//!   `try_acquire`s, and verifies — through an independent per-resource atomic
//!   counter — that the manager never lets an exclusive lock coexist with any
//!   other holder.
//! - **`stress_deadlock_storm_always_makes_progress`** has many threads acquire
//!   pairs of resources in random order — the recipe for deadlock — through the
//!   deadlock-aware `request`. With detect-and-abort, every transaction must
//!   eventually commit; the test fails (rather than hangs) if progress stalls.
//!
//! Counts are kept modest so the suite stays fast, but the assertions are
//! schedule-independent: they hold for every interleaving, not just the lucky
//! ones.

#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;

use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};

/// A tiny per-thread PRNG (xorshift64). Deterministic given the seed, so a
/// failing schedule is at least reproducible for that thread's choices.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn stress_mixed_shared_exclusive_mutual_exclusion() {
    const THREADS: u64 = 8;
    const RESOURCES: u64 = 16;
    const ITERS: usize = 20_000;

    let lm = Arc::new(LockManager::new());
    // Independent shadow of the lock state: per resource, how many readers and
    // writers the *test* believes are inside the critical section. If the
    // manager is correct, a writer is only ever alone.
    let readers: Arc<Vec<AtomicUsize>> =
        Arc::new((0..RESOURCES).map(|_| AtomicUsize::new(0)).collect());
    let writers: Arc<Vec<AtomicUsize>> =
        Arc::new((0..RESOURCES).map(|_| AtomicUsize::new(0)).collect());

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let lm = Arc::clone(&lm);
        let readers = Arc::clone(&readers);
        let writers = Arc::clone(&writers);
        handles.push(thread::spawn(move || {
            let txn = TxnId::new(t);
            let mut rng = t.wrapping_add(1);
            for _ in 0..ITERS {
                let r = (next_rand(&mut rng) % RESOURCES) as usize;
                let res = ResourceId::new(r as u64);
                let exclusive = next_rand(&mut rng) & 1 == 0;
                let mode = if exclusive {
                    LockMode::Exclusive
                } else {
                    LockMode::Shared
                };

                if lm.try_acquire(txn, res, mode).is_err() {
                    continue;
                }

                if exclusive {
                    // Entering an exclusive section: nobody else may be inside.
                    let prior_writers = writers[r].fetch_add(1, Ordering::SeqCst);
                    assert_eq!(prior_writers, 0, "two writers in resource {r}");
                    assert_eq!(
                        readers[r].load(Ordering::SeqCst),
                        0,
                        "writer coexists with a reader in resource {r}"
                    );
                    writers[r].fetch_sub(1, Ordering::SeqCst);
                } else {
                    // Entering a shared section: no writer may be inside.
                    readers[r].fetch_add(1, Ordering::SeqCst);
                    assert_eq!(
                        writers[r].load(Ordering::SeqCst),
                        0,
                        "reader coexists with a writer in resource {r}"
                    );
                    readers[r].fetch_sub(1, Ordering::SeqCst);
                }

                lm.release(txn, res).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    // Every acquire was paired with a release.
    for r in 0..RESOURCES {
        assert_eq!(
            lm.holder_count(ResourceId::new(r)),
            0,
            "resource {r} leaked"
        );
    }
}

#[test]
fn stress_deadlock_storm_always_makes_progress() {
    const THREADS: u64 = 8;
    const RESOURCES: u64 = 6;
    const COMMITS_PER_THREAD: usize = 100;
    // Hard ceilings so a liveness bug fails loudly instead of hanging CI.
    const ATTEMPT_CAP: usize = 200_000;
    const SPIN_BUDGET: usize = 50_000;

    let lm = Arc::new(LockManager::new());
    let next_txn = Arc::new(AtomicU64::new(1));

    // Acquire two resources in the given order; abort (return false) on a
    // detected deadlock or if waiting exhausts its spin budget. The caller
    // releases whatever was acquired.
    fn acquire_pair(
        lm: &LockManager,
        txn: TxnId,
        first: u64,
        second: u64,
        spin_budget: usize,
    ) -> bool {
        for &r in &[first, second] {
            let res = ResourceId::new(r);
            let mut spins = 0;
            loop {
                match lm.request(txn, res, LockMode::Exclusive) {
                    Acquisition::Granted => break,
                    Acquisition::Deadlock(_) => return false,
                    Acquisition::Waiting => {
                        spins += 1;
                        if spins > spin_budget {
                            return false;
                        }
                        thread::yield_now();
                    }
                }
            }
        }
        true
    }

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let lm = Arc::clone(&lm);
        let next_txn = Arc::clone(&next_txn);
        handles.push(thread::spawn(move || {
            let mut rng = t.wrapping_add(1).wrapping_mul(0x9E37_79B9);
            let mut committed = 0usize;
            let mut attempts = 0usize;

            while committed < COMMITS_PER_THREAD {
                attempts += 1;
                assert!(
                    attempts < ATTEMPT_CAP,
                    "thread {t} made no progress: {committed} commits in {attempts} attempts",
                );

                // A fresh transaction id per attempt (a restarted transaction).
                let txn = TxnId::new(next_txn.fetch_add(1, Ordering::Relaxed));

                // Two distinct resources, in an order that varies per attempt so
                // pairs collide head-on and deadlock.
                let a = next_rand(&mut rng) % RESOURCES;
                let mut b = next_rand(&mut rng) % RESOURCES;
                if b == a {
                    b = (a + 1) % RESOURCES;
                }

                let ok = acquire_pair(&lm, txn, a, b, SPIN_BUDGET);
                // Commit or abort: either way, drop everything this txn holds and
                // clear its wait.
                let _ = lm.release_all(txn);
                if ok {
                    committed += 1;
                }
            }
            committed
        }));
    }

    let mut total = 0;
    for h in handles {
        total += h.join().expect("worker thread panicked");
    }

    // Liveness: every transaction the threads set out to commit did commit.
    assert_eq!(total, THREADS as usize * COMMITS_PER_THREAD);
    // Cleanliness: no lingering locks or waits.
    assert_eq!(lm.waiting_count(), 0, "waits leaked");
    for r in 0..RESOURCES {
        assert_eq!(
            lm.holder_count(ResourceId::new(r)),
            0,
            "resource {r} leaked"
        );
    }
    assert!(
        lm.find_deadlock().is_none(),
        "a deadlock survived the storm"
    );
}
