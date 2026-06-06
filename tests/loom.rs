//! Loom concurrency model checks for the sharded lock table.
//!
//! These run only under `--cfg loom`, where the lock manager swaps its
//! `std::sync::Mutex` for loom's instrumented version. Loom then explores every
//! legal thread interleaving of the operations below and fails if any of them
//! violates the asserted invariant. They are excluded from the normal test run
//! because exhaustive interleaving exploration is far slower than a unit test.
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --test loom --release
//! ```

#![cfg(loom)]
#![allow(clippy::unwrap_used)]

use lock_db::{Acquisition, KeyRange, LockManager, LockMode, ResourceId, TxnId};
use loom::sync::Arc;

/// Two transactions race to take the same resource exclusively. Whoever wins,
/// the manager must never report two simultaneous holders, and the loser's
/// request must fail cleanly.
#[test]
fn loom_exclusive_is_mutually_exclusive() {
    loom::model(|| {
        let lm = Arc::new(LockManager::with_shards(1));
        let res = ResourceId::new(1);

        let lm2 = Arc::clone(&lm);
        let other = loom::thread::spawn(move || {
            if lm2
                .try_acquire(TxnId::new(2), res, LockMode::Exclusive)
                .is_ok()
            {
                assert_eq!(lm2.holder_count(res), 1);
                lm2.release(TxnId::new(2), res).unwrap();
            }
        });

        if lm
            .try_acquire(TxnId::new(1), res, LockMode::Exclusive)
            .is_ok()
        {
            assert_eq!(lm.holder_count(res), 1);
            lm.release(TxnId::new(1), res).unwrap();
        }

        other.join().unwrap();

        // Every successful acquire was released; the resource ends up free.
        assert_eq!(lm.holder_count(res), 0);
    });
}

/// A reader and a writer contend on one resource. The reader and writer locks
/// are incompatible, so at most one of the two can hold the resource at a time.
#[test]
fn loom_shared_and_exclusive_never_coexist() {
    loom::model(|| {
        let lm = Arc::new(LockManager::with_shards(1));
        let res = ResourceId::new(1);

        let lm2 = Arc::clone(&lm);
        let writer = loom::thread::spawn(move || {
            if lm2
                .try_acquire(TxnId::new(2), res, LockMode::Exclusive)
                .is_ok()
            {
                // Holding X means no reader can be present.
                assert_eq!(lm2.mode_held(TxnId::new(1), res), None);
                lm2.release(TxnId::new(2), res).unwrap();
            }
        });

        if lm.try_acquire(TxnId::new(1), res, LockMode::Shared).is_ok() {
            // Holding S means no writer can be present.
            assert_eq!(lm.mode_held(TxnId::new(2), res), None);
            lm.release(TxnId::new(1), res).unwrap();
        }

        writer.join().unwrap();
        assert_eq!(lm.holder_count(res), 0);
    });
}

/// Two transactions race to take overlapping range locks in incompatible modes.
/// At most one can win; the manager must never end with both held.
#[test]
fn loom_overlapping_ranges_are_mutually_exclusive() {
    loom::model(|| {
        let lm = Arc::new(LockManager::with_shards(1));
        let space = ResourceId::new(1);
        let range = KeyRange::new(0, 10).unwrap();

        let lm2 = Arc::clone(&lm);
        let other = loom::thread::spawn(move || {
            if lm2
                .try_acquire_range(TxnId::new(2), space, range, LockMode::Exclusive)
                .is_ok()
            {
                assert_eq!(lm2.range_count(space), 1);
                lm2.release_range(TxnId::new(2), space, range).unwrap();
            }
        });

        if lm
            .try_acquire_range(TxnId::new(1), space, range, LockMode::Exclusive)
            .is_ok()
        {
            assert_eq!(lm.range_count(space), 1);
            lm.release_range(TxnId::new(1), space, range).unwrap();
        }

        other.join().unwrap();
        assert_eq!(lm.range_count(space), 0);
    });
}

/// The classic two-transaction deadlock under every interleaving: T1 holds A and
/// wants B, T2 holds B and wants A. Whatever the schedule, the deadlock-aware
/// `request` path must never leave a cycle undetected — after each thread acts
/// (aborting its victim on detection), no deadlock may remain. This also
/// exercises the `waits` / shard lock ordering: loom would flag a mutex-ordering
/// deadlock in the manager itself if one existed.
#[test]
fn loom_two_transaction_deadlock_is_always_resolved() {
    loom::model(|| {
        let lm = Arc::new(LockManager::with_shards(1));
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));

        let lm2 = Arc::clone(&lm);
        let other = loom::thread::spawn(move || {
            if lm2.request(t2, b, LockMode::Exclusive) == Acquisition::Granted {
                if let Acquisition::Deadlock(d) = lm2.request(t2, a, LockMode::Exclusive) {
                    lm2.release_all(d.victim);
                }
            }
        });

        if lm.request(t1, a, LockMode::Exclusive) == Acquisition::Granted {
            if let Acquisition::Deadlock(d) = lm.request(t1, b, LockMode::Exclusive) {
                lm.release_all(d.victim);
            }
        }

        other.join().unwrap();

        // No cycle may survive: a missed detection would leave one here.
        assert!(lm.find_deadlock().is_none());
    });
}
