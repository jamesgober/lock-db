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

use lock_db::{KeyRange, LockManager, LockMode, ResourceId, TxnId};
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
