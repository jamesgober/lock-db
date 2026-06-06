//! Many threads sharing one lock manager. Each thread acts as a transaction
//! that repeatedly takes and releases an exclusive lock on a contended row,
//! while a separate atomic counter verifies the lock is genuinely exclusive.
//!
//! Run with: `cargo run --example concurrent`

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use lock_db::prelude::*;

const THREADS: u64 = 8;
const ITERS: u64 = 50_000;

fn main() {
    let lm = Arc::new(LockManager::new());
    // Counts how many threads believe they hold the exclusive lock at once.
    // It must never exceed 1.
    let inside = Arc::new(AtomicUsize::new(0));
    let row = ResourceId::new(1);

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let lm = Arc::clone(&lm);
        let inside = Arc::clone(&inside);
        handles.push(thread::spawn(move || {
            let txn = TxnId::new(t);
            let mut acquired = 0u64;
            for _ in 0..ITERS {
                if lm.try_acquire(txn, row, LockMode::Exclusive).is_ok() {
                    // Critical section: assert mutual exclusion.
                    let concurrent = inside.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(concurrent, 0, "exclusive lock was not exclusive");
                    inside.fetch_sub(1, Ordering::SeqCst);

                    lm.release(txn, row).expect("we hold the row");
                    acquired += 1;
                }
            }
            acquired
        }));
    }

    let mut total = 0;
    for h in handles {
        total += h.join().expect("worker thread panicked");
    }

    assert_eq!(lm.holder_count(row), 0);
    println!(
        "{THREADS} threads x {ITERS} attempts: {total} exclusive grants, mutual exclusion held"
    );
}
