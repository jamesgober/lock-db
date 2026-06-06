//! Range locking for phantom protection.
//!
//! A transaction that reads "every key in [100, 200]" must stop other
//! transactions from inserting a new key inside that span before it commits —
//! otherwise the same query run twice returns different rows (a phantom).
//! Locking the range, not just the rows that exist now, closes that gap.
//!
//! Run with: `cargo run --example range_locks`

use lock_db::prelude::*;

fn main() {
    let lm = LockManager::new();
    let index = ResourceId::new(1); // the key space (e.g. an index) being protected

    // Reader scans and range-locks [100, 200].
    let reader = TxnId::new(1);
    let scanned = KeyRange::new(100, 200).unwrap();
    lm.try_acquire_range(reader, index, scanned, LockMode::Shared)
        .expect("range is free");
    println!("reader holds a shared lock over keys [100, 200]");

    // Another reader may share the overlapping range.
    let reader2 = TxnId::new(2);
    lm.try_acquire_range(
        reader2,
        index,
        KeyRange::new(150, 250).unwrap(),
        LockMode::Shared,
    )
    .expect("shared ranges overlap freely");
    println!("a second reader shares the overlapping range [150, 250]");

    // A writer trying to insert key 150 — inside the scanned range — is blocked.
    let writer = TxnId::new(3);
    match lm.try_acquire_range(writer, index, KeyRange::point(150), LockMode::Exclusive) {
        Err(LockError::Conflict) => println!("writer blocked from inserting key 150 (no phantom)"),
        other => panic!("expected conflict, got {other:?}"),
    }

    // A write outside the scanned range is fine.
    lm.try_acquire_range(writer, index, KeyRange::point(500), LockMode::Exclusive)
        .expect("key 500 is outside any locked range");
    println!("writer inserts key 500 outside the locked range");

    // Readers commit, releasing their ranges.
    lm.release_all(reader);
    lm.release_all(reader2);

    // Now the previously blocked insert at 150 succeeds.
    lm.try_acquire_range(writer, index, KeyRange::point(150), LockMode::Exclusive)
        .expect("range is free after readers commit");
    println!("after readers commit, the writer inserts key 150");
    lm.release_all(writer);
}
