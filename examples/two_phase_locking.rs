//! Two-phase locking: take locks during the growing phase, then drop the whole
//! set at once at commit or abort with `release_all`.
//!
//! Run with: `cargo run --example two_phase_locking`

use lock_db::prelude::*;

/// Simulates one transaction reading two rows and writing a third, then
/// committing. Returns the number of locks released at commit.
fn run_transaction(lm: &LockManager, txn: TxnId) -> usize {
    // Growing phase: acquire as the transaction touches data.
    lm.try_acquire(txn, ResourceId::new(10), LockMode::Shared)
        .expect("row 10 free");
    lm.try_acquire(txn, ResourceId::new(11), LockMode::Shared)
        .expect("row 11 free");
    lm.try_acquire(txn, ResourceId::new(12), LockMode::Exclusive)
        .expect("row 12 free");

    // ... transaction does its work here ...

    // Shrinking phase: a single call drops every lock the transaction holds.
    lm.release_all(txn)
}

fn main() {
    let lm = LockManager::new();
    let txn = TxnId::new(1);

    let released = run_transaction(&lm, txn);
    println!(
        "transaction {} committed, released {released} locks",
        txn.get()
    );
    assert_eq!(released, 3);

    // Everything is free again, so the rows can be locked exclusively.
    for id in [10, 11, 12] {
        let res = ResourceId::new(id);
        assert_eq!(lm.holder_count(res), 0);
        lm.try_acquire(TxnId::new(2), res, LockMode::Exclusive)
            .expect("row free after commit");
    }
    println!("all rows lockable again after commit");
}
