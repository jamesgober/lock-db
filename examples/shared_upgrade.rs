//! Read-then-write: take a row shared, then upgrade it to exclusive once you
//! decide to write. The upgrade succeeds only while you are the sole holder.
//!
//! Run with: `cargo run --example shared_upgrade`

use lock_db::prelude::*;

fn main() {
    let lm = LockManager::new();
    let row = ResourceId::new(1);
    let me = TxnId::new(1);

    // Read the row under a shared lock.
    lm.try_acquire(me, row, LockMode::Shared).expect("row free");
    println!("txn {} holds row {} shared", me.get(), row.get());

    // Sole holder: the upgrade to exclusive succeeds in place.
    lm.try_acquire(me, row, LockMode::Exclusive)
        .expect("sole holder can upgrade");
    assert_eq!(lm.mode_held(me, row), Some(LockMode::Exclusive));
    println!("txn {} upgraded row {} to exclusive", me.get(), row.get());
    lm.release(me, row).expect("we hold the row");

    // Now show the blocked case: a second reader prevents the upgrade.
    let other = TxnId::new(2);
    lm.try_acquire(me, row, LockMode::Shared).expect("row free");
    lm.try_acquire(other, row, LockMode::Shared)
        .expect("shared locks coexist");

    match lm.try_acquire(me, row, LockMode::Exclusive) {
        Err(LockError::Conflict) => {
            println!(
                "upgrade refused: another reader still holds row {}",
                row.get()
            );
            // The original shared lock is untouched.
            assert_eq!(lm.mode_held(me, row), Some(LockMode::Shared));
        }
        other => panic!("expected a conflict, got {other:?}"),
    }

    lm.release_all(me);
    lm.release_all(other);
}
