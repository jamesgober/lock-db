//! The shortest end-to-end use of the lock manager.
//!
//! Run with: `cargo run --example quick_start`

use lock_db::prelude::*;

fn main() {
    let lm = LockManager::new();
    let row = ResourceId::new(1);
    let writer = TxnId::new(1);
    let reader = TxnId::new(2);

    // The writer takes the row exclusively.
    lm.try_acquire(writer, row, LockMode::Exclusive)
        .expect("row is free");
    println!("txn {} holds row {} exclusively", writer.get(), row.get());

    // A concurrent reader is refused while the write lock is held.
    match lm.try_acquire(reader, row, LockMode::Shared) {
        Err(LockError::Conflict) => {
            println!("txn {} was refused: row is write-locked", reader.get());
        }
        other => panic!("expected a conflict, got {other:?}"),
    }

    // Once the writer releases, the reader gets in.
    lm.release(writer, row).expect("writer holds the row");
    lm.try_acquire(reader, row, LockMode::Shared)
        .expect("row is free for reading");
    println!("txn {} now holds row {} shared", reader.get(), row.get());

    lm.release(reader, row).expect("reader holds the row");
    assert_eq!(lm.holder_count(row), 0);
    println!("row {} is free", row.get());
}
