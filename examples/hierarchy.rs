//! Multi-granularity locking over a database -> table -> page -> row hierarchy.
//!
//! The protocol: before locking a node in `S` or `X`, hold an intention lock on
//! every coarser node above it — `IX` above an `X`, `IS` above an `S`. Acquire
//! coarse-to-fine; release fine-to-coarse. This lets a writer lock one row
//! exclusively while readers still descend other parts of the same table.
//!
//! Run with: `cargo run --example hierarchy`

use lock_db::prelude::*;

/// The four levels, modelled as four distinct resource ids.
const DB: ResourceId = ResourceId::new(1);
const TABLE: ResourceId = ResourceId::new(2);
const PAGE: ResourceId = ResourceId::new(3);
const ROW: ResourceId = ResourceId::new(4);

fn main() {
    let lm = LockManager::new();

    // Writer: intends to write one row, so takes IX coarse-to-fine, then X.
    let writer = TxnId::new(1);
    for node in [DB, TABLE, PAGE] {
        lm.try_acquire(writer, node, LockMode::IntentionExclusive)
            .expect("intention path is free");
    }
    lm.try_acquire(writer, ROW, LockMode::Exclusive)
        .expect("row is free");
    println!("writer holds IX on db/table/page and X on the row");

    // Reader: wants to read elsewhere in the same table. IS coexists with the
    // writer's IX at every coarse level.
    let reader = TxnId::new(2);
    for node in [DB, TABLE, PAGE] {
        lm.try_acquire(reader, node, LockMode::IntentionShared)
            .expect("IS coexists with IX");
    }
    println!("reader holds IS on db/table/page alongside the writer");

    // But the reader cannot read the specific row the writer holds exclusively.
    match lm.try_acquire(reader, ROW, LockMode::Shared) {
        Err(LockError::Conflict) => println!("reader is correctly blocked from the locked row"),
        other => panic!("expected conflict, got {other:?}"),
    }

    // Commit: each transaction drops its whole set at once.
    let n = lm.release_all(writer);
    println!("writer released {n} locks at commit");
    lm.release_all(reader);

    // With the writer gone, the row is free again.
    lm.try_acquire(reader, ROW, LockMode::Shared)
        .expect("row free after writer commits");
    println!("reader now reads the row");
    lm.release_all(reader);
}
