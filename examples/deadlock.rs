//! Deadlock detection with the wait-for graph.
//!
//! Two transactions take locks in opposite order — the textbook deadlock. The
//! deadlock-aware `request` records each wait in the wait-for graph and, when
//! the second wait closes the cycle, reports it with a victim to abort. Aborting
//! the victim releases its locks and lets the survivor proceed.
//!
//! Run with: `cargo run --example deadlock`

use lock_db::prelude::*;

fn main() {
    let lm = LockManager::new();
    let (account_a, account_b) = (ResourceId::new(1), ResourceId::new(2));
    let (t1, t2) = (TxnId::new(1), TxnId::new(2));

    // Each transaction locks one account, then reaches for the other.
    assert_eq!(
        lm.request(t1, account_a, LockMode::Exclusive),
        Acquisition::Granted
    );
    assert_eq!(
        lm.request(t2, account_b, LockMode::Exclusive),
        Acquisition::Granted
    );
    println!("t1 holds account A, t2 holds account B");

    // t1 wants B (held by t2): it waits. No cycle yet.
    match lm.request(t1, account_b, LockMode::Exclusive) {
        Acquisition::Waiting => println!("t1 waits for account B"),
        other => panic!("expected waiting, got {other:?}"),
    }

    // t2 wants A (held by t1): this closes the cycle.
    match lm.request(t2, account_a, LockMode::Exclusive) {
        Acquisition::Deadlock(deadlock) => {
            println!(
                "deadlock detected: cycle {:?}, aborting victim t{}",
                deadlock.cycle.iter().map(|t| t.get()).collect::<Vec<_>>(),
                deadlock.victim.get(),
            );

            // Break the cycle by aborting the victim — drop all its locks.
            let released = lm.release_all(deadlock.victim);
            println!("victim released {released} locks");

            // The survivor can now take the lock it was blocked on.
            let survivor = if deadlock.victim == t1 { t2 } else { t1 };
            let wanted = if survivor == t1 { account_b } else { account_a };
            assert_eq!(
                lm.request(survivor, wanted, LockMode::Exclusive),
                Acquisition::Granted
            );
            println!("t{} proceeds; no deadlock remains", survivor.get());
        }
        other => panic!("expected deadlock, got {other:?}"),
    }

    assert!(lm.find_deadlock().is_none());
}
