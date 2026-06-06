//! Property-based tests for the lock-table invariants.
//!
//! The central correctness property of a lock manager is that it never grants
//! two incompatible locks on the same resource. Rather than assert that
//! directly on the manager's private state, these tests run a stream of
//! arbitrary acquire/release operations against both the real `LockManager` and
//! a tiny reference model that applies the compatibility rules by hand, then
//! assert the two agree after every step. If the manager ever grants a lock the
//! model rejects (or vice versa), the property fails and proptest shrinks the
//! operation sequence to a minimal counterexample.

// The lock manager is only built with the `std` feature; these properties
// exercise it, so they compile away entirely in a `no_std` configuration.
#![cfg(feature = "std")]
#![allow(clippy::unwrap_used)]

use std::collections::HashMap;

use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};
use proptest::prelude::*;

/// Small fixed universe keeps contention high and the shrunk cases readable.
const TXNS: u64 = 4;
const RESOURCES: u64 = 3;

#[derive(Clone, Copy, Debug)]
enum Op {
    Acquire { txn: u64, res: u64, mode: LockMode },
    Release { txn: u64, res: u64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let txn = 0..TXNS;
    let res = 0..RESOURCES;
    prop_oneof![
        (txn.clone(), res.clone(), any::<bool>()).prop_map(|(txn, res, exclusive)| Op::Acquire {
            txn,
            res,
            mode: if exclusive {
                LockMode::Exclusive
            } else {
                LockMode::Shared
            },
        }),
        (txn, res).prop_map(|(txn, res)| Op::Release { txn, res }),
    ]
}

/// Reference model: the mode each transaction holds on each resource.
#[derive(Default)]
struct Model {
    held: HashMap<(u64, u64), LockMode>,
}

impl Model {
    fn holders_of(&self, res: u64) -> Vec<(u64, LockMode)> {
        self.held
            .iter()
            .filter(|((_, r), _)| *r == res)
            .map(|((t, _), m)| (*t, *m))
            .collect()
    }

    /// Returns the result an honest lock manager must produce for this acquire,
    /// updating the model when the grant succeeds.
    fn apply_acquire(&mut self, txn: u64, res: u64, mode: LockMode) -> Result<(), LockError> {
        if let Some(&current) = self.held.get(&(txn, res)) {
            if current.covers(mode) {
                return Ok(());
            }
            // shared -> exclusive upgrade: only legal as the sole holder.
            let others = self
                .holders_of(res)
                .into_iter()
                .filter(|(t, _)| *t != txn)
                .count();
            if others == 0 {
                let _ = self.held.insert((txn, res), mode);
                return Ok(());
            }
            return Err(LockError::Conflict);
        }

        let compatible = self
            .holders_of(res)
            .into_iter()
            .all(|(_, held)| held.compatible_with(mode));
        if compatible {
            let _ = self.held.insert((txn, res), mode);
            Ok(())
        } else {
            Err(LockError::Conflict)
        }
    }

    fn apply_release(&mut self, txn: u64, res: u64) -> Result<(), LockError> {
        if self.held.remove(&(txn, res)).is_some() {
            Ok(())
        } else {
            Err(LockError::NotHeld)
        }
    }

    /// The compatibility invariant the manager must always satisfy: every
    /// resource is held either by any number of shared locks or by exactly one
    /// exclusive lock.
    fn assert_internally_consistent(&self) {
        let mut by_res: HashMap<u64, Vec<LockMode>> = HashMap::new();
        for ((_, res), mode) in &self.held {
            by_res.entry(*res).or_default().push(*mode);
        }
        for modes in by_res.values() {
            let exclusives = modes.iter().filter(|m| m.is_exclusive()).count();
            if exclusives > 0 {
                assert_eq!(exclusives, 1, "more than one exclusive holder");
                assert_eq!(
                    modes.len(),
                    1,
                    "exclusive lock coexists with another holder"
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    /// The manager agrees with the reference model after every operation, for
    /// any interleaving of acquires and releases, across several shard counts.
    #[test]
    fn manager_matches_model(
        ops in proptest::collection::vec(op_strategy(), 1..200),
        shards in prop_oneof![Just(1usize), Just(4), Just(16)],
    ) {
        let lm = LockManager::with_shards(shards);
        let mut model = Model::default();

        for op in ops {
            match op {
                Op::Acquire { txn, res, mode } => {
                    let want = model.apply_acquire(txn, res, mode);
                    let got = lm.try_acquire(TxnId::new(txn), ResourceId::new(res), mode);
                    prop_assert_eq!(got, want, "acquire mismatch on op {:?}", op);
                }
                Op::Release { txn, res } => {
                    let want = model.apply_release(txn, res);
                    let got = lm.release(TxnId::new(txn), ResourceId::new(res));
                    prop_assert_eq!(got, want, "release mismatch on op {:?}", op);
                }
            }

            model.assert_internally_consistent();

            // Cross-check observable manager state against the model.
            for t in 0..TXNS {
                for r in 0..RESOURCES {
                    let expected = model.held.get(&(t, r)).copied();
                    prop_assert_eq!(lm.mode_held(TxnId::new(t), ResourceId::new(r)), expected);
                }
                prop_assert_eq!(
                    lm.holder_count(ResourceId::new(0)),
                    model.holders_of(0).len()
                );
            }
        }
    }

    /// `release_all` drops exactly the locks the model says a transaction holds,
    /// and leaves every other transaction's locks untouched.
    #[test]
    fn release_all_matches_model(
        ops in proptest::collection::vec(op_strategy(), 1..200),
        victim in 0..TXNS,
    ) {
        let lm = LockManager::with_shards(8);
        let mut model = Model::default();

        for op in ops {
            match op {
                Op::Acquire { txn, res, mode } => {
                    if model.apply_acquire(txn, res, mode).is_ok() {
                        let _ = lm.try_acquire(TxnId::new(txn), ResourceId::new(res), mode);
                    }
                }
                Op::Release { txn, res } => {
                    if model.apply_release(txn, res).is_ok() {
                        let _ = lm.release(TxnId::new(txn), ResourceId::new(res));
                    }
                }
            }
        }

        let expected_count = model.held.keys().filter(|(t, _)| *t == victim).count();
        let released = lm.release_all(TxnId::new(victim));
        prop_assert_eq!(released, expected_count);

        // The victim now holds nothing; everyone else is unchanged.
        for r in 0..RESOURCES {
            prop_assert_eq!(lm.mode_held(TxnId::new(victim), ResourceId::new(r)), None);
        }
        for t in 0..TXNS {
            if t == victim {
                continue;
            }
            for r in 0..RESOURCES {
                let expected = model.held.get(&(t, r)).copied();
                prop_assert_eq!(lm.mode_held(TxnId::new(t), ResourceId::new(r)), expected);
            }
        }
    }
}
