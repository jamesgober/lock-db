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

use lock_db::{KeyRange, LockError, LockManager, LockMode, ResourceId, TxnId};
use proptest::prelude::*;

/// Small fixed universe keeps contention high and the shrunk cases readable.
const TXNS: u64 = 4;
const RESOURCES: u64 = 3;

#[derive(Clone, Copy, Debug)]
enum Op {
    Acquire { txn: u64, res: u64, mode: LockMode },
    Release { txn: u64, res: u64 },
}

const MODES: [LockMode; 5] = [
    LockMode::IntentionShared,
    LockMode::IntentionExclusive,
    LockMode::Shared,
    LockMode::SharedIntentionExclusive,
    LockMode::Exclusive,
];

fn op_strategy() -> impl Strategy<Value = Op> {
    let txn = 0..TXNS;
    let res = 0..RESOURCES;
    prop_oneof![
        (txn.clone(), res.clone(), 0..MODES.len()).prop_map(|(txn, res, m)| Op::Acquire {
            txn,
            res,
            mode: MODES[m],
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
            // Upgrade to the join, allowed only if compatible with other holders.
            let target = current.join(mode);
            let blocked = self
                .holders_of(res)
                .into_iter()
                .any(|(t, held)| t != txn && !held.compatible_with(target));
            if blocked {
                return Err(LockError::Conflict);
            }
            let _ = self.held.insert((txn, res), target);
            return Ok(());
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

    /// The compatibility invariant the manager must always satisfy: every pair
    /// of distinct holders of the same resource holds compatible modes.
    fn assert_internally_consistent(&self) {
        let mut by_res: HashMap<u64, Vec<LockMode>> = HashMap::new();
        for ((_, res), mode) in &self.held {
            by_res.entry(*res).or_default().push(*mode);
        }
        for modes in by_res.values() {
            for i in 0..modes.len() {
                for j in (i + 1)..modes.len() {
                    assert!(
                        modes[i].compatible_with(modes[j]),
                        "incompatible holders coexist: {:?} and {:?}",
                        modes[i],
                        modes[j],
                    );
                }
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

// ---- range locks ----

const SPACES: u64 = 2;
const KEY_MAX: u64 = 7;

#[derive(Clone, Copy, Debug)]
enum RangeOp {
    Acquire {
        txn: u64,
        space: u64,
        range: KeyRange,
        mode: LockMode,
    },
    Release {
        txn: u64,
        space: u64,
        range: KeyRange,
    },
}

fn range_strategy() -> impl Strategy<Value = KeyRange> {
    (0..=KEY_MAX, 0..=KEY_MAX).prop_map(|(a, b)| {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        KeyRange::new(lo, hi).unwrap()
    })
}

fn range_op_strategy() -> impl Strategy<Value = RangeOp> {
    let txn = 0..TXNS;
    let space = 0..SPACES;
    prop_oneof![
        (txn.clone(), space.clone(), range_strategy(), 0..MODES.len()).prop_map(
            |(txn, space, range, m)| RangeOp::Acquire {
                txn,
                space,
                range,
                mode: MODES[m],
            }
        ),
        (txn, space, range_strategy()).prop_map(|(txn, space, range)| RangeOp::Release {
            txn,
            space,
            range
        }),
    ]
}

/// Reference model for range locks, mirroring the manager's storage exactly: a
/// per-space `Vec` of holders, pushed on acquire and `swap_remove`d on release,
/// so both stay in identical order and the "first matching range" picked on
/// release is the same element in both.
#[derive(Default)]
struct RangeModel {
    spaces: HashMap<u64, Vec<(u64, KeyRange, LockMode)>>,
}

impl RangeModel {
    fn apply_acquire(
        &mut self,
        txn: u64,
        space: u64,
        range: KeyRange,
        mode: LockMode,
    ) -> Result<(), LockError> {
        let holders = self.spaces.entry(space).or_default();
        let conflict = holders
            .iter()
            .any(|(t, r, m)| *t != txn && r.overlaps(range) && !m.compatible_with(mode));
        if conflict {
            return Err(LockError::Conflict);
        }
        holders.push((txn, range, mode));
        Ok(())
    }

    fn apply_release(&mut self, txn: u64, space: u64, range: KeyRange) -> Result<(), LockError> {
        if let Some(holders) = self.spaces.get_mut(&space) {
            if let Some(pos) = holders
                .iter()
                .position(|(t, r, _)| *t == txn && *r == range)
            {
                let _ = holders.swap_remove(pos);
                return Ok(());
            }
        }
        Err(LockError::NotHeld)
    }

    fn assert_no_incompatible_overlap(&self) {
        for holders in self.spaces.values() {
            for i in 0..holders.len() {
                for j in (i + 1)..holders.len() {
                    let (ti, ri, mi) = holders[i];
                    let (tj, rj, mj) = holders[j];
                    if ti != tj && ri.overlaps(rj) {
                        assert!(
                            mi.compatible_with(mj),
                            "incompatible overlapping ranges: {ri:?}/{mi:?} and {rj:?}/{mj:?}",
                        );
                    }
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    /// The range-lock facility agrees with the reference model after every
    /// operation, and never lets two transactions hold overlapping ranges in
    /// incompatible modes.
    #[test]
    fn range_manager_matches_model(
        ops in proptest::collection::vec(range_op_strategy(), 1..200),
    ) {
        let lm = LockManager::with_shards(4);
        let mut model = RangeModel::default();

        for op in ops {
            match op {
                RangeOp::Acquire { txn, space, range, mode } => {
                    let want = model.apply_acquire(txn, space, range, mode);
                    let got = lm.try_acquire_range(TxnId::new(txn), ResourceId::new(space), range, mode);
                    prop_assert_eq!(got, want, "acquire mismatch on {:?}", op);
                }
                RangeOp::Release { txn, space, range } => {
                    let want = model.apply_release(txn, space, range);
                    let got = lm.release_range(TxnId::new(txn), ResourceId::new(space), range);
                    prop_assert_eq!(got, want, "release mismatch on {:?}", op);
                }
            }

            model.assert_no_incompatible_overlap();

            for s in 0..SPACES {
                let expected = model.spaces.get(&s).map_or(0, Vec::len);
                prop_assert_eq!(lm.range_count(ResourceId::new(s)), expected);
            }
        }
    }
}
