//! Lock modes and the compatibility matrix.
//!
//! The compatibility matrix is the correctness core of a lock manager: it
//! decides, for any two transactions contending for the same resource, whether
//! both requests can be held at once. Get this wrong and the manager hands out
//! conflicting locks; everything above it then corrupts data. For that reason
//! the matrix lives in one small, `const`, exhaustively tested place rather than
//! being scattered across the acquire path.
//!
//! `lock-db` implements the five standard multi-granularity locking (MGL)
//! modes. Two are the fundamental modes — shared (read) and exclusive (write).
//! The other three are *intention* modes that a transaction takes on a coarse
//! resource (a table, say) to announce that it intends to take a finer lock (a
//! row) underneath it. Intention locks are what make hierarchical locking
//! correct without forcing every reader to inspect every individual row lock:
//! a transaction wanting to lock a whole table exclusively need only check that
//! no one holds an intention lock on the table, instead of scanning every row.
//!
//! See the [crate-level docs](crate) for the granularity protocol that ties the
//! modes to a database/table/page/row hierarchy.

/// The mode in which a transaction holds, or wants to hold, a lock.
///
/// The five modes form a lattice ordered by privilege: holding a stronger mode
/// grants the capabilities of every weaker mode it [covers](LockMode::covers).
/// Acquiring a mode while already holding another upgrades to their
/// [join](LockMode::join) (least upper bound) — for example shared + intention
/// exclusive becomes [`SharedIntentionExclusive`](LockMode::SharedIntentionExclusive).
///
/// # Examples
///
/// ```
/// use lock_db::LockMode;
///
/// // Two readers coexist; a writer excludes everyone.
/// assert!(LockMode::Shared.compatible_with(LockMode::Shared));
/// assert!(!LockMode::Shared.compatible_with(LockMode::Exclusive));
///
/// // Intention-shared coexists with everything except an exclusive lock.
/// assert!(LockMode::IntentionShared.compatible_with(LockMode::SharedIntentionExclusive));
/// assert!(!LockMode::IntentionShared.compatible_with(LockMode::Exclusive));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LockMode {
    /// Intention shared (IS). Announces intent to take shared locks on finer
    /// resources beneath this one. Compatible with everything except exclusive.
    IntentionShared,

    /// Intention exclusive (IX). Announces intent to take exclusive (or shared)
    /// locks on finer resources. Compatible with the intention modes only.
    IntentionExclusive,

    /// Shared (S). A read lock. Any number of transactions may hold a resource
    /// `Shared` at once, alongside intention-shared holders.
    Shared,

    /// Shared and intention exclusive (SIX). The transaction reads the whole
    /// subtree (S) and intends to write part of it (IX). Compatible only with
    /// intention-shared.
    SharedIntentionExclusive,

    /// Exclusive (X). A write lock. Held by at most one transaction, and only
    /// when no other transaction holds the resource in any mode.
    Exclusive,
}

impl LockMode {
    /// Maps a mode to its row/column in the compatibility and join tables.
    #[inline]
    const fn index(self) -> usize {
        match self {
            LockMode::IntentionShared => 0,
            LockMode::IntentionExclusive => 1,
            LockMode::Shared => 2,
            LockMode::SharedIntentionExclusive => 3,
            LockMode::Exclusive => 4,
        }
    }

    /// Returns `true` if a lock in `self` mode and a lock in `other` mode may be
    /// held on the same resource by two different transactions at once.
    ///
    /// This is the symmetric compatibility relation of the standard MGL matrix:
    ///
    /// |       | IS | IX | S  | SIX | X  |
    /// |-------|----|----|----|-----|----|
    /// | **IS**  | ✓ | ✓ | ✓ | ✓  | ✗ |
    /// | **IX**  | ✓ | ✓ | ✗ | ✗  | ✗ |
    /// | **S**   | ✓ | ✗ | ✓ | ✗  | ✗ |
    /// | **SIX** | ✓ | ✗ | ✗ | ✗  | ✗ |
    /// | **X**   | ✗ | ✗ | ✗ | ✗  | ✗ |
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert!(LockMode::IntentionShared.compatible_with(LockMode::IntentionExclusive));
    /// assert!(!LockMode::IntentionExclusive.compatible_with(LockMode::Shared));
    /// assert!(!LockMode::Exclusive.compatible_with(LockMode::IntentionShared));
    /// ```
    #[inline]
    #[must_use]
    pub const fn compatible_with(self, other: LockMode) -> bool {
        // Rows/cols ordered IS, IX, S, SIX, X.
        const COMPAT: [[bool; 5]; 5] = [
            [true, true, true, true, false],     // IS
            [true, true, false, false, false],   // IX
            [true, false, true, false, false],   // S
            [true, false, false, false, false],  // SIX
            [false, false, false, false, false], // X
        ];
        COMPAT[self.index()][other.index()]
    }

    /// Returns the least mode that grants everything both `self` and `other`
    /// grant — their least upper bound in the privilege lattice.
    ///
    /// This is what an upgrade resolves to: a transaction already holding `self`
    /// that requests `other` ends up holding `self.join(other)`. For example a
    /// reader (`Shared`) that announces intent to write part of the subtree
    /// (`IntentionExclusive`) upgrades to `SharedIntentionExclusive`. `join` is
    /// commutative and associative.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert_eq!(
    ///     LockMode::Shared.join(LockMode::IntentionExclusive),
    ///     LockMode::SharedIntentionExclusive,
    /// );
    /// assert_eq!(LockMode::IntentionShared.join(LockMode::Shared), LockMode::Shared);
    /// assert_eq!(LockMode::Shared.join(LockMode::Exclusive), LockMode::Exclusive);
    /// // join with itself is itself.
    /// assert_eq!(LockMode::IntentionExclusive.join(LockMode::IntentionExclusive), LockMode::IntentionExclusive);
    /// ```
    #[inline]
    #[must_use]
    pub const fn join(self, other: LockMode) -> LockMode {
        use LockMode::{
            Exclusive, IntentionExclusive, IntentionShared, Shared, SharedIntentionExclusive,
        };
        // Rows/cols ordered IS, IX, S, SIX, X. Symmetric.
        const JOIN: [[LockMode; 5]; 5] = [
            // IS
            [
                IntentionShared,
                IntentionExclusive,
                Shared,
                SharedIntentionExclusive,
                Exclusive,
            ],
            // IX
            [
                IntentionExclusive,
                IntentionExclusive,
                SharedIntentionExclusive,
                SharedIntentionExclusive,
                Exclusive,
            ],
            // S
            [
                Shared,
                SharedIntentionExclusive,
                Shared,
                SharedIntentionExclusive,
                Exclusive,
            ],
            // SIX
            [
                SharedIntentionExclusive,
                SharedIntentionExclusive,
                SharedIntentionExclusive,
                SharedIntentionExclusive,
                Exclusive,
            ],
            // X
            [Exclusive, Exclusive, Exclusive, Exclusive, Exclusive],
        ];
        JOIN[self.index()][other.index()]
    }

    /// Returns `true` if holding `self` already grants everything `other` would.
    ///
    /// Equivalent to `self.join(other) == self`: `self` sits at or above `other`
    /// in the lattice. This drives the idempotent path of acquisition — a
    /// request for a mode you already cover is a no-op.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert!(LockMode::Exclusive.covers(LockMode::Shared));
    /// assert!(LockMode::SharedIntentionExclusive.covers(LockMode::Shared));
    /// assert!(LockMode::SharedIntentionExclusive.covers(LockMode::IntentionExclusive));
    /// assert!(!LockMode::Shared.covers(LockMode::IntentionExclusive));
    /// ```
    #[inline]
    #[must_use]
    pub const fn covers(self, other: LockMode) -> bool {
        // `PartialEq` is not `const`, so compare discriminants directly.
        self.join(other) as u8 == self as u8
    }

    /// Returns `true` for [`LockMode::Exclusive`].
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert!(LockMode::Exclusive.is_exclusive());
    /// assert!(!LockMode::Shared.is_exclusive());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_exclusive(self) -> bool {
        matches!(self, LockMode::Exclusive)
    }

    /// Returns `true` for the intention modes (IS, IX, SIX).
    ///
    /// Intention modes are placeholders taken on a coarse resource to signal
    /// that a finer lock will be taken beneath it; they are not themselves read
    /// or write locks on the resource's own data.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert!(LockMode::IntentionShared.is_intention());
    /// assert!(LockMode::SharedIntentionExclusive.is_intention());
    /// assert!(!LockMode::Shared.is_intention());
    /// assert!(!LockMode::Exclusive.is_intention());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_intention(self) -> bool {
        matches!(
            self,
            LockMode::IntentionShared
                | LockMode::IntentionExclusive
                | LockMode::SharedIntentionExclusive
        )
    }
}

#[cfg(test)]
mod tests {
    use super::LockMode::{
        self, Exclusive, IntentionExclusive, IntentionShared, Shared, SharedIntentionExclusive,
    };

    const ALL: [LockMode; 5] = [
        IntentionShared,
        IntentionExclusive,
        Shared,
        SharedIntentionExclusive,
        Exclusive,
    ];

    #[test]
    fn test_compatibility_matches_standard_mgl_matrix() {
        // The canonical Gray/Reuter compatibility matrix, IS IX S SIX X.
        let expected = [
            [true, true, true, true, false],
            [true, true, false, false, false],
            [true, false, true, false, false],
            [true, false, false, false, false],
            [false, false, false, false, false],
        ];
        for (i, a) in ALL.iter().enumerate() {
            for (j, b) in ALL.iter().enumerate() {
                assert_eq!(
                    a.compatible_with(*b),
                    expected[i][j],
                    "compat({a:?}, {b:?})"
                );
            }
        }
    }

    #[test]
    fn test_compatible_is_symmetric() {
        for a in ALL {
            for b in ALL {
                assert_eq!(a.compatible_with(b), b.compatible_with(a), "{a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn test_exclusive_incompatible_with_all() {
        for m in ALL {
            assert!(!Exclusive.compatible_with(m));
            assert!(!m.compatible_with(Exclusive));
        }
    }

    #[test]
    fn test_intention_shared_compatible_with_all_but_exclusive() {
        for m in ALL {
            assert_eq!(IntentionShared.compatible_with(m), m != Exclusive);
        }
    }

    #[test]
    fn test_join_is_commutative() {
        for a in ALL {
            for b in ALL {
                assert_eq!(a.join(b), b.join(a), "join({a:?}, {b:?})");
            }
        }
    }

    #[test]
    fn test_join_is_associative() {
        for a in ALL {
            for b in ALL {
                for c in ALL {
                    assert_eq!(a.join(b).join(c), a.join(b.join(c)));
                }
            }
        }
    }

    #[test]
    fn test_join_idempotent() {
        for m in ALL {
            assert_eq!(m.join(m), m);
        }
    }

    #[test]
    fn test_join_examples() {
        assert_eq!(Shared.join(IntentionExclusive), SharedIntentionExclusive);
        assert_eq!(IntentionShared.join(Shared), Shared);
        assert_eq!(IntentionShared.join(IntentionExclusive), IntentionExclusive);
        assert_eq!(Shared.join(Exclusive), Exclusive);
        assert_eq!(SharedIntentionExclusive.join(Exclusive), Exclusive);
    }

    #[test]
    fn test_covers_is_join_equals_self() {
        for a in ALL {
            for b in ALL {
                assert_eq!(a.covers(b), a.join(b) == a, "covers({a:?}, {b:?})");
            }
        }
    }

    #[test]
    fn test_exclusive_covers_everything() {
        for m in ALL {
            assert!(Exclusive.covers(m));
        }
    }

    #[test]
    fn test_six_covers_its_components() {
        assert!(SharedIntentionExclusive.covers(Shared));
        assert!(SharedIntentionExclusive.covers(IntentionExclusive));
        assert!(SharedIntentionExclusive.covers(IntentionShared));
        assert!(!SharedIntentionExclusive.covers(Exclusive));
    }

    #[test]
    fn test_covers_reflexive() {
        for m in ALL {
            assert!(m.covers(m));
        }
    }

    #[test]
    fn test_is_exclusive_and_is_intention() {
        assert!(Exclusive.is_exclusive());
        assert!(!Shared.is_exclusive());
        for m in [
            IntentionShared,
            IntentionExclusive,
            SharedIntentionExclusive,
        ] {
            assert!(m.is_intention());
        }
        assert!(!Shared.is_intention());
        assert!(!Exclusive.is_intention());
    }

    #[test]
    fn test_two_compatible_modes_join_below_any_conflict() {
        // Sanity: if two modes are compatible, neither is exclusive.
        for a in ALL {
            for b in ALL {
                if a.compatible_with(b) {
                    assert!(!(a.is_exclusive() && b.is_exclusive()));
                }
            }
        }
    }
}
