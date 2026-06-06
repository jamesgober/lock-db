//! Lock modes and the compatibility matrix.
//!
//! The compatibility matrix is the correctness core of a lock manager: it
//! decides, for any two transactions contending for the same resource, whether
//! both requests can be held at once. Get this wrong and the manager hands out
//! conflicting locks; everything above it then corrupts data. For that reason
//! the matrix lives in one small, `const`, exhaustively tested function rather
//! than being scattered across the acquire path.
//!
//! This milestone (v0.2.0) ships the two fundamental modes, shared and
//! exclusive. The hierarchical intention modes (IS, IX, SIX) arrive with
//! multi-granularity locking in a later release and extend this same matrix.

/// The mode in which a transaction holds, or wants to hold, a lock.
///
/// # Examples
///
/// ```
/// use lock_db::LockMode;
///
/// // Two readers coexist; a writer excludes everyone.
/// assert!(LockMode::Shared.compatible_with(LockMode::Shared));
/// assert!(!LockMode::Shared.compatible_with(LockMode::Exclusive));
/// assert!(!LockMode::Exclusive.compatible_with(LockMode::Exclusive));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LockMode {
    /// A read lock. Any number of transactions may hold a resource `Shared` at
    /// the same time, but none may hold it `Exclusive` while they do.
    Shared,

    /// A write lock. Held by at most one transaction, and only when no other
    /// transaction holds the resource in any mode.
    Exclusive,
}

impl LockMode {
    /// Returns `true` if a lock in `self` mode and a lock in `other` mode may be
    /// held on the same resource by two different transactions at once.
    ///
    /// This is the symmetric compatibility relation: `a.compatible_with(b)`
    /// always equals `b.compatible_with(a)`. The only compatible pair is
    /// shared/shared.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// for a in [LockMode::Shared, LockMode::Exclusive] {
    ///     for b in [LockMode::Shared, LockMode::Exclusive] {
    ///         // Symmetry holds for every pair.
    ///         assert_eq!(a.compatible_with(b), b.compatible_with(a));
    ///     }
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub const fn compatible_with(self, other: LockMode) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }

    /// Returns `true` if holding `self` already grants everything `other` would.
    ///
    /// A transaction that already holds a resource exclusively does not need to
    /// re-acquire it to read; a transaction holding it shared still needs an
    /// upgrade to write. This drives the idempotent and upgrade paths of
    /// [`LockManager::try_acquire`](crate::LockManager::try_acquire).
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockMode;
    ///
    /// assert!(LockMode::Exclusive.covers(LockMode::Shared));
    /// assert!(LockMode::Exclusive.covers(LockMode::Exclusive));
    /// assert!(LockMode::Shared.covers(LockMode::Shared));
    /// assert!(!LockMode::Shared.covers(LockMode::Exclusive));
    /// ```
    #[inline]
    #[must_use]
    pub const fn covers(self, other: LockMode) -> bool {
        matches!(
            (self, other),
            (LockMode::Exclusive, _) | (LockMode::Shared, LockMode::Shared)
        )
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
}

#[cfg(test)]
mod tests {
    use super::LockMode::{Exclusive, Shared};

    #[test]
    fn test_compatible_matrix_only_shared_shared_is_true() {
        assert!(Shared.compatible_with(Shared));
        assert!(!Shared.compatible_with(Exclusive));
        assert!(!Exclusive.compatible_with(Shared));
        assert!(!Exclusive.compatible_with(Exclusive));
    }

    #[test]
    fn test_compatible_is_symmetric() {
        for a in [Shared, Exclusive] {
            for b in [Shared, Exclusive] {
                assert_eq!(a.compatible_with(b), b.compatible_with(a));
            }
        }
    }

    #[test]
    fn test_covers_reflexive() {
        for m in [Shared, Exclusive] {
            assert!(m.covers(m));
        }
    }

    #[test]
    fn test_covers_exclusive_covers_everything() {
        assert!(Exclusive.covers(Shared));
        assert!(Exclusive.covers(Exclusive));
    }

    #[test]
    fn test_covers_shared_does_not_cover_exclusive() {
        assert!(!Shared.covers(Exclusive));
    }

    #[test]
    fn test_is_exclusive() {
        assert!(Exclusive.is_exclusive());
        assert!(!Shared.is_exclusive());
    }

    #[test]
    fn test_ordering_shared_below_exclusive() {
        assert!(Shared < Exclusive);
    }
}
