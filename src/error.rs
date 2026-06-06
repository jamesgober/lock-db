//! Error types returned by the lock manager.

use core::fmt;

/// Reasons a lock operation can fail.
///
/// Every fallible entry point on [`LockManager`](crate::LockManager) returns
/// `Result<_, LockError>`. The variants are deliberately coarse: a caller
/// either got the lock or it did not, and the few ways "did not" can happen are
/// distinct enough to branch on. The type is `#[non_exhaustive]` because later
/// milestones (wait queues, deadlock detection) add variants such as a timeout
/// and a deadlock-victim signal; matching code must keep a wildcard arm.
///
/// # Examples
///
/// ```
/// use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};
///
/// let lm = LockManager::new();
/// let row = ResourceId::new(1);
///
/// // Txn 1 takes an exclusive lock.
/// lm.try_acquire(TxnId::new(1), row, LockMode::Exclusive).unwrap();
///
/// // Txn 2 cannot get any lock on the same row right now.
/// assert_eq!(lm.try_acquire(TxnId::new(2), row, LockMode::Shared), Err(LockError::Conflict));
///
/// // Releasing a lock you never held is a distinct error.
/// assert_eq!(lm.release(TxnId::new(9), row), Err(LockError::NotHeld));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum LockError {
    /// The lock could not be granted without blocking.
    ///
    /// The requested mode is incompatible with a mode another transaction
    /// already holds on the resource, or the request is an upgrade
    /// (shared to exclusive) that other shared holders are blocking. Returned
    /// by the non-blocking [`try_acquire`](crate::LockManager::try_acquire);
    /// the caller decides whether to retry, wait, or abort.
    Conflict,

    /// A release named a (transaction, resource) pair that holds no lock.
    ///
    /// Returned by [`release`](crate::LockManager::release) when the
    /// transaction does not currently hold a lock on the resource. This usually
    /// signals a double release or a bookkeeping bug in the caller's
    /// transaction layer.
    NotHeld,
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict => f.write_str("lock request conflicts with an existing lock"),
            Self::NotHeld => f.write_str("no lock held for this transaction and resource"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for LockError {}

#[cfg(test)]
mod tests {
    use super::LockError;

    // `to_string` needs an allocator, so this one is only built with `std`.
    #[cfg(feature = "std")]
    #[test]
    fn test_display_messages_are_distinct_and_nonempty() {
        let conflict = LockError::Conflict.to_string();
        let not_held = LockError::NotHeld.to_string();
        assert!(!conflict.is_empty());
        assert!(!not_held.is_empty());
        assert_ne!(conflict, not_held);
    }

    #[test]
    fn test_variants_compare_equal_to_themselves() {
        assert_eq!(LockError::Conflict, LockError::Conflict);
        assert_ne!(LockError::Conflict, LockError::NotHeld);
    }
}
