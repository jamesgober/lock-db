//! Opaque identifiers for transactions and lockable resources.
//!
//! Both identifiers are thin newtypes over `u64`. lock-db does not assign or
//! interpret them: the transaction layer decides what a transaction id means,
//! and the storage layer decides how to map a database, table, page, or row to
//! a single [`ResourceId`]. Keeping them opaque integers means the lock table
//! never owns variable-length keys, so a lookup is a hash of one machine word
//! with no allocation on the hot path.

/// Identifies the transaction that owns a lock request.
///
/// The lock manager uses this only for equality and hashing: it tracks which
/// requests belong together so a transaction can re-acquire, upgrade, or
/// release its own locks. Reusing a retired id for a new transaction is safe as
/// long as the previous transaction has released everything first.
///
/// # Examples
///
/// ```
/// use lock_db::TxnId;
///
/// let t = TxnId::new(42);
/// assert_eq!(t.get(), 42);
/// assert_eq!(TxnId::from(42), t);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct TxnId(u64);

impl TxnId {
    /// Wraps a raw transaction number.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the underlying number.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for TxnId {
    #[inline]
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<TxnId> for u64 {
    #[inline]
    fn from(id: TxnId) -> Self {
        id.0
    }
}

/// Identifies a lockable resource.
///
/// A resource is whatever the caller decides to protect with a lock: an entire
/// database, a table, a page, or a single row. The caller is responsible for
/// mapping its own object identity to a stable, collision-free `u64`. Two
/// distinct resources that map to the same id will share a lock queue, which is
/// a correctness bug in the caller's id scheme, not in lock-db.
///
/// # Examples
///
/// ```
/// use lock_db::ResourceId;
///
/// let page = ResourceId::new(0xDEAD_BEEF);
/// assert_eq!(page.get(), 0xDEAD_BEEF);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct ResourceId(u64);

impl ResourceId {
    /// Wraps a raw resource number.
    #[inline]
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the underlying number.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ResourceId {
    #[inline]
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<ResourceId> for u64 {
    #[inline]
    fn from(id: ResourceId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::{ResourceId, TxnId};

    #[test]
    fn test_txn_roundtrip_through_u64() {
        let t = TxnId::new(u64::MAX);
        assert_eq!(t.get(), u64::MAX);
        assert_eq!(u64::from(t), u64::MAX);
        assert_eq!(TxnId::from(7), TxnId::new(7));
    }

    #[test]
    fn test_resource_roundtrip_through_u64() {
        let r = ResourceId::new(0);
        assert_eq!(r.get(), 0);
        assert_eq!(u64::from(r), 0);
        assert_eq!(ResourceId::from(7), ResourceId::new(7));
    }

    #[test]
    fn test_distinct_ids_are_unequal() {
        assert_ne!(TxnId::new(1), TxnId::new(2));
        assert_ne!(ResourceId::new(1), ResourceId::new(2));
    }

    #[test]
    fn test_ids_are_ordered_by_value() {
        assert!(TxnId::new(1) < TxnId::new(2));
        assert!(ResourceId::new(1) < ResourceId::new(2));
    }
}
