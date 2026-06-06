//! Key ranges for range locking.
//!
//! A point lock protects one resource; a *range* lock protects a contiguous
//! span of keys. Range locks exist to stop phantoms: if a transaction reads
//! "every order with id between 100 and 200", locking only the rows that exist
//! today does not stop another transaction from inserting id 150 underneath it.
//! Locking the whole range `[100, 200]` does.
//!
//! [`KeyRange`] is the span itself — an inclusive `[start, end]` interval over
//! `u64` keys. It carries no lock state; it is the key a range lock is taken on.
//! Inclusive bounds avoid the overflow corner that half-open intervals hit at
//! `u64::MAX` and make a single-key lock simply `KeyRange::point(k)`.

/// An inclusive range of `u64` keys, `[start, end]`.
///
/// Construct one with [`new`](KeyRange::new) (which rejects `start > end`) or
/// [`point`](KeyRange::point) for a single key. Two ranges held by different
/// transactions conflict only when they [overlap](KeyRange::overlaps) *and*
/// their lock modes are incompatible.
///
/// # Examples
///
/// ```
/// use lock_db::KeyRange;
///
/// let r = KeyRange::new(100, 200).unwrap();
/// assert!(r.contains(150));
/// assert!(!r.contains(201));
/// assert!(r.overlaps(KeyRange::point(200)));
/// assert!(!r.overlaps(KeyRange::new(201, 300).unwrap()));
///
/// // An inverted range is rejected.
/// assert!(KeyRange::new(5, 4).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct KeyRange {
    start: u64,
    end: u64,
}

impl KeyRange {
    /// Creates the inclusive range `[start, end]`, or `None` if `start > end`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::KeyRange;
    ///
    /// assert!(KeyRange::new(0, 0).is_some()); // a single key
    /// assert!(KeyRange::new(1, 9).is_some());
    /// assert!(KeyRange::new(9, 1).is_none());
    /// ```
    #[inline]
    #[must_use]
    pub const fn new(start: u64, end: u64) -> Option<Self> {
        if start <= end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Creates a range covering the single key `key`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::KeyRange;
    ///
    /// let k = KeyRange::point(42);
    /// assert_eq!(k.start(), 42);
    /// assert_eq!(k.end(), 42);
    /// ```
    #[inline]
    #[must_use]
    pub const fn point(key: u64) -> Self {
        Self {
            start: key,
            end: key,
        }
    }

    /// Returns the inclusive lower bound.
    #[inline]
    #[must_use]
    pub const fn start(self) -> u64 {
        self.start
    }

    /// Returns the inclusive upper bound.
    #[inline]
    #[must_use]
    pub const fn end(self) -> u64 {
        self.end
    }

    /// Returns `true` if `key` falls within the range.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::KeyRange;
    ///
    /// let r = KeyRange::new(10, 20).unwrap();
    /// assert!(r.contains(10) && r.contains(20));
    /// assert!(!r.contains(9) && !r.contains(21));
    /// ```
    #[inline]
    #[must_use]
    pub const fn contains(self, key: u64) -> bool {
        self.start <= key && key <= self.end
    }

    /// Returns `true` if the two ranges share at least one key.
    ///
    /// Overlap is symmetric: `a.overlaps(b) == b.overlaps(a)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::KeyRange;
    ///
    /// let a = KeyRange::new(10, 20).unwrap();
    /// assert!(a.overlaps(KeyRange::new(20, 30).unwrap())); // touch at 20
    /// assert!(!a.overlaps(KeyRange::new(21, 30).unwrap()));
    /// ```
    #[inline]
    #[must_use]
    pub const fn overlaps(self, other: KeyRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::KeyRange;

    #[test]
    fn test_new_rejects_inverted_range() {
        assert!(KeyRange::new(5, 4).is_none());
        assert!(KeyRange::new(4, 4).is_some());
        assert!(KeyRange::new(4, 5).is_some());
    }

    #[test]
    fn test_point_is_single_key() {
        let p = KeyRange::point(7);
        assert_eq!((p.start(), p.end()), (7, 7));
        assert!(p.contains(7));
        assert!(!p.contains(6));
        assert!(!p.contains(8));
    }

    #[test]
    fn test_contains_bounds_inclusive() {
        let r = KeyRange::new(10, 20).unwrap();
        assert!(r.contains(10));
        assert!(r.contains(15));
        assert!(r.contains(20));
        assert!(!r.contains(9));
        assert!(!r.contains(21));
    }

    #[test]
    fn test_overlap_is_symmetric() {
        let a = KeyRange::new(10, 20).unwrap();
        let cases = [
            KeyRange::new(0, 9).unwrap(),
            KeyRange::new(0, 10).unwrap(),
            KeyRange::new(15, 25).unwrap(),
            KeyRange::new(20, 30).unwrap(),
            KeyRange::new(21, 30).unwrap(),
            KeyRange::new(5, 25).unwrap(),
        ];
        for b in cases {
            assert_eq!(a.overlaps(b), b.overlaps(a), "{a:?} vs {b:?}");
        }
    }

    #[test]
    fn test_adjacent_ranges_overlap_at_shared_bound() {
        // Inclusive bounds: [10,20] and [20,30] share key 20.
        assert!(
            KeyRange::new(10, 20)
                .unwrap()
                .overlaps(KeyRange::new(20, 30).unwrap())
        );
        // [10,20] and [21,30] do not.
        assert!(
            !KeyRange::new(10, 20)
                .unwrap()
                .overlaps(KeyRange::new(21, 30).unwrap())
        );
    }

    #[test]
    fn test_contained_range_overlaps() {
        let outer = KeyRange::new(0, 100).unwrap();
        let inner = KeyRange::new(40, 60).unwrap();
        assert!(outer.overlaps(inner));
        assert!(inner.overlaps(outer));
    }

    #[test]
    fn test_max_key_has_no_overflow() {
        let r = KeyRange::point(u64::MAX);
        assert!(r.contains(u64::MAX));
        assert!(r.overlaps(KeyRange::new(0, u64::MAX).unwrap()));
    }
}
