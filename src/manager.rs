//! The lock table: a sharded, contention-aware map from resources to holders.
//!
//! # Design
//!
//! A single global mutex over the whole lock table would serialise every
//! acquire and release in the database, turning the lock manager itself into
//! the bottleneck it exists to manage. Instead the table is split into a fixed
//! number of independent shards, each guarding its own slice of the resource
//! space behind its own mutex. Two transactions touching resources in different
//! shards never contend on the same lock. The shard for a resource is chosen by
//! Fibonacci hashing its id, which spreads sequential ids (the common case for
//! page and row numbers) evenly across shards without paying for a
//! general-purpose hasher on the hot path.
//!
//! Each shard also keeps a reverse index from transaction to the resources it
//! holds in that shard, so releasing every lock a transaction owns is
//! proportional to the number of locks held, not to the size of the table.
//!
//! This release ([crate-level docs](crate)) provides non-blocking acquisition:
//! a request that cannot be granted immediately returns [`LockError::Conflict`]
//! rather than waiting. Blocking acquisition with wait queues, and the
//! deadlock detection that requires it, arrive in a later milestone.

#[cfg(loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::{Mutex, MutexGuard};

use std::collections::HashMap;

use crate::{LockError, LockMode, ResourceId, TxnId};

/// Multiplier for Fibonacci hashing: 2^64 divided by the golden ratio.
const FIB_HASH: u64 = 0x9E37_79B9_7F4A_7C15;

/// A transaction holding a resource, and the mode it holds it in.
#[derive(Clone, Copy)]
struct Holder {
    txn: TxnId,
    mode: LockMode,
}

/// The set of transactions currently holding one resource.
///
/// Holders are kept in an unordered `Vec` because the common case is a handful
/// of shared readers or a single writer; a linear scan over a short, contiguous
/// slice beats the constant overhead and indirection of a map for those sizes.
struct LockEntry {
    holders: Vec<Holder>,
}

impl LockEntry {
    #[inline]
    fn new() -> Self {
        Self {
            holders: Vec::new(),
        }
    }
}

/// The mutable state of one shard.
struct ShardInner {
    /// Resources with at least one holder, keyed by resource id.
    locks: HashMap<ResourceId, LockEntry>,
    /// Reverse index: the resources each transaction holds *in this shard*.
    by_txn: HashMap<TxnId, Vec<ResourceId>>,
}

impl ShardInner {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
            by_txn: HashMap::new(),
        }
    }
}

/// One independently locked partition of the table.
struct Shard {
    inner: Mutex<ShardInner>,
}

/// A sharded lock table mapping resources to the transactions that hold them.
///
/// `LockManager` is the primary entry point of the crate. It is `Send + Sync`
/// and is meant to be shared behind an [`std::sync::Arc`] across all worker
/// threads; every method takes `&self`, so no outer lock is needed.
///
/// # Examples
///
/// ```
/// use lock_db::{LockManager, LockMode, ResourceId, TxnId};
///
/// let lm = LockManager::new();
/// let row = ResourceId::new(100);
/// let (t1, t2) = (TxnId::new(1), TxnId::new(2));
///
/// // Two transactions read the same row concurrently.
/// lm.try_acquire(t1, row, LockMode::Shared).unwrap();
/// lm.try_acquire(t2, row, LockMode::Shared).unwrap();
/// assert_eq!(lm.holder_count(row), 2);
///
/// // Neither can take it exclusively while the other reads.
/// assert!(lm.try_acquire(t1, row, LockMode::Exclusive).is_err());
///
/// // After both release, an exclusive lock is free to take.
/// lm.release(t1, row).unwrap();
/// lm.release(t2, row).unwrap();
/// lm.try_acquire(t1, row, LockMode::Exclusive).unwrap();
/// ```
#[must_use = "a LockManager that is dropped immediately releases every lock it holds"]
pub struct LockManager {
    shards: Box<[Shard]>,
    /// `log2(shards.len())`; `0` when there is a single shard.
    bits: u32,
}

impl LockManager {
    /// Creates a lock manager with a shard count chosen for the current machine.
    ///
    /// The count scales with the number of available CPUs (rounded up to a power
    /// of two) so that contention on any single shard mutex stays low on
    /// multi-core systems. Use [`with_shards`](Self::with_shards) to pin an
    /// exact count, for example in tests or on memory-constrained targets.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockManager;
    ///
    /// let lm = LockManager::new();
    /// assert!(lm.shards().is_power_of_two());
    /// ```
    pub fn new() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let target = (parallelism.saturating_mul(4))
            .next_power_of_two()
            .clamp(16, 1024);
        Self::with_shards(target)
    }

    /// Creates a lock manager with an explicit shard count.
    ///
    /// `shards` is rounded up to the next power of two (and a request of `0` is
    /// treated as `1`), which lets the shard lookup use a shift instead of a
    /// remainder. More shards reduce contention but cost a mutex and two small
    /// maps each; fewer shards save memory at the cost of more collisions.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::LockManager;
    ///
    /// // Rounded up to the next power of two.
    /// assert_eq!(LockManager::with_shards(5).shards(), 8);
    /// assert_eq!(LockManager::with_shards(0).shards(), 1);
    /// ```
    pub fn with_shards(shards: usize) -> Self {
        let n = shards.max(1).next_power_of_two();
        let bits = n.trailing_zeros();
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(Shard {
                inner: Mutex::new(ShardInner::new()),
            });
        }
        Self {
            shards: v.into_boxed_slice(),
            bits,
        }
    }

    /// Returns the number of shards in the table.
    ///
    /// Always a power of two.
    #[inline]
    #[must_use]
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    /// Tries to acquire `mode` on `res` for `txn` without blocking.
    ///
    /// The request is granted immediately and `Ok(())` is returned when:
    ///
    /// - `txn` already holds a lock on `res` that covers `mode` (re-acquisition
    ///   is idempotent, and asking for a weaker mode than you hold is a no-op);
    /// - `txn` already holds `res` shared, wants it exclusive, and is the only
    ///   holder (an in-place upgrade); or
    /// - no other transaction holds `res` in a mode incompatible with `mode`.
    ///
    /// Otherwise nothing is changed and [`LockError::Conflict`] is returned. The
    /// caller decides whether to retry, wait, or abort; this method never blocks
    /// the calling thread.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Conflict`] if the lock cannot be granted right now.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let key = ResourceId::new(7);
    /// let t = TxnId::new(1);
    ///
    /// // Upgrade a shared lock to exclusive while sole holder.
    /// lm.try_acquire(t, key, LockMode::Shared).unwrap();
    /// lm.try_acquire(t, key, LockMode::Exclusive).unwrap();
    /// assert_eq!(lm.mode_held(t, key), Some(LockMode::Exclusive));
    ///
    /// // A second reader now conflicts with the upgraded exclusive lock.
    /// let r = lm.try_acquire(TxnId::new(2), key, LockMode::Shared);
    /// assert_eq!(r, Err(LockError::Conflict));
    /// ```
    pub fn try_acquire(
        &self,
        txn: TxnId,
        res: ResourceId,
        mode: LockMode,
    ) -> Result<(), LockError> {
        let mut guard = self.lock_shard(res);
        let ShardInner { locks, by_txn } = &mut *guard;
        let entry = locks.entry(res).or_insert_with(LockEntry::new);

        if let Some(pos) = entry.holders.iter().position(|h| h.txn == txn) {
            let current = entry.holders[pos].mode;
            if current.covers(mode) {
                return Ok(());
            }
            // Upgrade request (shared -> exclusive): only when sole holder.
            if entry.holders.len() == 1 {
                entry.holders[pos].mode = mode;
                return Ok(());
            }
            return Err(LockError::Conflict);
        }

        if entry.holders.iter().all(|h| h.mode.compatible_with(mode)) {
            entry.holders.push(Holder { txn, mode });
            by_txn.entry(txn).or_default().push(res);
            Ok(())
        } else {
            // The entry already had holders (an empty one would have matched the
            // vacuous `all` above and been granted), so nothing to clean up.
            Err(LockError::Conflict)
        }
    }

    /// Releases the lock `txn` holds on `res`.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::NotHeld`] if `txn` holds no lock on `res`, which
    /// usually means a double release or a bookkeeping mismatch in the caller.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{LockError, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let key = ResourceId::new(3);
    /// let t = TxnId::new(1);
    ///
    /// lm.try_acquire(t, key, LockMode::Exclusive).unwrap();
    /// lm.release(t, key).unwrap();
    /// assert_eq!(lm.release(t, key), Err(LockError::NotHeld));
    /// ```
    pub fn release(&self, txn: TxnId, res: ResourceId) -> Result<(), LockError> {
        let mut guard = self.lock_shard(res);
        let ShardInner { locks, by_txn } = &mut *guard;

        let entry = match locks.get_mut(&res) {
            Some(entry) => entry,
            None => return Err(LockError::NotHeld),
        };
        let pos = match entry.holders.iter().position(|h| h.txn == txn) {
            Some(pos) => pos,
            None => return Err(LockError::NotHeld),
        };

        let _ = entry.holders.swap_remove(pos);
        if entry.holders.is_empty() {
            let _ = locks.remove(&res);
        }
        Self::forget_resource(by_txn, txn, res);
        Ok(())
    }

    /// Releases every lock held by `txn` across the whole table.
    ///
    /// This is the call a transaction layer makes at commit or abort to drop a
    /// transaction's entire lock set at once. It returns the number of locks
    /// released, and is proportional to that number rather than to the size of
    /// the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let t = TxnId::new(1);
    /// for id in 0..5 {
    ///     lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive).unwrap();
    /// }
    ///
    /// assert_eq!(lm.release_all(t), 5);
    /// assert_eq!(lm.release_all(t), 0); // idempotent once empty
    /// ```
    pub fn release_all(&self, txn: TxnId) -> usize {
        let mut released = 0;
        for shard in self.shards.iter() {
            let mut guard = Self::lock(shard);
            let ShardInner { locks, by_txn } = &mut *guard;
            let Some(resources) = by_txn.remove(&txn) else {
                continue;
            };
            for res in resources {
                if let Some(entry) = locks.get_mut(&res) {
                    if let Some(pos) = entry.holders.iter().position(|h| h.txn == txn) {
                        let _ = entry.holders.swap_remove(pos);
                        released += 1;
                        if entry.holders.is_empty() {
                            let _ = locks.remove(&res);
                        }
                    }
                }
            }
        }
        released
    }

    /// Returns the number of transactions currently holding `res`.
    ///
    /// Mostly useful for diagnostics and tests; in steady state this is `0`,
    /// `1` for an exclusive lock, or the reader count for a shared lock.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let key = ResourceId::new(1);
    /// assert_eq!(lm.holder_count(key), 0);
    /// lm.try_acquire(TxnId::new(1), key, LockMode::Shared).unwrap();
    /// assert_eq!(lm.holder_count(key), 1);
    /// ```
    #[must_use]
    pub fn holder_count(&self, res: ResourceId) -> usize {
        let guard = self.lock_shard(res);
        guard.locks.get(&res).map_or(0, |e| e.holders.len())
    }

    /// Returns the mode in which `txn` holds `res`, or `None` if it holds no
    /// lock on it.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let key = ResourceId::new(1);
    /// let t = TxnId::new(1);
    /// assert_eq!(lm.mode_held(t, key), None);
    /// lm.try_acquire(t, key, LockMode::Shared).unwrap();
    /// assert_eq!(lm.mode_held(t, key), Some(LockMode::Shared));
    /// ```
    #[must_use]
    pub fn mode_held(&self, txn: TxnId, res: ResourceId) -> Option<LockMode> {
        let guard = self.lock_shard(res);
        guard
            .locks
            .get(&res)
            .and_then(|e| e.holders.iter().find(|h| h.txn == txn))
            .map(|h| h.mode)
    }

    /// Drops `res` from a transaction's reverse-index entry, removing the entry
    /// entirely once the transaction holds nothing else in the shard.
    #[inline]
    fn forget_resource(by_txn: &mut HashMap<TxnId, Vec<ResourceId>>, txn: TxnId, res: ResourceId) {
        if let Some(resources) = by_txn.get_mut(&txn) {
            if let Some(pos) = resources.iter().position(|r| *r == res) {
                let _ = resources.swap_remove(pos);
            }
            if resources.is_empty() {
                let _ = by_txn.remove(&txn);
            }
        }
    }

    /// Locks and returns the shard that owns `res`.
    #[inline]
    fn lock_shard(&self, res: ResourceId) -> MutexGuard<'_, ShardInner> {
        Self::lock(&self.shards[self.shard_index(res)])
    }

    /// Locks a shard, recovering its guard if the mutex was poisoned.
    ///
    /// Critical sections in this module perform only infallible map and vector
    /// operations and never panic, so poisoning cannot leave inconsistent
    /// state. Recovering the guard keeps the lock manager available rather than
    /// propagating a poison error that no caller could act on.
    #[inline]
    fn lock(shard: &Shard) -> MutexGuard<'_, ShardInner> {
        match shard.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Maps a resource id to a shard index via Fibonacci hashing.
    #[inline]
    fn shard_index(&self, res: ResourceId) -> usize {
        if self.bits == 0 {
            return 0;
        }
        let hash = res.get().wrapping_mul(FIB_HASH);
        // Take the top `bits` bits: the most-mixed end of a multiplicative hash.
        (hash >> (u64::BITS - self.bits)) as usize
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{FIB_HASH, LockManager};
    use crate::{LockError, LockMode, ResourceId, TxnId};

    fn ids(t: u64, r: u64) -> (TxnId, ResourceId) {
        (TxnId::new(t), ResourceId::new(r))
    }

    #[test]
    fn test_shared_locks_coexist() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(3), r, LockMode::Shared).unwrap();
        assert_eq!(lm.holder_count(r), 3);
    }

    #[test]
    fn test_exclusive_excludes_shared() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Exclusive).unwrap();
        assert_eq!(
            lm.try_acquire(TxnId::new(2), r, LockMode::Shared),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_exclusive_excludes_exclusive() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Exclusive).unwrap();
        assert_eq!(
            lm.try_acquire(TxnId::new(2), r, LockMode::Exclusive),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_shared_blocks_other_exclusive() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        assert_eq!(
            lm.try_acquire(TxnId::new(2), r, LockMode::Exclusive),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_reacquire_same_mode_is_idempotent() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        assert_eq!(lm.holder_count(r), 1);
    }

    #[test]
    fn test_request_weaker_than_held_is_noop() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Exclusive).unwrap();
        // Asking for shared while holding exclusive keeps the stronger mode.
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        assert_eq!(lm.mode_held(t1, r), Some(LockMode::Exclusive));
        assert_eq!(lm.holder_count(r), 1);
    }

    #[test]
    fn test_upgrade_sole_holder_succeeds() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        lm.try_acquire(t1, r, LockMode::Exclusive).unwrap();
        assert_eq!(lm.mode_held(t1, r), Some(LockMode::Exclusive));
        assert_eq!(lm.holder_count(r), 1);
    }

    #[test]
    fn test_upgrade_blocked_by_other_reader() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::Shared).unwrap();
        assert_eq!(
            lm.try_acquire(TxnId::new(1), r, LockMode::Exclusive),
            Err(LockError::Conflict)
        );
        // The failed upgrade left the original shared lock intact.
        assert_eq!(lm.mode_held(TxnId::new(1), r), Some(LockMode::Shared));
    }

    #[test]
    fn test_release_frees_resource_for_exclusive() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::Shared).unwrap();
        lm.release(TxnId::new(1), r).unwrap();
        // One reader remains, exclusive still blocked.
        assert!(
            lm.try_acquire(TxnId::new(3), r, LockMode::Exclusive)
                .is_err()
        );
        lm.release(TxnId::new(2), r).unwrap();
        lm.try_acquire(TxnId::new(3), r, LockMode::Exclusive)
            .unwrap();
    }

    #[test]
    fn test_release_not_held_errors() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        assert_eq!(lm.release(t1, r), Err(LockError::NotHeld));
        lm.try_acquire(t1, r, LockMode::Shared).unwrap();
        assert_eq!(lm.release(TxnId::new(9), r), Err(LockError::NotHeld));
    }

    #[test]
    fn test_double_release_errors() {
        let lm = LockManager::new();
        let (t1, r) = ids(1, 1);
        lm.try_acquire(t1, r, LockMode::Exclusive).unwrap();
        lm.release(t1, r).unwrap();
        assert_eq!(lm.release(t1, r), Err(LockError::NotHeld));
    }

    #[test]
    fn test_release_all_drops_every_lock() {
        let lm = LockManager::with_shards(8);
        let t = TxnId::new(1);
        for id in 0..50 {
            lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive)
                .unwrap();
        }
        assert_eq!(lm.release_all(t), 50);
        for id in 0..50 {
            assert_eq!(lm.holder_count(ResourceId::new(id)), 0);
        }
        assert_eq!(lm.release_all(t), 0);
    }

    #[test]
    fn test_release_all_leaves_other_txns_alone() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::Shared).unwrap();
        assert_eq!(lm.release_all(TxnId::new(1)), 1);
        assert_eq!(lm.mode_held(TxnId::new(2), r), Some(LockMode::Shared));
        assert_eq!(lm.holder_count(r), 1);
    }

    #[test]
    fn test_resource_fully_released_can_be_taken_exclusively() {
        let lm = LockManager::new();
        let r = ResourceId::new(42);
        lm.try_acquire(TxnId::new(1), r, LockMode::Exclusive)
            .unwrap();
        lm.release(TxnId::new(1), r).unwrap();
        assert_eq!(lm.holder_count(r), 0);
        lm.try_acquire(TxnId::new(2), r, LockMode::Exclusive)
            .unwrap();
    }

    #[test]
    fn test_with_shards_rounds_up_to_power_of_two() {
        assert_eq!(LockManager::with_shards(1).shards(), 1);
        assert_eq!(LockManager::with_shards(3).shards(), 4);
        assert_eq!(LockManager::with_shards(5).shards(), 8);
        assert_eq!(LockManager::with_shards(0).shards(), 1);
        assert_eq!(LockManager::with_shards(64).shards(), 64);
    }

    #[test]
    fn test_single_shard_routes_everything_to_index_zero() {
        let lm = LockManager::with_shards(1);
        for id in 0..1000 {
            assert_eq!(lm.shard_index(ResourceId::new(id)), 0);
        }
    }

    #[test]
    fn test_shard_index_within_bounds() {
        let lm = LockManager::with_shards(16);
        for id in 0..10_000 {
            assert!(lm.shard_index(ResourceId::new(id)) < 16);
        }
    }

    #[test]
    fn test_sequential_ids_spread_across_shards() {
        let lm = LockManager::with_shards(16);
        let mut seen = [false; 16];
        for id in 0..256 {
            seen[lm.shard_index(ResourceId::new(id))] = true;
        }
        // Fibonacci hashing should touch every shard well before 256 ids.
        assert!(seen.iter().all(|&hit| hit));
    }

    #[test]
    fn test_locks_in_different_shards_are_independent() {
        // Two resources that hash to different shards do not interfere.
        let lm = LockManager::with_shards(16);
        let a = ResourceId::new(1);
        let b = ResourceId::new(2);
        lm.try_acquire(TxnId::new(1), a, LockMode::Exclusive)
            .unwrap();
        lm.try_acquire(TxnId::new(2), b, LockMode::Exclusive)
            .unwrap();
        assert_eq!(lm.holder_count(a), 1);
        assert_eq!(lm.holder_count(b), 1);
    }

    #[test]
    fn test_fib_hash_constant_is_odd() {
        // A multiplicative-hash multiplier must be odd to be a bijection mod 2^64.
        assert_eq!(FIB_HASH & 1, 1);
    }

    #[test]
    fn test_concurrent_shared_acquire_release_is_consistent() {
        use std::sync::Arc;
        use std::thread;

        let lm = Arc::new(LockManager::new());
        let r = ResourceId::new(7);
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let lm = Arc::clone(&lm);
            handles.push(thread::spawn(move || {
                let txn = TxnId::new(t);
                for _ in 0..1000 {
                    lm.try_acquire(txn, r, LockMode::Shared).unwrap();
                    lm.release(txn, r).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Every acquire was paired with a release; the resource is free.
        assert_eq!(lm.holder_count(r), 0);
    }

    #[test]
    fn test_concurrent_exclusive_is_mutually_exclusive() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let lm = Arc::new(LockManager::new());
        let active = Arc::new(AtomicUsize::new(0));
        let r = ResourceId::new(11);
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let lm = Arc::clone(&lm);
            let active = Arc::clone(&active);
            handles.push(thread::spawn(move || {
                let txn = TxnId::new(t);
                for _ in 0..2000 {
                    if lm.try_acquire(txn, r, LockMode::Exclusive).is_ok() {
                        // While we hold X, no one else may be inside this region.
                        let inside = active.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(inside, 0);
                        active.fetch_sub(1, Ordering::SeqCst);
                        lm.release(txn, r).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(lm.holder_count(r), 0);
    }
}
