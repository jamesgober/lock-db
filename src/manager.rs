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

use crate::deadlock::{Deadlock, VictimPolicy, WaitForGraph};
use crate::{KeyRange, LockError, LockMode, ResourceId, TxnId};

/// The victim policy the deadlock-aware acquisition path uses.
const DEADLOCK_VICTIM_POLICY: VictimPolicy = VictimPolicy::Youngest;

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

/// A transaction holding a key range in a space, and the mode it holds.
#[derive(Clone, Copy)]
struct RangeHolder {
    txn: TxnId,
    range: KeyRange,
    mode: LockMode,
}

/// The active range locks in one key space.
///
/// Held in an unordered `Vec` and scanned linearly for overlap on each request.
/// Overlap is not a key-equality lookup, so a hash map does not help; an
/// interval tree would lower the asymptotic cost but is heavier and is left for
/// a later release if profiling shows range contention dominates.
struct RangeSpace {
    holders: Vec<RangeHolder>,
}

impl RangeSpace {
    #[inline]
    fn new() -> Self {
        Self {
            holders: Vec::new(),
        }
    }
}

/// The mutable state of one shard.
struct ShardInner {
    /// Point locks: resources with at least one holder, keyed by resource id.
    locks: HashMap<ResourceId, LockEntry>,
    /// Reverse index: the resources each transaction holds *in this shard*.
    by_txn: HashMap<TxnId, Vec<ResourceId>>,
    /// Range locks, keyed by the space (e.g. an index) they protect.
    ranges: HashMap<ResourceId, RangeSpace>,
    /// Reverse index for range locks: the (space, range) pairs each transaction
    /// holds *in this shard*.
    range_by_txn: HashMap<TxnId, Vec<(ResourceId, KeyRange)>>,
}

impl ShardInner {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
            by_txn: HashMap::new(),
            ranges: HashMap::new(),
            range_by_txn: HashMap::new(),
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
    /// The deadlock-aware wait set: each waiting transaction and the single
    /// (resource, mode) request it is blocked on. A global mutex, taken only by
    /// the deadlock-aware [`request`](LockManager::request) path — the
    /// non-blocking `try_acquire`/`release` fast path never touches it.
    ///
    /// Lock ordering: this mutex is always the *outer* lock. `request` takes it
    /// and then a shard mutex; nothing ever takes a shard mutex and then this
    /// one. `release_all` clears its own entry in a separate critical section,
    /// never nested with a shard lock, so no cycle is possible.
    waits: Mutex<HashMap<TxnId, (ResourceId, LockMode)>>,
}

/// The outcome of a deadlock-aware [`request`](LockManager::request).
///
/// Unlike [`try_acquire`](LockManager::try_acquire), `request` does not just
/// fail on conflict — it records the wait and tells the caller whether to
/// proceed, suspend, or abort.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "the outcome decides whether the transaction proceeds, waits, or aborts"]
pub enum Acquisition {
    /// The lock was granted; the transaction holds it and may proceed.
    Granted,
    /// The lock is held incompatibly. The transaction is now registered as
    /// waiting; the caller should suspend it and retry `request` later (for
    /// example after a release). No deadlock was found.
    Waiting,
    /// Granting the wait would close a cycle in the wait-for graph. The caller
    /// must abort the named victim (with [`release_all`](LockManager::release_all))
    /// to break the deadlock. The victim may be the requesting transaction
    /// itself or another transaction in the cycle.
    Deadlock(Deadlock),
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
            waits: Mutex::new(HashMap::new()),
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
    /// - `txn` already holds a lock on `res` that [covers](LockMode::covers)
    ///   `mode` (re-acquisition is idempotent, and asking for a weaker mode than
    ///   you hold is a no-op);
    /// - `txn` already holds `res` in some mode and the
    ///   [join](LockMode::join) of that mode with `mode` is compatible with
    ///   every other holder (an in-place upgrade — for example shared to
    ///   exclusive when sole holder, or shared plus intention-exclusive to SIX);
    ///   or
    /// - `txn` holds nothing on `res` and `mode` is compatible with every
    ///   current holder.
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
        let ShardInner { locks, by_txn, .. } = &mut *guard;
        if Self::try_grant_locked(locks, by_txn, txn, res, mode) {
            Ok(())
        } else {
            Err(LockError::Conflict)
        }
    }

    /// Attempts to grant `mode` on `res` to `txn` against an already-locked
    /// shard. Returns `true` if granted (idempotent re-acquire, in-place
    /// upgrade, or fresh grant), `false` on conflict. Shared by
    /// [`try_acquire`](Self::try_acquire) and [`request`](Self::request).
    fn try_grant_locked(
        locks: &mut HashMap<ResourceId, LockEntry>,
        by_txn: &mut HashMap<TxnId, Vec<ResourceId>>,
        txn: TxnId,
        res: ResourceId,
        mode: LockMode,
    ) -> bool {
        let entry = locks.entry(res).or_insert_with(LockEntry::new);

        if let Some(pos) = entry.holders.iter().position(|h| h.txn == txn) {
            let current = entry.holders[pos].mode;
            if current.covers(mode) {
                return true;
            }
            // Upgrade: the transaction ends up holding the join (least upper
            // bound) of what it has and what it asked for. The upgraded mode
            // must be compatible with every *other* holder.
            let target = current.join(mode);
            let blocked = entry
                .holders
                .iter()
                .enumerate()
                .any(|(i, h)| i != pos && !h.mode.compatible_with(target));
            if blocked {
                return false;
            }
            entry.holders[pos].mode = target;
            return true;
        }

        if entry.holders.iter().all(|h| h.mode.compatible_with(mode)) {
            entry.holders.push(Holder { txn, mode });
            by_txn.entry(txn).or_default().push(res);
            true
        } else {
            // The entry already had holders (an empty one would have matched the
            // vacuous `all` above and been granted), so nothing to clean up.
            false
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
        let ShardInner { locks, by_txn, .. } = &mut *guard;

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

    /// Releases every lock held by `txn` across the whole table — both point
    /// locks and range locks.
    ///
    /// This is the call a transaction layer makes at commit or abort to drop a
    /// transaction's entire lock set at once. It returns the number of locks
    /// released, and is proportional to that number rather than to the size of
    /// the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{KeyRange, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let t = TxnId::new(1);
    /// for id in 0..5 {
    ///     lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive).unwrap();
    /// }
    /// lm.try_acquire_range(t, ResourceId::new(99), KeyRange::point(1), LockMode::Shared).unwrap();
    ///
    /// assert_eq!(lm.release_all(t), 6); // 5 point locks + 1 range lock
    /// assert_eq!(lm.release_all(t), 0); // idempotent once empty
    /// ```
    pub fn release_all(&self, txn: TxnId) -> usize {
        // Clear any pending wait first, in its own critical section. This never
        // nests with a shard lock, so it cannot deadlock against `request`
        // (which takes `waits` then a shard); see the `waits` field docs.
        {
            let mut waits = self.lock_waits();
            let _ = waits.remove(&txn);
        }

        let mut released = 0;
        for shard in self.shards.iter() {
            let mut guard = Self::lock(shard);
            let ShardInner {
                locks,
                by_txn,
                ranges,
                range_by_txn,
            } = &mut *guard;

            if let Some(resources) = by_txn.remove(&txn) {
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

            if let Some(spaces) = range_by_txn.remove(&txn) {
                for (space, range) in spaces {
                    if let Some(rs) = ranges.get_mut(&space) {
                        if let Some(pos) = rs
                            .holders
                            .iter()
                            .position(|h| h.txn == txn && h.range == range)
                        {
                            let _ = rs.holders.swap_remove(pos);
                            released += 1;
                            if rs.holders.is_empty() {
                                let _ = ranges.remove(&space);
                            }
                        }
                    }
                }
            }
        }
        released
    }

    /// Acquires `mode` on `res` for `txn`, registering a wait and detecting
    /// deadlock if it cannot be granted.
    ///
    /// This is the deadlock-aware counterpart to
    /// [`try_acquire`](Self::try_acquire). The three outcomes are:
    ///
    /// - [`Acquisition::Granted`] — the lock was granted; proceed.
    /// - [`Acquisition::Waiting`] — the lock is held incompatibly and `txn` is
    ///   now recorded in the wait-for graph. The caller should suspend the
    ///   transaction and call `request` again later (for example after a
    ///   release) to retry. No deadlock was found.
    /// - [`Acquisition::Deadlock`] — granting the wait would close a cycle. The
    ///   caller must abort the [`Deadlock::victim`] with
    ///   [`release_all`](Self::release_all). The victim may be `txn` or another
    ///   transaction in the cycle.
    ///
    /// Detection is exact: the wait-for graph is rebuilt from the current lock
    /// table on every call, so a wait left over from a lock that has since been
    /// released contributes no edge, and a transaction is never reported as
    /// deadlocked unless it genuinely is. The victim is chosen by the
    /// [`VictimPolicy::Youngest`] policy; callers wanting a different policy can
    /// apply [`WaitForGraph::pick_victim`] to [`Deadlock::cycle`] themselves.
    ///
    /// Only transactions that wait through `request` appear in the graph; a
    /// transaction that spins on `try_acquire` is invisible to deadlock
    /// detection. Range locks ([`try_acquire_range`](Self::try_acquire_range))
    /// are likewise not tracked here.
    ///
    /// `request` serializes on a single wait-registry mutex, unlike the sharded
    /// `try_acquire`; it is the path to use when you need deadlock detection.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let (a, b) = (ResourceId::new(1), ResourceId::new(2));
    /// let (t1, t2) = (TxnId::new(1), TxnId::new(2));
    ///
    /// // T1 holds A, T2 holds B.
    /// assert_eq!(lm.request(t1, a, LockMode::Exclusive), Acquisition::Granted);
    /// assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Granted);
    ///
    /// // T1 waits for B (held by T2): no cycle yet.
    /// assert_eq!(lm.request(t1, b, LockMode::Exclusive), Acquisition::Waiting);
    ///
    /// // T2 now waits for A (held by T1): that closes the cycle.
    /// match lm.request(t2, a, LockMode::Exclusive) {
    ///     Acquisition::Deadlock(d) => {
    ///         assert_eq!(d.victim, TxnId::new(2)); // youngest in the cycle
    ///         lm.release_all(d.victim);            // abort to break the deadlock
    ///     }
    ///     other => panic!("expected a deadlock, got {other:?}"),
    /// }
    /// ```
    pub fn request(&self, txn: TxnId, res: ResourceId, mode: LockMode) -> Acquisition {
        // `waits` is the outer lock; the grant attempt and graph build both take
        // shard locks underneath it, never the reverse.
        let mut waits = self.lock_waits();

        let granted = {
            let mut guard = self.lock_shard(res);
            let ShardInner { locks, by_txn, .. } = &mut *guard;
            Self::try_grant_locked(locks, by_txn, txn, res, mode)
        };
        if granted {
            let _ = waits.remove(&txn);
            return Acquisition::Granted;
        }

        let _ = waits.insert(txn, (res, mode));
        let graph = self.build_wait_graph(&waits);
        match graph.cycle_from(txn) {
            Some(cycle) => {
                let victim =
                    WaitForGraph::pick_victim(&cycle, DEADLOCK_VICTIM_POLICY).unwrap_or(txn);
                Acquisition::Deadlock(Deadlock { victim, cycle })
            }
            None => Acquisition::Waiting,
        }
    }

    /// Removes any pending wait for `txn` from the wait-for graph.
    ///
    /// Call this when a transaction that previously got [`Acquisition::Waiting`]
    /// stops waiting without acquiring the lock (for example it timed out or was
    /// aborted for another reason). [`release_all`](Self::release_all) already
    /// clears the wait, so this is only needed when releasing nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let res = ResourceId::new(1);
    /// lm.request(TxnId::new(1), res, LockMode::Exclusive);
    /// // T2 waits, then gives up.
    /// assert_eq!(lm.request(TxnId::new(2), res, LockMode::Exclusive), Acquisition::Waiting);
    /// lm.cancel_wait(TxnId::new(2));
    /// assert_eq!(lm.waiting_count(), 0);
    /// ```
    pub fn cancel_wait(&self, txn: TxnId) {
        let mut waits = self.lock_waits();
        let _ = waits.remove(&txn);
    }

    /// Scans the current wait set for a deadlock, returning one if found.
    ///
    /// This is the periodic-detection counterpart to the at-wait detection in
    /// [`request`](Self::request): a background task can call it on an interval
    /// instead of (or in addition to) acting on `request`'s result. It rebuilds
    /// the wait-for graph from the current lock table, so it reports only
    /// genuine deadlocks. Returns `None` when no cycle exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{Acquisition, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let (a, b) = (ResourceId::new(1), ResourceId::new(2));
    /// lm.request(TxnId::new(1), a, LockMode::Exclusive);
    /// lm.request(TxnId::new(2), b, LockMode::Exclusive);
    /// lm.request(TxnId::new(1), b, LockMode::Exclusive); // T1 waits for T2
    /// assert!(lm.find_deadlock().is_none());
    /// lm.request(TxnId::new(2), a, LockMode::Exclusive); // T2 waits for T1: cycle
    /// assert!(lm.find_deadlock().is_some());
    /// ```
    #[must_use]
    pub fn find_deadlock(&self) -> Option<Deadlock> {
        let waits = self.lock_waits();
        let graph = self.build_wait_graph(&waits);
        let cycle = graph.detect_cycle()?;
        let victim = WaitForGraph::pick_victim(&cycle, DEADLOCK_VICTIM_POLICY)?;
        Some(Deadlock { victim, cycle })
    }

    /// Returns the number of transactions currently registered as waiting.
    ///
    /// Mostly useful for diagnostics and tests.
    #[must_use]
    pub fn waiting_count(&self) -> usize {
        self.lock_waits().len()
    }

    /// Builds a wait-for graph from the live wait set, reading the *current*
    /// holders of each waited resource from the lock table. Rebuilding from
    /// truth on every detection is what keeps detection from acting on a stale
    /// edge. Called while holding the `waits` lock; takes shard locks underneath.
    fn build_wait_graph(&self, waits: &HashMap<TxnId, (ResourceId, LockMode)>) -> WaitForGraph {
        let mut graph = WaitForGraph::new();
        for (&waiter, &(res, mode)) in waits {
            let blockers = self.holders_blocking(waiter, res, mode);
            graph.add_waits(waiter, &blockers);
        }
        graph
    }

    /// Returns the transactions, other than `waiter`, that currently hold `res`
    /// in a mode incompatible with `mode` — the transactions `waiter` is blocked
    /// by.
    fn holders_blocking(&self, waiter: TxnId, res: ResourceId, mode: LockMode) -> Vec<TxnId> {
        let guard = self.lock_shard(res);
        guard.locks.get(&res).map_or_else(Vec::new, |entry| {
            entry
                .holders
                .iter()
                .filter(|h| h.txn != waiter && !h.mode.compatible_with(mode))
                .map(|h| h.txn)
                .collect()
        })
    }

    /// Locks the wait registry, recovering its guard if the mutex was poisoned.
    #[inline]
    fn lock_waits(&self) -> MutexGuard<'_, HashMap<TxnId, (ResourceId, LockMode)>> {
        match self.waits.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
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

    /// Tries to acquire `mode` over the key range `range` in key space `space`,
    /// for `txn`, without blocking.
    ///
    /// A range lock protects a contiguous span of keys — use it to stop another
    /// transaction from inserting into, or writing within, a range you have
    /// read (phantom and predicate protection). `space` identifies the key space
    /// the range lives in, typically an index; ranges in different spaces never
    /// conflict.
    ///
    /// The request is granted unless some **other** transaction already holds an
    /// [overlapping](KeyRange::overlaps) range in `space` in an
    /// [incompatible](LockMode::compatible_with) mode. The same transaction may
    /// hold several ranges in a space, including overlapping ones; range locks
    /// are not merged or upgraded.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Conflict`] if an overlapping, incompatible range is
    /// held by another transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{KeyRange, LockError, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let index = ResourceId::new(1);
    ///
    /// // A read lock over [100, 200].
    /// lm.try_acquire_range(TxnId::new(1), index, KeyRange::new(100, 200).unwrap(), LockMode::Shared).unwrap();
    ///
    /// // Another reader may share the overlapping range...
    /// lm.try_acquire_range(TxnId::new(2), index, KeyRange::new(150, 250).unwrap(), LockMode::Shared).unwrap();
    ///
    /// // ...but a writer inside it conflicts.
    /// assert_eq!(
    ///     lm.try_acquire_range(TxnId::new(3), index, KeyRange::point(150), LockMode::Exclusive),
    ///     Err(LockError::Conflict),
    /// );
    /// ```
    pub fn try_acquire_range(
        &self,
        txn: TxnId,
        space: ResourceId,
        range: KeyRange,
        mode: LockMode,
    ) -> Result<(), LockError> {
        let mut guard = self.lock_shard(space);
        let ShardInner {
            ranges,
            range_by_txn,
            ..
        } = &mut *guard;
        let rs = ranges.entry(space).or_insert_with(RangeSpace::new);

        let conflict = rs
            .holders
            .iter()
            .any(|h| h.txn != txn && h.range.overlaps(range) && !h.mode.compatible_with(mode));
        if conflict {
            // A conflict implies a pre-existing holder, so the space entry is
            // non-empty and there is nothing to clean up.
            return Err(LockError::Conflict);
        }

        rs.holders.push(RangeHolder { txn, range, mode });
        range_by_txn.entry(txn).or_default().push((space, range));
        Ok(())
    }

    /// Releases a range lock `txn` holds over `range` in `space`.
    ///
    /// Matches on the transaction and the exact range. If the transaction holds
    /// several locks on the identical range (in different modes), one is
    /// released per call.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::NotHeld`] if `txn` holds no lock on that exact range
    /// in `space`.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{KeyRange, LockError, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let index = ResourceId::new(1);
    /// let r = KeyRange::new(1, 10).unwrap();
    /// let t = TxnId::new(1);
    ///
    /// lm.try_acquire_range(t, index, r, LockMode::Exclusive).unwrap();
    /// lm.release_range(t, index, r).unwrap();
    /// assert_eq!(lm.release_range(t, index, r), Err(LockError::NotHeld));
    /// ```
    pub fn release_range(
        &self,
        txn: TxnId,
        space: ResourceId,
        range: KeyRange,
    ) -> Result<(), LockError> {
        let mut guard = self.lock_shard(space);
        let ShardInner {
            ranges,
            range_by_txn,
            ..
        } = &mut *guard;

        let rs = match ranges.get_mut(&space) {
            Some(rs) => rs,
            None => return Err(LockError::NotHeld),
        };
        let pos = match rs
            .holders
            .iter()
            .position(|h| h.txn == txn && h.range == range)
        {
            Some(pos) => pos,
            None => return Err(LockError::NotHeld),
        };

        let _ = rs.holders.swap_remove(pos);
        if rs.holders.is_empty() {
            let _ = ranges.remove(&space);
        }
        Self::forget_range(range_by_txn, txn, space, range);
        Ok(())
    }

    /// Returns the number of range locks currently held in `space`.
    ///
    /// Counts every holder, across all transactions and modes. Mostly useful
    /// for diagnostics and tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{KeyRange, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let index = ResourceId::new(1);
    /// assert_eq!(lm.range_count(index), 0);
    /// lm.try_acquire_range(TxnId::new(1), index, KeyRange::point(1), LockMode::Shared).unwrap();
    /// assert_eq!(lm.range_count(index), 1);
    /// ```
    #[must_use]
    pub fn range_count(&self, space: ResourceId) -> usize {
        let guard = self.lock_shard(space);
        guard.ranges.get(&space).map_or(0, |rs| rs.holders.len())
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

    /// Drops one `(space, range)` pair from a transaction's range reverse-index
    /// entry, removing the entry entirely once it is empty.
    #[inline]
    fn forget_range(
        range_by_txn: &mut HashMap<TxnId, Vec<(ResourceId, KeyRange)>>,
        txn: TxnId,
        space: ResourceId,
        range: KeyRange,
    ) {
        if let Some(held) = range_by_txn.get_mut(&txn) {
            if let Some(pos) = held.iter().position(|(s, r)| *s == space && *r == range) {
                let _ = held.swap_remove(pos);
            }
            if held.is_empty() {
                let _ = range_by_txn.remove(&txn);
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
    use super::{Acquisition, FIB_HASH, LockManager};
    use crate::{KeyRange, LockError, LockMode, ResourceId, TxnId};

    fn ids(t: u64, r: u64) -> (TxnId, ResourceId) {
        (TxnId::new(t), ResourceId::new(r))
    }

    fn kr(start: u64, end: u64) -> KeyRange {
        KeyRange::new(start, end).unwrap()
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
    fn test_intention_shared_and_intention_exclusive_coexist() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::IntentionShared)
            .unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::IntentionExclusive)
            .unwrap();
        assert_eq!(lm.holder_count(r), 2);
    }

    #[test]
    fn test_intention_exclusive_blocks_shared() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::IntentionExclusive)
            .unwrap();
        assert_eq!(
            lm.try_acquire(TxnId::new(2), r, LockMode::Shared),
            Err(LockError::Conflict)
        );
        // ...but another IX or an IS is fine.
        lm.try_acquire(TxnId::new(3), r, LockMode::IntentionExclusive)
            .unwrap();
        lm.try_acquire(TxnId::new(4), r, LockMode::IntentionShared)
            .unwrap();
    }

    #[test]
    fn test_shared_plus_intention_exclusive_upgrades_to_six() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let t = TxnId::new(1);
        lm.try_acquire(t, r, LockMode::Shared).unwrap();
        // Same txn now intends to write part of the subtree: S join IX = SIX.
        lm.try_acquire(t, r, LockMode::IntentionExclusive).unwrap();
        assert_eq!(lm.mode_held(t, r), Some(LockMode::SharedIntentionExclusive));
        // An intention-shared holder still coexists with SIX.
        lm.try_acquire(TxnId::new(2), r, LockMode::IntentionShared)
            .unwrap();
        // But a second reader does not.
        assert_eq!(
            lm.try_acquire(TxnId::new(3), r, LockMode::Shared),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_intention_shared_upgrades_to_exclusive_when_sole_holder() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let t = TxnId::new(1);
        lm.try_acquire(t, r, LockMode::IntentionShared).unwrap();
        lm.try_acquire(t, r, LockMode::Exclusive).unwrap();
        assert_eq!(lm.mode_held(t, r), Some(LockMode::Exclusive));
    }

    #[test]
    fn test_upgrade_to_six_blocked_by_other_reader() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        lm.try_acquire(TxnId::new(1), r, LockMode::Shared).unwrap();
        lm.try_acquire(TxnId::new(2), r, LockMode::Shared).unwrap();
        // Txn 1 wants IX too (-> SIX), but SIX is incompatible with txn 2's S.
        assert_eq!(
            lm.try_acquire(TxnId::new(1), r, LockMode::IntentionExclusive),
            Err(LockError::Conflict)
        );
        // The original shared lock is intact.
        assert_eq!(lm.mode_held(TxnId::new(1), r), Some(LockMode::Shared));
    }

    #[test]
    fn test_hierarchy_protocol_row_write_under_table_intent() {
        // Model a database/table/page/row hierarchy as four resources, and run
        // the standard protocol: IX coarse-to-fine, then X on the row.
        let lm = LockManager::new();
        let (db, table, page, row) = (
            ResourceId::new(1),
            ResourceId::new(2),
            ResourceId::new(3),
            ResourceId::new(4),
        );
        let writer = TxnId::new(1);
        for res in [db, table, page] {
            lm.try_acquire(writer, res, LockMode::IntentionExclusive)
                .unwrap();
        }
        lm.try_acquire(writer, row, LockMode::Exclusive).unwrap();

        // A concurrent reader can still take IS down to a different page/row.
        let reader = TxnId::new(2);
        for res in [db, table] {
            lm.try_acquire(reader, res, LockMode::IntentionShared)
                .unwrap();
        }
        // But it cannot read the row the writer holds exclusively.
        assert_eq!(
            lm.try_acquire(reader, row, LockMode::Shared),
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

    // ---- range locks ----

    #[test]
    fn test_range_shared_overlap_coexists() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 100), LockMode::Shared)
            .unwrap();
        lm.try_acquire_range(TxnId::new(2), space, kr(50, 150), LockMode::Shared)
            .unwrap();
        assert_eq!(lm.range_count(space), 2);
    }

    #[test]
    fn test_range_exclusive_conflicts_on_overlap() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(100, 200), LockMode::Shared)
            .unwrap();
        assert_eq!(
            lm.try_acquire_range(
                TxnId::new(2),
                space,
                KeyRange::point(150),
                LockMode::Exclusive
            ),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_range_disjoint_ranges_do_not_conflict() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 100), LockMode::Exclusive)
            .unwrap();
        lm.try_acquire_range(TxnId::new(2), space, kr(101, 200), LockMode::Exclusive)
            .unwrap();
    }

    #[test]
    fn test_range_adjacent_inclusive_bounds_conflict() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 100), LockMode::Exclusive)
            .unwrap();
        // [100, 200] shares key 100 with [0, 100].
        assert_eq!(
            lm.try_acquire_range(TxnId::new(2), space, kr(100, 200), LockMode::Shared),
            Err(LockError::Conflict)
        );
    }

    #[test]
    fn test_range_different_spaces_independent() {
        let lm = LockManager::new();
        lm.try_acquire_range(
            TxnId::new(1),
            ResourceId::new(1),
            kr(0, 100),
            LockMode::Exclusive,
        )
        .unwrap();
        // Same range, different space: no conflict.
        lm.try_acquire_range(
            TxnId::new(2),
            ResourceId::new(2),
            kr(0, 100),
            LockMode::Exclusive,
        )
        .unwrap();
    }

    #[test]
    fn test_range_same_txn_overlap_allowed() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        let t = TxnId::new(1);
        lm.try_acquire_range(t, space, kr(0, 100), LockMode::Exclusive)
            .unwrap();
        // A transaction does not conflict with its own ranges.
        lm.try_acquire_range(t, space, kr(50, 150), LockMode::Exclusive)
            .unwrap();
        assert_eq!(lm.range_count(space), 2);
    }

    #[test]
    fn test_range_release_frees_overlap() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        let r = kr(100, 200);
        lm.try_acquire_range(TxnId::new(1), space, r, LockMode::Exclusive)
            .unwrap();
        lm.release_range(TxnId::new(1), space, r).unwrap();
        assert_eq!(lm.range_count(space), 0);
        // Now another writer can take an overlapping range.
        lm.try_acquire_range(
            TxnId::new(2),
            space,
            KeyRange::point(150),
            LockMode::Exclusive,
        )
        .unwrap();
    }

    #[test]
    fn test_range_release_not_held_errors() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        assert_eq!(
            lm.release_range(TxnId::new(1), space, kr(0, 10)),
            Err(LockError::NotHeld)
        );
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 10), LockMode::Shared)
            .unwrap();
        // Wrong range is NotHeld.
        assert_eq!(
            lm.release_range(TxnId::new(1), space, kr(0, 11)),
            Err(LockError::NotHeld)
        );
    }

    #[test]
    fn test_release_all_drops_point_and_range_locks() {
        let lm = LockManager::new();
        let t = TxnId::new(1);
        for id in 0..3 {
            lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive)
                .unwrap();
        }
        lm.try_acquire_range(t, ResourceId::new(100), kr(0, 10), LockMode::Shared)
            .unwrap();
        lm.try_acquire_range(t, ResourceId::new(100), kr(20, 30), LockMode::Shared)
            .unwrap();
        assert_eq!(lm.release_all(t), 5); // 3 point + 2 range
        assert_eq!(lm.range_count(ResourceId::new(100)), 0);
        assert_eq!(lm.release_all(t), 0);
    }

    #[test]
    fn test_release_all_range_leaves_other_txn() {
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 100), LockMode::Shared)
            .unwrap();
        lm.try_acquire_range(TxnId::new(2), space, kr(0, 100), LockMode::Shared)
            .unwrap();
        assert_eq!(lm.release_all(TxnId::new(1)), 1);
        assert_eq!(lm.range_count(space), 1);
    }

    #[test]
    fn test_range_intention_modes_coexist() {
        // IS and IX range locks are compatible, just like point locks.
        let lm = LockManager::new();
        let space = ResourceId::new(1);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 100), LockMode::IntentionShared)
            .unwrap();
        lm.try_acquire_range(
            TxnId::new(2),
            space,
            kr(0, 100),
            LockMode::IntentionExclusive,
        )
        .unwrap();
        assert_eq!(lm.range_count(space), 2);
    }

    // ---- deadlock-aware request ----

    #[test]
    fn test_request_granted_on_free_resource() {
        let lm = LockManager::new();
        let (t, r) = ids(1, 1);
        assert_eq!(lm.request(t, r, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.mode_held(t, r), Some(LockMode::Exclusive));
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_request_waiting_registers_wait() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        assert_eq!(
            lm.request(TxnId::new(1), r, LockMode::Exclusive),
            Acquisition::Granted
        );
        assert_eq!(
            lm.request(TxnId::new(2), r, LockMode::Exclusive),
            Acquisition::Waiting
        );
        assert_eq!(lm.waiting_count(), 1);
    }

    #[test]
    fn test_request_grant_clears_prior_wait() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let _ = lm.request(TxnId::new(1), r, LockMode::Exclusive);
        assert_eq!(
            lm.request(TxnId::new(2), r, LockMode::Exclusive),
            Acquisition::Waiting
        );
        // T1 releases; T2 retries and is granted, clearing its wait.
        lm.release(TxnId::new(1), r).unwrap();
        assert_eq!(
            lm.request(TxnId::new(2), r, LockMode::Exclusive),
            Acquisition::Granted
        );
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_classic_two_transaction_deadlock() {
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));

        assert_eq!(lm.request(t1, a, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.request(t1, b, LockMode::Exclusive), Acquisition::Waiting);

        match lm.request(t2, a, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => {
                assert_eq!(d.victim, t2); // youngest in the cycle
                assert_eq!(d.cycle.len(), 2);
                assert!(d.cycle.contains(&t1) && d.cycle.contains(&t2));
            }
            other => panic!("expected deadlock, got {other:?}"),
        }
    }

    #[test]
    fn test_three_transaction_deadlock_cycle() {
        let lm = LockManager::new();
        let (a, b, c) = (ResourceId::new(1), ResourceId::new(2), ResourceId::new(3));
        let (t1, t2, t3) = (TxnId::new(1), TxnId::new(2), TxnId::new(3));

        let _ = lm.request(t1, a, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let _ = lm.request(t3, c, LockMode::Exclusive);
        // T1->B(T2), T2->C(T3), T3->A(T1): closes the loop on the third wait.
        assert_eq!(lm.request(t1, b, LockMode::Exclusive), Acquisition::Waiting);
        assert_eq!(lm.request(t2, c, LockMode::Exclusive), Acquisition::Waiting);
        match lm.request(t3, a, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => {
                assert_eq!(d.cycle.len(), 3);
                assert_eq!(d.victim, t3); // youngest
            }
            other => panic!("expected deadlock, got {other:?}"),
        }
    }

    #[test]
    fn test_aborting_victim_breaks_deadlock() {
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));

        let _ = lm.request(t1, a, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let _ = lm.request(t1, b, LockMode::Exclusive);
        let victim = match lm.request(t2, a, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => d.victim,
            other => panic!("expected deadlock, got {other:?}"),
        };
        // Abort the victim: releases its locks and clears its wait.
        lm.release_all(victim);
        // The other transaction can now make progress.
        let survivor = if victim == t1 { t2 } else { t1 };
        let want = if survivor == t1 { b } else { a };
        assert_eq!(
            lm.request(survivor, want, LockMode::Exclusive),
            Acquisition::Granted
        );
        assert!(lm.find_deadlock().is_none());
    }

    #[test]
    fn test_no_false_deadlock_after_release() {
        // T1 waits for T2; T2 releases (not via the wait path). A later detection
        // must not report a deadlock from the now-stale wait edge.
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));

        let _ = lm.request(t1, a, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let _ = lm.request(t1, b, LockMode::Exclusive); // T1 waits for T2 on B
        lm.release(t2, b).unwrap(); // B is now free; T1's edge is stale
        // T2 wants A (held by T1). Were T1's stale edge still counted, this would
        // look like a cycle. It must not: B is free, so T1 has no real out-edge.
        assert_eq!(lm.request(t2, a, LockMode::Exclusive), Acquisition::Waiting);
        assert!(lm.find_deadlock().is_none());
    }

    #[test]
    fn test_cancel_wait_removes_from_graph() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let _ = lm.request(TxnId::new(1), r, LockMode::Exclusive);
        assert_eq!(
            lm.request(TxnId::new(2), r, LockMode::Exclusive),
            Acquisition::Waiting
        );
        lm.cancel_wait(TxnId::new(2));
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_release_all_clears_wait() {
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let _ = lm.request(TxnId::new(1), r, LockMode::Exclusive);
        let _ = lm.request(TxnId::new(2), r, LockMode::Exclusive); // T2 waits
        assert_eq!(lm.waiting_count(), 1);
        lm.release_all(TxnId::new(2));
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_find_deadlock_none_without_cycle() {
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let _ = lm.request(TxnId::new(1), a, LockMode::Exclusive);
        let _ = lm.request(TxnId::new(2), b, LockMode::Exclusive);
        let _ = lm.request(TxnId::new(1), b, LockMode::Exclusive); // one-way wait
        assert!(lm.find_deadlock().is_none());
    }

    #[test]
    fn test_shared_requests_do_not_deadlock() {
        // Two shared requests on the same resource both grant; no waiting.
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        assert_eq!(
            lm.request(TxnId::new(1), r, LockMode::Shared),
            Acquisition::Granted
        );
        assert_eq!(
            lm.request(TxnId::new(2), r, LockMode::Shared),
            Acquisition::Granted
        );
        assert_eq!(lm.waiting_count(), 0);
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
