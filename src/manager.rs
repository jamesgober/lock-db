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
//! Each shard keeps a reverse index from transaction to the resources it holds
//! in that shard. A second, smaller table sharded by transaction id records,
//! per transaction, the shards it holds locks in and the one resource it is
//! waiting for, so releasing every lock a transaction owns visits only the
//! shards it used.
//!
//! [`try_acquire`](LockManager::try_acquire) is non-blocking: a request that
//! cannot be granted returns [`LockError::Conflict`] and is not tracked.
//! [`request`](LockManager::request) is the deadlock-aware path: a request that
//! cannot be granted joins a first-in, first-out wait queue on the resource and
//! is checked for a deadlock by walking the wait-for edges reachable from it.
//! lock-db does not park threads; the caller suspends and retries.
//!
//! # Lock ordering
//!
//! There are three kinds of mutex, always taken in this order: a resource
//! shard, then a transaction shard, then the victim registry. Any of them may be
//! skipped, but none is ever taken while holding one that comes later, and no
//! code holds two resource shards or two transaction shards at once. That order
//! rules out a deadlock inside the manager itself.

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// The mode `txn` holds this resource in, if any.
    #[inline]
    fn mode_of(&self, txn: TxnId) -> Option<LockMode> {
        self.holders.iter().find(|h| h.txn == txn).map(|h| h.mode)
    }
}

/// A transaction waiting in a resource's queue, and the mode it asked for.
#[derive(Clone, Copy)]
struct Waiter {
    txn: TxnId,
    mode: LockMode,
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

/// The mutable state of one resource shard.
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
    /// Wait queues for [`LockManager::request`], per resource, in grant order:
    /// waiting upgraders (transactions that already hold the resource) first,
    /// then everyone else in arrival order.
    queues: HashMap<ResourceId, Vec<Waiter>>,
}

impl ShardInner {
    fn new() -> Self {
        Self {
            locks: HashMap::new(),
            by_txn: HashMap::new(),
            ranges: HashMap::new(),
            range_by_txn: HashMap::new(),
            queues: HashMap::new(),
        }
    }

    /// Returns `true` if `txn` holds any point or range lock in this shard.
    #[inline]
    fn holds_any(&self, txn: TxnId) -> bool {
        self.by_txn.contains_key(&txn) || self.range_by_txn.contains_key(&txn)
    }

    /// Removes `txn` from the wait queue on `res`, if it is queued there.
    fn dequeue(&mut self, txn: TxnId, res: ResourceId) {
        if let Some(queue) = self.queues.get_mut(&res) {
            if let Some(pos) = queue.iter().position(|w| w.txn == txn) {
                let _ = queue.remove(pos);
            }
            if queue.is_empty() {
                let _ = self.queues.remove(&res);
            }
        }
    }
}

/// One independently locked partition of the table.
struct Shard {
    inner: Mutex<ShardInner>,
}

/// How many shard indices a [`ShardSet`] holds inline before it spills to a
/// bitset. Most transactions touch only a few shards.
const INLINE_SHARDS: usize = 4;

/// The set of resource shards a transaction holds locks in.
///
/// Up to [`INLINE_SHARDS`] indices are kept inline, so the common short
/// transaction never allocates for this bookkeeping; past that the set becomes
/// a bitset over the shard count.
#[derive(Default)]
struct ShardSet {
    /// Inline indices, valid while `bits` is empty.
    small: [usize; INLINE_SHARDS],
    /// Number of valid entries in `small`.
    len: usize,
    /// Bitset of shard indices, used once more than `INLINE_SHARDS` distinct
    /// shards have been recorded.
    bits: Vec<u64>,
}

impl ShardSet {
    #[inline]
    fn insert(&mut self, idx: usize) {
        if self.bits.is_empty() {
            if self.small[..self.len].contains(&idx) {
                return;
            }
            if self.len < INLINE_SHARDS {
                self.small[self.len] = idx;
                self.len += 1;
                return;
            }
            let spilled = self.small;
            self.len = 0;
            for i in spilled {
                self.set_bit(i);
            }
        }
        self.set_bit(idx);
    }

    #[inline]
    fn set_bit(&mut self, idx: usize) {
        let word = idx / 64;
        if self.bits.len() <= word {
            self.bits.resize(word + 1, 0);
        }
        self.bits[word] |= 1 << (idx % 64);
    }

    #[inline]
    fn remove(&mut self, idx: usize) {
        if self.bits.is_empty() {
            if let Some(p) = self.small[..self.len].iter().position(|&i| i == idx) {
                self.small[p] = self.small[self.len - 1];
                self.len -= 1;
            }
        } else if let Some(w) = self.bits.get_mut(idx / 64) {
            *w &= !(1 << (idx % 64));
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0 && self.bits.iter().all(|w| *w == 0)
    }

    /// Calls `f` with every index in the set.
    fn for_each(&self, mut f: impl FnMut(usize)) {
        for &i in &self.small[..self.len] {
            f(i);
        }
        for (w, word) in self.bits.iter().enumerate() {
            let mut bits = *word;
            while bits != 0 {
                f(w * 64 + bits.trailing_zeros() as usize);
                bits &= bits - 1;
            }
        }
    }
}

/// Per-transaction bookkeeping, kept in a table sharded by transaction id.
#[derive(Default)]
struct TxnEntry {
    /// The resource and mode this transaction is waiting for through
    /// [`LockManager::request`], if any.
    wait: Option<(ResourceId, LockMode)>,
    /// The resource shards this transaction holds locks in.
    shards: ShardSet,
}

impl TxnEntry {
    #[inline]
    fn set_shard(&mut self, idx: usize) {
        self.shards.insert(idx);
    }

    #[inline]
    fn clear_shard(&mut self, idx: usize) {
        self.shards.remove(idx);
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.wait.is_none() && self.shards.is_empty()
    }
}

/// One partition of the per-transaction table.
type TxnMap = HashMap<TxnId, TxnEntry, BuildTxnHasher>;

/// A fast hasher for the per-transaction table.
///
/// Transaction ids are assigned by the transaction layer, not taken from
/// untrusted input, so the table does not need SipHash's flooding resistance.
/// The id is multiplied by an odd constant and its halves folded, so that the
/// low bits (bucket index) and the high bits (hashbrown's tag) both vary even
/// within one shard, whose ids share the top bits of the Fibonacci hash.
#[derive(Default, Clone, Copy)]
struct BuildTxnHasher;

impl core::hash::BuildHasher for BuildTxnHasher {
    type Hasher = TxnHasher;
    #[inline]
    fn build_hasher(&self) -> TxnHasher {
        TxnHasher(0)
    }
}

/// The hasher built by [`BuildTxnHasher`]; only `write_u64` is ever used,
/// since `TxnId` hashes as one `u64`.
struct TxnHasher(u64);

impl core::hash::Hasher for TxnHasher {
    #[inline]
    fn finish(&self) -> u64 {
        let h = self.0.wrapping_mul(0xD6E8_FEB8_6659_FD93);
        h ^ (h >> 32)
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = self.0.rotate_left(5) ^ n;
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(5) ^ u64::from(b);
        }
    }
}

/// A transaction chosen as a deadlock victim and not yet released.
struct VictimMark {
    /// The cycle it was chosen from, reported again on each refused call.
    cycle: Vec<TxnId>,
    /// Whether the victim itself has been told (a refused call, a
    /// [`Acquisition::Deadlock`] naming the requester, or
    /// [`LockManager::is_aborted`]).
    told: bool,
    /// Whether it has been released once since it was marked.
    released: bool,
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
    /// Per-transaction bookkeeping (pending wait, shards used), sharded by
    /// transaction id with the same count as `shards`.
    txns: Box<[Mutex<TxnMap>]>,
    /// `log2(shards.len())`; `0` when there is a single shard.
    bits: u32,
    /// Deadlock victims that have not been released since being told.
    victims: Mutex<HashMap<TxnId, VictimMark>>,
    /// `victims.len()`, readable without the mutex so that the acquire paths
    /// skip the registry entirely while no victim is outstanding.
    victim_count: AtomicUsize,
}

/// The outcome of a deadlock-aware [`request`](LockManager::request).
///
/// Unlike [`try_acquire`](LockManager::try_acquire), `request` does not just
/// fail on conflict: it records the wait and tells the caller whether to
/// proceed, suspend, or abort.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "the outcome decides whether the transaction proceeds, waits, or aborts"]
pub enum Acquisition {
    /// The lock was granted; the transaction holds it and may proceed.
    Granted,
    /// The lock is held incompatibly, or an earlier waiter in the resource's
    /// queue wants a conflicting mode. The transaction is now queued and
    /// registered as waiting; the caller should suspend it and retry `request`
    /// later (for example after a release), or give up with
    /// [`cancel_wait`](LockManager::cancel_wait). No deadlock was found.
    Waiting,
    /// Granting the wait would close a cycle in the wait-for graph. The caller
    /// must abort the named victim (with [`release_all`](LockManager::release_all))
    /// to break the deadlock. The victim may be the requesting transaction
    /// itself or another transaction in the cycle.
    ///
    /// The same outcome, with `victim` equal to the requester, is returned to a
    /// transaction that was already chosen as a victim and has not yet been
    /// released: it must roll back and call `release_all` on itself.
    Deadlock(Deadlock),
}

/// What [`LockManager::request`] decided while holding the resource shard.
enum Decision {
    Granted,
    Waiting,
    Victim(Vec<TxnId>),
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
    /// remainder. More shards reduce contention but cost a mutex and a few small
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
        let mut t = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(Shard {
                inner: Mutex::new(ShardInner::new()),
            });
            t.push(Mutex::new(HashMap::default()));
        }
        Self {
            shards: v.into_boxed_slice(),
            txns: t.into_boxed_slice(),
            bits,
            victims: Mutex::new(HashMap::new()),
            victim_count: AtomicUsize::new(0),
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
    ///   every other holder (an in-place upgrade, for example shared to
    ///   exclusive when sole holder, or shared plus intention-exclusive to SIX);
    ///   or
    /// - `txn` holds nothing on `res` and `mode` is compatible with every
    ///   current holder.
    ///
    /// Otherwise nothing is changed and [`LockError::Conflict`] is returned. The
    /// caller decides whether to retry, wait, or abort; this method never blocks
    /// the calling thread.
    ///
    /// `try_acquire` checks the current holders only. It does not wait its turn
    /// behind transactions queued through [`request`](Self::request); use
    /// `request` everywhere when first-in, first-out fairness matters.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Conflict`] if the lock cannot be granted right now,
    /// and [`LockError::Aborted`] if `txn` was chosen as a deadlock victim and
    /// has not been released since.
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
        let idx = self.shard_index(res);
        let mut guard = Self::lock(&self.shards[idx]);
        if self.victim_cycle(txn).is_some() {
            return Err(LockError::Aborted);
        }
        let first = !guard.holds_any(txn);
        let ShardInner { locks, by_txn, .. } = &mut *guard;
        if Self::try_grant_locked(locks, by_txn, txn, res, mode) {
            if first {
                self.note_shard(txn, idx);
            }
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
        let idx = self.shard_index(res);
        let mut guard = Self::lock(&self.shards[idx]);
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
        if !guard.holds_any(txn) {
            self.forget_shard(txn, idx);
        }
        Ok(())
    }

    /// Releases every lock held by `txn` across the whole table (both point
    /// locks and range locks) and clears any wait it has registered.
    ///
    /// This is the call a transaction layer makes at commit or abort to drop a
    /// transaction's entire lock set at once. It returns the number of locks
    /// released. Its cost is proportional to the number of shards the
    /// transaction holds locks in, not to the size of the table or the shard
    /// count. Releasing a deadlock victim from another thread visits every
    /// shard, so that a lock the victim's own thread took concurrently cannot
    /// be missed.
    ///
    /// For a transaction chosen as a deadlock victim, `release_all` frees its
    /// locks but keeps it marked as a victim until the victim itself has been
    /// told (see [`is_aborted`](Self::is_aborted)), so that a release by the
    /// thread that found the deadlock does not let the victim carry on. The
    /// mark is cleared by the first `release_all` after the victim was told, or
    /// by the second `release_all` since it was marked (the victim's own
    /// release after another thread released it), and the id can then be used
    /// again.
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
        let entry = {
            let mut txns = self.lock_txns(txn);
            txns.remove(&txn).unwrap_or_default()
        };
        // Read the mark only after the wait is gone: a victim is marked only
        // while it has a wait (under its transaction shard), so a mark set
        // concurrently is either visible here or never set at all.
        let victim = self.victim_state(txn);
        if let Some((res, _)) = entry.wait {
            Self::lock(&self.shards[self.shard_index(res)]).dequeue(txn, res);
        }

        let mut released = 0;
        if victim.is_some() {
            for shard in self.shards.iter() {
                released += Self::sweep(&mut Self::lock(shard), txn);
            }
        } else {
            entry.shards.for_each(|idx| {
                if let Some(shard) = self.shards.get(idx) {
                    released += Self::sweep(&mut Self::lock(shard), txn);
                }
            });
        }

        if victim.is_some() {
            self.settle_victim(txn);
        }
        released
    }

    /// Drops every point and range lock `txn` holds in one shard, returning how
    /// many were dropped.
    fn sweep(inner: &mut ShardInner, txn: TxnId) -> usize {
        let ShardInner {
            locks,
            by_txn,
            ranges,
            range_by_txn,
            ..
        } = inner;
        let mut released = 0;

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
        released
    }

    /// Acquires `mode` on `res` for `txn`, queueing a wait and detecting
    /// deadlock if it cannot be granted.
    ///
    /// This is the deadlock-aware counterpart to
    /// [`try_acquire`](Self::try_acquire). The three outcomes are:
    ///
    /// - [`Acquisition::Granted`]: the lock was granted; proceed.
    /// - [`Acquisition::Waiting`]: `txn` is now queued on `res` and recorded
    ///   as waiting. The caller should suspend the transaction and call
    ///   `request` again later (for example after a release) to retry. No
    ///   deadlock was found.
    /// - [`Acquisition::Deadlock`]: granting the wait would close a cycle. The
    ///   caller must abort the [`Deadlock::victim`] with
    ///   [`release_all`](Self::release_all). The victim may be `txn` or another
    ///   transaction in the cycle. It is also returned, naming `txn` itself, when
    ///   `txn` was already chosen as a victim and has not been released.
    ///
    /// # Fairness
    ///
    /// Each resource has a first-in, first-out wait queue. A request is granted
    /// only when it is compatible with every current holder and with every
    /// transaction queued ahead of it, so a waiting writer is not overtaken by
    /// a stream of newer readers. A transaction that already holds the resource
    /// and asks for a stronger mode (an upgrade) queues ahead of transactions
    /// that hold nothing on it, which is the standard rule. A transaction can
    /// wait on one resource at a time; a `request` for a different resource
    /// moves its wait there.
    ///
    /// A queued transaction holds its place until it is granted, calls
    /// [`cancel_wait`](Self::cancel_wait), or is released with
    /// [`release_all`](Self::release_all). A waiter that gives up without
    /// either call keeps blocking the transactions queued behind it, and leaves
    /// a stale wait-for edge.
    ///
    /// # Detection
    ///
    /// Detection runs at the moment of the wait, from the new waiter only. It
    /// walks the wait-for edges reachable from `txn`, reading each waiter's
    /// edges from the current lock table (holders in a conflicting mode, and
    /// conflicting waiters queued ahead of it), so its cost depends on the part
    /// of the graph the new wait can reach, not on the number of waiters in the
    /// system. A wait left over from a lock that has since been released
    /// contributes no edge. Detection is exact provided every abandoned wait is
    /// cleared with `cancel_wait` or `release_all`: a stale wait still counts as
    /// waiting and can close a cycle that no live transaction is in.
    ///
    /// The victim is chosen by the [`VictimPolicy::Youngest`] policy; callers
    /// wanting a different policy can apply [`WaitForGraph::pick_victim`] to
    /// [`Deadlock::cycle`] themselves. The victim is marked: its next
    /// `request`, [`try_acquire`](Self::try_acquire), or
    /// [`try_acquire_range`](Self::try_acquire_range) is refused until it is
    /// released after being told (see [`is_aborted`](Self::is_aborted)). A
    /// victim is marked only while it still has a registered wait; if the walk
    /// read an edge that has since gone and the victim has stopped waiting,
    /// there is no cycle left and `Waiting` is returned instead.
    ///
    /// Only transactions that wait through `request` appear in the graph; a
    /// transaction that spins on `try_acquire` is invisible to deadlock
    /// detection. Range locks ([`try_acquire_range`](Self::try_acquire_range))
    /// are likewise not tracked here.
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
        let idx = self.shard_index(res);
        let (decision, previous) = {
            let mut guard = Self::lock(&self.shards[idx]);
            if let Some(cycle) = self.victim_cycle(txn) {
                (Decision::Victim(cycle), None)
            } else {
                let first = !guard.holds_any(txn);
                let ShardInner {
                    locks,
                    by_txn,
                    queues,
                    ..
                } = &mut *guard;
                let entry = locks.get(&res);
                let held = entry.and_then(|e| e.mode_of(txn));
                let covered = held.is_some_and(|h| h.covers(mode));
                let target = held.map_or(mode, |h| h.join(mode));
                let queue = queues.get(&res);
                let pos = queue.and_then(|q| q.iter().position(|w| w.txn == txn));

                let blocked_by_queue = !covered
                    && queue.is_some_and(|q| {
                        let ahead = match pos {
                            Some(p) => &q[..p],
                            None if held.is_some() => &q[..Self::upgraders(q, entry)],
                            None => &q[..],
                        };
                        ahead.iter().any(|w| {
                            w.txn != txn && !Self::queued_target(entry, w).compatible_with(target)
                        })
                    });

                if !blocked_by_queue && Self::try_grant_locked(locks, by_txn, txn, res, mode) {
                    if pos.is_some() {
                        guard.dequeue(txn, res);
                    }
                    let previous = self.set_wait(txn, None, first.then_some(idx));
                    (Decision::Granted, previous)
                } else {
                    let queue = queues.entry(res).or_default();
                    match pos {
                        Some(p) => queue[p].mode = mode,
                        None => {
                            let at = if held.is_some() {
                                Self::upgraders(queue, locks.get(&res))
                            } else {
                                queue.len()
                            };
                            queue.insert(at, Waiter { txn, mode });
                        }
                    }
                    let previous = self.set_wait(txn, Some((res, mode)), None);
                    (Decision::Waiting, previous)
                }
            }
        };

        // A transaction waits on one resource at a time: leave the old queue.
        if let Some((old, _)) = previous {
            if old != res {
                Self::lock(&self.shards[self.shard_index(old)]).dequeue(txn, old);
            }
        }

        match decision {
            Decision::Granted => Acquisition::Granted,
            Decision::Victim(cycle) => Acquisition::Deadlock(Deadlock { victim: txn, cycle }),
            Decision::Waiting => {
                let found = self.walk(core::iter::once(txn)).and_then(|cycle| {
                    let victim =
                        WaitForGraph::pick_victim(&cycle, DEADLOCK_VICTIM_POLICY).unwrap_or(txn);
                    self.mark_victim(victim, &cycle, victim == txn)
                        .then_some(Deadlock { victim, cycle })
                });
                match found {
                    Some(d) => Acquisition::Deadlock(d),
                    None => Acquisition::Waiting,
                }
            }
        }
    }

    /// The number of leading queue entries that already hold the resource
    /// (waiting upgraders), which is where a new upgrader is inserted.
    fn upgraders(queue: &[Waiter], entry: Option<&LockEntry>) -> usize {
        queue
            .iter()
            .take_while(|w| entry.is_some_and(|e| e.mode_of(w.txn).is_some()))
            .count()
    }

    /// The mode a queued waiter would end up holding if granted: the join with
    /// what it already holds for an upgrader, the requested mode otherwise.
    #[inline]
    fn queued_target(entry: Option<&LockEntry>, w: &Waiter) -> LockMode {
        entry
            .and_then(|e| e.mode_of(w.txn))
            .map_or(w.mode, |h| h.join(w.mode))
    }

    /// Records (or clears) `txn`'s pending wait and, when `first_shard` is
    /// given, that `txn` now holds a lock in that shard. Returns the previous
    /// wait. Called under a resource shard lock (resource before transaction).
    fn set_wait(
        &self,
        txn: TxnId,
        wait: Option<(ResourceId, LockMode)>,
        first_shard: Option<usize>,
    ) -> Option<(ResourceId, LockMode)> {
        let mut txns = self.lock_txns(txn);
        if wait.is_none() && first_shard.is_none() {
            // Clearing only: avoid creating an entry for a transaction that has
            // none.
            let entry = txns.get_mut(&txn)?;
            let previous = entry.wait.take();
            if entry.is_empty() {
                let _ = txns.remove(&txn);
            }
            return previous;
        }
        let entry = txns.entry(txn).or_default();
        let previous = core::mem::replace(&mut entry.wait, wait);
        if let Some(idx) = first_shard {
            entry.set_shard(idx);
        }
        if entry.is_empty() {
            let _ = txns.remove(&txn);
        }
        previous
    }

    /// Records that `txn` holds a lock in shard `idx`.
    fn note_shard(&self, txn: TxnId, idx: usize) {
        self.lock_txns(txn).entry(txn).or_default().set_shard(idx);
    }

    /// Records that `txn` no longer holds any lock in shard `idx`.
    fn forget_shard(&self, txn: TxnId, idx: usize) {
        let mut txns = self.lock_txns(txn);
        if let Some(entry) = txns.get_mut(&txn) {
            entry.clear_shard(idx);
            if entry.is_empty() {
                let _ = txns.remove(&txn);
            }
        }
    }

    /// The wait-for edges out of `txn`, read from the current lock table: the
    /// holders of the resource it waits for whose mode conflicts with what it
    /// would hold, and the conflicting waiters queued ahead of it.
    fn wait_edges(&self, txn: TxnId) -> Vec<TxnId> {
        let wait = self.lock_txns(txn).get(&txn).and_then(|e| e.wait);
        let Some((res, mode)) = wait else {
            return Vec::new();
        };
        let guard = self.lock_shard(res);
        let entry = guard.locks.get(&res);
        let target = entry
            .and_then(|e| e.mode_of(txn))
            .map_or(mode, |h| h.join(mode));
        let mut out = Vec::new();
        if let Some(e) = entry {
            out.extend(
                e.holders
                    .iter()
                    .filter(|h| h.txn != txn && !h.mode.compatible_with(target))
                    .map(|h| h.txn),
            );
        }
        if let Some(q) = guard.queues.get(&res) {
            if let Some(p) = q.iter().position(|w| w.txn == txn) {
                out.extend(
                    q[..p]
                        .iter()
                        .filter(|w| !Self::queued_target(entry, w).compatible_with(target))
                        .map(|w| w.txn),
                );
            }
        }
        out
    }

    /// Depth-first search for a cycle in the wait-for graph, from each start in
    /// turn, with one visited set shared by every start so that each node and
    /// edge is examined at most once. Edges are read from the lock table as
    /// nodes are reached; no lock is held across the walk.
    fn walk(&self, starts: impl IntoIterator<Item = TxnId>) -> Option<Vec<TxnId>> {
        // 1: on the current path, 2: fully explored.
        let mut colour: HashMap<TxnId, u8> = HashMap::new();
        let mut path: Vec<TxnId> = Vec::new();
        let mut stack: Vec<(Vec<TxnId>, usize)> = Vec::new();

        for start in starts {
            if colour.contains_key(&start) {
                continue;
            }
            let _ = colour.insert(start, 1);
            path.push(start);
            stack.push((self.wait_edges(start), 0));

            while let Some((edges, idx)) = stack.last_mut() {
                if let Some(&next) = edges.get(*idx) {
                    *idx += 1;
                    match colour.get(&next).copied() {
                        Some(1) => {
                            let pos = path.iter().position(|t| *t == next).unwrap_or(0);
                            return Some(path[pos..].to_vec());
                        }
                        Some(_) => {}
                        None => {
                            let _ = colour.insert(next, 1);
                            path.push(next);
                            let edges = self.wait_edges(next);
                            stack.push((edges, 0));
                        }
                    }
                } else {
                    if let Some(done) = path.pop() {
                        let _ = colour.insert(done, 2);
                    }
                    let _ = stack.pop();
                }
            }
        }
        None
    }

    /// Removes any pending wait for `txn` and takes it out of the resource's
    /// wait queue.
    ///
    /// Call this when a transaction that previously got [`Acquisition::Waiting`]
    /// stops waiting without acquiring the lock (for example it timed out or was
    /// aborted for another reason). [`release_all`](Self::release_all) already
    /// clears the wait, so this is only needed when releasing nothing. Skipping
    /// it leaves the transaction queued, blocking later requests on the
    /// resource, and leaves a stale wait-for edge.
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
        let previous = self.set_wait(txn, None, None);
        if let Some((res, _)) = previous {
            Self::lock(&self.shards[self.shard_index(res)]).dequeue(txn, res);
        }
    }

    /// Scans the current wait set for a deadlock, returning one if found.
    ///
    /// This is the periodic-detection counterpart to the at-wait detection in
    /// [`request`](Self::request): a background task can call it on an interval
    /// instead of (or in addition to) acting on `request`'s result. It reads
    /// the wait-for edges from the current lock table and visits each waiter
    /// and edge once, so its cost is linear in the size of the wait-for graph.
    /// No lock is held across the walk. Returns `None` when no cycle exists.
    ///
    /// The victim it names is marked, exactly as for `request`: abort it with
    /// [`release_all`](Self::release_all).
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
        let mut waiters = Vec::new();
        for shard in self.txns.iter() {
            let txns = Self::lock_map(shard);
            waiters.extend(
                txns.iter()
                    .filter(|(_, e)| e.wait.is_some())
                    .map(|(t, _)| *t),
            );
        }
        let cycle = self.walk(waiters)?;
        let victim = WaitForGraph::pick_victim(&cycle, DEADLOCK_VICTIM_POLICY)?;
        self.mark_victim(victim, &cycle, false)
            .then_some(Deadlock { victim, cycle })
    }

    /// Returns the number of transactions currently registered as waiting.
    ///
    /// Mostly useful for diagnostics and tests.
    #[must_use]
    pub fn waiting_count(&self) -> usize {
        self.txns
            .iter()
            .map(|shard| {
                Self::lock_map(shard)
                    .values()
                    .filter(|e| e.wait.is_some())
                    .count()
            })
            .sum()
    }

    /// Returns `true` if `txn` was chosen as a deadlock victim and has not been
    /// released since being told.
    ///
    /// When the victim is not the transaction whose `request` found the
    /// deadlock, its own thread is not told by that call. It learns from its
    /// next `request` (an [`Acquisition::Deadlock`] naming itself),
    /// `try_acquire` or `try_acquire_range` ([`LockError::Aborted`]), or from
    /// this method. A `true` answer counts as telling it: the victim then rolls
    /// back and calls [`release_all`](Self::release_all) on itself, which
    /// clears the mark. A victim that gives up and calls `release_all` without
    /// being told also clears it, if another thread had already released it.
    /// A caller that aborts some other transaction instead of the named victim
    /// leaves the victim marked until the victim next calls into the manager
    /// or is released twice.
    ///
    /// # Examples
    ///
    /// ```
    /// use lock_db::{Acquisition, LockError, LockManager, LockMode, ResourceId, TxnId};
    ///
    /// let lm = LockManager::new();
    /// let (a, b) = (ResourceId::new(1), ResourceId::new(2));
    /// let (t1, t2) = (TxnId::new(1), TxnId::new(2));
    /// lm.request(t2, a, LockMode::Exclusive);
    /// lm.request(t1, b, LockMode::Exclusive);
    /// lm.request(t2, b, LockMode::Exclusive); // T2 waits for T1
    ///
    /// // T1 closes the cycle; the victim is T2, not T1.
    /// let Acquisition::Deadlock(d) = lm.request(t1, a, LockMode::Exclusive) else { panic!() };
    /// assert_eq!(d.victim, t2);
    /// lm.release_all(t2); // T1's thread aborts T2
    ///
    /// // T2's own thread is refused until it rolls back and releases itself.
    /// assert_eq!(lm.try_acquire(t2, b, LockMode::Shared), Err(LockError::Aborted));
    /// assert!(lm.is_aborted(t2));
    /// lm.release_all(t2);
    /// assert!(!lm.is_aborted(t2));
    /// ```
    #[must_use]
    pub fn is_aborted(&self, txn: TxnId) -> bool {
        self.victim_cycle(txn).is_some()
    }

    /// If `txn` is a marked victim, marks it told and returns its cycle.
    fn victim_cycle(&self, txn: TxnId) -> Option<Vec<TxnId>> {
        if self.victim_count.load(Ordering::SeqCst) == 0 {
            return None;
        }
        let mut victims = self.lock_victims();
        let mark = victims.get_mut(&txn)?;
        mark.told = true;
        Some(mark.cycle.clone())
    }

    /// `Some(told)` if `txn` is a marked victim, without changing the mark.
    fn victim_state(&self, txn: TxnId) -> Option<bool> {
        if self.victim_count.load(Ordering::SeqCst) == 0 {
            return None;
        }
        self.lock_victims().get(&txn).map(|m| m.told)
    }

    /// Marks `victim` as chosen to abort from `cycle`, and returns `true`, if
    /// it still has a registered wait. A victim that is no longer waiting has
    /// left the cycle (the walk read an edge that has since gone), so there is
    /// nothing to break and nothing is marked. The check and the mark happen
    /// under the victim's transaction shard, which `release_all` also takes
    /// before reading the mark.
    fn mark_victim(&self, victim: TxnId, cycle: &[TxnId], told: bool) -> bool {
        let txns = self.lock_txns(victim);
        if txns.get(&victim).and_then(|e| e.wait).is_none() {
            return false;
        }
        let mut victims = self.lock_victims();
        match victims.get_mut(&victim) {
            Some(mark) => mark.told |= told,
            None => {
                let _ = victims.insert(
                    victim,
                    VictimMark {
                        cycle: cycle.to_vec(),
                        told,
                        released: false,
                    },
                );
                let _ = self.victim_count.fetch_add(1, Ordering::SeqCst);
            }
        }
        true
    }

    /// Called after `txn`, a marked victim, was released: clears the mark if
    /// the victim had been told or this is its second release, and otherwise
    /// records the first release.
    fn settle_victim(&self, txn: TxnId) {
        let mut victims = self.lock_victims();
        let clear = match victims.get_mut(&txn) {
            Some(mark) if mark.told || mark.released => true,
            Some(mark) => {
                mark.released = true;
                false
            }
            None => false,
        };
        if clear {
            let _ = victims.remove(&txn);
            let _ = self.victim_count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Locks the victim registry, recovering its guard if the mutex was
    /// poisoned.
    #[inline]
    fn lock_victims(&self) -> MutexGuard<'_, HashMap<TxnId, VictimMark>> {
        match self.victims.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Locks the transaction shard that owns `txn`.
    #[inline]
    fn lock_txns(&self, txn: TxnId) -> MutexGuard<'_, TxnMap> {
        Self::lock_map(&self.txns[self.index_of(txn.get())])
    }

    /// Locks a transaction shard, recovering its guard if it was poisoned.
    #[inline]
    fn lock_map(shard: &Mutex<TxnMap>) -> MutexGuard<'_, TxnMap> {
        match shard.lock() {
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
        guard.locks.get(&res).and_then(|e| e.mode_of(txn))
    }

    /// Tries to acquire `mode` over the key range `range` in key space `space`,
    /// for `txn`, without blocking.
    ///
    /// A range lock protects a contiguous span of keys: use it to stop another
    /// transaction from inserting into, or writing within, a range you have
    /// read (phantom and predicate protection). `space` identifies the key space
    /// the range lives in, typically an index; ranges in different spaces never
    /// conflict.
    ///
    /// The request is granted unless some **other** transaction already holds an
    /// [overlapping](KeyRange::overlaps) range in `space` in an
    /// [incompatible](LockMode::compatible_with) mode. Re-acquisition is
    /// idempotent: when `txn` already holds a range in `space` that contains
    /// `range` in a mode that [covers](LockMode::covers) `mode`, nothing
    /// changes, and a request for exactly a range it already holds upgrades that
    /// lock in place to the [join](LockMode::join) of the two modes. Other
    /// ranges of the same transaction, including overlapping ones, are kept as
    /// separate locks.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Conflict`] if an overlapping, incompatible range is
    /// held by another transaction, and [`LockError::Aborted`] if `txn` was
    /// chosen as a deadlock victim and has not been released since.
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
        let idx = self.shard_index(space);
        let mut guard = Self::lock(&self.shards[idx]);
        if self.victim_cycle(txn).is_some() {
            return Err(LockError::Aborted);
        }
        let first = !guard.holds_any(txn);
        let ShardInner {
            ranges,
            range_by_txn,
            ..
        } = &mut *guard;

        // Idempotence: only a range this transaction already holds in `space`
        // that contains the request can cover it or be upgraded in place. The
        // transaction's own list is short, so check it before touching the
        // space's holders.
        let contains = |r: &KeyRange| r.start() <= range.start() && range.end() <= r.end();
        let own_hit = range_by_txn
            .get(&txn)
            .is_some_and(|held| held.iter().any(|(s, r)| *s == space && contains(r)));

        let rs = ranges.entry(space).or_insert_with(RangeSpace::new);
        let mut same = None;
        if own_hit {
            for (i, h) in rs.holders.iter().enumerate() {
                if h.txn == txn && contains(&h.range) {
                    if h.mode.covers(mode) {
                        return Ok(());
                    }
                    if h.range == range {
                        same = Some(i);
                    }
                }
            }
        }
        let target = same.map_or(mode, |p| rs.holders[p].mode.join(mode));

        let conflict = rs
            .holders
            .iter()
            .any(|h| h.txn != txn && h.range.overlaps(range) && !h.mode.compatible_with(target));
        if conflict {
            // A conflict implies a pre-existing holder, so the space entry is
            // non-empty and there is nothing to clean up.
            return Err(LockError::Conflict);
        }

        if let Some(p) = same {
            rs.holders[p].mode = target;
            return Ok(());
        }
        rs.holders.push(RangeHolder { txn, range, mode });
        range_by_txn.entry(txn).or_default().push((space, range));
        if first {
            self.note_shard(txn, idx);
        }
        Ok(())
    }

    /// Releases a range lock `txn` holds over `range` in `space`.
    ///
    /// Matches on the transaction and the exact range. A transaction holds at
    /// most one lock on any exact range (re-acquiring it upgrades in place), so
    /// one call releases it.
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
        let idx = self.shard_index(space);
        let mut guard = Self::lock(&self.shards[idx]);
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
        if !guard.holds_any(txn) {
            self.forget_shard(txn, idx);
        }
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
        self.index_of(res.get())
    }

    /// Maps an id to a shard index via Fibonacci hashing.
    #[inline]
    fn index_of(&self, id: u64) -> usize {
        if self.bits == 0 {
            return 0;
        }
        let hash = id.wrapping_mul(FIB_HASH);
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

    // ---- victim fencing ----

    #[test]
    fn test_victim_is_fenced_until_released_after_being_told() {
        // T2 holds A and waits for B; T1 holds B and requests A. The victim is
        // T2 (youngest), not the requester.
        let lm = LockManager::with_shards(1);
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        assert_eq!(lm.request(t2, a, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.request(t1, b, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Waiting);
        let d = match lm.request(t1, a, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => d,
            other => panic!("expected deadlock, got {other:?}"),
        };
        assert_eq!(d.victim, t2);

        // T1's thread aborts T2, then proceeds and commits.
        assert_eq!(lm.release_all(t2), 1);
        assert_eq!(lm.request(t1, a, LockMode::Exclusive), Acquisition::Granted);
        assert_eq!(lm.release_all(t1), 2);

        // T2's own thread retries: it must be told it was aborted, not handed
        // a fresh lock while it believes it still holds A.
        match lm.request(t2, b, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => assert_eq!(d.victim, t2),
            other => panic!("victim was not fenced: {other:?}"),
        }
        assert_eq!(
            lm.try_acquire(t2, b, LockMode::Shared),
            Err(LockError::Aborted)
        );
        assert_eq!(
            lm.try_acquire_range(t2, b, kr(0, 1), LockMode::Shared),
            Err(LockError::Aborted)
        );
        assert!(lm.is_aborted(t2));
        assert_eq!(lm.mode_held(t2, b), None);

        // Once the victim rolls back and releases itself, the id is usable.
        assert_eq!(lm.release_all(t2), 0);
        assert!(!lm.is_aborted(t2));
        assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Granted);
    }

    #[test]
    fn test_release_by_other_thread_keeps_mark_until_victim_is_told() {
        let lm = LockManager::with_shards(4);
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        let _ = lm.request(t2, a, LockMode::Exclusive);
        let _ = lm.request(t1, b, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let Acquisition::Deadlock(d) = lm.request(t1, a, LockMode::Exclusive) else {
            panic!("expected deadlock");
        };
        // Released by the requester's side: the mark survives, because the
        // victim itself has not been told yet.
        let _ = lm.release_all(d.victim);
        assert_eq!(
            lm.try_acquire(t2, ResourceId::new(9), LockMode::Shared),
            Err(LockError::Aborted)
        );
        let _ = lm.release_all(t2);
        lm.try_acquire(t2, ResourceId::new(9), LockMode::Shared)
            .unwrap();
    }

    #[test]
    fn test_victim_that_gives_up_untold_does_not_leak_its_mark() {
        // The requester releases the victim; the victim's thread then times
        // out and releases itself without another call into the manager. The
        // second release clears the mark.
        let lm = LockManager::with_shards(4);
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        let _ = lm.request(t2, a, LockMode::Exclusive);
        let _ = lm.request(t1, b, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let Acquisition::Deadlock(d) = lm.request(t1, a, LockMode::Exclusive) else {
            panic!("expected deadlock");
        };
        assert_eq!(d.victim, t2);
        let _ = lm.release_all(t2); // requester's thread
        let _ = lm.release_all(t2); // victim's own thread, untold
        assert_eq!(lm.lock_victims().len(), 0);
        assert_eq!(lm.victim_count.load(super::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_requester_victim_is_cleared_by_its_own_release() {
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        let _ = lm.request(t1, a, LockMode::Exclusive);
        let _ = lm.request(t2, b, LockMode::Exclusive);
        let _ = lm.request(t1, b, LockMode::Exclusive);
        let Acquisition::Deadlock(d) = lm.request(t2, a, LockMode::Exclusive) else {
            panic!("expected deadlock");
        };
        assert_eq!(d.victim, t2); // the requester itself: told directly
        let _ = lm.release_all(t2);
        assert!(!lm.is_aborted(t2));
        assert_eq!(lm.request(t2, b, LockMode::Exclusive), Acquisition::Waiting);
    }

    #[test]
    fn test_find_deadlock_marks_its_victim() {
        let lm = LockManager::new();
        let (a, b) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        lm.try_acquire(t1, a, LockMode::Exclusive).unwrap();
        lm.try_acquire(t2, b, LockMode::Exclusive).unwrap();
        let _ = lm.request(t1, b, LockMode::Exclusive);
        // The second wait is detected by `request` itself and marks T2; clear
        // that so the periodic detector is the one that names the victim.
        let Acquisition::Deadlock(d) = lm.request(t2, a, LockMode::Exclusive) else {
            panic!("expected deadlock");
        };
        assert!(lm.is_aborted(d.victim));
        let found = lm.find_deadlock().unwrap();
        assert_eq!(found.victim, d.victim);
        let _ = lm.release_all(found.victim);
        assert!(lm.find_deadlock().is_none());
    }

    // ---- FIFO fairness ----

    #[test]
    fn test_waiting_writer_is_not_overtaken_by_new_readers() {
        let lm = LockManager::with_shards(1);
        let row = ResourceId::new(1);
        let writer = TxnId::new(2);
        assert_eq!(
            lm.request(TxnId::new(1), row, LockMode::Shared),
            Acquisition::Granted
        );
        assert_eq!(
            lm.request(writer, row, LockMode::Exclusive),
            Acquisition::Waiting
        );
        // A newer reader is compatible with the holder but not with the
        // queued writer, so it queues behind it.
        assert_eq!(
            lm.request(TxnId::new(3), row, LockMode::Shared),
            Acquisition::Waiting
        );
        let _ = lm.release_all(TxnId::new(1));
        // The writer is first in line and gets the lock; the reader still waits.
        assert_eq!(
            lm.request(TxnId::new(3), row, LockMode::Shared),
            Acquisition::Waiting
        );
        assert_eq!(
            lm.request(writer, row, LockMode::Exclusive),
            Acquisition::Granted
        );
        let _ = lm.release_all(writer);
        assert_eq!(
            lm.request(TxnId::new(3), row, LockMode::Shared),
            Acquisition::Granted
        );
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_compatible_newcomer_passes_compatible_waiter() {
        // Queue holds an IX waiter blocked by an S holder; a new IS is
        // compatible with both, so it is granted (no needless blocking).
        let lm = LockManager::with_shards(1);
        let table = ResourceId::new(1);
        let _ = lm.request(TxnId::new(1), table, LockMode::Shared);
        assert_eq!(
            lm.request(TxnId::new(2), table, LockMode::IntentionExclusive),
            Acquisition::Waiting
        );
        assert_eq!(
            lm.request(TxnId::new(3), table, LockMode::IntentionShared),
            Acquisition::Granted
        );
    }

    #[test]
    fn test_upgrader_goes_ahead_of_queued_newcomers() {
        let lm = LockManager::with_shards(1);
        let row = ResourceId::new(1);
        let (t1, t2, t3) = (TxnId::new(1), TxnId::new(2), TxnId::new(3));
        let _ = lm.request(t1, row, LockMode::Shared);
        let _ = lm.request(t2, row, LockMode::Shared);
        // T3 queues for X behind both readers.
        assert_eq!(
            lm.request(t3, row, LockMode::Exclusive),
            Acquisition::Waiting
        );
        // T1 upgrades: blocked by T2's S, queued ahead of T3.
        assert_eq!(
            lm.request(t1, row, LockMode::Exclusive),
            Acquisition::Waiting
        );
        let _ = lm.release_all(t2);
        // T3 arrived first but T1 is an upgrader at the head.
        assert_eq!(
            lm.request(t3, row, LockMode::Exclusive),
            Acquisition::Waiting
        );
        assert_eq!(
            lm.request(t1, row, LockMode::Exclusive),
            Acquisition::Granted
        );
        assert_eq!(lm.mode_held(t1, row), Some(LockMode::Exclusive));
    }

    #[test]
    fn test_queue_edge_closes_a_deadlock() {
        // T1 holds S on R; T2 queues for X on R (waits for T1); T3 holds A and
        // asks S on R, which queues behind T2 (waits for T2); T1 then waits for
        // A (held by T3). The cycle T1 -> T3 -> T2 -> T1 runs through a queue
        // edge, not only holder edges.
        let lm = LockManager::with_shards(4);
        let (r, a) = (ResourceId::new(1), ResourceId::new(2));
        let (t1, t2, t3) = (TxnId::new(1), TxnId::new(2), TxnId::new(3));
        let _ = lm.request(t1, r, LockMode::Shared);
        let _ = lm.request(t3, a, LockMode::Exclusive);
        assert_eq!(lm.request(t2, r, LockMode::Exclusive), Acquisition::Waiting);
        assert_eq!(lm.request(t3, r, LockMode::Shared), Acquisition::Waiting);
        match lm.request(t1, a, LockMode::Exclusive) {
            Acquisition::Deadlock(d) => assert_eq!(d.cycle.len(), 3),
            other => panic!("expected deadlock, got {other:?}"),
        }
    }

    #[test]
    fn test_two_upgraders_to_six_deadlock() {
        // T1 and T2 both hold S on R. T1 asks IX (S join IX = SIX, blocked by
        // T2's S). T2 asks IX too: each waits for the other.
        let lm = LockManager::new();
        let r = ResourceId::new(1);
        let (t1, t2) = (TxnId::new(1), TxnId::new(2));
        let _ = lm.request(t1, r, LockMode::Shared);
        let _ = lm.request(t2, r, LockMode::Shared);
        assert_eq!(
            lm.request(t1, r, LockMode::IntentionExclusive),
            Acquisition::Waiting
        );
        assert!(matches!(
            lm.request(t2, r, LockMode::IntentionExclusive),
            Acquisition::Deadlock(_)
        ));
    }

    #[test]
    fn test_request_on_new_resource_leaves_old_queue() {
        let lm = LockManager::with_shards(4);
        let (r1, r2) = (ResourceId::new(1), ResourceId::new(2));
        let _ = lm.request(TxnId::new(1), r1, LockMode::Exclusive);
        assert_eq!(
            lm.request(TxnId::new(2), r1, LockMode::Exclusive),
            Acquisition::Waiting
        );
        // T2 moves on to r2 and gets it; it no longer blocks r1's queue.
        assert_eq!(
            lm.request(TxnId::new(2), r2, LockMode::Exclusive),
            Acquisition::Granted
        );
        let _ = lm.release_all(TxnId::new(1));
        assert_eq!(
            lm.request(TxnId::new(3), r1, LockMode::Exclusive),
            Acquisition::Granted
        );
        assert_eq!(lm.waiting_count(), 0);
    }

    #[test]
    fn test_cancel_wait_unblocks_queue() {
        let lm = LockManager::with_shards(1);
        let r = ResourceId::new(1);
        let _ = lm.request(TxnId::new(1), r, LockMode::Shared);
        let _ = lm.request(TxnId::new(2), r, LockMode::Exclusive);
        assert_eq!(
            lm.request(TxnId::new(3), r, LockMode::Shared),
            Acquisition::Waiting
        );
        lm.cancel_wait(TxnId::new(2));
        assert_eq!(
            lm.request(TxnId::new(3), r, LockMode::Shared),
            Acquisition::Granted
        );
    }

    // ---- detection cost ----

    #[test]
    fn test_long_acyclic_chain_detection_is_linear() {
        // An acyclic wait chain of 20,000 transactions. The 1.0.0 detector
        // restarted its search from every waiter (quadratic: minutes here);
        // a shared visited set makes it linear.
        let lm = LockManager::new();
        let n = 20_000u64;
        for i in 0..=n {
            assert_eq!(
                lm.request(TxnId::new(i + 1), ResourceId::new(i), LockMode::Exclusive),
                Acquisition::Granted
            );
        }
        // T(i+1) holds R(i) and waits for R(i+1), held by T(i+2). Linked from
        // the head, so each new wait walks one edge to a holder not yet waiting.
        for i in 0..n {
            assert_eq!(
                lm.request(
                    TxnId::new(i + 1),
                    ResourceId::new(i + 1),
                    LockMode::Exclusive
                ),
                Acquisition::Waiting
            );
        }
        let start = std::time::Instant::now();
        assert!(lm.find_deadlock().is_none());
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "find_deadlock took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn test_wait_set_is_not_rebuilt_per_request() {
        // 5,000 unrelated waiters. Each new conflicting request must cost the
        // same as with none: 1.0.0 rebuilt the whole graph on every call.
        let lm = LockManager::new();
        for i in 0..5_000u64 {
            let res = ResourceId::new(1_000_000 + i);
            let _ = lm.request(TxnId::new(10_000_000 + i), res, LockMode::Exclusive);
            let _ = lm.request(TxnId::new(i + 1), res, LockMode::Exclusive);
        }
        let hot = ResourceId::new(42);
        let _ = lm.request(TxnId::new(99_999_999), hot, LockMode::Exclusive);
        let start = std::time::Instant::now();
        for j in 0..2_000u64 {
            let who = TxnId::new(50_000_000 + j);
            assert_eq!(lm.request(who, hot, LockMode::Shared), Acquisition::Waiting);
            lm.cancel_wait(who);
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "2,000 requests took {:?}",
            start.elapsed()
        );
    }

    // ---- bookkeeping ----

    #[test]
    fn test_release_all_visits_only_touched_shards() {
        let lm = LockManager::with_shards(1024);
        let t = TxnId::new(7);
        let r = ResourceId::new(5);
        lm.try_acquire(t, r, LockMode::Exclusive).unwrap();
        let idx = lm.shard_index(r);
        {
            let txns = lm.lock_txns(t);
            let entry = txns.get(&t).unwrap();
            let mut seen = Vec::new();
            entry.shards.for_each(|i| seen.push(i));
            assert_eq!(seen, vec![idx]);
        }
        assert_eq!(lm.release_all(t), 1);
        assert!(lm.lock_txns(t).get(&t).is_none());
    }

    #[test]
    fn test_shard_set_spills_and_tracks_every_shard() {
        let mut set = super::ShardSet::default();
        for i in [3, 900, 3, 17, 64, 5, 1023, 17] {
            set.insert(i);
        }
        let mut seen = Vec::new();
        set.for_each(|i| seen.push(i));
        seen.sort_unstable();
        assert_eq!(seen, vec![3, 5, 17, 64, 900, 1023]);
        for i in [3, 5, 17, 64, 900, 1023] {
            set.remove(i);
        }
        assert!(set.is_empty());

        // Through the manager: locks in many shards are all released.
        let lm = LockManager::with_shards(1024);
        let t = TxnId::new(1);
        for id in 0..200 {
            lm.try_acquire(t, ResourceId::new(id), LockMode::Exclusive)
                .unwrap();
        }
        assert_eq!(lm.release_all(t), 200);
        for id in 0..200 {
            assert_eq!(lm.holder_count(ResourceId::new(id)), 0);
        }
    }

    #[test]
    fn test_single_releases_leave_no_bookkeeping() {
        let lm = LockManager::with_shards(8);
        for i in 0..1_000u64 {
            let t = TxnId::new(i);
            lm.try_acquire(t, ResourceId::new(i), LockMode::Exclusive)
                .unwrap();
            lm.try_acquire_range(t, ResourceId::new(i + 7), kr(0, 1), LockMode::Shared)
                .unwrap();
            lm.release(t, ResourceId::new(i)).unwrap();
            lm.release_range(t, ResourceId::new(i + 7), kr(0, 1))
                .unwrap();
        }
        let left: usize = lm.txns.iter().map(|s| LockManager::lock_map(s).len()).sum();
        assert_eq!(left, 0);
    }

    #[test]
    fn test_range_reacquire_is_idempotent() {
        let lm = LockManager::with_shards(1);
        let space = ResourceId::new(7);
        let t = TxnId::new(1);
        for _ in 0..10_000 {
            lm.try_acquire_range(t, space, kr(10, 20), LockMode::Shared)
                .unwrap();
        }
        assert_eq!(lm.range_count(space), 1);
        // A sub-range in a covered mode adds nothing either.
        lm.try_acquire_range(t, space, kr(12, 15), LockMode::IntentionShared)
            .unwrap();
        assert_eq!(lm.range_count(space), 1);
        // The same range in a stronger mode upgrades in place.
        lm.try_acquire_range(t, space, kr(10, 20), LockMode::Exclusive)
            .unwrap();
        assert_eq!(lm.range_count(space), 1);
        assert_eq!(
            lm.try_acquire_range(TxnId::new(2), space, kr(15, 15), LockMode::Shared),
            Err(LockError::Conflict)
        );
        assert_eq!(lm.release_all(t), 1);
    }

    #[test]
    fn test_range_upgrade_blocked_by_other_reader() {
        let lm = LockManager::with_shards(1);
        let space = ResourceId::new(7);
        lm.try_acquire_range(TxnId::new(1), space, kr(0, 9), LockMode::Shared)
            .unwrap();
        lm.try_acquire_range(TxnId::new(2), space, kr(5, 5), LockMode::Shared)
            .unwrap();
        assert_eq!(
            lm.try_acquire_range(TxnId::new(1), space, kr(0, 9), LockMode::Exclusive),
            Err(LockError::Conflict)
        );
        assert_eq!(lm.range_count(space), 2);
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
