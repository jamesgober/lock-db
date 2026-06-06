//! # lock-db
//!
//! Lock manager and deadlock detection for Rust databases — row/range locks,
//! multiple granularities, and wait-for cycle detection.
//!
//! A lock manager is the component that lets many transactions touch shared
//! data at once without corrupting it. Each transaction asks for a lock on a
//! resource in a [`LockMode`]; the manager grants it only when the mode is
//! compatible with what every other transaction already holds. That single
//! rule — the compatibility matrix — is what keeps concurrent reads and writes
//! correct.
//!
//! ## What is in this release
//!
//! This is v1.0.0 — the stable release. The public API is frozen until 2.0. The
//! crate provides multi-granularity locking, range locking, and wait-for
//! deadlock detection:
//!
//! - [`LockMode`] — the five standard MGL modes (IS, IX, S, SIX, X) and their
//!   compatibility matrix, plus the lattice [join](LockMode::join) that drives
//!   upgrades.
//! - [`LockManager`] — a sharded lock table with acquire, release, bulk release,
//!   lattice upgrades, range locks, and the deadlock-aware
//!   [`request`](LockManager::request).
//! - [`WaitForGraph`] — a wait-for graph with cycle detection and
//!   [victim selection](VictimPolicy); the manager builds one to detect
//!   deadlocks, and it is reusable on its own.
//! - [`KeyRange`] — an inclusive key interval, the unit a range lock protects
//!   (phantom / predicate protection).
//! - [`TxnId`] and [`ResourceId`] — opaque identifiers the caller assigns.
//! - [`LockError`] — the small, exhaustive set of ways an operation can fail.
//!
//! [`try_acquire`](LockManager::try_acquire) is the non-blocking fast path: a
//! request that cannot be granted returns [`LockError::Conflict`] and is not
//! tracked. [`request`](LockManager::request) is the deadlock-aware path: a
//! request that cannot be granted is recorded in the wait-for graph and reports
//! [`Acquisition::Waiting`] or, if it closes a cycle,
//! [`Acquisition::Deadlock`]. lock-db detects deadlocks and names a victim; the
//! transaction layer above suspends, retries, and aborts.
//!
//! ## Hierarchical locking
//!
//! The intention modes exist to lock a hierarchy — database, table, page, row —
//! correctly and cheaply. The protocol is: before locking a resource in `S` or
//! `X`, hold an intention lock on each coarser resource above it (`IS` above an
//! `S`, `IX` above an `X`), acquiring coarse-to-fine and releasing fine-to-
//! coarse. lock-db enforces the compatibility matrix at each level; the caller
//! follows the protocol and maps each hierarchy node to a [`ResourceId`].
//!
//! ## Example
//!
//! ```
//! use lock_db::prelude::*;
//!
//! let lm = LockManager::new();
//! let row = ResourceId::new(1);
//! let (writer, reader) = (TxnId::new(1), TxnId::new(2));
//!
//! // The writer takes the row exclusively.
//! lm.try_acquire(writer, row, LockMode::Exclusive).unwrap();
//!
//! // A concurrent reader is refused while the write lock is held.
//! assert_eq!(lm.try_acquire(reader, row, LockMode::Shared), Err(LockError::Conflict));
//!
//! // Once the writer commits and releases, the reader gets in.
//! lm.release(writer, row).unwrap();
//! lm.try_acquire(reader, row, LockMode::Shared).unwrap();
//! ```
//!
//! Range locking, to keep another transaction from inserting into a span you
//! have read:
//!
//! ```
//! use lock_db::prelude::*;
//!
//! let lm = LockManager::new();
//! let index = ResourceId::new(10); // the key space being protected
//!
//! // Txn 1 read-locks the key range [100, 200].
//! lm.try_acquire_range(TxnId::new(1), index, KeyRange::new(100, 200).unwrap(), LockMode::Shared).unwrap();
//!
//! // Txn 2 cannot write key 150 inside that range.
//! let conflict = lm.try_acquire_range(TxnId::new(2), index, KeyRange::point(150), LockMode::Exclusive);
//! assert_eq!(conflict, Err(LockError::Conflict));
//!
//! // But a disjoint range is free.
//! lm.try_acquire_range(TxnId::new(2), index, KeyRange::new(201, 300).unwrap(), LockMode::Exclusive).unwrap();
//! ```
//!
//! Deadlock-aware acquisition, which records waits and reports cycles:
//!
//! ```
//! use lock_db::prelude::*;
//!
//! let lm = LockManager::new();
//! let (a, b) = (ResourceId::new(1), ResourceId::new(2));
//! let (t1, t2) = (TxnId::new(1), TxnId::new(2));
//!
//! lm.request(t1, a, LockMode::Exclusive); // T1 holds A
//! lm.request(t2, b, LockMode::Exclusive); // T2 holds B
//! lm.request(t1, b, LockMode::Exclusive); // T1 waits for T2
//!
//! // T2 waiting for A closes the cycle; abort the named victim to break it.
//! if let Acquisition::Deadlock(d) = lm.request(t2, a, LockMode::Exclusive) {
//!     lm.release_all(d.victim);
//! }
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![deny(unused_must_use)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![forbid(unsafe_code)]

mod error;
mod id;
mod mode;
mod range;

#[cfg(feature = "std")]
mod deadlock;
#[cfg(feature = "std")]
mod manager;

pub use crate::error::LockError;
pub use crate::id::{ResourceId, TxnId};
pub use crate::mode::LockMode;
pub use crate::range::KeyRange;

#[cfg(feature = "std")]
pub use crate::deadlock::{Deadlock, VictimPolicy, WaitForGraph};
#[cfg(feature = "std")]
pub use crate::manager::{Acquisition, LockManager};

/// The crate's common imports.
///
/// Glob-import this to bring the lock manager, the mode enum, the identifiers,
/// and the error type into scope in one line:
///
/// ```
/// use lock_db::prelude::*;
///
/// let lm = LockManager::new();
/// lm.try_acquire(TxnId::new(1), ResourceId::new(1), LockMode::Shared).unwrap();
/// ```
pub mod prelude {
    pub use crate::error::LockError;
    pub use crate::id::{ResourceId, TxnId};
    pub use crate::mode::LockMode;
    pub use crate::range::KeyRange;

    #[cfg(feature = "std")]
    pub use crate::deadlock::{Deadlock, VictimPolicy, WaitForGraph};
    #[cfg(feature = "std")]
    pub use crate::manager::{Acquisition, LockManager};
}
