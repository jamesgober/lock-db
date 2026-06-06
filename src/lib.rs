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
//! This is the v0.3.0 milestone. It provides multi-granularity and range
//! locking on top of the lock-table core:
//!
//! - [`LockMode`] — the five standard MGL modes (IS, IX, S, SIX, X) and their
//!   compatibility matrix, plus the lattice [join](LockMode::join) that drives
//!   upgrades.
//! - [`LockManager`] — a sharded, non-blocking lock table with acquire,
//!   release, bulk release, lattice upgrades, and range locks.
//! - [`KeyRange`] — an inclusive key interval, the unit a range lock protects
//!   (phantom / predicate protection).
//! - [`TxnId`] and [`ResourceId`] — opaque identifiers the caller assigns.
//! - [`LockError`] — the small, exhaustive set of ways an operation can fail.
//!
//! Acquisition is non-blocking: a request that cannot be granted returns
//! [`LockError::Conflict`] instead of waiting. Blocking acquisition with wait
//! queues and wait-for deadlock detection land in later 0.x releases (see
//! `dev/ROADMAP.md`).
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
mod manager;

pub use crate::error::LockError;
pub use crate::id::{ResourceId, TxnId};
pub use crate::mode::LockMode;
pub use crate::range::KeyRange;

#[cfg(feature = "std")]
pub use crate::manager::LockManager;

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
    pub use crate::manager::LockManager;
}
