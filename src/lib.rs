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
//! This is the v0.2.0 milestone. It provides the lock-table core:
//!
//! - [`LockMode`] — shared and exclusive modes and their compatibility matrix.
//! - [`LockManager`] — a sharded, non-blocking lock table with acquire,
//!   release, bulk release, and shared-to-exclusive upgrade.
//! - [`TxnId`] and [`ResourceId`] — opaque identifiers the caller assigns.
//! - [`LockError`] — the small, exhaustive set of ways an operation can fail.
//!
//! Acquisition is non-blocking: a request that cannot be granted returns
//! [`LockError::Conflict`] instead of waiting. Blocking acquisition with wait
//! queues, hierarchical and range locks, and wait-for deadlock detection land
//! across later 0.x releases (see `dev/ROADMAP.md`).
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

#[cfg(feature = "std")]
mod manager;

pub use crate::error::LockError;
pub use crate::id::{ResourceId, TxnId};
pub use crate::mode::LockMode;

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

    #[cfg(feature = "std")]
    pub use crate::manager::LockManager;
}
