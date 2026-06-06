//! Criterion benchmarks for the lock-table hot paths.
//!
//! These measure the operations a transaction layer calls most: taking and
//! dropping a lock, upgrading one, and releasing a whole transaction's set at
//! commit. The uncontended numbers are the per-call floor; the sharded design
//! exists to keep that floor flat as threads are added, which the contended
//! benchmark exercises.
//!
//! The lock manager lives behind the `std` feature, so the whole harness is
//! gated on it; without `std` the bench binary is just an empty `main`.

#![allow(clippy::unwrap_used)]

// Empty entry point so the bench target still links in a `no_std` build.
#[cfg(not(feature = "std"))]
fn main() {}

#[cfg(feature = "std")]
use std::sync::Arc;
#[cfg(feature = "std")]
use std::thread;

#[cfg(feature = "std")]
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "std")]
use lock_db::{LockManager, LockMode, ResourceId, TxnId};

/// Single-threaded acquire+release of a shared lock on a fresh resource.
#[cfg(feature = "std")]
fn bench_acquire_release_shared(c: &mut Criterion) {
    let lm = LockManager::new();
    let txn = TxnId::new(1);
    let mut next = 0u64;
    c.bench_function("acquire_release/shared", |b| {
        b.iter(|| {
            let res = ResourceId::new(next);
            next = next.wrapping_add(1);
            lm.try_acquire(txn, res, LockMode::Shared).unwrap();
            lm.release(txn, res).unwrap();
        });
    });
}

/// Single-threaded acquire+release of an exclusive lock on a fresh resource.
#[cfg(feature = "std")]
fn bench_acquire_release_exclusive(c: &mut Criterion) {
    let lm = LockManager::new();
    let txn = TxnId::new(1);
    let mut next = 0u64;
    c.bench_function("acquire_release/exclusive", |b| {
        b.iter(|| {
            let res = ResourceId::new(next);
            next = next.wrapping_add(1);
            lm.try_acquire(txn, res, LockMode::Exclusive).unwrap();
            lm.release(txn, res).unwrap();
        });
    });
}

/// Shared-to-exclusive upgrade of a sole-held lock, then release.
#[cfg(feature = "std")]
fn bench_upgrade(c: &mut Criterion) {
    let lm = LockManager::new();
    let txn = TxnId::new(1);
    let mut next = 0u64;
    c.bench_function("upgrade/shared_to_exclusive", |b| {
        b.iter(|| {
            let res = ResourceId::new(next);
            next = next.wrapping_add(1);
            lm.try_acquire(txn, res, LockMode::Shared).unwrap();
            lm.try_acquire(txn, res, LockMode::Exclusive).unwrap();
            lm.release(txn, res).unwrap();
        });
    });
}

/// `release_all` over a transaction holding a varying number of locks.
#[cfg(feature = "std")]
fn bench_release_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("release_all");
    for &count in &[16u64, 256, 4096] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    let lm = LockManager::new();
                    let txn = TxnId::new(1);
                    for id in 0..count {
                        lm.try_acquire(txn, ResourceId::new(id), LockMode::Exclusive)
                            .unwrap();
                    }
                    lm
                },
                |lm| lm.release_all(TxnId::new(1)),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Many threads acquire+release exclusive locks across a wide resource space.
/// With distinct resources per thread, shard partitioning should keep
/// throughput scaling rather than collapsing onto one mutex.
#[cfg(feature = "std")]
fn bench_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("contended/exclusive_disjoint");
    for &threads in &[1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let lm = Arc::new(LockManager::new());
                b.iter(|| {
                    let mut handles = Vec::with_capacity(threads);
                    for t in 0..threads {
                        let lm = Arc::clone(&lm);
                        handles.push(thread::spawn(move || {
                            let txn = TxnId::new(t as u64);
                            let base = (t as u64) << 32;
                            for i in 0..1000u64 {
                                let res = ResourceId::new(base | i);
                                lm.try_acquire(txn, res, LockMode::Exclusive).unwrap();
                                lm.release(txn, res).unwrap();
                            }
                        }));
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

#[cfg(feature = "std")]
criterion_group!(
    benches,
    bench_acquire_release_shared,
    bench_acquire_release_exclusive,
    bench_upgrade,
    bench_release_all,
    bench_contended,
);
#[cfg(feature = "std")]
criterion_main!(benches);
