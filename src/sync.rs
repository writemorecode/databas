//! Synchronization primitives shared by production code and Loom models.
//!
//! Loom mirrors the standard library synchronization API, so keeping the
//! substitution here lets the concurrent implementation use one code path.

#[cfg(all(test, loom))]
pub(crate) use loom::sync::{
    Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

#[cfg(not(all(test, loom)))]
pub(crate) use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};
