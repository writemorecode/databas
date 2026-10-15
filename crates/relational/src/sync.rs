//! Synchronization primitives shared by production code and Loom models.
//!
//! Loom mirrors the standard library synchronization API, so keeping the
//! substitution here lets the concurrent implementation use one code path.

#[cfg(all(test, loom))]
pub use loom::sync::{Arc, Condvar, Mutex, MutexGuard};
#[cfg(not(all(test, loom)))]
pub use std::sync::{Arc, Condvar, Mutex, MutexGuard};
