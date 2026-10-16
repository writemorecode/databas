//! Shared page cache with explicit pin and page-access guards.
//!
//! [`PageCache`] is a cheap-to-clone handle that shares cache state through
//! [`Arc`]. The cache uses synchronized interior
//! mutability to allow multiple concurrent pins without requiring a mutable
//! borrow of the cache handle itself.
//!
//! The cache distinguishes between two kinds of ownership:
//!
//! - [`PinGuard`] keeps a frame resident in the cache and prevents eviction.
//! - [`PageReadGuard`] and [`PageWriteGuard`] provide temporary access to the
//!   page bytes stored in a pinned frame.
//!
//! This split makes the ownership model explicit: pinning controls residency,
//! while read and write guards control access to the page contents. Dropping a
//! [`PinGuard`] decrements the frame pin count. Dirty pages are written only by
//! explicit flushes or eviction.

use std::{
    collections::{HashMap, TryReserveError},
    sync::TryLockError,
};

use crate::Lsn;
use std::sync::atomic::AtomicU32;
use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use thiserror::Error;

use crate::error::StorageError;
use crate::log_manager::ZERO_LSN;
use crate::page::{NodeMarker, Page, PageResult, Read, Write};
use crate::page_replacement::ClockPolicy;
use crate::storage_runtime::StorageRuntime;
use crate::transaction_manager::PageRestore;
use crate::{PAGE_SIZE, PageId, TxnId};

#[derive(Debug, Error)]
pub enum PageCacheError {
    #[error("storage runtime error: {0}")]
    Storage(Box<StorageError>),
    #[error("no evictable frame available")]
    NoEvictableFrame,
    #[error("page {page_id} is pinned")]
    PinnedPage { page_id: PageId },
    #[error("page {page_id} cannot be borrowed immutably while a mutable borrow is active")]
    PageImmutableBorrowConflict { page_id: PageId },
    #[error("page {page_id} cannot be borrowed mutably while another borrow is active")]
    PageMutableBorrowConflict { page_id: PageId },
    #[error("invalid frame count: {frame_count}")]
    InvalidFrameCount { frame_count: usize },
    #[error("failed to allocate {frame_count} page cache frames: {source}")]
    FrameAllocationFailed { frame_count: usize, source: TryReserveError },
    #[error(
        "corrupt page table entry: page {page_id} maps to invalid frame {frame_id} (frame count: {frame_count})"
    )]
    CorruptPageTableEntry { page_id: PageId, frame_id: usize, frame_count: usize },
    #[error("page {page_id} pin count overflowed")]
    PinCountOverflow { page_id: PageId },
    #[error("synchronization lock poisoned: {lock}")]
    Poisoned { lock: &'static str },
}

pub type PageCacheResult<T> = Result<T, PageCacheError>;

fn runtime_error(error: StorageError) -> PageCacheError {
    PageCacheError::Storage(Box::new(error))
}

fn try_read_page_data(
    data: &RwLock<[u8; PAGE_SIZE]>,
    page_id: PageId,
) -> PageCacheResult<RwLockReadGuard<'_, [u8; PAGE_SIZE]>> {
    match data.try_read() {
        Ok(page) => Ok(page),
        Err(TryLockError::WouldBlock) => {
            Err(PageCacheError::PageImmutableBorrowConflict { page_id })
        }
        Err(TryLockError::Poisoned(_poisoned)) => {
            Err(PageCacheError::Poisoned { lock: "page data" })
        }
    }
}

fn try_write_page_data(
    data: &RwLock<[u8; PAGE_SIZE]>,
    page_id: PageId,
) -> PageCacheResult<RwLockWriteGuard<'_, [u8; PAGE_SIZE]>> {
    match data.try_write() {
        Ok(page) => Ok(page),
        Err(TryLockError::WouldBlock) => Err(PageCacheError::PageMutableBorrowConflict { page_id }),
        Err(TryLockError::Poisoned(_poisoned)) => {
            Err(PageCacheError::Poisoned { lock: "page data" })
        }
    }
}

pub type FrameId = usize;

#[derive(Debug)]
struct Frame {
    page_id: AtomicU64,
    data: RwLock<[u8; PAGE_SIZE]>,
    dirty: AtomicBool,
    lsn: AtomicU64,
    /// Earliest WAL dependency for this dirty page, used to bound checkpoint
    /// reclamation. Before a write is logged, this is a conservative lower bound.
    oldest_unflushed_lsn: AtomicU64,
    pin_count: AtomicU32,
}

impl Frame {
    fn page_id(&self) -> Option<PageId> {
        match self.page_id.load(Ordering::Acquire) {
            u64::MAX => None,
            page_id => Some(page_id),
        }
    }

    fn set_page_id(&self, page_id: Option<PageId>) {
        self.page_id.store(page_id.unwrap_or(u64::MAX), Ordering::Release);
    }

    /// Creates an empty frame with zeroed page data and cleared metadata bits.
    fn empty() -> Self {
        Self {
            page_id: AtomicU64::new(u64::MAX),
            data: RwLock::new([0u8; PAGE_SIZE]),
            dirty: AtomicBool::new(false),
            lsn: AtomicU64::new(ZERO_LSN),
            oldest_unflushed_lsn: AtomicU64::new(ZERO_LSN),
            pin_count: AtomicU32::new(0),
        }
    }
}

struct CacheMeta {
    page_table: HashMap<PageId, FrameId>,
    replacement: ClockPolicy,
    tree_mutation_epochs: HashMap<PageId, Arc<AtomicU64>>,
}

struct PageCacheInner {
    runtime: Arc<StorageRuntime>,
    meta: Mutex<CacheMeta>,
    frames: Vec<Frame>,
}

impl PageCacheInner {
    fn lock_meta(&self) -> PageCacheResult<MutexGuard<'_, CacheMeta>> {
        self.meta
            .lock()
            .map_err(|_poisoned| PageCacheError::Poisoned { lock: "page cache metadata" })
    }
}

/// Thread-safe shared handle to the page cache.
///
/// Cloning the handle shares the same cache state through [`Arc`]. The handle
/// itself does not represent a pin or a page borrow; it only provides access to
/// cache operations. Use [`PinGuard`] to keep pages resident and use
/// [`PageReadGuard`] or [`PageWriteGuard`] for temporary access to the page
/// bytes.
pub struct PageCache {
    inner: Arc<PageCacheInner>,
}

impl Clone for PageCache {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl PageCache {
    /// Creates a new page cache with a fixed number of preallocated frames.
    ///
    /// Returns an error when `frame_count` is zero.
    pub(crate) fn new(runtime: Arc<StorageRuntime>, frame_count: usize) -> PageCacheResult<Self> {
        if frame_count == 0 {
            return Err(PageCacheError::InvalidFrameCount { frame_count });
        }

        let mut frames = Vec::new();
        frames
            .try_reserve_exact(frame_count)
            .map_err(|source| PageCacheError::FrameAllocationFailed { frame_count, source })?;
        frames.extend((0..frame_count).map(|_| Frame::empty()));

        Ok(Self {
            inner: Arc::new(PageCacheInner {
                runtime,
                meta: Mutex::new(CacheMeta {
                    page_table: HashMap::new(),
                    replacement: ClockPolicy::new(frame_count),
                    tree_mutation_epochs: HashMap::new(),
                }),
                frames,
            }),
        })
    }

    /// Returns the mutation epoch shared by cursors over one B+-tree root.
    pub(crate) fn tree_mutation_epoch(
        &self,
        root_page_id: PageId,
    ) -> PageCacheResult<Arc<AtomicU64>> {
        let mut meta = self.inner.lock_meta()?;
        Ok(Arc::clone(
            meta.tree_mutation_epochs
                .entry(root_page_id)
                .or_insert_with(|| Arc::new(AtomicU64::new(0))),
        ))
    }

    /// Fetches an existing page into the cache and returns a pin guard.
    ///
    /// Cache hits update replacement state and increment pin count.
    /// Cache misses use CLOCK replacement and may evict a dirty page.
    pub(crate) fn fetch_page(&self, page_id: PageId) -> PageCacheResult<PinGuard> {
        let mut meta = self.inner.lock_meta()?;
        if let Some(frame_id) = self.resident_frame_id(&meta, page_id)? {
            let frame = &self.inner.frames[frame_id];
            let pin_count = frame
                .pin_count
                .load(Ordering::Acquire)
                .checked_add(1)
                .ok_or(PageCacheError::PinCountOverflow { page_id })?;
            frame.pin_count.store(pin_count, Ordering::Release);
            meta.replacement.record_access(frame_id);
            return Ok(PinGuard::new(Arc::clone(&self.inner), frame_id, page_id));
        }

        let frame_id =
            self.select_victim_frame(&mut meta).ok_or(PageCacheError::NoEvictableFrame)?;
        self.replace_frame(&mut meta, frame_id, page_id)?;
        Ok(PinGuard::new(Arc::clone(&self.inner), frame_id, page_id))
    }

    /// Allocates a new on-disk page and returns it pinned in the cache.
    ///
    /// A victim frame is selected before allocation so a full pinned cache
    /// returns `NoEvictableFrame` without growing the file.
    pub(crate) fn new_page(&self, txn_id: Option<TxnId>) -> PageCacheResult<(PageId, PinGuard)> {
        let mut meta = self.inner.lock_meta()?;
        let frame_id =
            self.select_victim_frame(&mut meta).ok_or(PageCacheError::NoEvictableFrame)?;
        let page_id = self.inner.runtime.new_page().map_err(runtime_error)?;
        if let Err(err) = self.inner.runtime.record_page_alloc(txn_id, page_id) {
            return Err(PageCacheError::Storage(Box::new(err)));
        }
        self.replace_frame(&mut meta, frame_id, page_id)?;
        Ok((page_id, PinGuard::new(Arc::clone(&self.inner), frame_id, page_id)))
    }

    /// Flushes one resident page if dirty.
    ///
    /// Non-resident pages are a no-op. Pinned pages return `PinnedPage`.
    pub(crate) fn flush_page(&self, page_id: PageId) -> PageCacheResult<()> {
        let meta = self.inner.lock_meta()?;
        let Some(frame_id) = self.resident_frame_id(&meta, page_id)? else {
            return Ok(());
        };

        let frame = &self.inner.frames[frame_id];
        if frame.pin_count.load(Ordering::Acquire) > 0 {
            return Err(PageCacheError::PinnedPage { page_id });
        }

        self.flush_frame_if_dirty(frame_id)
    }

    /// Flushes all dirty pages that are currently unpinned.
    ///
    /// Returns `PinnedPage` if a dirty page is pinned.
    pub(crate) fn flush_all(&self) -> PageCacheResult<()> {
        let _meta = self.inner.lock_meta()?;
        for (frame_id, frame) in self.inner.frames.iter().enumerate() {
            let page_id = frame.page_id();
            let pin_count = frame.pin_count.load(Ordering::Acquire);
            let dirty = frame.dirty.load(Ordering::Acquire);

            if !dirty {
                continue;
            }

            let Some(page_id) = page_id else {
                continue;
            };

            if pin_count > 0 {
                return Err(PageCacheError::PinnedPage { page_id });
            }

            self.flush_frame_if_dirty(frame_id)?;
        }

        Ok(())
    }

    /// Makes one bounded pass, releasing cache metadata between frames.
    /// Pinned pages are deferred, not errors. The returned LSN bounds WAL
    /// reclamation for skipped dirty pages, including in-flight writes.
    pub(crate) fn flush_checkpoint(&self) -> PageCacheResult<Option<Lsn>> {
        let mut oldest = None;
        for frame_id in 0..self.inner.frames.len() {
            let _meta = self.inner.lock_meta()?;
            let frame = &self.inner.frames[frame_id];
            if !frame.dirty.load(Ordering::Acquire) {
                continue;
            }
            if frame.pin_count.load(Ordering::Acquire) > 0 {
                let lsn = frame.oldest_unflushed_lsn.load(Ordering::Acquire);
                oldest = Some(oldest.map_or(lsn, |previous: Lsn| previous.min(lsn)));
            } else {
                self.flush_frame_if_dirty(frame_id)?;
            }
        }
        Ok(oldest)
    }

    fn resident_frame_id(
        &self,
        meta: &CacheMeta,
        page_id: PageId,
    ) -> PageCacheResult<Option<FrameId>> {
        let Some(&frame_id) = meta.page_table.get(&page_id) else {
            return Ok(None);
        };
        self.validate_frame_id(page_id, frame_id)?;
        Ok(Some(frame_id))
    }

    fn validate_frame_id(&self, page_id: PageId, frame_id: FrameId) -> PageCacheResult<()> {
        if frame_id >= self.inner.frames.len() {
            return Err(PageCacheError::CorruptPageTableEntry {
                page_id,
                frame_id,
                frame_count: self.inner.frames.len(),
            });
        }
        Ok(())
    }

    fn select_victim_frame(&self, meta: &mut CacheMeta) -> Option<FrameId> {
        let frames = &self.inner.frames;
        meta.replacement
            .select_victim(|frame_id| frames[frame_id].pin_count.load(Ordering::Acquire) > 0)
    }

    /// Replaces frame contents with `new_page_id`, flushing old dirty data first.
    fn replace_frame(
        &self,
        meta: &mut CacheMeta,
        frame_id: FrameId,
        new_page_id: PageId,
    ) -> PageCacheResult<()> {
        self.flush_frame_if_dirty(frame_id)?;

        let frame = &self.inner.frames[frame_id];
        let old_page_id = frame.page_id();

        let mut data = [0u8; PAGE_SIZE];
        self.inner.runtime.read_page(new_page_id, &mut data).map_err(runtime_error)?;

        {
            let mut frame_data =
                try_write_page_data(&frame.data, old_page_id.unwrap_or(new_page_id))?;
            *frame_data = data;
        }

        frame.set_page_id(Some(new_page_id));
        frame.dirty.store(false, Ordering::Release);
        frame.lsn.store(ZERO_LSN, Ordering::Release);
        frame.oldest_unflushed_lsn.store(ZERO_LSN, Ordering::Release);
        frame.pin_count.store(1, Ordering::Release);

        if let Some(old_page_id) = old_page_id {
            meta.page_table.remove(&old_page_id);
        }
        meta.replacement.record_insert(frame_id);
        meta.page_table.insert(new_page_id, frame_id);
        Ok(())
    }

    /// Writes a dirty resident frame to disk and clears its dirty bit.
    fn flush_frame_if_dirty(&self, frame_id: FrameId) -> PageCacheResult<()> {
        let frame = &self.inner.frames[frame_id];
        if !frame.dirty.load(Ordering::Acquire) {
            return Ok(());
        }

        let Some(page_id) = frame.page_id() else {
            return Ok(());
        };

        let page = try_read_page_data(&frame.data, page_id)?;
        self.inner
            .runtime
            .flush_wal_through(frame.lsn.load(Ordering::Acquire))
            .map_err(|err| PageCacheError::Storage(Box::new(err)))?;
        self.inner.runtime.write_page(page_id, &page).map_err(runtime_error)?;
        frame.dirty.store(false, Ordering::Release);
        frame.oldest_unflushed_lsn.store(ZERO_LSN, Ordering::Release);
        Ok(())
    }

    pub(crate) fn restore_rollback_pages(
        &self,
        restore_pages: Vec<PageRestore>,
    ) -> PageCacheResult<()> {
        for restore in restore_pages {
            let pin = self.fetch_page(restore.page_id)?;
            let frame = &self.inner.frames[pin.frame_id];
            {
                let mut data = try_write_page_data(&frame.data, restore.page_id)?;
                *data = restore.image;
                let oldest = if frame.dirty.load(Ordering::Acquire) {
                    frame.oldest_unflushed_lsn.load(Ordering::Acquire).min(restore.wal_flush_lsn)
                } else {
                    restore.wal_flush_lsn
                };
                frame.oldest_unflushed_lsn.store(oldest, Ordering::Release);
                frame.lsn.store(restore.wal_flush_lsn, Ordering::Release);
                frame.dirty.store(true, Ordering::Release);
            }
        }
        Ok(())
    }
}

/// Residency guard for a pinned page.
///
/// Holding a `PinGuard` increments the frame pin count and guarantees that the
/// underlying frame cannot be selected for eviction. A pin does not itself
/// expose the page bytes. Call [`PinGuard::read`] or [`PinGuard::write`] to
/// borrow the page contents temporarily.
///
/// Dropping the guard decrements the frame pin count.
pub struct PinGuard {
    page_cache: Arc<PageCacheInner>,
    frame_id: FrameId,
    page_id: PageId,
}

impl PinGuard {
    /// Creates a new pin guard for a specific frame.
    fn new(page_cache: Arc<PageCacheInner>, frame_id: FrameId, page_id: PageId) -> Self {
        Self { page_cache, frame_id, page_id }
    }

    /// Returns the page ID associated with this pin.
    /// Borrows the pinned page immutably.
    ///
    /// Multiple read guards may coexist for the same page, but immutable access
    /// fails while a write guard is active.
    pub(crate) fn read(&self) -> PageCacheResult<PageReadGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = try_read_page_data(&frame.data, self.page_id)?;
        Ok(PageReadGuard { page })
    }

    /// Borrows the pinned page mutably and marks it dirty immediately.
    ///
    /// Mutable access fails while any read or write guard is active for the
    /// same frame. Acquiring a write guard marks the frame dirty even if the
    /// caller later decides not to mutate the page bytes.
    pub(crate) fn write(&self, txn_id: Option<TxnId>) -> PageCacheResult<PageWriteGuard<'_>> {
        let frame = &self.page_cache.frames[self.frame_id];
        let page = try_write_page_data(&frame.data, self.page_id)?;
        let before = *page;
        let was_dirty = frame.dirty.load(Ordering::Acquire);
        if !was_dirty {
            // An in-flight guard may straddle checkpoint capture. Its actual
            // update LSN cannot precede this conservative pre-logging bound.
            let oldest_unflushed_lsn = if txn_id.is_some() {
                self.page_cache.runtime.next_wal_lsn().map_err(runtime_error)?
            } else {
                ZERO_LSN
            };
            frame.oldest_unflushed_lsn.store(oldest_unflushed_lsn, Ordering::Release);
        }
        frame.dirty.store(true, Ordering::Release);
        Ok(PageWriteGuard {
            page,
            before,
            was_dirty,
            runtime: Arc::clone(&self.page_cache.runtime),
            frame,
            page_id: self.page_id,
            txn_id,
        })
    }
}

impl Drop for PinGuard {
    /// Decrements the frame pin count when the guard leaves scope.
    fn drop(&mut self) {
        // A poisoned cache is permanently fail-closed. Retaining this pin is
        // safer than mutating cache state whose invariants may no longer hold.
        let Ok(_meta) = self.page_cache.lock_meta() else {
            return;
        };
        let frame = &self.page_cache.frames[self.frame_id];
        let pin_count = frame.pin_count.load(Ordering::Acquire);
        debug_assert!(pin_count > 0, "pin count underflow");
        if pin_count > 0 {
            frame.pin_count.store(pin_count - 1, Ordering::Release);
        }
    }
}

/// Immutable page-byte borrow for a pinned frame.
///
/// `PageReadGuard` owns the active immutable borrow of the page bytes. It does
/// not affect eviction on its own; the associated [`PinGuard`] must remain alive
/// for the page to stay resident. Use this guard for raw byte inspection or to
/// construct typed read-only page views.
pub struct PageReadGuard<'a> {
    page: RwLockReadGuard<'a, [u8; PAGE_SIZE]>,
}

impl PageReadGuard<'_> {
    /// Returns the pinned page bytes.
    pub(crate) fn page(&self) -> &[u8; PAGE_SIZE] {
        &self.page
    }

    /// Opens a typed immutable view over the page bytes.
    pub(crate) fn open<N>(&self) -> PageResult<Page<Read<'_>, N>>
    where
        N: NodeMarker,
    {
        Page::<Read<'_>, N>::open(self.page())
    }
}

/// Mutable page-byte borrow for a pinned frame.
///
/// `PageWriteGuard` owns the active mutable borrow of the page bytes. Only one
/// write guard may exist for a frame at a time, and no read guards may coexist
/// with it. Creating a write guard marks the frame dirty immediately.
pub struct PageWriteGuard<'a> {
    page: RwLockWriteGuard<'a, [u8; PAGE_SIZE]>,
    before: [u8; PAGE_SIZE],
    was_dirty: bool,
    runtime: Arc<StorageRuntime>,
    frame: &'a Frame,
    page_id: PageId,
    txn_id: Option<TxnId>,
}

impl PageWriteGuard<'_> {
    /// Returns the pinned page bytes immutably.
    pub(crate) fn page(&self) -> &[u8; PAGE_SIZE] {
        &self.page
    }

    /// Returns the pinned page bytes mutably.
    pub(crate) fn page_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.page
    }

    /// Opens a typed mutable view over the page bytes.
    pub(crate) fn open_mut<N>(&mut self) -> PageResult<Page<Write<'_>, N>>
    where
        N: NodeMarker,
    {
        Page::<Write<'_>, N>::open(self.page_mut())
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        if *self.page == self.before {
            self.frame.dirty.store(self.was_dirty, Ordering::Release);
            if !self.was_dirty {
                self.frame.oldest_unflushed_lsn.store(ZERO_LSN, Ordering::Release);
            }
            return;
        }

        match self.runtime.record_page_update(self.txn_id, self.page_id, &self.before, &self.page) {
            Ok(Some(update)) => {
                *self.page = update.redo;
                self.frame.lsn.store(update.lsn, Ordering::Release);
                if !self.was_dirty {
                    self.frame.oldest_unflushed_lsn.store(update.lsn, Ordering::Release);
                }
            }
            Ok(None) => {
                self.frame.lsn.store(ZERO_LSN, Ordering::Release);
                self.frame.oldest_unflushed_lsn.store(ZERO_LSN, Ordering::Release);
            }
            Err(_) => {
                *self.page = self.before;
                self.frame.dirty.store(self.was_dirty, Ordering::Release);
                if let Some(txn_id) = self.txn_id {
                    let _ = self.runtime.record_transaction_failure(txn_id);
                }
            }
        }
    }
}
