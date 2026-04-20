use crate::{
    PageId, RowId,
    error::StorageResult,
    page::{self, Page, Write},
    page_cache::PageCache,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Logical cursor state exposed by the public cursor API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// The cursor is anchored to a page but not yet to a specific slot.
    Page {
        /// The page currently referenced by the cursor.
        page_id: PageId,
    },
    /// The cursor currently references a slot inside a leaf page.
    Positioned {
        /// The leaf page currently holding the cursor record.
        page_id: PageId,
        /// The slot index within the leaf page.
        slot_index: u16,
    },
    /// The cursor ran past the end of the tree.
    Exhausted,
}

/// Public handle to a single b+-tree rooted at `root_page_id`.
///
/// `Pager` returns this cursor already anchored at the root page. Future
/// searches, scans, and mutations update the cursor state in place.
#[derive(Clone)]
pub struct TreeCursor {
    page_cache: PageCache,
    root_page_id: PageId,
    state: CursorState,
}

impl TreeCursor {
    pub(crate) fn new(page_cache: PageCache, root_page_id: PageId) -> Self {
        Self { page_cache, root_page_id, state: CursorState::Page { page_id: root_page_id } }
    }

    /// Returns the root page id that anchors this tree.
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Returns the cursor's current logical state.
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Returns the page currently referenced by the cursor, if any.
    pub fn current_page_id(&self) -> Option<PageId> {
        match self.state {
            CursorState::Page { page_id } | CursorState::Positioned { page_id, .. } => {
                Some(page_id)
            }
            CursorState::Exhausted => None,
        }
    }

    /// Returns `true` when the cursor is currently positioned on a record.
    pub fn is_positioned(&self) -> bool {
        matches!(self.state, CursorState::Positioned { .. })
    }

    /// Resets the cursor back to the tree root page.
    pub fn seek_to_root(&mut self) {
        self.state = CursorState::Page { page_id: self.root_page_id };
    }
}

impl TreeCursor {
    /// Searches the table tree for `row_id`.
    ///
    /// The cursor is expected to end on the matching row when found, or on the
    /// leaf page where `row_id` would be inserted when absent.
    pub fn get(&mut self, row_id: RowId) -> StorageResult<Option<Record<'_>>> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("tree lookup is not implemented yet")
    }

    /// Inserts a new row payload into the table tree.
    ///
    /// Returns [`crate::error::ConstraintError::DuplicateKey`] if `row_id`
    /// already exists.
    pub fn insert(&mut self, row_id: RowId, payload: &[u8]) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = (row_id, payload);
        todo!("tree insert is not implemented yet")
    }

    /// Replaces the payload stored for an existing `row_id`.
    pub fn update(&mut self, row_id: RowId, payload: &[u8]) -> StorageResult<Record<'_>> {
        let _ = &self.page_cache;
        let _ = (row_id, payload);
        todo!("tree update is not implemented yet")
    }

    /// Deletes the row identified by `row_id`.
    pub fn delete(&mut self, row_id: RowId) -> StorageResult<()> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("tree delete is not implemented yet")
    }

    /// Positions the cursor on the smallest row id in the table tree.
    pub fn seek_to_first(&mut self) -> StorageResult<bool> {
        let _ = &self.page_cache;
        todo!("tree cursor positioning is not implemented yet")
    }

    /// Positions the cursor on `row_id` if it exists.
    pub fn seek_to_row_id(&mut self, row_id: RowId) -> StorageResult<bool> {
        let _ = &self.page_cache;
        let _ = row_id;
        todo!("tree cursor positioning is not implemented yet")
    }

    /// Reads the currently selected row, if any.
    pub fn current(&self) -> StorageResult<Option<Record<'_>>> {
        let _ = &self.page_cache;
        todo!("tree cursor reads are not implemented yet")
    }

    /// Advances to the next row in sorted row-id order.
    pub fn next_row(&mut self) -> StorageResult<Option<Record<'_>>> {
        let _ = &self.page_cache;
        todo!("tree cursor iteration is not implemented yet")
    }

    /// Moves to the previous row in sorted row-id order.
    pub fn prev_row(&mut self) -> StorageResult<Option<Record<'_>>> {
        let _ = &self.page_cache;
        todo!("tree cursor iteration is not implemented yet")
    }
}

pub(crate) fn initialize_empty_root(page_cache: &PageCache) -> StorageResult<PageId> {
    let (page_id, pin) = page_cache.new_page()?;
    let mut page = pin.write()?;
    let _ = Page::<Write<'_>, page::Leaf>::initialize(page.page_mut());
    Ok(page_id)
}
