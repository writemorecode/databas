use std::path::Path;

use crate::{
    disk_manager::DiskManager,
    error::{LimitExceededError, StorageError},
    page_cache::{PageCache, PinGuard},
    table_page::{
        TableInteriorPageMut, TableInteriorPageRef, TableLeafPageMut, TableLeafPageRef,
        TablePageCorruptionKind, TablePageError, TablePageRef,
    },
    types::PAGE_SIZE,
};

pub type PageId = u64;
pub type RowId = u64;

const DEFAULT_PAGE_CACHE_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BTree {
    root_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordLocation {
    page_id: PageId,
    slot_id: u16,
    key: RowId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BTreePathEntry {
    page_id: PageId,
    child_index: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InteriorEntry {
    row_id: RowId,
    left_child_page_id: PageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildSplitEvent {
    separator_key: RowId,
    left_child_page_id: PageId,
    right_child_page_id: PageId,
}

pub struct Engine {
    pub(crate) page_cache: PageCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef<'a> {
    pub key: RowId,
    pub value: &'a [u8],
}

pub struct RecordGuard<'tree> {
    guard: PinGuard<'tree>,
    slot_id: u16,
}

impl<'tree> RecordGuard<'tree> {
    pub fn key(&self) -> Result<RowId, StorageError> {
        Ok(TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?.row_id)
    }

    pub fn value(&self) -> Result<&[u8], StorageError> {
        Ok(TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?.payload)
    }

    pub fn record(&self) -> Result<RecordRef<'_>, StorageError> {
        let cell = TableLeafPageRef::from_bytes(self.guard.page())?.cell_at_slot(self.slot_id)?;
        Ok(RecordRef { key: cell.row_id, value: cell.payload })
    }
}

pub struct BTreeHandle<'engine> {
    engine: &'engine mut Engine,
    tree: BTree,
}

pub struct BTreeCursor<'tree> {
    engine: &'tree mut Engine,
    tree: BTree,
    position: Option<RecordLocation>,
}

impl Engine {
    pub fn new(file: &Path) -> Result<Self, StorageError> {
        let disk_manager = DiskManager::new(file)?;
        let page_cache = PageCache::new(disk_manager, DEFAULT_PAGE_CACHE_SIZE)?;
        Ok(Self { page_cache })
    }

    pub fn open_btree(&mut self, root_page_id: PageId) -> Result<BTreeHandle<'_>, StorageError> {
        let tree = BTree { root_page_id };
        Ok(BTreeHandle { engine: self, tree })
    }

    pub fn create_btree(&mut self) -> Result<BTreeHandle<'_>, StorageError> {
        let root_page_id = {
            let (page_id, mut page_guard) = self.page_cache.new_page()?;
            TableLeafPageMut::init_empty(page_guard.page_mut())?;
            page_id
        };

        self.open_btree(root_page_id)
    }

    fn btree_find_leaf_page_for_row_id(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<PageId, StorageError> {
        let mut current_page_id = root_page_id;

        let leaf_page_id = loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => break current_page_id,
                TablePageRef::Interior(interior) => {
                    let child_index = interior.child_index_for_row_id(row_id)?;
                    let child_page_id = interior.child_at(child_index)?;
                    current_page_id = child_page_id;
                }
            }
        };

        Ok(leaf_page_id)
    }

    fn btree_find_leaf_page_and_path_for_row_id(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<(PageId, Vec<BTreePathEntry>), StorageError> {
        let mut current_page_id = root_page_id;
        let mut path = Vec::new();

        let leaf_page_id = loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => break current_page_id,
                TablePageRef::Interior(interior) => {
                    let child_index = interior.child_index_for_row_id(row_id)?;
                    let child_page_id = interior.child_at(child_index)?;
                    path.push(BTreePathEntry { page_id: current_page_id, child_index });
                    current_page_id = child_page_id;
                }
            }
        };

        Ok((leaf_page_id, path))
    }

    fn leaf_insert_slot(
        leaf_page: &TableLeafPageRef<'_>,
        row_id: RowId,
    ) -> Result<usize, StorageError> {
        let insert_slot = usize::from(leaf_page.lower_bound_slot(row_id)?);
        if insert_slot < usize::from(leaf_page.cell_count()) {
            let existing = leaf_page.cell_at_slot(insert_slot as u16)?;
            if existing.row_id == row_id {
                return Err(TablePageError::DuplicateRowId { row_id }.into());
            }
        }
        Ok(insert_slot)
    }

    fn logical_leaf_row_id(
        leaf_page: &TableLeafPageRef<'_>,
        insert_slot: usize,
        insert_row_id: RowId,
        logical_index: usize,
    ) -> Result<RowId, StorageError> {
        if logical_index == insert_slot {
            return Ok(insert_row_id);
        }

        let source_index = if logical_index < insert_slot {
            logical_index
        } else {
            logical_index.checked_sub(1).ok_or_else(|| {
                StorageError::from(TablePageError::CorruptPage(
                    TablePageCorruptionKind::SlotIndexOutOfBounds,
                ))
            })?
        };

        Ok(leaf_page.cell_at_slot(source_index as u16)?.row_id)
    }

    fn insert_logical_leaf_entry(
        destination: &mut TableLeafPageMut<'_>,
        source_leaf_page: &TableLeafPageRef<'_>,
        insert_slot: usize,
        insert_row_id: RowId,
        insert_payload: &[u8],
        logical_index: usize,
    ) -> Result<(), StorageError> {
        if logical_index == insert_slot {
            destination.insert(insert_row_id, insert_payload)?;
            return Ok(());
        }

        let source_index = if logical_index < insert_slot {
            logical_index
        } else {
            logical_index.checked_sub(1).ok_or_else(|| {
                StorageError::from(TablePageError::CorruptPage(
                    TablePageCorruptionKind::SlotIndexOutOfBounds,
                ))
            })?
        };

        let cell = source_leaf_page.cell_at_slot(source_index as u16)?;
        destination.insert(cell.row_id, cell.payload)?;
        Ok(())
    }

    fn logical_leaf_range_fits_single_page(
        source_leaf_page: &TableLeafPageRef<'_>,
        insert_slot: usize,
        insert_row_id: RowId,
        insert_payload: &[u8],
        start: usize,
        end: usize,
    ) -> Result<bool, StorageError> {
        let mut page = [0u8; PAGE_SIZE];
        let mut leaf_page = TableLeafPageMut::init_empty(&mut page)?;
        for logical_index in start..end {
            match Self::insert_logical_leaf_entry(
                &mut leaf_page,
                source_leaf_page,
                insert_slot,
                insert_row_id,
                insert_payload,
                logical_index,
            ) {
                Ok(()) => {}
                Err(StorageError::LimitExceeded(LimitExceededError::PageFull { .. })) => {
                    return Ok(false);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    fn choose_leaf_split_index(
        source_leaf_page: &TableLeafPageRef<'_>,
        insert_slot: usize,
        insert_row_id: RowId,
        insert_payload: &[u8],
    ) -> Result<Option<usize>, StorageError> {
        let total_entries = usize::from(source_leaf_page.cell_count()) + 1;
        if total_entries < 2 {
            return Ok(None);
        }

        let midpoint = total_entries / 2;
        for distance in 0..total_entries {
            let left_candidate = midpoint.checked_sub(distance);
            if let Some(split_index) = left_candidate
                && (0..total_entries).contains(&split_index)
            {
                let left_fits = Self::logical_leaf_range_fits_single_page(
                    source_leaf_page,
                    insert_slot,
                    insert_row_id,
                    insert_payload,
                    0,
                    split_index,
                )?;
                if left_fits {
                    let right_fits = Self::logical_leaf_range_fits_single_page(
                        source_leaf_page,
                        insert_slot,
                        insert_row_id,
                        insert_payload,
                        split_index,
                        total_entries,
                    )?;
                    if right_fits {
                        return Ok(Some(split_index));
                    }
                }
            }

            if distance == 0 {
                continue;
            }

            let split_index = midpoint + distance;
            if split_index == 0 || split_index >= total_entries {
                continue;
            }

            let left_fits = Self::logical_leaf_range_fits_single_page(
                source_leaf_page,
                insert_slot,
                insert_row_id,
                insert_payload,
                0,
                split_index,
            )?;
            if !left_fits {
                continue;
            }

            let right_fits = Self::logical_leaf_range_fits_single_page(
                source_leaf_page,
                insert_slot,
                insert_row_id,
                insert_payload,
                split_index,
                total_entries,
            )?;
            if right_fits {
                return Ok(Some(split_index));
            }
        }

        Ok(None)
    }

    fn write_leaf_entry_range_to_page_bytes(
        page: &mut [u8; PAGE_SIZE],
        source_leaf_page: &TableLeafPageRef<'_>,
        insert_slot: usize,
        insert_row_id: RowId,
        insert_payload: &[u8],
        start: usize,
        end: usize,
    ) -> Result<(), StorageError> {
        let mut leaf_page = TableLeafPageMut::init_empty(page)?;
        for logical_index in start..end {
            Self::insert_logical_leaf_entry(
                &mut leaf_page,
                source_leaf_page,
                insert_slot,
                insert_row_id,
                insert_payload,
                logical_index,
            )?;
        }
        Ok(())
    }

    fn interior_split_child_index(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: u16,
    ) -> Result<usize, StorageError> {
        let child_index = usize::from(child_index);
        if child_index > usize::from(source_interior_page.cell_count()) {
            return Err(
                TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds).into()
            );
        }
        Ok(child_index)
    }

    fn logical_interior_entry_at(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
        logical_index: usize,
    ) -> Result<InteriorEntry, StorageError> {
        let source_cell_count = usize::from(source_interior_page.cell_count());
        if logical_index >= source_cell_count + 1 {
            return Err(
                TablePageError::CorruptPage(TablePageCorruptionKind::SlotIndexOutOfBounds).into()
            );
        }

        if logical_index == child_index {
            return Ok(InteriorEntry {
                row_id: split_event.separator_key,
                left_child_page_id: split_event.left_child_page_id,
            });
        }

        let source_index = if logical_index < child_index {
            logical_index
        } else {
            logical_index.checked_sub(1).ok_or_else(|| {
                StorageError::from(TablePageError::CorruptPage(
                    TablePageCorruptionKind::SlotIndexOutOfBounds,
                ))
            })?
        };

        let source_cell = source_interior_page.cell_at_slot(source_index as u16)?;
        let left_child_page_id =
            if child_index < source_cell_count && logical_index == child_index + 1 {
                split_event.right_child_page_id
            } else {
                source_cell.left_child
            };

        Ok(InteriorEntry { row_id: source_cell.row_id, left_child_page_id })
    }

    fn logical_interior_rightmost_child(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
    ) -> PageId {
        if child_index == usize::from(source_interior_page.cell_count()) {
            split_event.right_child_page_id
        } else {
            source_interior_page.rightmost_child()
        }
    }

    fn insert_logical_interior_entry(
        destination: &mut TableInteriorPageMut<'_>,
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
        logical_index: usize,
    ) -> Result<(), StorageError> {
        let entry = Self::logical_interior_entry_at(
            source_interior_page,
            child_index,
            split_event,
            logical_index,
        )?;
        destination.insert(entry.row_id, entry.left_child_page_id)?;
        Ok(())
    }

    fn logical_interior_range_fits_single_page(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
        start: usize,
        end: usize,
        rightmost_child_page_id: PageId,
    ) -> Result<bool, StorageError> {
        let mut page = [0u8; PAGE_SIZE];
        let mut interior_page =
            TableInteriorPageMut::init_empty(&mut page, rightmost_child_page_id)?;
        for logical_index in start..end {
            match Self::insert_logical_interior_entry(
                &mut interior_page,
                source_interior_page,
                child_index,
                split_event,
                logical_index,
            ) {
                Ok(()) => {}
                Err(StorageError::LimitExceeded(LimitExceededError::PageFull { .. })) => {
                    return Ok(false);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    fn interior_promotion_candidate_fits(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
        promotion_index: usize,
    ) -> Result<bool, StorageError> {
        let total_entries = usize::from(source_interior_page.cell_count()) + 1;
        if promotion_index >= total_entries {
            return Ok(false);
        }

        let promoted_entry = Self::logical_interior_entry_at(
            source_interior_page,
            child_index,
            split_event,
            promotion_index,
        )?;
        let left_rightmost_child_page_id = promoted_entry.left_child_page_id;
        let right_rightmost_child_page_id =
            Self::logical_interior_rightmost_child(source_interior_page, child_index, split_event);

        let left_fits = Self::logical_interior_range_fits_single_page(
            source_interior_page,
            child_index,
            split_event,
            0,
            promotion_index,
            left_rightmost_child_page_id,
        )?;
        if !left_fits {
            return Ok(false);
        }

        let right_fits = Self::logical_interior_range_fits_single_page(
            source_interior_page,
            child_index,
            split_event,
            promotion_index + 1,
            total_entries,
            right_rightmost_child_page_id,
        )?;
        Ok(right_fits)
    }

    fn choose_interior_promotion_index(
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
    ) -> Result<Option<usize>, StorageError> {
        let total_entries = usize::from(source_interior_page.cell_count()) + 1;
        if total_entries == 0 {
            return Ok(None);
        }

        let midpoint = total_entries / 2;
        for distance in 0..total_entries {
            if let Some(promotion_index) = midpoint.checked_sub(distance) {
                if Self::interior_promotion_candidate_fits(
                    source_interior_page,
                    child_index,
                    split_event,
                    promotion_index,
                )? {
                    return Ok(Some(promotion_index));
                }
            }

            if distance == 0 {
                continue;
            }

            let promotion_index = midpoint + distance;
            if promotion_index >= total_entries {
                continue;
            }

            if Self::interior_promotion_candidate_fits(
                source_interior_page,
                child_index,
                split_event,
                promotion_index,
            )? {
                return Ok(Some(promotion_index));
            }
        }

        Ok(None)
    }

    fn write_interior_entry_range_to_page_bytes(
        page: &mut [u8; PAGE_SIZE],
        source_interior_page: &TableInteriorPageRef<'_>,
        child_index: usize,
        split_event: ChildSplitEvent,
        start: usize,
        end: usize,
        rightmost_child_page_id: PageId,
    ) -> Result<(), StorageError> {
        let mut interior_page = TableInteriorPageMut::init_empty(page, rightmost_child_page_id)?;
        for logical_index in start..end {
            Self::insert_logical_interior_entry(
                &mut interior_page,
                source_interior_page,
                child_index,
                split_event,
                logical_index,
            )?;
        }
        Ok(())
    }

    fn apply_child_split_to_interior_page(
        &mut self,
        root_page_id: PageId,
        parent_entry: BTreePathEntry,
        split_event: ChildSplitEvent,
        is_root: bool,
    ) -> Result<Option<ChildSplitEvent>, StorageError> {
        let source_page_bytes = {
            let page_guard = self.page_cache.fetch_page(parent_entry.page_id)?;
            *page_guard.page()
        };
        let source_interior_page = TableInteriorPageRef::from_bytes(&source_page_bytes)?;
        let child_index =
            Self::interior_split_child_index(&source_interior_page, parent_entry.child_index)?;
        let total_entries = usize::from(source_interior_page.cell_count()) + 1;
        let updated_rightmost_child_page_id =
            Self::logical_interior_rightmost_child(&source_interior_page, child_index, split_event);

        if Self::logical_interior_range_fits_single_page(
            &source_interior_page,
            child_index,
            split_event,
            0,
            total_entries,
            updated_rightmost_child_page_id,
        )? {
            let mut parent_page_guard = self.page_cache.fetch_page(parent_entry.page_id)?;
            Self::write_interior_entry_range_to_page_bytes(
                parent_page_guard.page_mut(),
                &source_interior_page,
                child_index,
                split_event,
                0,
                total_entries,
                updated_rightmost_child_page_id,
            )?;
            return Ok(None);
        }

        let promotion_index = match Self::choose_interior_promotion_index(
            &source_interior_page,
            child_index,
            split_event,
        )? {
            Some(promotion_index) => promotion_index,
            None => {
                return Err(TablePageError::CorruptPage(
                    TablePageCorruptionKind::CellContentUnderflow,
                )
                .into());
            }
        };
        let promoted_entry = Self::logical_interior_entry_at(
            &source_interior_page,
            child_index,
            split_event,
            promotion_index,
        )?;
        let left_rightmost_child_page_id = promoted_entry.left_child_page_id;
        let right_rightmost_child_page_id = updated_rightmost_child_page_id;

        if is_root {
            let left_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_interior_entry_range_to_page_bytes(
                    page_guard.page_mut(),
                    &source_interior_page,
                    child_index,
                    split_event,
                    0,
                    promotion_index,
                    left_rightmost_child_page_id,
                )?;
                page_id
            };
            let right_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_interior_entry_range_to_page_bytes(
                    page_guard.page_mut(),
                    &source_interior_page,
                    child_index,
                    split_event,
                    promotion_index + 1,
                    total_entries,
                    right_rightmost_child_page_id,
                )?;
                page_id
            };

            let mut root_page_guard = self.page_cache.fetch_page(root_page_id)?;
            let mut root_page =
                TableInteriorPageMut::init_empty(root_page_guard.page_mut(), right_page_id)?;
            root_page.insert(promoted_entry.row_id, left_page_id)?;
            return Ok(None);
        }

        let right_page_id = {
            let (page_id, mut page_guard) = self.page_cache.new_page()?;
            Self::write_interior_entry_range_to_page_bytes(
                page_guard.page_mut(),
                &source_interior_page,
                child_index,
                split_event,
                promotion_index + 1,
                total_entries,
                right_rightmost_child_page_id,
            )?;
            page_id
        };

        {
            let mut left_page_guard = self.page_cache.fetch_page(parent_entry.page_id)?;
            Self::write_interior_entry_range_to_page_bytes(
                left_page_guard.page_mut(),
                &source_interior_page,
                child_index,
                split_event,
                0,
                promotion_index,
                left_rightmost_child_page_id,
            )?;
        }

        Ok(Some(ChildSplitEvent {
            separator_key: promoted_entry.row_id,
            left_child_page_id: parent_entry.page_id,
            right_child_page_id: right_page_id,
        }))
    }

    fn propagate_child_split_event(
        &mut self,
        root_page_id: PageId,
        path: &[BTreePathEntry],
        mut split_event: ChildSplitEvent,
    ) -> Result<(), StorageError> {
        for (depth, parent_entry) in path.iter().enumerate().rev() {
            let is_root = depth == 0;
            match self.apply_child_split_to_interior_page(
                root_page_id,
                *parent_entry,
                split_event,
                is_root,
            )? {
                Some(next_split_event) => split_event = next_split_event,
                None => return Ok(()),
            }
        }
        Ok(())
    }

    fn btree_insert_with_leaf_split(
        &mut self,
        root_page_id: PageId,
        leaf_page_id: PageId,
        path: Vec<BTreePathEntry>,
        row_id: RowId,
        value: &[u8],
        original_page_full_error: TablePageError,
    ) -> Result<(), StorageError> {
        let source_page_bytes = {
            let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
            *page_guard.page()
        };
        let source_leaf_page = TableLeafPageRef::from_bytes(&source_page_bytes)?;
        let insert_slot = Self::leaf_insert_slot(&source_leaf_page, row_id)?;

        let split_index =
            match Self::choose_leaf_split_index(&source_leaf_page, insert_slot, row_id, value)? {
                Some(split_index) => split_index,
                None => return Err(original_page_full_error.into()),
            };
        let total_entries = usize::from(source_leaf_page.cell_count()) + 1;
        let split_key =
            Self::logical_leaf_row_id(&source_leaf_page, insert_slot, row_id, split_index)?;

        if path.is_empty() {
            let left_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_leaf_entry_range_to_page_bytes(
                    page_guard.page_mut(),
                    &source_leaf_page,
                    insert_slot,
                    row_id,
                    value,
                    0,
                    split_index,
                )?;
                page_id
            };

            let right_page_id = {
                let (page_id, mut page_guard) = self.page_cache.new_page()?;
                Self::write_leaf_entry_range_to_page_bytes(
                    page_guard.page_mut(),
                    &source_leaf_page,
                    insert_slot,
                    row_id,
                    value,
                    split_index,
                    total_entries,
                )?;
                page_id
            };

            let mut root_page_guard = self.page_cache.fetch_page(root_page_id)?;
            let mut root_page =
                TableInteriorPageMut::init_empty(root_page_guard.page_mut(), right_page_id)?;
            root_page.insert(split_key, left_page_id)?;
            return Ok(());
        }

        let right_page_id = {
            let (page_id, mut page_guard) = self.page_cache.new_page()?;
            Self::write_leaf_entry_range_to_page_bytes(
                page_guard.page_mut(),
                &source_leaf_page,
                insert_slot,
                row_id,
                value,
                split_index,
                total_entries,
            )?;
            page_id
        };

        {
            let mut left_page_guard = self.page_cache.fetch_page(leaf_page_id)?;
            Self::write_leaf_entry_range_to_page_bytes(
                left_page_guard.page_mut(),
                &source_leaf_page,
                insert_slot,
                row_id,
                value,
                0,
                split_index,
            )?;
        }

        let split_event = ChildSplitEvent {
            separator_key: split_key,
            left_child_page_id: leaf_page_id,
            right_child_page_id: right_page_id,
        };

        self.propagate_child_split_event(root_page_id, &path, split_event)
    }

    fn btree_search<'a>(
        &'a mut self,
        root_page_id: PageId,
        row_id: RowId,
    ) -> Result<Option<RecordGuard<'a>>, StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let page = TableLeafPageRef::from_bytes(page_guard.page())?;
        match page.search_for_slot_id(row_id)? {
            Some(slot_id) => Ok(Some(RecordGuard { guard: page_guard, slot_id })),
            None => Ok(None),
        }
    }

    fn btree_insert(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let (leaf_page_id, path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, row_id)?;
        let insert_result = {
            let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
            let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
            leaf_page.insert(row_id, value)
        };

        match insert_result {
            Ok(()) => Ok(()),
            Err(err @ TablePageError::PageFull { .. }) => self.btree_insert_with_leaf_split(
                root_page_id,
                leaf_page_id,
                path,
                row_id,
                value,
                err,
            ),
            Err(err) => Err(err.into()),
        }
    }

    fn btree_update(
        &mut self,
        root_page_id: PageId,
        row_id: RowId,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
        leaf_page.update(row_id, value)?;
        Ok(())
    }

    fn btree_delete(&mut self, root_page_id: PageId, row_id: RowId) -> Result<bool, StorageError> {
        let leaf_page_id = self.btree_find_leaf_page_for_row_id(root_page_id, row_id)?;
        let mut page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_page = TableLeafPageMut::from_bytes(page_guard.page_mut())?;
        match leaf_page.delete(row_id) {
            Ok(()) => Ok(true),
            Err(TablePageError::RowIdNotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn btree_leaf_location_at_slot(
        &mut self,
        leaf_page_id: PageId,
        slot_id: u16,
    ) -> Result<RecordLocation, StorageError> {
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
        let cell = leaf_page.cell_at_slot(slot_id)?;
        Ok(RecordLocation { page_id: leaf_page_id, slot_id, key: cell.row_id })
    }

    fn btree_first_location_in_leaf(
        &mut self,
        leaf_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
        if leaf_page.cell_count() == 0 {
            return Ok(None);
        }
        let cell = leaf_page.cell_at_slot(0)?;
        Ok(Some(RecordLocation { page_id: leaf_page_id, slot_id: 0, key: cell.row_id }))
    }

    fn btree_last_location_in_leaf(
        &mut self,
        leaf_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
        let Some(last_slot_id) = leaf_page.cell_count().checked_sub(1) else {
            return Ok(None);
        };
        let cell = leaf_page.cell_at_slot(last_slot_id)?;
        Ok(Some(RecordLocation { page_id: leaf_page_id, slot_id: last_slot_id, key: cell.row_id }))
    }

    fn btree_leftmost_leaf_page_and_path(
        &mut self,
        start_page_id: PageId,
        mut path: Vec<BTreePathEntry>,
    ) -> Result<(PageId, Vec<BTreePathEntry>), StorageError> {
        let mut current_page_id = start_page_id;

        loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => return Ok((current_page_id, path)),
                TablePageRef::Interior(interior) => {
                    let child_index = 0;
                    let child_page_id = interior.child_at(child_index)?;
                    path.push(BTreePathEntry { page_id: current_page_id, child_index });
                    current_page_id = child_page_id;
                }
            }
        }
    }

    fn btree_rightmost_leaf_page_and_path(
        &mut self,
        start_page_id: PageId,
        mut path: Vec<BTreePathEntry>,
    ) -> Result<(PageId, Vec<BTreePathEntry>), StorageError> {
        let mut current_page_id = start_page_id;

        loop {
            let current_page_guard = self.page_cache.fetch_page(current_page_id)?;
            let current_page = TablePageRef::from_bytes(current_page_guard.page())?;
            match current_page {
                TablePageRef::Leaf(_) => return Ok((current_page_id, path)),
                TablePageRef::Interior(interior) => {
                    let child_count = interior.child_count();
                    let child_index = child_count.checked_sub(1).ok_or_else(|| {
                        StorageError::from(TablePageError::CorruptPage(
                            TablePageCorruptionKind::SlotIndexOutOfBounds,
                        ))
                    })?;
                    let child_page_id = interior.child_at(child_index)?;
                    path.push(BTreePathEntry { page_id: current_page_id, child_index });
                    current_page_id = child_page_id;
                }
            }
        }
    }

    fn btree_next_leaf_from_path(
        &mut self,
        path: &[BTreePathEntry],
    ) -> Result<Option<(PageId, Vec<BTreePathEntry>)>, StorageError> {
        for (path_index, entry) in path.iter().enumerate().rev() {
            let maybe_next_sibling = {
                let page_guard = self.page_cache.fetch_page(entry.page_id)?;
                let page = TablePageRef::from_bytes(page_guard.page())?;
                let interior = match page {
                    TablePageRef::Interior(interior) => interior,
                    TablePageRef::Leaf(_) => {
                        return Err(TablePageError::CorruptPage(
                            TablePageCorruptionKind::SlotIndexOutOfBounds,
                        )
                        .into());
                    }
                };
                let child_count = interior.child_count();
                if entry.child_index < child_count.saturating_sub(1) {
                    let next_child_index = entry.child_index + 1;
                    Some((next_child_index, interior.child_at(next_child_index)?))
                } else {
                    None
                }
            };

            if let Some((next_child_index, sibling_page_id)) = maybe_next_sibling {
                let mut sibling_path = path[..path_index].to_vec();
                sibling_path
                    .push(BTreePathEntry { page_id: entry.page_id, child_index: next_child_index });
                let (leaf_page_id, leaf_path) =
                    self.btree_leftmost_leaf_page_and_path(sibling_page_id, sibling_path)?;
                return Ok(Some((leaf_page_id, leaf_path)));
            }
        }

        Ok(None)
    }

    fn btree_prev_leaf_from_path(
        &mut self,
        path: &[BTreePathEntry],
    ) -> Result<Option<(PageId, Vec<BTreePathEntry>)>, StorageError> {
        for (path_index, entry) in path.iter().enumerate().rev() {
            let maybe_prev_sibling = {
                let page_guard = self.page_cache.fetch_page(entry.page_id)?;
                let page = TablePageRef::from_bytes(page_guard.page())?;
                let interior = match page {
                    TablePageRef::Interior(interior) => interior,
                    TablePageRef::Leaf(_) => {
                        return Err(TablePageError::CorruptPage(
                            TablePageCorruptionKind::SlotIndexOutOfBounds,
                        )
                        .into());
                    }
                };
                if entry.child_index > 0 {
                    let prev_child_index = entry.child_index - 1;
                    Some((prev_child_index, interior.child_at(prev_child_index)?))
                } else {
                    None
                }
            };

            if let Some((prev_child_index, sibling_page_id)) = maybe_prev_sibling {
                let mut sibling_path = path[..path_index].to_vec();
                sibling_path
                    .push(BTreePathEntry { page_id: entry.page_id, child_index: prev_child_index });
                let (leaf_page_id, leaf_path) =
                    self.btree_rightmost_leaf_page_and_path(sibling_page_id, sibling_path)?;
                return Ok(Some((leaf_page_id, leaf_path)));
            }
        }

        Ok(None)
    }

    fn btree_leftmost_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_leftmost_leaf_page_and_path(root_page_id, Vec::new())?;

        loop {
            if let Some(location) = self.btree_first_location_in_leaf(leaf_page_id)? {
                return Ok(Some(location));
            }
            let Some((next_leaf_page_id, next_path)) = self.btree_next_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = next_leaf_page_id;
            path = next_path;
        }
    }

    fn btree_rightmost_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_rightmost_leaf_page_and_path(root_page_id, Vec::new())?;

        loop {
            if let Some(location) = self.btree_last_location_in_leaf(leaf_page_id)? {
                return Ok(Some(location));
            }
            let Some((prev_leaf_page_id, prev_path)) = self.btree_prev_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = prev_leaf_page_id;
            path = prev_path;
        }
    }

    fn btree_seek_first_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        self.btree_leftmost_location(root_page_id)
    }

    fn btree_seek_last_location(
        &mut self,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        self.btree_rightmost_location(root_page_id)
    }

    fn btree_seek_ge_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, key)?;

        loop {
            let maybe_slot_id = {
                let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
                let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
                let slot_id = leaf_page.lower_bound_slot(key)?;
                (slot_id < leaf_page.cell_count()).then_some(slot_id)
            };

            if let Some(slot_id) = maybe_slot_id {
                return Ok(Some(self.btree_leaf_location_at_slot(leaf_page_id, slot_id)?));
            }

            let Some((next_leaf_page_id, next_path)) = self.btree_next_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = next_leaf_page_id;
            path = next_path;
        }
    }

    fn btree_seek_gt_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, key)?;

        loop {
            let maybe_slot_id = {
                let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
                let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
                let cell_count = usize::from(leaf_page.cell_count());
                let lower_bound = usize::from(leaf_page.lower_bound_slot(key)?);

                if lower_bound >= cell_count {
                    None
                } else {
                    let lower_bound_slot =
                        u16::try_from(lower_bound).expect("leaf lower-bound slot fits in u16");
                    let cell = leaf_page.cell_at_slot(lower_bound_slot)?;
                    if cell.row_id > key {
                        Some(lower_bound_slot)
                    } else {
                        let next_slot = lower_bound + 1;
                        (next_slot < cell_count).then(|| {
                            u16::try_from(next_slot).expect("leaf successor slot fits in u16")
                        })
                    }
                }
            };

            if let Some(slot_id) = maybe_slot_id {
                return Ok(Some(self.btree_leaf_location_at_slot(leaf_page_id, slot_id)?));
            }

            let Some((next_leaf_page_id, next_path)) = self.btree_next_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = next_leaf_page_id;
            path = next_path;
        }
    }

    fn btree_seek_le_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, key)?;

        loop {
            let maybe_slot_id = {
                let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
                let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
                let cell_count = usize::from(leaf_page.cell_count());

                if cell_count == 0 {
                    None
                } else {
                    let lower_bound = usize::from(leaf_page.lower_bound_slot(key)?);
                    if lower_bound >= cell_count {
                        Some(u16::try_from(cell_count - 1).expect("leaf last slot fits in u16"))
                    } else {
                        let lower_bound_slot =
                            u16::try_from(lower_bound).expect("leaf lower-bound slot fits in u16");
                        let cell = leaf_page.cell_at_slot(lower_bound_slot)?;
                        if cell.row_id == key {
                            Some(lower_bound_slot)
                        } else if lower_bound > 0 {
                            Some(
                                u16::try_from(lower_bound - 1)
                                    .expect("leaf predecessor slot fits in u16"),
                            )
                        } else {
                            None
                        }
                    }
                }
            };

            if let Some(slot_id) = maybe_slot_id {
                return Ok(Some(self.btree_leaf_location_at_slot(leaf_page_id, slot_id)?));
            }

            let Some((prev_leaf_page_id, prev_path)) = self.btree_prev_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = prev_leaf_page_id;
            path = prev_path;
        }
    }

    fn btree_seek_lt_location(
        &mut self,
        root_page_id: PageId,
        key: RowId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let (mut leaf_page_id, mut path) =
            self.btree_find_leaf_page_and_path_for_row_id(root_page_id, key)?;

        loop {
            let maybe_slot_id = {
                let page_guard = self.page_cache.fetch_page(leaf_page_id)?;
                let leaf_page = TableLeafPageRef::from_bytes(page_guard.page())?;
                let cell_count = usize::from(leaf_page.cell_count());

                if cell_count == 0 {
                    None
                } else {
                    let lower_bound = usize::from(leaf_page.lower_bound_slot(key)?);
                    if lower_bound == 0 {
                        None
                    } else if lower_bound <= cell_count {
                        Some(
                            u16::try_from(lower_bound - 1)
                                .expect("leaf predecessor slot fits in u16"),
                        )
                    } else {
                        Some(u16::try_from(cell_count - 1).expect("leaf last slot fits in u16"))
                    }
                }
            };

            if let Some(slot_id) = maybe_slot_id {
                return Ok(Some(self.btree_leaf_location_at_slot(leaf_page_id, slot_id)?));
            }

            let Some((prev_leaf_page_id, prev_path)) = self.btree_prev_leaf_from_path(&path)?
            else {
                return Ok(None);
            };
            leaf_page_id = prev_leaf_page_id;
            path = prev_path;
        }
    }
}

impl<'engine> BTreeHandle<'engine> {
    pub fn root_page_id(&self) -> PageId {
        self.tree.root_page_id
    }

    pub fn get(&mut self, key: RowId) -> Result<Option<RecordGuard<'_>>, StorageError> {
        self.engine.btree_search(self.tree.root_page_id, key)
    }

    pub fn contains(&mut self, key: RowId) -> Result<bool, StorageError> {
        Ok(self.engine.btree_search(self.tree.root_page_id, key)?.is_some())
    }

    pub fn insert(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        self.engine.btree_insert(self.tree.root_page_id, key, value)
    }

    pub fn update(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        self.engine.btree_update(self.tree.root_page_id, key, value)
    }

    pub fn upsert(&mut self, key: RowId, value: &[u8]) -> Result<(), StorageError> {
        if self.contains(key)? { self.update(key, value) } else { self.insert(key, value) }
    }

    pub fn delete(&mut self, key: RowId) -> Result<bool, StorageError> {
        self.engine.btree_delete(self.tree.root_page_id, key)
    }

    pub fn cursor(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        Ok(BTreeCursor::new(self.engine, self.tree))
    }

    pub fn seek_first(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_first()?;
        Ok(cursor)
    }

    pub fn seek_last(&mut self) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_last()?;
        Ok(cursor)
    }

    pub fn seek(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek(key)?;
        Ok(cursor)
    }

    pub fn seek_ge(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_ge(key)?;
        Ok(cursor)
    }

    pub fn seek_gt(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_gt(key)?;
        Ok(cursor)
    }

    pub fn seek_le(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_le(key)?;
        Ok(cursor)
    }

    pub fn seek_lt(&mut self, key: RowId) -> Result<BTreeCursor<'_>, StorageError> {
        let mut cursor = self.cursor()?;
        cursor.seek_lt(key)?;
        Ok(cursor)
    }
}

impl<'tree> BTreeCursor<'tree> {
    fn new(engine: &'tree mut Engine, tree: BTree) -> Self {
        Self { engine, tree, position: None }
    }

    pub fn is_valid(&self) -> bool {
        self.position.is_some()
    }

    pub fn key(&self) -> Result<Option<RowId>, StorageError> {
        Ok(self.position.map(|position| position.key))
    }

    pub fn record(&mut self) -> Result<Option<RecordGuard<'_>>, StorageError> {
        let Some(position) = self.position else {
            return Ok(None);
        };

        let page_guard = self.engine.page_cache.fetch_page(position.page_id)?;
        Ok(Some(RecordGuard { guard: page_guard, slot_id: position.slot_id }))
    }

    pub fn seek_first(&mut self) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_first_location(self.tree.root_page_id)?;
        Ok(())
    }

    pub fn seek_last(&mut self) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_last_location(self.tree.root_page_id)?;
        Ok(())
    }

    pub fn seek(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, key)?;
        if self.position.map(|position| position.key) != Some(key) {
            self.position = None;
        }
        Ok(())
    }

    pub fn seek_ge(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_gt(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_gt_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_le(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_le_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn seek_lt(&mut self, key: RowId) -> Result<(), StorageError> {
        self.position = self.engine.btree_seek_lt_location(self.tree.root_page_id, key)?;
        Ok(())
    }

    pub fn next_position(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.position = self.engine.btree_seek_gt_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn prev_position(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.position = self.engine.btree_seek_lt_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn delete_current(&mut self) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        let _deleted = self.engine.btree_delete(self.tree.root_page_id, position.key)?;
        self.position = self.engine.btree_seek_ge_location(self.tree.root_page_id, position.key)?;
        Ok(())
    }

    pub fn update_current(&mut self, value: &[u8]) -> Result<(), StorageError> {
        let Some(position) = self.position else {
            return Ok(());
        };

        self.engine.btree_update(self.tree.root_page_id, position.key, value)?;
        self.position = self.engine.btree_seek(position.key, self.tree.root_page_id)?;
        Ok(())
    }
}

impl Engine {
    fn btree_seek(
        &mut self,
        key: RowId,
        root_page_id: PageId,
    ) -> Result<Option<RecordLocation>, StorageError> {
        let location = self.btree_seek_ge_location(root_page_id, key)?;
        Ok(location.filter(|location| location.key == key))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, BTreeSet};
    use std::ops::Bound::{Excluded, Unbounded};

    use proptest::prelude::*;
    use tempfile::NamedTempFile;

    use crate::{
        error::{InvalidArgumentError, LimitExceededError, StorageError},
        table_page::TableInteriorPageMut,
        types::PAGE_SIZE,
    };

    use super::*;

    fn get_temp_engine() -> Engine {
        let file = NamedTempFile::new().expect("temp file");
        let disk_manager = DiskManager::new(file.path()).expect("disk manager");
        let page_cache = PageCache::new(disk_manager, 8).expect("page cache");
        Engine { page_cache }
    }

    fn expect_payload(record: Option<RecordGuard<'_>>, expected: Option<&[u8]>) {
        match (record, expected) {
            (Some(record), Some(expected)) => assert_eq!(record.value().unwrap(), expected),
            (None, None) => {}
            _ => panic!("record/payload mismatch"),
        }
    }

    fn fixed_payload(byte: u8, len: usize) -> Vec<u8> {
        vec![byte; len]
    }

    fn is_payload_limit_error(err: &StorageError) -> bool {
        matches!(
            err,
            StorageError::LimitExceeded(
                LimitExceededError::CellTooLarge { .. } | LimitExceededError::PageFull { .. }
            )
        )
    }

    fn is_row_id_not_found_error(err: &StorageError) -> bool {
        matches!(err, StorageError::InvalidArgument(InvalidArgumentError::RowIdNotFound { .. }))
    }

    fn fill_leaf_until_page_full(
        engine: &mut Engine,
        page_id: PageId,
        start_key: RowId,
        payload: &[u8],
    ) -> RowId {
        let mut page_guard = engine.page_cache.fetch_page(page_id).unwrap();
        let mut leaf = TableLeafPageMut::from_bytes(page_guard.page_mut()).unwrap();
        let mut next_key = start_key;
        loop {
            match leaf.insert(next_key, payload) {
                Ok(()) => next_key += 1,
                Err(TablePageError::PageFull { .. }) => return next_key,
                Err(err) => panic!("unexpected leaf insert error while filling page: {err:?}"),
            }
        }
    }

    fn fill_interior_with_leaf_children_until_full(
        engine: &mut Engine,
        interior_page_id: PageId,
        start_separator: RowId,
        step: RowId,
    ) -> RowId {
        let mut separator = start_separator;
        loop {
            let left_child_page_id = {
                let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
                TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
                page_id
            };

            let insert_result = {
                let mut interior_guard = engine.page_cache.fetch_page(interior_page_id).unwrap();
                let mut interior =
                    TableInteriorPageMut::from_bytes(interior_guard.page_mut()).unwrap();
                interior.insert(separator, left_child_page_id)
            };

            match insert_result {
                Ok(()) => separator += step,
                Err(TablePageError::PageFull { .. }) => return separator,
                Err(err) => panic!("unexpected interior fill error: {err:?}"),
            }
        }
    }

    #[test]
    fn point_ops_match_expected_semantics() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();

        assert!(!tree.contains(10).unwrap());
        assert!(!tree.delete(10).unwrap());

        let missing_update = tree.update(10, &[1]).unwrap_err();
        assert!(matches!(
            missing_update,
            StorageError::InvalidArgument(InvalidArgumentError::RowIdNotFound { row_id: 10 })
        ));

        tree.insert(10, &[1, 2]).unwrap();
        assert!(tree.contains(10).unwrap());
        expect_payload(tree.get(10).unwrap(), Some(&[1, 2]));

        let duplicate_insert = tree.insert(10, &[3]).unwrap_err();
        assert!(matches!(duplicate_insert, StorageError::Constraint(..)));

        tree.update(10, &[9, 9, 9]).unwrap();
        expect_payload(tree.get(10).unwrap(), Some(&[9, 9, 9]));

        tree.upsert(10, &[4]).unwrap();
        tree.upsert(11, &[5, 6]).unwrap();
        expect_payload(tree.get(10).unwrap(), Some(&[4]));
        expect_payload(tree.get(11).unwrap(), Some(&[5, 6]));

        assert!(tree.delete(10).unwrap());
        assert!(!tree.delete(10).unwrap());
        expect_payload(tree.get(10).unwrap(), None);
    }

    #[test]
    fn cursor_seek_and_movement_behave_as_expected() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        for key in [10, 20, 30] {
            tree.insert(key, &[key as u8]).unwrap();
        }

        let mut cursor = tree.seek_first().unwrap();
        assert!(cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), Some(10));
        assert_eq!(cursor.record().unwrap().unwrap().value().unwrap(), &[10]);

        cursor.next_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.next_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.next_position().unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), None);

        cursor.seek_last().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.prev_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.prev_position().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));
        cursor.prev_position().unwrap();
        assert!(!cursor.is_valid());

        cursor.seek_ge(25).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(30));
        cursor.seek_gt(30).unwrap();
        assert!(!cursor.is_valid());
        cursor.seek_le(20).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(20));
        cursor.seek_lt(20).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));
        cursor.seek(999).unwrap();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn empty_tree_cursor_is_invalid() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();

        let mut cursor = tree.seek_first().unwrap();
        assert!(!cursor.is_valid());
        assert_eq!(cursor.key().unwrap(), None);
        assert!(cursor.record().unwrap().is_none());

        cursor.next_position().unwrap();
        cursor.prev_position().unwrap();
        cursor.delete_current().unwrap();
        cursor.update_current(&[1]).unwrap();
        assert!(!cursor.is_valid());
    }

    #[test]
    fn cursor_delete_and_update_current() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        tree.insert(1, &[1]).unwrap();
        tree.insert(2, &[2]).unwrap();
        tree.insert(3, &[3]).unwrap();

        let mut cursor = tree.seek_ge(2).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(2));

        cursor.update_current(&[8, 8]).unwrap();
        assert_eq!(cursor.record().unwrap().unwrap().value().unwrap(), &[8, 8]);

        cursor.delete_current().unwrap();
        assert_eq!(cursor.key().unwrap(), Some(3));
    }

    #[test]
    fn seek_variants_cross_leaf_boundaries() {
        let mut engine = get_temp_engine();

        let left_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(1, &[1]).unwrap();
            leaf.insert(2, &[2]).unwrap();
            page_id
        };

        let right_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(10, &[10]).unwrap();
            leaf.insert(20, &[20]).unwrap();
            page_id
        };

        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut interior =
                TableInteriorPageMut::init_empty(page_guard.page_mut(), right_leaf).unwrap();
            interior.insert(10, left_leaf).unwrap();
            page_id
        };

        let mut tree = engine.open_btree(root_page_id).unwrap();

        let cursor = tree.seek_ge(3).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));

        let cursor = tree.seek_le(9).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(2));

        let cursor = tree.seek_gt(2).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(10));

        let cursor = tree.seek_lt(10).unwrap();
        assert_eq!(cursor.key().unwrap(), Some(2));
    }

    #[test]
    fn traversal_and_lookup_work_across_multiple_leaf_pages() {
        let mut engine = get_temp_engine();

        let left_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(1, &[1]).unwrap();
            leaf.insert(2, &[2]).unwrap();
            page_id
        };

        let right_leaf = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(10, &[10]).unwrap();
            leaf.insert(20, &[20]).unwrap();
            page_id
        };

        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut interior =
                TableInteriorPageMut::init_empty(page_guard.page_mut(), right_leaf).unwrap();
            interior.insert(10, left_leaf).unwrap();
            page_id
        };

        let mut tree = engine.open_btree(root_page_id).unwrap();
        expect_payload(tree.get(1).unwrap(), Some(&[1]));
        expect_payload(tree.get(10).unwrap(), Some(&[10]));
        expect_payload(tree.get(20).unwrap(), Some(&[20]));
        expect_payload(tree.get(99).unwrap(), None);

        let mut cursor = tree.seek_first().unwrap();
        let mut keys = Vec::new();
        while cursor.is_valid() {
            keys.push(cursor.key().unwrap().unwrap());
            cursor.next_position().unwrap();
        }
        assert_eq!(keys, vec![1, 2, 10, 20]);

        tree.insert(15, &[15]).unwrap();
        expect_payload(tree.get(15).unwrap(), Some(&[15]));
    }

    #[test]
    fn seek_matrix_matches_btreeset_oracle_on_multi_level_tree() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        let payload = fixed_payload(17, 192);
        let mut oracle = BTreeSet::new();

        for key in (10u64..5_010).step_by(2) {
            tree.insert(key, &payload).unwrap();
            oracle.insert(key);
        }

        let root_page_id = tree.root_page_id();
        let root_child_page_ids = {
            let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
            let root_page = TablePageRef::from_bytes(root_guard.page()).unwrap();
            let interior = match root_page {
                TablePageRef::Leaf(_) => panic!("expected interior root"),
                TablePageRef::Interior(interior) => interior,
            };
            let mut child_page_ids = Vec::new();
            for child_index in 0..interior.child_count() {
                child_page_ids.push(interior.child_at(child_index).unwrap());
            }
            child_page_ids
        };

        let has_interior_child = root_child_page_ids.into_iter().any(|child_page_id| {
            let child_guard = tree.engine.page_cache.fetch_page(child_page_id).unwrap();
            matches!(
                TablePageRef::from_bytes(child_guard.page()).unwrap(),
                TablePageRef::Interior(_)
            )
        });
        assert!(has_interior_child, "expected a multi-level b-tree");

        let mut probe_keys = vec![0u64, 1, 9, 10, 11, 12, 13, 5_008, 5_009, 5_010, u64::MAX];
        for key in (0u64..5_100).step_by(37) {
            probe_keys.push(key);
        }
        probe_keys.sort_unstable();
        probe_keys.dedup();

        for probe in probe_keys {
            let expected_seek = oracle.get(&probe).copied();
            let expected_ge = oracle.range(probe..).next().copied();
            let expected_gt = oracle.range((Excluded(probe), Unbounded)).next().copied();
            let expected_le = oracle.range(..=probe).next_back().copied();
            let expected_lt = oracle.range(..probe).next_back().copied();

            let cursor = tree.seek(probe).unwrap();
            assert_eq!(cursor.key().unwrap(), expected_seek);

            let cursor = tree.seek_ge(probe).unwrap();
            assert_eq!(cursor.key().unwrap(), expected_ge);

            let cursor = tree.seek_gt(probe).unwrap();
            assert_eq!(cursor.key().unwrap(), expected_gt);

            let cursor = tree.seek_le(probe).unwrap();
            assert_eq!(cursor.key().unwrap(), expected_le);

            let cursor = tree.seek_lt(probe).unwrap();
            assert_eq!(cursor.key().unwrap(), expected_lt);
        }
    }

    #[test]
    fn insert_splits_root_leaf_into_interior_page() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        let root_page_id = tree.root_page_id();

        for key in 0u64..14 {
            let payload = fixed_payload((key % 255) as u8, 512);
            tree.insert(key, &payload).unwrap();
        }

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        let root_page = TablePageRef::from_bytes(root_guard.page()).unwrap();
        match root_page {
            TablePageRef::Leaf(_) => panic!("expected root to become an interior page"),
            TablePageRef::Interior(interior) => assert!(interior.cell_count() > 0),
        }
        drop(root_guard);

        for key in 0u64..14 {
            let expected = fixed_payload((key % 255) as u8, 512);
            expect_payload(tree.get(key).unwrap(), Some(&expected));
        }
    }

    #[test]
    fn repeated_inserts_split_multiple_leaves_while_parent_has_room() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();
        let root_page_id = tree.root_page_id();

        for key in 0u64..160 {
            let payload = fixed_payload((key % 251) as u8, 160);
            tree.insert(key, &payload).unwrap();
        }

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        let root_page = TablePageRef::from_bytes(root_guard.page()).unwrap();
        let child_count = match root_page {
            TablePageRef::Leaf(_) => panic!("expected interior root after repeated splits"),
            TablePageRef::Interior(interior) => interior.child_count(),
        };
        assert!(child_count > 2);
        drop(root_guard);

        let mut cursor = tree.seek_first().unwrap();
        let mut seen = Vec::new();
        while cursor.is_valid() {
            seen.push(cursor.key().unwrap().unwrap());
            cursor.next_position().unwrap();
        }
        let expected: Vec<RowId> = (0u64..160).collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn root_interior_split_keeps_root_page_id_stable() {
        let mut engine = get_temp_engine();

        let rightmost_leaf_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            page_id
        };

        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableInteriorPageMut::init_empty(page_guard.page_mut(), rightmost_leaf_page_id)
                .unwrap();
            page_id
        };

        let next_separator =
            fill_interior_with_leaf_children_until_full(&mut engine, root_page_id, 10, 10);

        let big_payload = fixed_payload(7, 256);
        let first_failing_key = fill_leaf_until_page_full(
            &mut engine,
            rightmost_leaf_page_id,
            next_separator + 100,
            &big_payload,
        );

        let mut tree = engine.open_btree(root_page_id).unwrap();
        tree.insert(first_failing_key, &big_payload).unwrap();
        assert_eq!(tree.root_page_id(), root_page_id);
        expect_payload(tree.get(first_failing_key).unwrap(), Some(&big_payload));

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        match TablePageRef::from_bytes(root_guard.page()).unwrap() {
            TablePageRef::Leaf(_) => panic!("expected interior root after root interior split"),
            TablePageRef::Interior(interior) => assert!(interior.child_count() >= 2),
        }
    }

    #[test]
    fn non_root_interior_split_propagates_to_parent() {
        let mut engine = get_temp_engine();

        let rightmost_leaf_of_right_interior = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            page_id
        };
        let right_interior_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            TableInteriorPageMut::init_empty(
                page_guard.page_mut(),
                rightmost_leaf_of_right_interior,
            )
            .unwrap();
            page_id
        };
        let next_right_separator = fill_interior_with_leaf_children_until_full(
            &mut engine,
            right_interior_page_id,
            2_000_000,
            10,
        );

        let split_payload = fixed_payload(9, 128);
        let first_failing_key = fill_leaf_until_page_full(
            &mut engine,
            rightmost_leaf_of_right_interior,
            next_right_separator + 100,
            &split_payload,
        );

        let left_leaf_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut leaf = TableLeafPageMut::init_empty(page_guard.page_mut()).unwrap();
            leaf.insert(1, &[1]).unwrap();
            page_id
        };
        let root_page_id = {
            let (page_id, mut page_guard) = engine.page_cache.new_page().unwrap();
            let mut root =
                TableInteriorPageMut::init_empty(page_guard.page_mut(), right_interior_page_id)
                    .unwrap();
            root.insert(1_000_000, left_leaf_page_id).unwrap();
            page_id
        };

        let mut tree = engine.open_btree(root_page_id).unwrap();
        tree.insert(first_failing_key, &split_payload).unwrap();
        expect_payload(tree.get(first_failing_key).unwrap(), Some(&split_payload));
        expect_payload(tree.get(1).unwrap(), Some(&[1]));

        let root_guard = tree.engine.page_cache.fetch_page(root_page_id).unwrap();
        match TablePageRef::from_bytes(root_guard.page()).unwrap() {
            TablePageRef::Leaf(_) => panic!("expected interior root"),
            TablePageRef::Interior(interior) => assert!(interior.cell_count() >= 2),
        }
    }

    #[test]
    fn multi_level_insert_stress_succeeds_with_recursive_splits() {
        let mut engine = get_temp_engine();
        let mut tree = engine.create_btree().unwrap();

        let payload = fixed_payload(42, 192);
        let key_count = 5_000u64;
        for key in 0..key_count {
            tree.insert(key, &payload).unwrap();
        }

        for key in [0u64, 17, 1_024, 2_048, 4_095, key_count - 1] {
            expect_payload(tree.get(key).unwrap(), Some(&payload));
        }

        let mut cursor = tree.seek_first().unwrap();
        let mut last_key = None;
        let mut seen_count = 0u64;
        while cursor.is_valid() {
            let key = cursor.key().unwrap().unwrap();
            if let Some(previous_key) = last_key {
                assert!(key > previous_key);
            }
            last_key = Some(key);
            seen_count += 1;
            cursor.next_position().unwrap();
        }
        assert_eq!(seen_count, key_count);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u8, Vec<u8>),
        Update(u8, Vec<u8>),
        Upsert(u8, Vec<u8>),
        Delete(u8),
        Get(u8),
        Contains(u8),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        const MAX_TEST_PAYLOAD_SIZE: usize = PAGE_SIZE * 3;
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX_TEST_PAYLOAD_SIZE);
        prop_oneof![
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Insert(key, value)),
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Update(key, value)),
            (any::<u8>(), payload.clone()).prop_map(|(key, value)| Op::Upsert(key, value)),
            any::<u8>().prop_map(Op::Delete),
            any::<u8>().prop_map(Op::Get),
            any::<u8>().prop_map(Op::Contains),
        ]
    }

    proptest! {
        #[test]
        fn prop_point_ops_match_btreemap_model(ops in prop::collection::vec(op_strategy(), 0..128)) {
            let mut engine = get_temp_engine();
            let mut tree = engine.create_btree().unwrap();
            let mut oracle = BTreeMap::<RowId, Vec<u8>>::new();

            for op in ops {
                match op {
                    Op::Insert(key, value) => {
                        let key = RowId::from(key);
                        let key_exists = oracle.contains_key(&key);
                        match tree.insert(key, &value) {
                            Ok(()) => {
                                prop_assert!(!key_exists);
                                oracle.insert(key, value);
                            }
                            Err(err) => {
                                if key_exists {
                                    prop_assert!(matches!(err, StorageError::Constraint(..)));
                                } else {
                                    prop_assert!(is_payload_limit_error(&err));
                                }
                            }
                        }
                    }
                    Op::Update(key, value) => {
                        let key = RowId::from(key);
                        let key_exists = oracle.contains_key(&key);
                        match tree.update(key, &value) {
                            Ok(()) => {
                                prop_assert!(key_exists);
                                oracle.insert(key, value);
                            }
                            Err(err) => {
                                if key_exists {
                                    prop_assert!(is_payload_limit_error(&err));
                                } else {
                                    prop_assert!(is_row_id_not_found_error(&err));
                                }
                            }
                        }
                    }
                    Op::Upsert(key, value) => {
                        let key = RowId::from(key);
                        match tree.upsert(key, &value) {
                            Ok(()) => {
                                oracle.insert(key, value);
                            }
                            Err(err) => {
                                prop_assert!(is_payload_limit_error(&err));
                            }
                        }
                    }
                    Op::Delete(key) => {
                        let key = RowId::from(key);
                        let expected = oracle.remove(&key).is_some();
                        let actual = tree.delete(key).unwrap();
                        prop_assert_eq!(actual, expected);
                    }
                    Op::Get(key) => {
                        let key = RowId::from(key);
                        let actual = tree.get(key).unwrap().map(|record| record.value().unwrap().to_vec());
                        let expected = oracle.get(&key).cloned();
                        prop_assert_eq!(actual, expected);
                    }
                    Op::Contains(key) => {
                        let key = RowId::from(key);
                        prop_assert_eq!(tree.contains(key).unwrap(), oracle.contains_key(&key));
                    }
                }
            }
        }

        #[test]
        fn prop_cursor_next_prev_are_monotonic(keys in prop::collection::vec(any::<u8>(), 0..64), payload_byte in any::<u8>()) {
            let mut engine = get_temp_engine();
            let mut tree = engine.create_btree().unwrap();
            let key_set: BTreeSet<RowId> = keys.into_iter().map(RowId::from).collect();

            for key in &key_set {
                tree.insert(*key, &[payload_byte]).unwrap();
            }

            let expected_forward: Vec<RowId> = key_set.iter().copied().collect();
            let expected_reverse: Vec<RowId> = key_set.iter().rev().copied().collect();

            let mut cursor = tree.seek_first().unwrap();
            let mut forward = Vec::new();
            while cursor.is_valid() {
                forward.push(cursor.key().unwrap().unwrap());
                cursor.next_position().unwrap();
            }
            prop_assert_eq!(forward, expected_forward);

            let mut cursor = tree.seek_last().unwrap();
            let mut reverse = Vec::new();
            while cursor.is_valid() {
                reverse.push(cursor.key().unwrap().unwrap());
                cursor.prev_position().unwrap();
            }
            prop_assert_eq!(reverse, expected_reverse);
        }
    }
}
