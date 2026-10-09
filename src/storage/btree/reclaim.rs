//! Reclaim storage only after its references have been removed from the tree.
//! Retirement is logical: physical page contents remain available for rollback
//! until commit publishes the pages for reuse. Root contraction transfers the
//! child's overflow chains to the root, so it retires only the child page.

use super::root::read_page_kind;
use super::*;

impl TreeCursor {
    pub(super) fn overflow_heads(&self, id: PageId) -> StorageResult<Vec<PageId>> {
        let pin = self.page_cache.fetch_page(id)?;
        let page = pin.read()?;
        let mut heads = Vec::new();
        if page.page().iter().all(|byte| *byte == 0) {
            return Ok(heads);
        }
        match read_page_kind(page.page(), id)? {
            PageKind::RawLeaf => {
                let leaf = page.open::<Leaf>()?;
                for slot in 0..leaf.slot_count() {
                    if let Some(head) = leaf.cell_payload_parts(slot)?.2 {
                        heads.push(head);
                    }
                }
            }
            PageKind::RawInterior => {
                let interior = page.open::<Interior>()?;
                for slot in 0..interior.slot_count() {
                    if let Some(head) = interior.cell_payload_parts(slot)?.2 {
                        heads.push(head);
                    }
                }
            }
        }
        Ok(heads)
    }

    pub(super) fn free_overflow(
        &self,
        heads: impl IntoIterator<Item = PageId>,
    ) -> StorageResult<()> {
        let mut seen = std::collections::HashSet::new();
        for head in heads {
            let mut next = Some(head);
            while let Some(id) = next {
                if !seen.insert(id) {
                    return Err(payload::overflow_corruption(
                        Some(id),
                        CorruptionKind::InvalidFreelist,
                    ));
                }
                let pin = self.page_cache.fetch_page(id)?;
                next = page::format::read_optional_u64(pin.read()?.page(), 0);
                drop(pin);
                self.page_cache.free_page(self.txn_id, id)?;
            }
        }
        Ok(())
    }

    /// Reclaim every tree and overflow page, following children, not siblings.
    pub(crate) fn destroy(&self) -> StorageResult<()> {
        let mut pending = vec![self.root_page_id()];
        let mut pages = Vec::new();
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                return Err(payload::cell_corruption(id, CorruptionKind::InvalidFreelist));
            }
            let pin = self.page_cache.fetch_page(id)?;
            let page = pin.read()?;
            if read_page_kind(page.page(), id)? == PageKind::RawInterior {
                let interior = page.open::<Interior>()?;
                pending.push(interior.rightmost_child());
                for slot in 0..interior.slot_count() {
                    pending.push(interior.cell_payload_parts(slot)?.0);
                }
            }
            pages.push(id);
        }
        for id in pages.into_iter().rev() {
            self.free_tree_page(id)?;
        }
        self.mark_tree_mutated();
        Ok(())
    }

    pub(super) fn free_tree_page(&self, id: PageId) -> StorageResult<()> {
        self.free_overflow(self.overflow_heads(id)?)?;
        self.page_cache.free_page(self.txn_id, id)?;
        Ok(())
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::storage::btree::test_support::cursor;

    #[test]
    fn destroying_a_multilevel_tree_retires_exactly_its_tree_and_overflow_pages() {
        use crate::storage::{
            engine::Storage, log_manager::read_recovery_log, page_allocator::replay_lifecycle,
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        let storage = Storage::open_or_create(file.path()).unwrap();
        for _ in 0..3 {
            storage.create_tree().unwrap();
        } // protected catalog roots
        let txn = storage.begin_transaction().unwrap();
        let mut tree = storage.transaction_create_tree(txn).unwrap();
        for id in 0..32 {
            let key = format!("{id:02}{}", "k".repeat(5000));
            tree.insert(key.as_bytes(), &[83; 5000]).unwrap();
        }
        tree.update(format!("00{}", "k".repeat(5000)).as_bytes(), &[84; 5000]).unwrap();
        tree.delete(format!("01{}", "k".repeat(5000)).as_bytes()).unwrap();
        storage.commit_transaction(txn).unwrap();
        storage.flush().unwrap();
        assert!(crate::storage::btree::test_support::height(&tree) > 1);
        let count = std::fs::metadata(file.path()).unwrap().len() / PAGE_SIZE as u64;
        let txn = storage.begin_transaction().unwrap();
        storage.transaction_tree_cursor(txn, tree.root_page_id()).unwrap().destroy().unwrap();
        storage.commit_transaction(txn).unwrap();
        let scan = read_recovery_log(file.path()).unwrap();
        let allocator = replay_lifecycle(&scan.records).unwrap().unwrap();
        assert_eq!(allocator.free, (4..count).collect());
        assert_eq!(storage.create_tree().unwrap().root_page_id(), 4);
    }

    #[test]
    fn reclamation_rejects_cyclic_or_shared_overflow_chains() {
        for cycle in [false, true] {
            let cursor = cursor(8);
            let (id, pin) = cursor.page_cache.new_page(None).unwrap();
            page::format::write_u64(
                pin.write(None).unwrap().page_mut(),
                0,
                if cycle { id } else { NO_OVERFLOW_PAGE_ID },
            );
            drop(pin);
            let heads = if cycle { vec![id] } else { vec![id, id] };
            assert!(matches!(
                cursor.free_overflow(heads),
                Err(StorageError::Corruption(CorruptionError {
                    kind: CorruptionKind::InvalidFreelist,
                    ..
                }))
            ));
        }
    }

    #[test]
    fn destruction_rejects_child_cycles_before_retiring_tree_pages() {
        let cursor = cursor(8);
        let root = cursor.root_page_id();
        let pin = cursor.page_cache.fetch_page(root).unwrap();
        {
            let mut guard = pin.write(None).unwrap();
            let mut interior = RawInterior::<Write<'_>>::initialize(guard.page_mut());
            interior.set_rightmost_child(root);
        }
        drop(pin);
        assert!(matches!(
            cursor.destroy(),
            Err(StorageError::Corruption(CorruptionError {
                kind: CorruptionKind::InvalidFreelist,
                ..
            }))
        ));
    }
}
