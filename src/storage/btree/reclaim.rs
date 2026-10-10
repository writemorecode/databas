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

    pub(super) fn free_tree_page(&self, id: PageId) -> StorageResult<()> {
        self.free_overflow(self.overflow_heads(id)?)?;
        self.page_cache.free_page(self.txn_id, id)?;
        Ok(())
    }
}
