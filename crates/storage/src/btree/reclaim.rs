//! Reclaim pages only after removing their owning links. Physical transactions
//! retain the bytes and reserve freed pages until the outcome is durable.

use super::root::read_page_kind;
use super::{
    Interior, Leaf, PAGE_SIZE, PageCache, PageId, PageKind, RawInterior, RawLeaf, Read,
    StorageResult, TreeCursor, TxnId, page,
};
use std::collections::HashSet;

pub(super) fn overflow_roots(
    bytes: &[u8; PAGE_SIZE],
    page_id: PageId,
) -> StorageResult<Vec<PageId>> {
    let mut roots = Vec::new();
    if bytes.iter().all(|byte| *byte == 0) {
        return Ok(roots);
    }
    match read_page_kind(bytes, page_id)? {
        PageKind::RawLeaf => {
            let leaf = RawLeaf::<Read<'_>>::open(bytes)?;
            for slot in 0..leaf.slot_count() {
                if let Some(id) = leaf.cell_payload_parts(slot)?.2 {
                    roots.push(id);
                }
            }
        }
        PageKind::RawInterior => {
            let interior = RawInterior::<Read<'_>>::open(bytes)?;
            for slot in 0..interior.slot_count() {
                if let Some(id) = interior.cell_payload_parts(slot)?.2 {
                    roots.push(id);
                }
            }
        }
    }
    Ok(roots)
}

pub(super) fn reclaim_chains(
    cache: &PageCache,
    txn_id: Option<TxnId>,
    roots: Vec<PageId>,
) -> StorageResult<()> {
    let mut visited = HashSet::new();
    for root in roots {
        let mut next = Some(root);
        while let Some(id) = next {
            if !visited.insert(id) {
                return Err(super::payload::cell_corruption(
                    id,
                    crate::error::CorruptionKind::InvalidPageGraph,
                ));
            }
            let pin = cache.fetch_page(id)?;
            next = page::format::read_optional_u64(pin.read()?.page(), 0);
            drop(pin);
            cache.dealloc_page(txn_id, id)?;
        }
    }
    Ok(())
}

impl TreeCursor {
    pub(super) fn reclaim_page_overflow(&self, page_id: PageId) -> StorageResult<()> {
        let pin = self.page_cache.fetch_page(page_id)?;
        let roots = overflow_roots(pin.read()?.page(), page_id)?;
        drop(pin);
        reclaim_chains(&self.page_cache, self.txn_id, roots)
    }
}

/// Enumerate ownership edges first, rejecting cycles and shared pages before
/// changing allocation state. Sibling links are navigation, not ownership.
pub fn destroy_tree(cache: &PageCache, txn_id: TxnId, root: PageId) -> StorageResult<()> {
    let mut pending = vec![(root, false)];
    let mut visited = HashSet::new();
    let mut pages = Vec::new();
    while let Some((id, overflow)) = pending.pop() {
        if !visited.insert(id) {
            return Err(super::payload::cell_corruption(
                id,
                crate::error::CorruptionKind::InvalidPageGraph,
            ));
        }
        let pin = cache.fetch_page(id)?;
        let bytes = pin.read()?;
        if overflow {
            if let Some(next) = page::format::read_optional_u64(bytes.page(), 0) {
                pending.push((next, true));
            }
        } else {
            page::validate_btree_page(bytes.page())?;
            pending.extend(overflow_roots(bytes.page(), id)?.into_iter().map(|id| (id, true)));
            match read_page_kind(bytes.page(), id)? {
                PageKind::RawLeaf => {
                    let _ = bytes.open::<Leaf>()?;
                }
                PageKind::RawInterior => {
                    let interior = bytes.open::<Interior>()?;
                    for slot in 0..interior.slot_count() {
                        pending.push((interior.cell_payload_parts(slot)?.0, false));
                    }
                    pending.push((interior.rightmost_child(), false));
                }
            }
        }
        pages.push(id);
    }
    cache.tree_mutation_epoch(root)?.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    for id in pages.into_iter().rev() {
        cache.dealloc_page(Some(txn_id), id)?;
    }
    Ok(())
}
