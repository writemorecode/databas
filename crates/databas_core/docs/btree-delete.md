# B+-tree Delete and Cursor Walkthrough

This document explains the raw byte-oriented B+-tree delete path implemented in
`src/btree.rs` and the page primitives under `src/page/`. The tree stores raw
key/value records in leaf pages and separator keys in interior pages.

The important design choice is that interior separator keys are the maximum key
reachable through the child to their left. Each interior page stores one extra
rightmost child pointer in the page header, so a page with `n` separator cells
has `n + 1` children.

## Data Structures Involved

`TreeCursor` owns the page cache handle, a shared root page id, and logical
cursor state:

```rust
pub enum CursorState {
    Page { page_id: PageId },
    Positioned { page_id: PageId, slot_index: u16 },
    Exhausted,
}

pub struct TreeCursor {
    page_cache: PageCache,
    root_page_id: Rc<Cell<PageId>>,
    state: CursorState,
}
```

During a delete descent, the cursor records the interior pages it traversed:

```rust
enum ChildSlotRef {
    Slot(u16),
    Rightmost,
}

struct PathFrame {
    page_id: PageId,
    child_ref: ChildSlotRef,
}
```

`PathFrame` is what lets delete repair the path after removing a record. It
identifies both the parent page and the exact child reference followed from that
parent.

## High-level Delete Flow

The public delete entry point is compact:

```rust
pub fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
    let (leaf_page_id, tree_path) = self.leaf_page_path_for_key(key)?;
    {
        let leaf_pin_guard = self.page_cache.fetch_page(leaf_page_id)?;
        let mut leaf_guard = leaf_pin_guard.write()?;
        let mut page = RawLeaf::<Write<'_>>::open(leaf_guard.page_mut())?;
        page.delete(key)?;
    }

    self.set_page_state(leaf_page_id);
    self.rebalance_after_leaf_delete(leaf_page_id, &tree_path)?;
    self.refresh_path_separators(&tree_path)?;
    self.shrink_root_if_empty()?;
    Ok(())
}
```

The operation has five phases:

1. Descend to the target leaf and capture the interior path.
2. Delete the cell from the leaf page.
3. Put the cursor in `Page` state, no longer on a concrete slot.
4. Rebalance underoccupied leaf and interior pages.
5. Refresh separator keys and shrink an empty interior root if possible.

If the key is missing, `RawLeaf::delete` returns `PageError::KeyNotFound`, which
is propagated as a storage error. No rebalance is attempted in that case.

## Step 1: Descend and Record the Path

`leaf_page_path_for_key` starts at the current root and repeatedly follows an
interior child until it reaches a leaf:

```rust
fn leaf_page_path_for_key(&self, key: &[u8]) -> StorageResult<(PageId, Vec<PathFrame>)> {
    let mut path = Vec::new();
    let mut page_id = self.root_page_id();

    loop {
        let pin = self.page_cache.fetch_page(page_id)?;
        let next = {
            let page = pin.read()?;
            match read_page_kind(page.page(), page_id)? {
                PageKind::RawLeaf => {
                    let _ = RawLeaf::<Read<'_>>::open(page.page())?;
                    return Ok((page_id, path));
                }
                PageKind::RawInterior => {
                    drop(page);
                    drop(pin);
                    match self.lower_bound_interior_slot(page_id, key)? {
                        BoundResult::At(slot_index) => {
                            let child_page_id =
                                self.read_interior_left_child(page_id, slot_index)?;
                            path.push(PathFrame {
                                page_id,
                                child_ref: ChildSlotRef::Slot(slot_index),
                            });
                            child_page_id
                        }
                        BoundResult::PastEnd => {
                            path.push(PathFrame {
                                page_id,
                                child_ref: ChildSlotRef::Rightmost,
                            });
                            ...
                            interior.rightmost_child()
                        }
                    }
                }
            }
        };
        page_id = next;
    }
}
```

The code uses lower-bound semantics on separator keys. If the key is less than
or equal to a separator, the matching child pointer is in that separator cell.
If the key is larger than all separators, the search follows the rightmost child
stored in the header.

The path is also used defensively after merges. A page on the original path may
have been removed from its parent, so later separator refresh only touches pages
that are still reachable.

## Step 2: Delete the Leaf Cell

The raw leaf page delete is local to one page:

```rust
pub fn delete(&mut self, key: &[u8]) -> PageResult<SlotId> {
    let slot_index = match self.search(key)? {
        SearchResult::Found(slot_index) => slot_index,
        SearchResult::InsertAt(_) => return Err(PageError::KeyNotFound),
    };

    let cell_offset = self.slot_offset(slot_index)?;
    let cell_len = self.cell_len(slot_index)?;
    self.remove_slot(slot_index)?;
    self.reclaim_space(cell_offset, cell_len)?;
    Ok(slot_index)
}
```

This removes the slot-directory entry and then returns the local cell bytes to
the page's free-space structures. It does not update ancestors directly and it
does not touch sibling pages.

`remove_slot` compacts the slot directory:

```rust
pub(crate) fn remove_slot(&mut self, slot_index: SlotId) -> PageResult<u16> {
    self.validate_slot_index(slot_index)?;
    let slot_count = self.slot_count();
    let header_size = N::KIND.header_size();
    let remove_at = format::slot_entry_offset(header_size, slot_index);
    let removed = format::read_u16(self.bytes(), remove_at);
    let tail_start = remove_at + format::SLOT_ENTRY_SIZE;
    let tail_end = format::slot_entry_offset(header_size, slot_count);

    self.bytes_mut().copy_within(tail_start..tail_end, remove_at);
    let last_slot = format::slot_entry_offset(header_size, slot_count - 1);
    self.bytes_mut()[last_slot..last_slot + format::SLOT_ENTRY_SIZE].fill(0);
    self.set_slot_count(slot_count - 1);
    Ok(removed)
}
```

`reclaim_space` then turns the removed cell body into either gap space,
fragmented bytes, or a freeblock:

```rust
pub(crate) fn reclaim_space(&mut self, cell_offset: u16, cell_len: usize) -> PageResult<()> {
    if self.slot_count() == 0 {
        self.reset_empty_page();
        return Ok(());
    }

    let reclaim_start = cell_offset as usize;
    if reclaim_start == self.content_start() as usize {
        self.set_content_start((reclaim_start + cell_len) as u16);
        self.absorb_freeblocks_into_gap()?;
        return Ok(());
    }

    let reclaim_end = reclaim_start + cell_len;
    ...
    let merged_with_previous = previous.filter(|freeblock| freeblock.end() == reclaim_start);
    let merged_with_next = next.filter(|freeblock| reclaim_end == freeblock.offset as usize);
    ...
}
```

The local page format has a slot directory growing upward and cell content
growing downward. If the deleted cell is at the content-start boundary, reclaim
can expand the contiguous gap. Otherwise, the deleted bytes join adjacent
freeblocks where possible. Very small spans become fragmented bytes.

## Step 3: Cursor State After Delete

Immediately after the raw cell delete, the cursor is intentionally moved to page
state:

```rust
self.set_page_state(leaf_page_id);
```

That means `current()` returns `None` after delete, even if the page still has
nearby records. Subsequent movement uses page-level scan behavior:

```rust
fn step_record(&mut self, direction: ScanDirection) -> StorageResult<Option<Record>> {
    match self.state {
        CursorState::Exhausted => Ok(None),
        CursorState::Page { page_id } => {
            let leaf_page_id = direction.descend_to_edge_leaf(self, page_id)?;
            self.edge_record_from_leaf(leaf_page_id, direction)
        }
        CursorState::Positioned { page_id, slot_index } => { ... }
    }
}
```

If rebalancing merges the deleted leaf into its left sibling, the rebalance code
updates the cursor page state to the surviving left page. If it merges with the
right sibling, the deleted leaf is the survivor and remains the cursor page. If
the root shrinks, `shrink_root_if_empty` moves the cursor to the new root child.

## Step 4: Detect Underoccupation

Rebalancing is skipped for a root leaf and for leaves that remain sufficiently
occupied:

```rust
fn rebalance_after_leaf_delete(
    &mut self,
    leaf_page_id: PageId,
    tree_path: &[PathFrame],
) -> StorageResult<()> {
    if tree_path.is_empty() {
        return Ok(());
    }
    if !self.leaf_page_underoccupied(leaf_page_id)? {
        return Ok(());
    }

    let mut depth = tree_path.len() - 1;
    let parent_page_id = tree_path[depth].page_id;
    if !self.rebalance_leaf_page(leaf_page_id, parent_page_id)? {
        return Ok(());
    }

    loop {
        let page_id = tree_path[depth].page_id;
        if page_id == self.root_page_id() {
            self.shrink_root_if_empty()?;
            return Ok(());
        }
        if !self.interior_page_underoccupied(page_id)? {
            return Ok(());
        }
        ...
    }
}
```

The page-level occupancy check is based on live slot bytes plus live cell bytes:

```rust
pub fn is_underoccupied(&self) -> PageResult<bool> {
    let header_size = N::KIND.header_size();
    let slot_bytes = self.slot_count() as usize * format::SLOT_ENTRY_SIZE;
    let occupied_variable_bytes = slot_bytes + self.live_cell_bytes()?;
    let usable_variable_bytes = USABLE_SPACE_END - header_size;
    Ok(occupied_variable_bytes * 2 < usable_variable_bytes)
}
```

So a page is underoccupied when it uses less than half of the usable variable
area. The root is special: it may be underoccupied without forcing a merge.

## Step 5: Leaf Redistribution and Merge

`rebalance_leaf_page` gets the deleted page's index in its parent, reads adjacent
siblings, and tries several repair strategies:

```rust
fn rebalance_leaf_page(
    &mut self,
    leaf_page_id: PageId,
    parent_page_id: PageId,
) -> StorageResult<bool> {
    let child_index = self.child_index_in_parent(parent_page_id, leaf_page_id)?;
    let parent_children = self.read_interior_child_page_ids(parent_page_id)?;

    if child_index > 0 {
        let left_page_id = parent_children[child_index - 1];
        let mut cells = self.read_leaf_cells(left_page_id)?;
        cells.extend(self.read_leaf_cells(leaf_page_id)?);
        if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
            self.redistribute_leaf_pair(left_page_id, leaf_page_id, &cells, split_index)?;
            return Ok(false);
        }
    }

    if child_index + 1 < parent_children.len() {
        let right_page_id = parent_children[child_index + 1];
        let mut cells = self.read_leaf_cells(leaf_page_id)?;
        cells.extend(self.read_leaf_cells(right_page_id)?);
        if let Some(split_index) = Self::choose_leaf_rebalance_split(&cells) {
            self.redistribute_leaf_pair(leaf_page_id, right_page_id, &cells, split_index)?;
            return Ok(false);
        }
    }

    ...
}
```

The strategy order is:

1. Try to redistribute with the left sibling while keeping both pages fit and
   not underoccupied.
2. Try the same with the right sibling.
3. If redistribution cannot satisfy occupancy, try to merge with the left
   sibling if the combined cells fit in one page.
4. Try to merge with the right sibling if the combined cells fit.
5. As a fallback, choose a split that merely fits both pages, even if one still
   remains underoccupied.

Redistribution rebuilds both pages from materialized cells and preserves the
outer sibling links:

```rust
fn redistribute_leaf_pair(
    &self,
    left_page_id: PageId,
    right_page_id: PageId,
    cells: &[LeafSplitCell],
    split_index: usize,
) -> StorageResult<()> {
    let (left_prev_page_id, _) = self.read_leaf_page_links(left_page_id)?;
    let (_, right_next_page_id) = self.read_leaf_page_links(right_page_id)?;
    self.rewrite_leaf_page(
        left_page_id,
        &cells[..split_index],
        left_prev_page_id,
        Some(right_page_id),
    )?;
    self.rewrite_leaf_page(
        right_page_id,
        &cells[split_index..],
        Some(left_page_id),
        right_next_page_id,
    )
}
```

A merge rewrites only the survivor, links it around the removed page, and fixes
the next page's `prev` pointer:

```rust
fn merge_leaf_pages(
    &self,
    survivor_page_id: PageId,
    removed_page_id: PageId,
    cells: &[LeafSplitCell],
) -> StorageResult<()> {
    let (survivor_prev_page_id, _) = self.read_leaf_page_links(survivor_page_id)?;
    let (_, removed_next_page_id) = self.read_leaf_page_links(removed_page_id)?;
    self.rewrite_leaf_page(
        survivor_page_id,
        cells,
        survivor_prev_page_id,
        removed_next_page_id,
    )?;
    if let Some(next_page_id) = removed_next_page_id {
        self.set_leaf_prev_page_id(next_page_id, Some(survivor_page_id))?;
    }
    Ok(())
}
```

After a merge, the parent no longer has a valid child pointer for the removed
page. `remove_child_from_parent` rebuilds the parent without that child:

```rust
fn remove_child_from_parent(
    &self,
    parent_page_id: PageId,
    child_page_id: PageId,
) -> StorageResult<()> {
    let mut children = self.read_interior_child_entries(parent_page_id)?;
    let child_index =
        children.iter().position(|child| child.page_id == child_page_id).ok_or(...)?;
    children.remove(child_index);
    let (prev_page_id, next_page_id) = self.read_interior_page_links(parent_page_id)?;
    self.rewrite_interior_page(parent_page_id, &children, prev_page_id, next_page_id)
}
```

The return value from `rebalance_leaf_page` tells the caller whether a merge
removed a child from the parent. Only that case may propagate underoccupation
upward.

## Step 6: Interior Redistribution and Merge

Interior page repair mirrors leaf repair, but it works with child entries rather
than leaf records. A `ChildEntry` is a child page id plus the maximum key
reachable through that child:

```rust
struct ChildEntry {
    page_id: PageId,
    max_key: Option<Vec<u8>>,
}
```

Child entries are recomputed from the current tree:

```rust
fn read_interior_child_entries(&self, page_id: PageId) -> StorageResult<Vec<ChildEntry>> {
    let child_page_ids = self.read_interior_child_page_ids(page_id)?;
    let mut children = Vec::with_capacity(child_page_ids.len());
    for child_page_id in child_page_ids {
        children.push(ChildEntry {
            page_id: child_page_id,
            max_key: self.subtree_max_key(child_page_id)?,
        });
    }
    Ok(children)
}
```

Interior pages are rebuilt from those child entries. All children except the
last become separator cells; the last child becomes the header's rightmost
child:

```rust
fn rewrite_interior_page(
    &self,
    page_id: PageId,
    children: &[ChildEntry],
    prev_page_id: Option<PageId>,
    next_page_id: Option<PageId>,
) -> StorageResult<()> {
    let rightmost_child = children.last().ok_or(...)?;
    let mut used_bytes = PageKind::RawInterior.header_size()
        + (children.len() - 1) * page::format::SLOT_ENTRY_SIZE;
    for child in &children[..children.len() - 1] {
        let key = child
            .max_key
            .as_deref()
            .ok_or_else(|| Self::missing_child_max_key_error(page_id))?;
        used_bytes += self.interior_cell_local_size(key)?;
    }
    if used_bytes > page::format::USABLE_SPACE_END {
        return Err(PageError::PageFull { ... }.into());
    }

    ...
    let mut interior = RawInterior::<Write<'_>>::initialize_with_rightmost(
        &mut page_image,
        rightmost_child.page_id,
    );
    ...
}
```

Interior rebalancing has the same strategy order as leaf rebalancing:

```rust
fn rebalance_interior_page(
    &self,
    interior_page_id: PageId,
    parent_page_id: PageId,
) -> StorageResult<bool> {
    let child_index = self.child_index_in_parent(parent_page_id, interior_page_id)?;
    let parent_children = self.read_interior_child_page_ids(parent_page_id)?;

    if child_index > 0 {
        let left_page_id = parent_children[child_index - 1];
        let mut children = self.read_interior_child_entries(left_page_id)?;
        children.extend(self.read_interior_child_entries(interior_page_id)?);
        if let Some(split_index) = Self::choose_interior_rebalance_split(&children) {
            self.redistribute_interior_pair(
                left_page_id,
                interior_page_id,
                &children,
                split_index,
            )?;
            return Ok(false);
        }
    }

    ...
}
```

Merging interior pages rewrites the survivor, updates the sibling link of the
next interior page, and asks the parent to drop the removed child:

```rust
fn merge_interior_pages(
    &self,
    survivor_page_id: PageId,
    removed_page_id: PageId,
    children: &[ChildEntry],
) -> StorageResult<()> {
    let (survivor_prev_page_id, _) = self.read_interior_page_links(survivor_page_id)?;
    let (_, removed_next_page_id) = self.read_interior_page_links(removed_page_id)?;
    self.rewrite_interior_page(
        survivor_page_id,
        children,
        survivor_prev_page_id,
        removed_next_page_id,
    )?;
    if let Some(next_page_id) = removed_next_page_id {
        self.set_interior_prev_page_id(next_page_id, Some(survivor_page_id))?;
    }
    Ok(())
}
```

Interior sibling links are maintained even though normal point lookup descends
from the root. They are still useful structural metadata and keep each tree
level consistently linked.

## Step 7: Separator Refresh

Even if a merge does not propagate upward, deleting the largest key in a child
can invalidate an ancestor separator. `refresh_path_separators` repairs this
from the bottom of the original path upward:

```rust
fn refresh_path_separators(&self, tree_path: &[PathFrame]) -> StorageResult<()> {
    if tree_path.is_empty() {
        return Ok(());
    }

    let mut reachable = Vec::with_capacity(tree_path.len());
    for (depth, frame) in tree_path.iter().enumerate() {
        let is_reachable = if depth == 0 {
            frame.page_id == self.root_page_id()
        } else {
            reachable[depth - 1]
                && self.interior_page_has_child(tree_path[depth - 1].page_id, frame.page_id)?
        };
        reachable.push(is_reachable);
    }

    for (frame, is_reachable) in tree_path.iter().zip(reachable).rev() {
        if is_reachable {
            self.refresh_interior_page_separators(frame.page_id)?;
        }
    }

    Ok(())
}
```

The reachability check matters because a page in the old path may have been
merged away. The refresh only rewrites pages still attached to the current root.

`refresh_interior_page_separators` recomputes each child max and rewrites the
page only if something actually changed:

```rust
fn refresh_interior_page_separators(&self, page_id: PageId) -> StorageResult<()> {
    let children = self.read_interior_child_entries(page_id)?;
    if self.interior_page_matches_children(page_id, &children)? {
        return Ok(());
    }

    let (prev_page_id, next_page_id) = self.read_interior_page_links(page_id)?;
    self.rewrite_interior_page(page_id, &children, prev_page_id, next_page_id)
}
```

## Step 8: Root Shrinking

An interior root with no separator cells has exactly one child: its rightmost
child pointer. Delete collapses that root:

```rust
fn shrink_root_if_empty(&mut self) -> StorageResult<()> {
    let root_page_id = self.root_page_id();
    let pin = self.page_cache.fetch_page(root_page_id)?;
    let child_page_id = {
        let page = pin.read()?;
        match read_page_kind(page.page(), root_page_id)? {
            PageKind::RawLeaf => return Ok(()),
            PageKind::RawInterior => {
                let interior = RawInterior::<Read<'_>>::open(page.page())?;
                if interior.slot_count() > 0 {
                    return Ok(());
                }
                interior.rightmost_child()
            }
        }
    };
    self.root_page_id.set(child_page_id);
    self.set_page_state(child_page_id);
    Ok(())
}
```

The old root page is not physically freed. The cursor's shared root id is simply
updated to the sole child.

## Overflow Payloads

Large payloads are split between the cell body and overflow pages. The on-page
format reserves an overflow pointer in every raw leaf and interior cell:

```rust
pub const MIN_INLINE_OVERFLOW_PAYLOAD_BYTES: usize = 64;
pub const MAX_INLINE_OVERFLOW_PAYLOAD_BYTES: usize = 512;
pub const OVERFLOW_NEXT_PAGE_ID_SIZE: usize = 8;
```

When inserting or rewriting a cell, the tree keeps at most 512 bytes inline and
writes the remaining bytes to a newly allocated overflow chain:

```rust
fn payload_storage_parts(&self, payload: &[u8]) -> StorageResult<(Option<PageId>, Vec<u8>)> {
    self.checked_payload_len(payload.len())?;
    if !payload_uses_overflow(payload.len()) {
        return Ok((None, payload.to_vec()));
    }

    let inline_payload = payload[..MAX_INLINE_OVERFLOW_PAYLOAD_BYTES].to_vec();
    let first_overflow_page_id =
        overflow::write_chain(&self.page_cache, &payload[MAX_INLINE_OVERFLOW_PAYLOAD_BYTES..])?
            .ok_or_else(|| {
                cell_corruption(self.root_page_id(), CorruptionKind::CellLengthOutOfBounds)
            })?;
    Ok((Some(first_overflow_page_id), inline_payload))
}
```

Overflow pages form a singly linked list:

```rust
pub(crate) fn write_chain(page_cache: &PageCache, payload: &[u8]) -> StorageResult<Option<PageId>> {
    if payload.is_empty() {
        return Ok(None);
    }

    let mut first_page_id = None;
    let mut previous_page_id = None;

    for chunk in payload.chunks(OVERFLOW_PAYLOAD_SIZE) {
        let (page_id, pin) = page_cache.new_page()?;
        {
            let mut page = pin.write()?;
            page.page_mut().fill(0);
            write_next_page_id(page.page_mut(), None);
            page.page_mut()[OVERFLOW_NEXT_PAGE_ID_SIZE..OVERFLOW_NEXT_PAGE_ID_SIZE + chunk.len()]
                .copy_from_slice(chunk);
        }
        ...
    }

    Ok(first_page_id)
}
```

Delete itself does not read or free overflow pages when it removes the target
cell. It only reclaims the local inline cell bytes. However, rebalancing and
separator refresh materialize cells before rebuilding pages:

```rust
fn read_leaf_cells(&self, page_id: PageId) -> StorageResult<Vec<LeafSplitCell>> {
    let slot_count = self.raw_leaf_slot_count(page_id)?;
    let mut cells = Vec::with_capacity(slot_count as usize);
    for slot_index in 0..slot_count {
        let (key, value) = read_leaf_cell(&self.page_cache, page_id, slot_index)?;
        cells.push(LeafSplitCell { key, value });
    }
    Ok(cells)
}
```

`read_leaf_cell` and `read_interior_cell` call `materialize_payload`, which reads
the overflow chain if one exists:

```rust
fn materialize_payload(
    page_cache: &PageCache,
    page_id: PageId,
    inline_payload: Vec<u8>,
    first_overflow_page_id: Option<PageId>,
    payload_len: usize,
) -> StorageResult<Vec<u8>> {
    ...
    match first_overflow_page_id {
        Some(first_overflow_page_id) => {
            let remaining = payload_len - payload.len();
            payload.extend(overflow::read_chain(page_cache, first_overflow_page_id, remaining)?);
        }
        ...
    }
    ...
}
```

When rebuilt pages are written, large surviving cells get new overflow chains.
The current storage layer has `new_page` but no free-page or free-overflow-chain
operation. As a result:

- Deleting a record with overflow makes its old overflow chain unreachable.
- Rewriting pages during redistribution, merge, or separator refresh can also
  leave old overflow chains unreachable and allocate replacement chains.
- The removed leaf/interior page from a merge is detached from the tree but not
  returned to a free list.

This is correct for lookup semantics but means delete is not space-reclaiming at
the file level yet.

## Edge Cases

### Deleting from a root leaf

If the tree has height one, `tree_path` is empty. The raw cell is removed and
the page's local free space is reclaimed, but no sibling rebalance is attempted.
If the last record is deleted, the root remains an empty leaf. `seek_to_first`
then returns `false` and moves the cursor to `Exhausted`.

### Deleting the largest key in a child

The leaf may still be occupied enough to avoid merging, but an ancestor
separator might now be stale. `refresh_path_separators` recomputes child maxima
and rewrites affected reachable interior pages.

### Merge removes a page on the original path

After a merge, a path frame may refer to a detached page. Separator refresh first
checks whether each path page is still reachable from the current root before
rewriting it.

### Parent underflows after a child merge

`rebalance_leaf_page` and `rebalance_interior_page` return `true` when they
remove a child from the parent. That is the signal for
`rebalance_after_leaf_delete` to move one level upward and repair the parent if
it is now underoccupied.

### Empty interior root

If propagation removes the final separator from the root, the root is replaced
by its sole child. This can cascade only one level per call to
`shrink_root_if_empty`, but delete calls it both during upward rebalance and
again after separator refresh.

### Oversized keys and values

Leaf values and interior separator keys can overflow. Comparisons often compare
the inline prefix first and only materialize overflow suffixes when necessary.
During delete repair, page rebuilds materialize complete logical keys and values
before choosing split points and writing new cells.

### Page rewrite failure

Interior rewrites are staged into a local `page_image` and assigned to the cache
page only after preparation succeeds. This protects the page bytes from partial
rewrites. The current overflow allocation path is still append-only, so overflow
pages allocated during a failed rewrite attempt are not reclaimed.

## Mental Model

The delete implementation is best read as a rebuild-based algorithm:

1. Remove the exact leaf cell locally.
2. If occupancy is still acceptable, only refresh separators.
3. If a page is underoccupied, materialize its cells or child entries together
   with a sibling.
4. Prefer redistribution that leaves both pages healthy.
5. If possible, merge into one page and remove the detached child from the
   parent.
6. Propagate only when a merge reduced the parent child count.
7. Recompute separators from subtree max keys rather than patching individual
   separator bytes in place.

This keeps the balancing logic simple and robust at the cost of extra
materialization and, until free-list support exists, append-only overflow/page
allocation during rewrites.
