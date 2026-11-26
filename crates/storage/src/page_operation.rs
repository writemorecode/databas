//! Page-local redo operations. Leaf operations replay the same slot mutation
//! as normal execution, without a tree search or an overflow allocation.

use crate::log_manager::LogManagerError;
use crate::page::{PageError, PageResult, RawLeaf, Write, format};
use crate::page_patch::PagePatch;
use crate::{PAGE_SIZE, PageId, SlotId, error::StorageResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageOperation {
    Patch(PagePatch),
    Leaf(LeafOperation),
}

impl PageOperation {
    pub fn apply(&self, bytes: &mut [u8; PAGE_SIZE]) -> StorageResult<()> {
        match self {
            Self::Patch(patch) => patch.apply(bytes).map_err(LogManagerError::from)?,
            Self::Leaf(operation) => {
                // Replay may read a baseline not observed by the live cache.
                crate::page::validate_btree_page(bytes)?;
                operation.apply(bytes)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeafOperation {
    Insert { slot: SlotId, cell: LeafPayload },
    Replace { slot: SlotId, cell: LeafPayload },
    Delete { slot: SlotId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafPayload {
    pub key_len: u16,
    pub value_len: u16,
    pub overflow: Option<PageId>,
    pub inline: Box<[u8]>,
}

impl LeafPayload {
    pub fn new(
        key_len: usize,
        value_len: usize,
        overflow: Option<PageId>,
        inline: &[u8],
    ) -> PageResult<Self> {
        let bad_length = || PageError::CellTooLarge {
            len: key_len.saturating_add(value_len),
            max: u16::MAX as usize,
        };
        let key_len = u16::try_from(key_len).map_err(|_out_of_range| bad_length())?;
        let value_len = u16::try_from(value_len).map_err(|_out_of_range| bad_length())?;
        if Self::inline_len(key_len, value_len, overflow) != Some(inline.len()) {
            return Err(bad_length());
        }
        Ok(Self { key_len, value_len, overflow, inline: inline.into() })
    }

    pub fn inline_len(key_len: u16, value_len: u16, overflow: Option<PageId>) -> Option<usize> {
        if matches!(overflow, Some(0 | PageId::MAX)) {
            return None;
        }
        let len =
            format::inline_payload_len(usize::from(key_len.checked_add(value_len)?), overflow)?;
        (len <= format::MAX_INLINE_OVERFLOW_PAYLOAD_BYTES).then_some(len)
    }
}

impl LeafOperation {
    pub fn apply(&self, bytes: &mut [u8; PAGE_SIZE]) -> PageResult<()> {
        let actual = bytes[format::KIND_OFFSET];
        let expected = format::PageKind::RawLeaf;
        if actual != expected as u8 {
            return Err(PageError::InvalidPageKind { expected, actual });
        }
        let actual = bytes[format::VERSION_OFFSET];
        if actual != format::FORMAT_VERSION {
            return Err(PageError::InvalidPageVersion { expected: format::FORMAT_VERSION, actual });
        }
        let mut leaf = RawLeaf::<Write<'_>>::open(bytes)?;
        match self {
            Self::Insert { slot, cell } => leaf.insert_payload_at(
                *slot,
                cell.key_len.into(),
                cell.value_len.into(),
                cell.overflow,
                &cell.inline,
            )?,
            Self::Replace { slot, cell } => leaf.update_payload_at(
                *slot,
                cell.key_len.into(),
                cell.value_len.into(),
                cell.overflow,
                &cell.inline,
            )?,
            Self::Delete { slot } => leaf.delete_at(*slot)?,
        };
        Ok(())
    }
}
