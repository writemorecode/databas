//! Low-level constants and helpers for the on-page binary format.
//!
//! The page layout consists of a small header, a slot directory that grows
//! upward from the header, a packed cell-content region that grows downward
//! from the end of usable space, and a zeroed reserved footer.

use crate::{PAGE_SIZE, SlotId};

/// Current on-disk page format version.
pub const FORMAT_VERSION: u8 = 3;
/// Number of bytes reserved at the end of every page.
pub const RESERVED_FOOTER_SIZE: usize = 4;
/// Exclusive end offset of the usable region within a page buffer.
pub const USABLE_SPACE_END: usize = PAGE_SIZE - RESERVED_FOOTER_SIZE;
/// Width in bytes of a single slot directory entry.
pub const SLOT_ENTRY_SIZE: usize = 2;
/// Width in bytes of the length prefix at the start of every cell.
pub const CELL_LENGTH_SIZE: usize = 2;
/// Width in bytes of a freeblock header: next freeblock plus total span size.
pub const FREEBLOCK_HEADER_SIZE: usize = 4;
/// Maximum fragmented free bytes permitted on a page before defragmentation.
pub const MAX_FRAGMENTED_FREE_BYTES: u16 = 60;

/// Offset of the page-kind tag in the shared page header.
pub const KIND_OFFSET: usize = 0;
/// Offset of the page-format version in the shared page header.
pub const VERSION_OFFSET: usize = 1;
/// Offset of the encoded slot count in the shared page header.
pub const SLOT_COUNT_OFFSET: usize = 2;
/// Offset of the content-start pointer in the shared page header.
pub const CONTENT_START_OFFSET: usize = 4;
/// Offset of the first freeblock pointer in the shared page header.
pub const FIRST_FREEBLOCK_OFFSET: usize = 6;
/// Offset of the fragmented free byte count in the shared page header.
pub const FRAGMENTED_FREE_BYTES_OFFSET: usize = 8;
/// Offset of the previous-page sibling pointer in the shared page header.
pub const PREV_PAGE_ID_OFFSET: usize = 10;
/// Offset of the next-page sibling pointer in the shared page header.
pub const NEXT_PAGE_ID_OFFSET: usize = PREV_PAGE_ID_OFFSET + 8;
/// Number of bytes in the header shared by all page kinds.
pub const SHARED_HEADER_SIZE: usize = NEXT_PAGE_ID_OFFSET + 8;
/// Offset of the rightmost-child pointer in an interior-page header.
pub const RIGHTMOST_CHILD_OFFSET: usize = SHARED_HEADER_SIZE;
/// Total header size for a leaf page.
pub const LEAF_HEADER_SIZE: usize = SHARED_HEADER_SIZE;
/// Total header size for an interior page.
pub const INTERIOR_HEADER_SIZE: usize = SHARED_HEADER_SIZE + 8;

/// Structural node kind carried by a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    /// A leaf page.
    Leaf,
    /// An interior page.
    Interior,
}

impl NodeKind {
    /// Returns the total header size for this node kind.
    pub const fn header_size(self) -> usize {
        match self {
            Self::Leaf => LEAF_HEADER_SIZE,
            Self::Interior => INTERIOR_HEADER_SIZE,
        }
    }
}

/// Encoded page kind tag stored in the page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    /// A raw leaf page containing byte keys and byte values.
    RawLeaf = 1,
    /// A raw interior page containing separator byte keys and child pointers.
    RawInterior = 2,
}

impl PageKind {
    /// Decodes a raw page-kind tag.
    pub fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::RawLeaf),
            2 => Some(Self::RawInterior),
            _ => None,
        }
    }

    /// Returns the encoded page kind for a structural node kind.
    pub const fn from_node_kind(node_kind: NodeKind) -> Self {
        match node_kind {
            NodeKind::Leaf => Self::RawLeaf,
            NodeKind::Interior => Self::RawInterior,
        }
    }

    /// Returns the node kind encoded in this page kind.
    pub const fn node_kind(self) -> NodeKind {
        match self {
            Self::RawLeaf => NodeKind::Leaf,
            Self::RawInterior => NodeKind::Interior,
        }
    }

    /// Returns the total header size for this page kind.
    pub const fn header_size(self) -> usize {
        self.node_kind().header_size()
    }
}

/// Returns the exclusive end offset of the usable page region.
pub const fn usable_space_end() -> usize {
    USABLE_SPACE_END
}

/// Returns the total number of usable bytes in the page.
pub const fn usable_space_len() -> usize {
    USABLE_SPACE_END
}

/// Returns the maximum number of slot entries a page of `kind` can address.
pub const fn max_slot_count(kind: PageKind) -> usize {
    (usable_space_len() - kind.header_size()) / SLOT_ENTRY_SIZE
}

/// Reads a little-endian `u16` from `bytes` at `offset`.
pub fn read_u16(bytes: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

/// Writes a little-endian `u16` into `bytes` at `offset`.
pub fn write_u16(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

/// Reads a sentinel-encoded optional `u16` from `bytes` at `offset`.
pub fn read_optional_u16(bytes: &[u8; PAGE_SIZE], offset: usize) -> Option<u16> {
    match read_u16(bytes, offset) {
        u16::MAX => None,
        value => Some(value),
    }
}

/// Writes a sentinel-encoded optional `u16` into `bytes` at `offset`.
pub fn write_optional_u16(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: Option<u16>) {
    write_u16(bytes, offset, value.unwrap_or(u16::MAX));
}

/// Reads a little-endian `u64` from `bytes` at `offset`.
pub fn read_u64(bytes: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice has fixed width"))
}

/// Reads a sentinel-encoded optional `u64` from `bytes` at `offset`.
pub fn read_optional_u64(bytes: &[u8; PAGE_SIZE], offset: usize) -> Option<u64> {
    match read_u64(bytes, offset) {
        u64::MAX => None,
        value => Some(value),
    }
}

/// Writes a little-endian `u64` into `bytes` at `offset`.
pub fn write_u64(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

/// Writes a sentinel-encoded optional `u64` into `bytes` at `offset`.
pub fn write_optional_u64(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: Option<u64>) {
    write_u64(bytes, offset, value.unwrap_or(u64::MAX));
}

/// Returns the byte offset of `slot_index` within a slot directory.
pub const fn slot_entry_offset(header_size: usize, slot_index: SlotId) -> usize {
    header_size + (slot_index as usize * SLOT_ENTRY_SIZE)
}
