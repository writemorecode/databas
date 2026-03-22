use crate::types::PAGE_SIZE;

pub const FORMAT_VERSION: u8 = 1;
pub const RESERVED_FOOTER_SIZE: usize = 4;
pub const USABLE_SPACE_END: usize = PAGE_SIZE - RESERVED_FOOTER_SIZE;
pub const SLOT_ENTRY_SIZE: usize = 2;
pub const CELL_LENGTH_SIZE: usize = 2;

pub const KIND_OFFSET: usize = 0;
pub const VERSION_OFFSET: usize = 1;
pub const SLOT_COUNT_OFFSET: usize = 2;
pub const CONTENT_START_OFFSET: usize = 4;
pub const SHARED_HEADER_SIZE: usize = 6;
pub const RIGHTMOST_CHILD_OFFSET: usize = SHARED_HEADER_SIZE;
pub const LEAF_HEADER_SIZE: usize = SHARED_HEADER_SIZE;
pub const INTERIOR_HEADER_SIZE: usize = SHARED_HEADER_SIZE + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageKind {
    Leaf = 1,
    Interior = 2,
}

impl PageKind {
    pub fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::Leaf),
            2 => Some(Self::Interior),
            _ => None,
        }
    }

    pub const fn header_size(self) -> usize {
        match self {
            Self::Leaf => LEAF_HEADER_SIZE,
            Self::Interior => INTERIOR_HEADER_SIZE,
        }
    }
}

pub const fn usable_space_end() -> usize {
    USABLE_SPACE_END
}

pub const fn usable_space_len() -> usize {
    USABLE_SPACE_END
}

pub const fn max_slot_count(kind: PageKind) -> usize {
    (usable_space_len() - kind.header_size()) / SLOT_ENTRY_SIZE
}

pub fn read_u16(bytes: &[u8; PAGE_SIZE], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

pub fn write_u16(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub fn read_u64(bytes: &[u8; PAGE_SIZE], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice has fixed width"))
}

pub fn write_u64(bytes: &mut [u8; PAGE_SIZE], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub const fn slot_entry_offset(header_size: usize, slot_index: u16) -> usize {
    header_size + (slot_index as usize * SLOT_ENTRY_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usable_space_excludes_reserved_footer() {
        assert_eq!(usable_space_end(), PAGE_SIZE - RESERVED_FOOTER_SIZE);
        assert_eq!(usable_space_len(), PAGE_SIZE - RESERVED_FOOTER_SIZE);
    }

    #[test]
    fn page_kind_helpers_match_layout() {
        assert_eq!(PageKind::from_raw(1), Some(PageKind::Leaf));
        assert_eq!(PageKind::from_raw(2), Some(PageKind::Interior));
        assert_eq!(PageKind::from_raw(0), None);
        assert_eq!(PageKind::Leaf.header_size(), LEAF_HEADER_SIZE);
        assert_eq!(PageKind::Interior.header_size(), INTERIOR_HEADER_SIZE);
    }

    #[test]
    fn max_slot_count_uses_kind_specific_header_size() {
        assert!(max_slot_count(PageKind::Leaf) > max_slot_count(PageKind::Interior));
    }
}
