//! Recovery metadata is independent of page contents and survives a rebuild.

use crate::page::{RawInterior, RawLeaf, Write, format};
use crate::types::{PAGE_CHECKSUM_OFFSET, PAGE_CONTENT_END};
use crate::{Lsn, PAGE_SIZE};

/// Fixed trailer shared by data pages, allocation bitmaps and page zero.
/// Data-page incarnations are their PageInit LSN. Zero is reserved for
/// fixed-address metadata pages and uninitialized file space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRecovery {
    pub incarnation: Lsn,
    pub lsn: Lsn,
}

impl PageRecovery {
    pub fn read(bytes: &[u8; PAGE_SIZE]) -> Self {
        Self {
            incarnation: format::read_u64(bytes, PAGE_CONTENT_END),
            lsn: format::read_u64(bytes, PAGE_CHECKSUM_OFFSET - size_of::<Lsn>()),
        }
    }

    pub fn write(self, bytes: &mut [u8; PAGE_SIZE]) {
        format::write_u64(bytes, PAGE_CONTENT_END, self.incarnation);
        format::write_u64(bytes, PAGE_CHECKSUM_OFFSET - size_of::<Lsn>(), self.lsn);
    }

    pub fn stamp_lsn(bytes: &mut [u8; PAGE_SIZE], lsn: Lsn) {
        Self { lsn, ..Self::read(bytes) }.write(bytes);
    }
}

/// Format of a new data page. Its initial contents are derived, not logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PageInitKind {
    Leaf = 1,
    Interior = 2,
    Overflow = 3,
}

impl PageInitKind {
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Leaf),
            2 => Some(Self::Interior),
            3 => Some(Self::Overflow),
            _ => None,
        }
    }

    pub fn image(self, lsn: Lsn) -> [u8; PAGE_SIZE] {
        let mut bytes = [0; PAGE_SIZE];
        match self {
            Self::Leaf => {
                let _ = RawLeaf::<Write<'_>>::initialize(&mut bytes);
            }
            Self::Interior => {
                let _ = RawInterior::<Write<'_>>::initialize(&mut bytes);
            }
            Self::Overflow => crate::overflow::initialize(&mut bytes),
        }
        PageRecovery { incarnation: lsn, lsn }.write(&mut bytes);
        bytes
    }
}

impl From<format::PageKind> for PageInitKind {
    fn from(kind: format::PageKind) -> Self {
        match kind {
            format::PageKind::RawLeaf => Self::Leaf,
            format::PageKind::RawInterior => Self::Interior,
        }
    }
}
