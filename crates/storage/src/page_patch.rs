//! Compact, checked byte runs within one page incarnation. These transitional
//! records leave the recovery trailer under the recovery layer's control.

use crate::PAGE_SIZE;
use crate::types::{PAGE_CONTENT_END, PAGE_CONTENT_END_U16};

const RUN_HEADER: usize = 4;
pub const MAX_PATCH_BYTES: usize = PAGE_CONTENT_END + RUN_HEADER;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagePatch(Box<[u8]>);

#[derive(Debug, thiserror::Error)]
#[error("invalid WAL page patch")]
pub struct InvalidPagePatch;

impl PagePatch {
    /// Generate matching redo and undo runs. Absorb gaps no larger than a run
    /// header, so each encoding costs at most one page body plus one header.
    pub fn between(before: &[u8; PAGE_SIZE], after: &[u8; PAGE_SIZE]) -> (Self, Self) {
        let mut redo = Vec::new();
        let mut undo = Vec::new();
        let mut changed = (0..PAGE_CONTENT_END_U16)
            .filter(|&i| before[usize::from(i)] != after[usize::from(i)])
            .peekable();
        while let Some(start) = changed.next() {
            let mut end = start + 1;
            while let Some(i) = changed.next_if(|&i| usize::from(i - end) <= RUN_HEADER) {
                end = i + 1;
            }
            let range = usize::from(start)..usize::from(end);
            for (output, page) in [(&mut redo, after), (&mut undo, before)] {
                output.extend_from_slice(&start.to_le_bytes());
                output.extend_from_slice(&(end - start).to_le_bytes());
                output.extend_from_slice(&page[range.clone()]);
            }
        }
        debug_assert!(Self::validate(&redo).is_ok());
        debug_assert!(Self::validate(&undo).is_ok());
        (Self(redo.into_boxed_slice()), Self(undo.into_boxed_slice()))
    }

    pub fn decode(bytes: &[u8], collect: bool) -> Result<Option<Self>, InvalidPagePatch> {
        Self::validate(bytes)?;
        Ok(collect.then(|| Self(Box::from(bytes))))
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn apply(&self, page: &mut [u8; PAGE_SIZE]) -> Result<(), InvalidPagePatch> {
        Self::visit(&self.0, |start, bytes| {
            page[start..start + bytes.len()].copy_from_slice(bytes);
        })
    }

    pub fn matches(&self, page: &[u8; PAGE_SIZE]) -> Result<bool, InvalidPagePatch> {
        let mut matches = true;
        Self::visit(&self.0, |start, bytes| {
            matches &= page[start..start + bytes.len()] == *bytes;
        })?;
        Ok(matches)
    }

    fn validate(bytes: &[u8]) -> Result<(), InvalidPagePatch> {
        if bytes.is_empty() || bytes.len() > MAX_PATCH_BYTES {
            return Err(InvalidPagePatch);
        }
        Self::visit(bytes, |_, _| {})
    }

    fn visit(mut bytes: &[u8], mut run: impl FnMut(usize, &[u8])) -> Result<(), InvalidPagePatch> {
        let mut previous_end = 0;
        while !bytes.is_empty() {
            let ([a, b, c, d], rest) =
                bytes.split_first_chunk::<RUN_HEADER>().ok_or(InvalidPagePatch)?;
            let start = usize::from(u16::from_le_bytes([*a, *b]));
            let len = usize::from(u16::from_le_bytes([*c, *d]));
            if len == 0 || start < previous_end || start + len > PAGE_CONTENT_END {
                return Err(InvalidPagePatch);
            }
            let (literal, tail) = rest.split_at_checked(len).ok_or(InvalidPagePatch)?;
            run(start, literal);
            previous_end = start + len;
            bytes = tail;
        }
        Ok(())
    }
}
