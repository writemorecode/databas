use proptest::prelude::*;

use super::super::{PageError, RawInterior, RawLeaf, Read, Write, format, validate_btree_page};
use crate::PAGE_SIZE;

struct FragmentedPage {
    bytes: [u8; PAGE_SIZE],
    header_size: usize,
    first_cell: u16,
    freeblock: u16,
    combined_len: usize,
}

fn fragmented_page(interior: bool, payload: &[u8]) -> Result<FragmentedPage, PageError> {
    let mut bytes = [0; PAGE_SIZE];
    let (header_size, first_cell, freeblock, combined_len) = if interior {
        let mut page = RawInterior::<Write<'_>>::initialize_with_rightmost(&mut bytes, 9);
        for slot in 0..3 {
            page.insert_payload_at(slot, 7, payload.len(), None, payload)?;
        }
        let positions = (page.slot_offset(0)?, page.slot_offset(1)?);
        let combined_len = page.cell_len(0)? + page.cell_len(1)?;
        let len = page.cell_len(1)?;
        let offset = page.remove_slot(1)?;
        page.reclaim_space(offset, len)?;
        (format::INTERIOR_HEADER_SIZE, positions.0, positions.1, combined_len)
    } else {
        let mut page = RawLeaf::<Write<'_>>::initialize(&mut bytes);
        for key in 0u8..3 {
            let inline = [&[key][..], payload].concat();
            page.insert_payload_at(u16::from(key), 1, payload.len(), None, &inline)?;
        }
        let positions = (page.slot_offset(0)?, page.slot_offset(1)?);
        let combined_len = page.cell_len(0)? + page.cell_len(1)?;
        page.delete(&[1])?;
        (format::LEAF_HEADER_SIZE, positions.0, positions.1, combined_len)
    };
    validate_btree_page(&bytes)?;
    Ok(FragmentedPage { bytes, header_size, first_cell, freeblock, combined_len })
}

proptest! {
    #[test]
    fn cyclic_freeblock_chains_are_rejected(
        interior in any::<bool>(),
        payload in prop::collection::vec(any::<u8>(), 4..=64),
    ) {
        let mut page = fragmented_page(interior, &payload)?;
        format::write_optional_u16(&mut page.bytes, usize::from(page.freeblock), Some(page.freeblock));
        prop_assert!(validate_btree_page(&page.bytes).is_err(), "accepted a cyclic freeblock chain");
    }

    #[test]
    fn overlapping_cells_and_freeblocks_are_rejected(
        interior in any::<bool>(),
        payload in prop::collection::vec(any::<u8>(), 4..=64),
        damage in prop::sample::select(vec![0u8, 2]),
    ) {
        let mut page = fragmented_page(interior, &payload)?;
        if damage == 0 {
            format::write_u16(&mut page.bytes, page.header_size + format::SLOT_ENTRY_SIZE, page.first_cell);
        } else {
            format::write_u16(&mut page.bytes, usize::from(page.freeblock) + 2, u16::try_from(page.combined_len)?);
        }
        prop_assert!(validate_btree_page(&page.bytes).is_err(), "accepted overlapping regions, damage={}", damage);
    }

    #[test]
    fn malformed_headers_slots_and_cells_are_rejected(
        interior in any::<bool>(),
        payload in prop::collection::vec(any::<u8>(), 1..=128),
        nonzero in 1u8..=255,
    ) {
        let mut valid = [0; PAGE_SIZE];
        let (header_size, cell_offset) = if interior {
            let mut page = RawInterior::<Write<'_>>::initialize_with_rightmost(&mut valid, 9);
            page.insert_payload_at(0, 7, payload.len(), None, &payload)?;
            (format::INTERIOR_HEADER_SIZE, usize::from(page.slot_offset(0)?))
        } else {
            let mut page = RawLeaf::<Write<'_>>::initialize(&mut valid);
            page.insert_payload_at(0, 1, payload.len() - 1, None, &payload)?;
            (format::LEAF_HEADER_SIZE, usize::from(page.slot_offset(0)?))
        };
        validate_btree_page(&valid)?;
        // Each patch violates a format rule; payload byte changes alone need not be corruption.
        let patches: &[(usize, &[u8])] = &[
            (format::KIND_OFFSET, &[0]),
            (format::VERSION_OFFSET, &[format::FORMAT_VERSION.wrapping_add(nonzero)]),
            (format::SLOT_COUNT_OFFSET, &[255, 255]),
            (format::CONTENT_START_OFFSET, &[255, 255]),
            (format::CONTENT_START_OFFSET, &[0, 0]),
            (format::FRAGMENTED_FREE_BYTES_OFFSET, &[255, 255]),
            (format::USABLE_SPACE_END, &[nonzero]),
            (header_size, &[0, 0]),
            (header_size, &[251, 15]), // A cell prefix cannot fit at byte 4091.
            (cell_offset, &[255, 255]), // Payload length disagrees with key/value lengths.
            (cell_offset + 2, &[0, 0, 0, 0, 0, 0, 0, 0]), // Short payload with overflow.
        ];
        for &(offset, patch) in patches {
            let mut damaged = valid;
            damaged[offset..offset + patch.len()].copy_from_slice(patch);
            prop_assert!(validate_btree_page(&damaged).is_err(), "accepted patch at {}: {:?}", offset, patch);
        }
    }

    #[test]
    fn validated_mutated_pages_have_readable_cell_views(
        interior in any::<bool>(),
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=64), 1..=8),
        patches in prop::collection::vec((0usize..PAGE_SIZE, any::<u8>()), 0..=12),
    ) {
        let mut bytes = [0; PAGE_SIZE];
        if interior {
            let mut page = RawInterior::<Write<'_>>::initialize_with_rightmost(&mut bytes, 9);
            for (index, payload) in payloads.iter().enumerate() {
                page.insert_payload_at(u16::try_from(index)?, 7, payload.len(), None, payload)?;
            }
        } else {
            let mut page = RawLeaf::<Write<'_>>::initialize(&mut bytes);
            for (index, payload) in payloads.iter().enumerate() {
                page.insert_payload_at(u16::try_from(index)?, 0, payload.len(), None, payload)?;
            }
        }
        for (offset, value) in patches {
            bytes[offset] = value;
        }
        if validate_btree_page(&bytes).is_ok() {
            // Use the validated tag: mutations are allowed to change the page kind.
            if bytes[format::KIND_OFFSET] == 1 {
                let page = RawLeaf::<Read<'_>>::open(&bytes)?;
                for slot in 0..page.slot_count() {
                    let (key_len, value_len, overflow, range) = page.cell_payload_parts(slot)?;
                    prop_assert!(range.start <= range.end && range.end <= format::USABLE_SPACE_END);
                    prop_assert!(bytes[range].len() <= key_len + value_len);
                    prop_assert!(overflow.is_none() || key_len + value_len > 512);
                }
            } else {
                let page = RawInterior::<Read<'_>>::open(&bytes)?;
                for slot in 0..page.slot_count() {
                    let (_, key_len, overflow, range) = page.cell_payload_parts(slot)?;
                    prop_assert!(range.start <= range.end && range.end <= format::USABLE_SPACE_END);
                    prop_assert!(bytes[range].len() <= key_len);
                    prop_assert!(overflow.is_none() || key_len > 512);
                    let _ = page.cell(slot)?.left_child();
                }
            }
        }
    }
}
