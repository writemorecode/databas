use proptest::prelude::*;

use super::super::{RawInterior, RawLeaf, Read, Write, interior, leaf, validate_btree_page};
use super::{check_leaf, key};
use crate::{PAGE_SIZE, PageId};

fn cell_payload() -> impl Strategy<Value = (usize, Option<PageId>, Vec<u8>)> {
    // Include both sides of the inline boundary and the largest encodable length.
    let len = prop_oneof![
        prop::sample::select(vec![0usize, 1, 511, 512, 513, 514, 65534, 65535]),
        0usize..=65535,
    ];
    (len, 0..PageId::MAX).prop_flat_map(|(len, page_id)| {
        let overflow = (len > 512).then_some(page_id);
        (Just(len), Just(overflow), prop::collection::vec(any::<u8>(), len.min(512)))
    })
}

proptest! {
    #[test]
    fn leaf_cells_preserve_lengths_overflow_references_and_inline_bytes(
        first in cell_payload(),
        replacement in cell_payload(),
        split in any::<u16>(),
    ) {
        let mut bytes = [0; PAGE_SIZE];
        RawLeaf::<Write<'_>>::initialize(&mut bytes);
        for (index, (len, overflow, inline)) in [first, replacement].into_iter().enumerate() {
            let key_len = usize::from(split) % (len + 1);
            let value_len = len - key_len;
            {
                let mut page = RawLeaf::<Write<'_>>::open(&mut bytes)?;
                if index == 0 {
                    page.insert_payload_at(0, key_len, value_len, overflow, &inline)?;
                } else {
                    page.update_payload_at(0, key_len, value_len, overflow, &inline)?;
                }
            }
            validate_btree_page(&bytes)?;
            let page = RawLeaf::<Read<'_>>::open(&bytes)?;
            let (actual_key_len, actual_value_len, actual_overflow, range) = page.cell_payload_parts(0)?;
            prop_assert_eq!((actual_key_len, actual_value_len, actual_overflow),
                (key_len, value_len, overflow));
            prop_assert_eq!(&bytes[range], &inline);
        }
    }

    #[test]
    fn interior_cell_child_updates_preserve_payload(
        (len, overflow, inline) in cell_payload(),
        child in any::<PageId>(),
        replacement in any::<PageId>(),
        rightmost in any::<PageId>(),
    ) {
        let mut bytes = [0; PAGE_SIZE];
        {
            let mut page = RawInterior::<Write<'_>>::initialize_with_rightmost(&mut bytes, rightmost);
            page.insert_payload_at(0, child, len, overflow, &inline)?;
        }
        for expected_child in [child, replacement] {
            {
                let mut page = RawInterior::<Write<'_>>::open(&mut bytes)?;
                page.cell_mut(0)?.set_left_child(expected_child);
            }
            validate_btree_page(&bytes)?;
            let page = RawInterior::<Read<'_>>::open(&bytes)?;
            let (actual_child, key_len, actual_overflow, range) = page.cell_payload_parts(0)?;
            prop_assert_eq!((actual_child, key_len, actual_overflow), (expected_child, len, overflow));
            prop_assert_eq!(page.cell(0)?.left_child(), expected_child);
            prop_assert_eq!(page.rightmost_child(), rightmost);
            prop_assert_eq!(&bytes[range], &inline);
        }
    }

    #[test]
    fn rejected_leaf_payloads_and_slots_preserve_existing_records(
        key in key(),
        value in prop::collection::vec(any::<u8>(), 0..=64),
        invalid_slot in 2u16..=u16::MAX,
    ) {
        let mut bytes = [0; PAGE_SIZE];
        let inline = [key.as_slice(), value.as_slice()].concat();
        RawLeaf::<Write<'_>>::initialize(&mut bytes)
            .insert_payload_at(0, key.len(), value.len(), None, &inline)?;
        let model = [(key, value)].into_iter().collect();
        // Invalid lengths, arithmetic overflow, mismatched bytes, and short overflow payloads.
        for (key_len, value_len, overflow, payload) in [
            (65536, 0, None, &[][..]),
            (usize::MAX, 1, None, &[][..]),
            (1, 65535, None, &[][..]),
            (1, 1, None, &b"x"[..]),
            (1, 0, Some(7), &b"x"[..]),
            (513, 0, Some(7), &b"x"[..]),
        ] {
            {
                let mut page = RawLeaf::<Write<'_>>::open(&mut bytes)?;
                prop_assert!(page.insert_payload_at(1, key_len, value_len, overflow, payload).is_err());
                prop_assert!(page.update_payload_at(0, key_len, value_len, overflow, payload).is_err());
            }
            check_leaf(&bytes, &model, b"")?;
        }
        {
            let mut page = RawLeaf::<Write<'_>>::open(&mut bytes)?;
            prop_assert!(page.insert_payload_at(invalid_slot, 0, 0, None, b"").is_err());
            prop_assert!(page.update_payload_at(invalid_slot, 0, 0, None, b"").is_err());
        }
        check_leaf(&bytes, &model, b"")?;
    }

    #[test]
    fn rejected_interior_payloads_and_slots_preserve_existing_cells(
        key in prop::collection::vec(any::<u8>(), 0..=64),
        child in any::<PageId>(),
        invalid_slot in 2u16..=u16::MAX,
    ) {
        let mut bytes = [0; PAGE_SIZE];
        RawInterior::<Write<'_>>::initialize_with_rightmost(&mut bytes, child)
            .insert_payload_at(0, child, key.len(), None, &key)?;
        for (key_len, overflow, payload) in [
            (65536, None, &[][..]),
            (usize::MAX, None, &[][..]),
            (2, None, &b"x"[..]),
            (1, Some(7), &b"x"[..]),
            (513, Some(7), &b"x"[..]),
        ] {
            {
                let mut page = RawInterior::<Write<'_>>::open(&mut bytes)?;
                prop_assert!(page.insert_payload_at(1, child, key_len, overflow, payload).is_err());
            }
            super::check_interior(&bytes, &[(key.clone(), child)], child)?;
        }
        {
            let mut page = RawInterior::<Write<'_>>::open(&mut bytes)?;
            prop_assert!(page.insert_payload_at(invalid_slot, child, 0, None, b"").is_err());
            prop_assert!(page.cell_mut(invalid_slot).is_err());
            prop_assert!(page.remove_slot(invalid_slot).is_err());
        }
        super::check_interior(&bytes, &[(key, child)], child)?;
    }
}

// Hand-authored format-v5 fixtures: little-endian fields, absent-overflow sentinel,
// and binary payloads. Neither the fixture bytes nor their offsets use encoder helpers.
const LEAF_CELL: [u8; 18] =
    [4, 0, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 3, 0, 0, 128, 255, 64];
const INTERIOR_CELL: [u8; 23] =
    [3, 0, 255, 255, 255, 255, 255, 255, 255, 255, 1, 2, 3, 4, 5, 6, 7, 8, 3, 0, 0, 128, 255];

#[test]
fn version_five_cells_match_independent_format_fixtures() -> proptest::test_runner::TestCaseResult {
    let mut bytes = [0; PAGE_SIZE];
    bytes[..10].copy_from_slice(&[1, 5, 1, 0, 234, 15, 255, 255, 0, 0]);
    bytes[10..26].fill(255);
    bytes[34..36].copy_from_slice(&[234, 15]);
    bytes[4074..4092].copy_from_slice(&LEAF_CELL);
    validate_btree_page(&bytes)?;
    let page = RawLeaf::<Read<'_>>::open(&bytes)?;
    let (key_len, value_len, overflow, range) = page.cell_payload_parts(0)?;
    prop_assert_eq!((key_len, value_len, overflow), (1, 3, None));
    prop_assert_eq!(&bytes[range], &[0, 128, 255, 64]);
    let mut encoded = [0; PAGE_SIZE];
    leaf::write_cell_with_payload(&mut encoded, 0, 4, 1, 3, None, &[0, 128, 255, 64]);
    prop_assert_eq!(&encoded[..18], &LEAF_CELL);

    bytes.fill(0);
    bytes[..10].copy_from_slice(&[2, 5, 1, 0, 229, 15, 255, 255, 0, 0]);
    bytes[10..26].fill(255);
    bytes[34..42].copy_from_slice(&[17, 18, 19, 20, 21, 22, 23, 24]);
    bytes[42..44].copy_from_slice(&[229, 15]);
    bytes[4069..4092].copy_from_slice(&INTERIOR_CELL);
    validate_btree_page(&bytes)?;
    let page = RawInterior::<Read<'_>>::open(&bytes)?;
    let (child, key_len, overflow, range) = page.cell_payload_parts(0)?;
    prop_assert_eq!((child, key_len, overflow), (0x0807_0605_0403_0201, 3, None));
    prop_assert_eq!(page.cell(0)?.left_child(), child);
    prop_assert_eq!(page.rightmost_child(), 0x1817_1615_1413_1211);
    prop_assert_eq!(&bytes[range], &[0, 128, 255]);
    interior::write_cell_with_payload(&mut encoded, 0, child, 3, None, &[0, 128, 255]);
    prop_assert_eq!(&encoded[..23], &INTERIOR_CELL);
    Ok(())
}
