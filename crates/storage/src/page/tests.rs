//! Model-based tests of logical contents, search, capacity, and rejection atomicity.
//! The models know encoded sizes, but no offsets, freeblocks, or packing policy.

use std::collections::BTreeMap;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::{
    PageError, RawInterior, RawLeaf, Read, SearchResult, Write, format, validate_btree_page,
};
use crate::{PAGE_SIZE, PageId};

fn payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        6 => prop::collection::vec(any::<u8>(), 0..=96),
        2 => prop::collection::vec(any::<u8>(), 256..=1024),
        1 => prop::collection::vec(any::<u8>(), 2000..=PAGE_SIZE),
    ]
}

fn key() -> impl Strategy<Value = Vec<u8>> {
    // A small domain creates repeated mutations; variable lengths exercise prefix ordering.
    prop_oneof![Just(Vec::new()), (0u8..24, 1usize..=4).prop_map(|(byte, len)| vec![byte; len])]
}

#[derive(Debug, Clone)]
enum LeafOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Clear,
}

fn leaf_op() -> impl Strategy<Value = LeafOp> {
    prop_oneof![
        6 => (key(), payload()).prop_map(|(key, value)| LeafOp::Put(key, value)),
        4 => key().prop_map(LeafOp::Delete),
        1 => Just(LeafOp::Clear),
    ]
}

fn check_leaf(
    bytes: &[u8; PAGE_SIZE],
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    probe: &[u8],
) -> TestCaseResult {
    validate_btree_page(bytes)?;
    let page = RawLeaf::<Read<'_>>::open(bytes)?;
    prop_assert_eq!(usize::from(page.slot_count()), model.len());
    for (index, (key, value)) in model.iter().enumerate() {
        let slot = u16::try_from(index)?;
        let (key_len, value_len, overflow, range) = page.cell_payload_parts(slot)?;
        prop_assert_eq!((key_len, value_len, overflow), (key.len(), value.len(), None));
        prop_assert_eq!(&bytes[range.clone()][..key_len], key);
        prop_assert_eq!(&bytes[range][key_len..], value);
        prop_assert_eq!(page.search(key)?, SearchResult::Found(slot));
    }
    let rank = u16::try_from(model.keys().take_while(|key| key.as_slice() < probe).count())?;
    let expected = if model.contains_key(probe) {
        SearchResult::Found(rank)
    } else {
        SearchResult::InsertAt(rank)
    };
    prop_assert_eq!(page.search(probe)?, expected);
    Ok(())
}

fn check_capacity(result: &Result<u16, PageError>, fits: bool) -> TestCaseResult {
    prop_assert_eq!(result.is_ok(), fits, "mutation result: {:?}", result);
    if !fits {
        prop_assert!(
            matches!(result, Err(PageError::PageFull { .. } | PageError::CellTooLarge { .. })),
            "expected a capacity error, got {:?}",
            result
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum InteriorOp {
    Insert(u16, Vec<u8>, PageId),
    Remove(u16),
    SetChild(u16, PageId),
    SetRightmost(PageId),
}

fn interior_op() -> impl Strategy<Value = InteriorOp> {
    prop_oneof![
        5 => (any::<u16>(), payload(), any::<PageId>())
            .prop_map(|(index, key, child)| InteriorOp::Insert(index, key, child)),
        3 => any::<u16>().prop_map(InteriorOp::Remove),
        2 => (any::<u16>(), any::<PageId>())
            .prop_map(|(index, child)| InteriorOp::SetChild(index, child)),
        1 => any::<PageId>().prop_map(InteriorOp::SetRightmost),
    ]
}

fn check_interior(
    bytes: &[u8; PAGE_SIZE],
    model: &[(Vec<u8>, PageId)],
    rightmost: PageId,
) -> TestCaseResult {
    validate_btree_page(bytes)?;
    let page = RawInterior::<Read<'_>>::open(bytes)?;
    prop_assert_eq!(usize::from(page.slot_count()), model.len());
    prop_assert_eq!(page.rightmost_child(), rightmost);
    for (index, (key, child)) in model.iter().enumerate() {
        let slot = u16::try_from(index)?;
        let (actual_child, key_len, overflow, range) = page.cell_payload_parts(slot)?;
        prop_assert_eq!((actual_child, key_len, overflow), (*child, key.len(), None));
        prop_assert_eq!(&bytes[range], key);
        prop_assert_eq!(page.cell(slot)?.left_child(), *child);
    }
    Ok(())
}

proptest! {
    #[test]
    fn leaf_mutations_match_ordered_map(
        ops in prop::collection::vec(leaf_op(), 1..=100),
        probe in prop::collection::vec(any::<u8>(), 0..=5),
        prev in prop::option::of(0..PageId::MAX),
        next in prop::option::of(0..PageId::MAX),
    ) {
        let mut bytes = [0; PAGE_SIZE];
        let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        {
            let mut page = RawLeaf::<Write<'_>>::initialize(&mut bytes);
            page.set_prev_page_id(prev);
            page.set_next_page_id(next);
        }
        check_leaf(&bytes, &model, &probe)?;
        for op in ops {
            {
                let mut page = RawLeaf::<Write<'_>>::open(&mut bytes)?;
                match op {
                    LeafOp::Put(key, value) => {
                        let rank = u16::try_from(model.keys().take_while(|k| *k < &key).count())?;
                        let used = format::LEAF_HEADER_SIZE + model.iter().map(|(k, v)| {
                            format::SLOT_ENTRY_SIZE + format::LEAF_CELL_PREFIX_SIZE + k.len() + v.len()
                        }).sum::<usize>();
                        let old_size = model.get(&key).map_or(0, |v| {
                            format::SLOT_ENTRY_SIZE + format::LEAF_CELL_PREFIX_SIZE + key.len() + v.len()
                        });
                        let new_size = format::SLOT_ENTRY_SIZE + format::LEAF_CELL_PREFIX_SIZE
                            + key.len() + value.len();
                        let fits = used - old_size + new_size <= format::USABLE_SPACE_END;
                        let inline = [key.as_slice(), value.as_slice()].concat();
                        let result = if model.contains_key(&key) {
                            page.update_payload_at(rank, key.len(), value.len(), None, &inline)
                        } else {
                            page.insert_payload_at(rank, key.len(), value.len(), None, &inline)
                        };
                        check_capacity(&result, fits)?;
                        if fits {
                            prop_assert_eq!(result?, rank);
                            model.insert(key, value);
                        }
                    }
                    LeafOp::Delete(key) => {
                        let rank = u16::try_from(model.keys().take_while(|k| *k < &key).count())?;
                        let expected = if model.remove(&key).is_some() {
                            Ok(rank)
                        } else {
                            Err(PageError::KeyNotFound)
                        };
                        prop_assert_eq!(page.delete(&key), expected);
                    }
                    LeafOp::Clear => {
                        for key in model.keys().rev() {
                            page.delete(key)?;
                        }
                        model.clear();
                    }
                }
            }
            // Reopen through the read API after every operation, including rejected ones.
            check_leaf(&bytes, &model, &probe)?;
            let page = RawLeaf::<Read<'_>>::open(&bytes)?;
            prop_assert_eq!((page.prev_page_id(), page.next_page_id()), (prev, next));
        }
    }

    #[test]
    fn interior_mutations_preserve_separators_and_children(
        ops in prop::collection::vec(interior_op(), 1..=100),
        initial_rightmost in any::<PageId>(),
    ) {
        let mut bytes = [0; PAGE_SIZE];
        let mut model = Vec::<(Vec<u8>, PageId)>::new();
        let mut rightmost = initial_rightmost;
        RawInterior::<Write<'_>>::initialize_with_rightmost(&mut bytes, rightmost);
        check_interior(&bytes, &model, rightmost)?;
        for op in ops {
            {
                let mut page = RawInterior::<Write<'_>>::open(&mut bytes)?;
                match op {
                    InteriorOp::Insert(index, key, child) => {
                        let index = usize::from(index) % (model.len() + 1);
                        let used = format::INTERIOR_HEADER_SIZE + model.iter().map(|(k, _)| {
                            format::SLOT_ENTRY_SIZE + format::INTERIOR_CELL_PREFIX_SIZE + k.len()
                        }).sum::<usize>();
                        let fits = used + format::SLOT_ENTRY_SIZE + format::INTERIOR_CELL_PREFIX_SIZE
                            + key.len() <= format::USABLE_SPACE_END;
                        let slot = u16::try_from(index)?;
                        let result = page.insert_payload_at(slot, child, key.len(), None, &key);
                        check_capacity(&result, fits)?;
                        if fits {
                            prop_assert_eq!(result?, slot);
                            model.insert(index, (key, child));
                        }
                    }
                    InteriorOp::Remove(index) if !model.is_empty() => {
                        let index = usize::from(index) % model.len();
                        let slot = u16::try_from(index)?;
                        let len = page.cell_len(slot)?;
                        let offset = page.remove_slot(slot)?;
                        page.reclaim_space(offset, len)?;
                        model.remove(index);
                    }
                    InteriorOp::SetChild(index, child) if !model.is_empty() => {
                        let index = usize::from(index) % model.len();
                        page.cell_mut(u16::try_from(index)?)?.set_left_child(child);
                        model[index].1 = child;
                    }
                    InteriorOp::SetRightmost(child) => {
                        page.set_rightmost_child(child);
                        rightmost = child;
                    }
                    _ => {}
                }
            }
            check_interior(&bytes, &model, rightmost)?;
        }
    }
}
