use proptest::prelude::*;

use super::BitSet;

#[test]
fn searches_start_at_the_requested_bit_and_stop_at_the_end() {
    let bytes = [0b0101_0101, 0b1111_1110];
    let bits = BitSet::new(bytes);
    assert_eq!(bits.first_clear_from(0), Some(1));
    assert_eq!(bits.first_clear_from(4), Some(5));
    assert_eq!(bits.first_clear_from(7), Some(7));
    assert_eq!(bits.first_clear_from(8), Some(8));
    assert_eq!(bits.first_clear_from(9), None);
    assert_eq!(bits.first_clear_from(16), None);
    assert!(!bits.any_set_from(16));
    assert!(!bits.contains(16));
    assert_eq!(BitSet::new([]).first_clear_from(0), None);
}

proptest! {
    #[test]
    fn mutations_and_searches_match_independent_boolean_slots(
        byte_count in 1usize..32,
        operations in prop::collection::vec((any::<usize>(), any::<bool>(), any::<usize>()), 1..96),
    ) {
        let mut bytes = vec![0; byte_count];
        let mut slots = vec![false; byte_count * 8];
        let mut bits = BitSet::new(bytes.as_mut_slice());
        for (index, value, start) in operations {
            let index = index % slots.len();
            bits.set(index, value);
            slots[index] = value;
            for (index, &expected) in slots.iter().enumerate() {
                prop_assert_eq!(bits.contains(index), expected);
            }
            let start = start % (slots.len() + 8);
            let expected_free = (start..slots.len()).find(|&index| !slots[index]);
            let expected_set = (start..slots.len()).any(|index| slots[index]);
            prop_assert_eq!(bits.first_clear_from(start), expected_free);
            prop_assert_eq!(bits.any_set_from(start), expected_set);
        }
    }
}

#[test]
fn owned_bitsets_keep_their_exact_length_and_never_search_padding() {
    for len in [0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 129] {
        let mut bits = BitSet::zeroed(len);
        assert_eq!(bits.len(), len);
        assert!(!bits.any_set_from(0));
        for index in 0..len {
            assert_eq!(bits.first_clear_from(0), Some(index));
            bits.set(index, true);
        }
        assert_eq!(bits.first_clear_from(0), None);
        assert_eq!(bits.first_clear_from(len), None);
        assert!(!bits.contains(len));
        assert!(!bits.any_set_from(len));
        for index in (0..len).rev() {
            bits.set(index, false);
            assert_eq!(bits.first_clear_from(0), Some(index));
        }
        assert!(!bits.any_set_from(0));
    }
}
