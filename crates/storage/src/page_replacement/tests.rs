//! Check victim eligibility, progress, and second chances without observing the
//! clock hand or bit representation, or fixing a tie-breaking order.

use std::collections::BTreeSet;

use proptest::{prelude::*, test_runner::TestCaseResult};

use super::ClockPolicy;

fn capacity() -> impl Strategy<Value = usize> {
    prop_oneof![
        prop::sample::select(vec![0usize, 1, 2, 3, 63, 64, 65, 127, 128, 129]),
        0usize..=256,
    ]
}

proptest! {
    #[test]
    fn victims_are_eligible_and_quiescent_frames_are_not_starved(
        capacity in capacity(),
        ops in prop::collection::vec((0u8..4, any::<usize>(), any::<bool>()), 0..=256),
    ) {
        let mut policy = ClockPolicy::new(capacity);
        let mut pinned = vec![false; capacity];
        for (op, index, value) in ops {
            if capacity > 0 {
                let index = index % capacity;
                match op {
                    0 => policy.record_access(index),
                    1 => policy.record_insert(index),
                    2 => pinned[index] = value,
                    _ => {}
                }
            }
            let victim = policy.select_victim(|frame| pinned[frame]);
            if let Some(victim) = victim {
                prop_assert!(victim < capacity);
                prop_assert!(!pinned[victim]);
            } else {
                prop_assert!(pinned.iter().all(|pinned| *pinned));
            }
        }
        let eligible: BTreeSet<_> = (0..capacity).filter(|frame| !pinned[*frame]).collect();
        let mut selected = BTreeSet::new();
        // After accesses cease, each eligible frame must eventually be offered,
        // even when it starts with a reference and other frames do not.
        for _ in 0..2 * capacity {
            if let Some(victim) = policy.select_victim(|frame| pinned[frame]) {
                prop_assert!(eligible.contains(&victim));
                selected.insert(victim);
            } else {
                prop_assert!(eligible.is_empty());
            }
        }
        prop_assert_eq!(selected, eligible);
    }

    #[test]
    fn accessed_and_inserted_frames_get_a_second_chance(
        capacity in 2usize..=130, hot in any::<usize>(), accesses in 1usize..=8, insert in any::<bool>(),
    ) {
        let mut policy = ClockPolicy::new(capacity);
        // Establish cold candidates through selections, not private state.
        for frame in 0..capacity { policy.record_insert(frame); }
        for _ in 0..capacity { prop_assert!(policy.select_victim(|_| false).is_some()); }
        let hot = hot % capacity;
        for _ in 0..accesses {
            if insert { policy.record_insert(hot); } else { policy.record_access(hot); }
        }
        let victim = policy.select_victim(|_| false);
        prop_assert!(victim.is_some());
        prop_assert_ne!(victim, Some(hot));
        // A second chance must not make the only eligible frame unevictable.
        prop_assert_eq!(policy.select_victim(|frame| frame != hot), Some(hot));
    }
}

#[test]
fn pinned_accesses_keep_their_second_chance_until_unpinned() -> TestCaseResult {
    for capacity in [2, 63, 64, 65, 127, 128, 129] {
        let mut policy = ClockPolicy::new(capacity);
        for frame in 0..capacity {
            policy.record_insert(frame);
        }
        for _ in 0..capacity {
            prop_assert!(policy.select_victim(|_| false).is_some());
        }
        for hot in [0, capacity - 1] {
            policy.record_access(hot);
            prop_assert_eq!(policy.select_victim(|_| true), None);
            prop_assert_ne!(policy.select_victim(|_| false), Some(hot));
        }
    }
    Ok(())
}
