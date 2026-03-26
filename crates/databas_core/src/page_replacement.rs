use crate::page_cache::FrameId;

const BITS_PER_WORD: usize = u64::BITS as usize;

#[derive(Debug)]
struct BitSet {
    words: Vec<u64>,
    len: usize,
}

impl BitSet {
    fn new(len: usize) -> Self {
        let word_count = len.saturating_add(BITS_PER_WORD - 1) / BITS_PER_WORD;
        Self { words: vec![0; word_count], len }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn get(&self, index: usize) -> bool {
        let (word_index, mask) = self.word_index_and_mask(index);
        self.words[word_index] & mask != 0
    }

    fn set(&mut self, index: usize) {
        let (word_index, mask) = self.word_index_and_mask(index);
        self.words[word_index] |= mask;
    }

    fn clear(&mut self, index: usize) {
        let (word_index, mask) = self.word_index_and_mask(index);
        self.words[word_index] &= !mask;
    }

    fn word_index_and_mask(&self, index: usize) -> (usize, u64) {
        assert!(index < self.len, "bit index out of bounds");
        let word_index = index / BITS_PER_WORD;
        let bit_offset = index % BITS_PER_WORD;
        (word_index, 1u64 << bit_offset)
    }
}

#[derive(Debug)]
pub(crate) struct ClockPolicy {
    hand: FrameId,
    reference_bits: BitSet,
}

impl ClockPolicy {
    pub(crate) fn new(frame_count: usize) -> Self {
        Self { hand: 0, reference_bits: BitSet::new(frame_count) }
    }

    pub(crate) fn record_access(&mut self, frame_id: FrameId) {
        self.reference_bits.set(frame_id);
    }

    pub(crate) fn record_insert(&mut self, frame_id: FrameId) {
        self.reference_bits.set(frame_id);
    }

    /// Selects a victim frame using CLOCK second-chance replacement.
    ///
    /// Pinned frames are skipped and referenced frames get one second chance.
    pub(crate) fn select_victim<F>(&mut self, mut is_pinned: F) -> Option<FrameId>
    where
        F: FnMut(FrameId) -> bool,
    {
        let max_scans = self.reference_bits.len().saturating_mul(2);

        for _ in 0..max_scans {
            let frame_id = self.hand;
            self.advance_hand();

            if is_pinned(frame_id) {
                continue;
            }

            if self.reference_bits.get(frame_id) {
                self.reference_bits.clear(frame_id);
                continue;
            }

            return Some(frame_id);
        }

        None
    }

    fn advance_hand(&mut self) {
        debug_assert!(
            self.reference_bits.len() > 0,
            "replacement policy requires at least one frame"
        );
        self.hand = (self.hand + 1) % self.reference_bits.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitset_starts_with_all_bits_cleared() {
        let bitset = BitSet::new(130);

        assert!(!bitset.get(0));
        assert!(!bitset.get(63));
        assert!(!bitset.get(64));
        assert!(!bitset.get(129));
    }

    #[test]
    fn bitset_set_makes_bit_visible() {
        let mut bitset = BitSet::new(130);

        bitset.set(64);

        assert!(bitset.get(64));
        assert!(!bitset.get(63));
        assert!(!bitset.get(65));
    }

    #[test]
    fn bitset_clear_resets_previously_set_bit() {
        let mut bitset = BitSet::new(130);

        bitset.set(65);
        bitset.clear(65);

        assert!(!bitset.get(65));
    }

    #[test]
    fn bitset_handles_bits_across_word_boundaries() {
        let mut bitset = BitSet::new(130);

        bitset.set(63);
        bitset.set(64);
        bitset.set(65);
        bitset.clear(64);

        assert!(bitset.get(63));
        assert!(!bitset.get(64));
        assert!(bitset.get(65));
    }
}
