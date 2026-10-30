use crate::bitset::BitSet;
use crate::page_cache::FrameId;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct ClockPolicy {
    hand: FrameId,
    reference_bits: BitSet<Vec<u8>>,
}

impl ClockPolicy {
    pub(crate) fn new(frame_count: usize) -> Self {
        Self { hand: 0, reference_bits: BitSet::zeroed(frame_count) }
    }

    pub(crate) fn record_access(&mut self, frame_id: FrameId) {
        self.reference_bits.set(frame_id, true);
    }

    pub(crate) fn record_insert(&mut self, frame_id: FrameId) {
        self.reference_bits.set(frame_id, true);
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

            if self.reference_bits.contains(frame_id) {
                self.reference_bits.set(frame_id, false);
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
