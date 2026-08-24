//! Page occupancy and split-selection policy for B+-tree rebalancing.
//!
//! Keeping these calculations separate from page mutation makes the balancing
//! rules reviewable without mixing them with sibling links, parent repair, and
//! rollback-sensitive writes.

use super::*;

#[derive(Clone, Copy)]
enum MinimumOccupancy {
    Required,
    NotRequired,
}

/// Selects the valid split with the smallest caller-defined imbalance.
///
/// Equal candidates retain their left-to-right ordering so split behavior stays
/// deterministic.
fn choose_balanced_split(
    item_count: usize,
    mut candidate: impl FnMut(usize) -> Option<usize>,
) -> Option<usize> {
    (1..item_count)
        .filter_map(|split_index| candidate(split_index).map(|score| (score, split_index)))
        .min_by_key(|(score, _)| *score)
        .map(|(_, split_index)| split_index)
}

impl TreeCursor {
    /// Chooses a split index that keeps both leaf siblings fit and occupied.
    pub(super) fn choose_leaf_rebalance_split(cells: &[LeafSplitCell<'_>]) -> Option<usize> {
        Self::choose_leaf_split(cells, MinimumOccupancy::Required)
    }

    /// Chooses a split index that keeps both leaf siblings within page capacity.
    pub(super) fn choose_leaf_fitting_split(cells: &[LeafSplitCell<'_>]) -> Option<usize> {
        Self::choose_leaf_split(cells, MinimumOccupancy::NotRequired)
    }

    fn choose_leaf_split(
        cells: &[LeafSplitCell<'_>],
        minimum_occupancy: MinimumOccupancy,
    ) -> Option<usize> {
        let total_cell_len = cells.iter().map(LeafSplitCell::encoded_size).sum::<usize>();
        let mut left_cell_len = 0;

        choose_balanced_split(cells.len(), |split_index| {
            left_cell_len += cells[split_index - 1].encoded_size();
            let right_count = cells.len() - split_index;
            let right_cell_len = total_cell_len - left_cell_len;
            let left_cells_fit = Self::leaf_cell_bytes_fit(split_index, left_cell_len);
            let right_cells_fit = Self::leaf_cell_bytes_fit(right_count, right_cell_len);
            let left_cells_underoccupied =
                Self::leaf_cell_bytes_underoccupied(split_index, left_cell_len);
            let right_cells_underoccupied =
                Self::leaf_cell_bytes_underoccupied(right_count, right_cell_len);

            let cells_fit = left_cells_fit && right_cells_fit;
            let occupancy_is_valid = match minimum_occupancy {
                MinimumOccupancy::Required => {
                    !left_cells_underoccupied && !right_cells_underoccupied
                }
                MinimumOccupancy::NotRequired => true,
            };

            (cells_fit && occupancy_is_valid).then(|| left_cell_len.abs_diff(right_cell_len))
        })
    }

    /// Returns whether `children` can be encoded in one interior page.
    pub(super) fn interior_children_fit(children: &[ChildEntry]) -> bool {
        if children.is_empty() {
            return false;
        }
        let mut cell_bytes = 0;
        for child in &children[..children.len() - 1] {
            let Some(key) = child.max_key.as_ref() else {
                return false;
            };
            cell_bytes += INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len());
        }
        let used_bytes = PageKind::RawInterior.header_size()
            + (children.len() - 1) * page::format::SLOT_ENTRY_SIZE
            + cell_bytes;
        used_bytes <= page::format::USABLE_SPACE_END
    }

    /// Returns whether an interior page rebuilt from `children` would be underoccupied.
    pub(super) fn interior_children_underoccupied(children: &[ChildEntry]) -> bool {
        let mut cell_bytes = 0;
        for child in &children[..children.len().saturating_sub(1)] {
            let Some(key) = child.max_key.as_ref() else {
                return true;
            };
            cell_bytes += INTERIOR_CELL_PREFIX_SIZE + local_payload_len(key.len());
        }
        let occupied_variable_bytes =
            children.len().saturating_sub(1) * page::format::SLOT_ENTRY_SIZE + cell_bytes;
        let usable_variable_bytes =
            page::format::USABLE_SPACE_END - PageKind::RawInterior.header_size();
        occupied_variable_bytes * 2 < usable_variable_bytes
    }

    /// Chooses a split index that keeps both interior siblings fit and occupied.
    pub(super) fn choose_interior_rebalance_split(children: &[ChildEntry]) -> Option<usize> {
        Self::choose_interior_split(children, MinimumOccupancy::Required)
    }

    /// Chooses a split index that keeps both interior siblings within page capacity.
    pub(super) fn choose_interior_fitting_split(children: &[ChildEntry]) -> Option<usize> {
        Self::choose_interior_split(children, MinimumOccupancy::NotRequired)
    }

    fn choose_interior_split(
        children: &[ChildEntry],
        minimum_occupancy: MinimumOccupancy,
    ) -> Option<usize> {
        choose_balanced_split(children.len(), |split_index| {
            let left = &children[..split_index];
            let right = &children[split_index..];
            let left_children_fit = Self::interior_children_fit(left);
            let right_children_fit = Self::interior_children_fit(right);
            let left_children_underoccupied = Self::interior_children_underoccupied(left);
            let right_children_underoccupied = Self::interior_children_underoccupied(right);

            let children_fit = left_children_fit && right_children_fit;
            let occupancy_is_valid = match minimum_occupancy {
                MinimumOccupancy::Required => {
                    !left_children_underoccupied && !right_children_underoccupied
                }
                MinimumOccupancy::NotRequired => true,
            };

            (children_fit && occupancy_is_valid)
                .then(|| split_index.abs_diff(children.len() - split_index))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::choose_balanced_split;

    #[test]
    fn split_selection_chooses_lowest_score() {
        let split = choose_balanced_split(5, |index| (index != 2).then_some(index.abs_diff(3)));

        assert_eq!(split, Some(3));
    }

    #[test]
    fn split_selection_prefers_leftmost_equal_candidate() {
        let split = choose_balanced_split(6, |_| Some(0));

        assert_eq!(split, Some(1));
    }

    #[test]
    fn split_selection_rejects_all_invalid_candidates() {
        let split = choose_balanced_split(4, |_| None);

        assert_eq!(split, None);
    }
}
