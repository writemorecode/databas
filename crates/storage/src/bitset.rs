//! Shared bit operations over owned or borrowed bytes. Index zero is the low bit of byte zero.

#[derive(Debug)]
pub struct BitSet<T> {
    bytes: T,
    len: usize,
}

impl<T: AsRef<[u8]>> BitSet<T> {
    pub(crate) fn new(bytes: T) -> Self {
        let len = bytes.as_ref().len().saturating_mul(8);
        Self { bytes, len }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn contains(&self, index: usize) -> bool {
        index < self.len
            && self.bytes.as_ref().get(index / 8).is_some_and(|byte| byte & (1 << (index % 8)) != 0)
    }

    pub(crate) fn first_clear_from(&self, start: usize) -> Option<usize> {
        self.find_from(start, false)
    }

    pub(crate) fn any_set_from(&self, start: usize) -> bool {
        self.find_from(start, true).is_some()
    }

    fn find_from(&self, start: usize, set: bool) -> Option<usize> {
        if start >= self.len {
            return None;
        }
        let first_byte = start / 8;
        for (index, &byte) in self.bytes.as_ref().iter().enumerate().skip(first_byte) {
            let mut candidates = if set { byte } else { !byte };
            if index == first_byte {
                candidates &= u8::MAX << (start % 8);
            }
            if candidates != 0 {
                let bit = index * 8 + candidates.trailing_zeros() as usize;
                return (bit < self.len).then_some(bit);
            }
        }
        None
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> BitSet<T> {
    /// The caller supplies an index within the logical bit length.
    pub(crate) fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.len, "bit index out of bounds");
        let byte = &mut self.bytes.as_mut()[index / 8];
        let mask = 1 << (index % 8);
        if value {
            *byte |= mask;
        } else {
            *byte &= !mask;
        }
    }
}

impl BitSet<Vec<u8>> {
    /// Allocate exactly `len` logical bits, initially clear. Padding is not searchable.
    pub(crate) fn zeroed(len: usize) -> Self {
        Self { bytes: vec![0; len.div_ceil(8)], len }
    }
}

#[cfg(test)]
mod tests;
