//! Tuple value serialization.
//!
//! A tuple is encoded as a value count followed by that many typed value
//! fields:
//!
//! ```text
//! u32 value_count_le
//! repeat value_count times:
//!   u8  value_tag
//!   u32 payload_len_be
//!   [u8; payload_len] canonical_payload
//! ```
//!
//! The tuple count is little-endian for compatibility with the existing tuple
//! container format. Per-value lengths are big-endian so the value header is
//! canonical and future raw-byte comparators do not need mixed-endian value
//! metadata.
//!
//! Value payloads use the following encodings:
//!
//! ```text
//! NULL             tag 0x05, len 0, no payload
//! Text             tag 0x01, len N, raw UTF-8 bytes
//! Boolean          tag 0x02, len 1, 0x00 false or 0x01 true
//! Integer(i32)     tag 0x03, len 4, (value ^ i32::MIN).to_be_bytes()
//! Float(f32)       tag 0x04, len 4, sortable IEEE-754 bits in big-endian order
//! UnsignedInteger  tag 0x06, len 8, value.to_be_bytes()
//! ```
//!
//! Fixed-width numeric payloads are encoded so bytewise comparison of payloads
//! matches their logical ascending order. Float values reject NaN and normalize
//! both zero signs to `+0.0` during serialization and decoding. Strings keep
//! length-based framing and compare by raw UTF-8 bytes when a caller slices out
//! each payload.
//!
//! [`TupleView`] validates all tags, lengths, UTF-8 text, boolean payloads,
//! float payloads, and trailing bytes before exposing zero-copy borrowed values.

use std::{
    cmp::Ordering,
    fmt::Display,
    io::{self, Read, Write},
    ops::Range,
};

use crate::core::error::TupleAllocationError;

const TAG_STRING: u8 = 0x01;
const TAG_BOOLEAN: u8 = 0x02;
const TAG_INTEGER: u8 = 0x03;
const TAG_FLOAT: u8 = 0x04;
const TAG_NULL: u8 = 0x05;
const TAG_UNSIGNED_INTEGER: u8 = 0x06;

const NULL_LENGTH: u32 = 0;
const BOOL_LENGTH: u32 = 1;
const I32_LENGTH: u32 = size_of::<i32>() as u32;
const F32_LENGTH: u32 = size_of::<f32>() as u32;
const U64_LENGTH: u32 = size_of::<u64>() as u32;

/// A single typed value stored in a [`Tuple`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    String(String),
    Boolean(bool),
    Integer(i32),
    Float(f32),
    UnsignedInteger(u64),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::String(s) => write!(f, "{s}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::UnsignedInteger(u) => write!(f, "{u}"),
        }
    }
}

/// A borrowed typed value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    String(&'a str),
    Boolean(bool),
    Integer(i32),
    Float(f32),
    UnsignedInteger(u64),
}

/// An ordered list of typed storage values.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple(Vec<Value>);

/// A borrowed ordered list of typed values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TupleRef<'a> {
    values: &'a [ValueRef<'a>],
}

/// A validated zero-copy view over count-prefixed TLV tuple bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTupleView<'a> {
    bytes: &'a [u8],
    values: Vec<ValueField>,
}

/// A validated zero-copy view over count-prefixed TLV tuple bytes.
pub type TupleView<'a> = EncodedTupleView<'a>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValueField {
    tag: u8,
    value_range: Range<usize>,
}

impl Tuple {
    /// Creates a tuple from ordered values.
    pub fn new(values: Vec<Value>) -> Self {
        Self(values)
    }

    /// Returns the values in tuple order.
    pub fn values(&self) -> &[Value] {
        &self.0
    }

    /// Consumes the tuple and returns its values.
    pub fn into_values(self) -> Vec<Value> {
        self.0
    }

    /// Returns the number of values in the tuple.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true when the tuple contains no values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Appends a value to the tuple.
    pub fn push(&mut self, value: Value) {
        self.0.push(value);
    }

    /// Returns this tuple's values as borrowed values.
    pub fn value_refs(&self) -> impl Iterator<Item = ValueRef<'_>> {
        self.0.iter().map(ValueRef::from)
    }

    /// Serializes this tuple to `writer` using count-prefixed TLV encoding.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        write_values(writer, self.0.iter().map(ValueRef::from))
    }

    /// Deserializes one count-prefixed TLV tuple from `reader`.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let value_count = read_u32(reader)?;
        let mut values = Vec::new();
        values.try_reserve_exact(value_count as usize).map_err(|source| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                TupleAllocationError::Values { value_count: value_count as usize, source },
            )
        })?;

        for _ in 0..value_count {
            let tag = read_u8(reader)?;
            let len = read_value_len(reader)?;
            values.push(read_value(reader, tag, len)?);
        }

        Ok(Self(values))
    }

    /// Deserializes one count-prefixed TLV tuple from `bytes`.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        EncodedTupleView::parse(bytes).map(|view| view.to_owned_tuple())
    }

    /// Serializes this tuple into a byte vector.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }
}

impl<'a> ValueRef<'a> {
    /// Serializes this value as a single TLV item without a tuple count prefix.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        write_value_ref(writer, *self)
    }
}

impl<'a> TupleRef<'a> {
    /// Creates a borrowed tuple from an ordered slice of borrowed values.
    pub fn new(values: &'a [ValueRef<'a>]) -> Self {
        Self { values }
    }

    /// Returns the values in tuple order.
    pub fn values(&self) -> &'a [ValueRef<'a>] {
        self.values
    }

    /// Returns the number of values in the tuple.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true when the tuple contains no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Serializes this tuple to `writer` using count-prefixed TLV encoding.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        write_values(writer, self.values.iter().copied())
    }

    /// Serializes this tuple into a byte vector.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }
}

impl<'a> EncodedTupleView<'a> {
    /// Validates `bytes` as one count-prefixed TLV tuple and returns a zero-copy view.
    pub fn parse(bytes: &'a [u8]) -> io::Result<Self> {
        let (value_count, mut offset) = read_u32_from_slice(bytes, 0)?;
        let mut values = Vec::new();
        values.try_reserve_exact(value_count as usize).map_err(|source| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                TupleAllocationError::Values { value_count: value_count as usize, source },
            )
        })?;

        for _ in 0..value_count {
            let (tag, next_offset) = read_u8_from_slice(bytes, offset)?;
            let (len, value_offset) = read_value_len_from_slice(bytes, next_offset)?;
            let value_len = usize::try_from(len).map_err(invalid_data)?;
            let value_end = value_offset.checked_add(value_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "tuple value length overflows usize")
            })?;
            let payload = bytes.get(value_offset..value_end).ok_or_else(unexpected_eof)?;

            validate_value_payload(tag, payload)?;
            values.push(ValueField { tag, value_range: value_offset..value_end });
            offset = value_end;
        }

        if offset != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes after tuple payload",
            ));
        }

        Ok(Self { bytes, values })
    }

    /// Returns the number of values in the tuple.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true when the tuple contains no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns an iterator over borrowed values in tuple order.
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> + '_ {
        self.values.iter().map(|field| self.value_ref(field))
    }

    /// Returns the original encoded tuple bytes.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Copies this encoded tuple view into an owned tuple.
    pub fn to_owned_tuple(&self) -> Tuple {
        self.values().map(Value::from).collect()
    }

    fn value_ref(&self, field: &ValueField) -> ValueRef<'a> {
        value_ref_from_field(self.bytes, field)
    }
}

fn value_ref_from_field<'a>(bytes: &'a [u8], field: &ValueField) -> ValueRef<'a> {
    let payload = &bytes[field.value_range.clone()];
    match field.tag {
        TAG_NULL => ValueRef::Null,
        TAG_STRING => {
            ValueRef::String(std::str::from_utf8(payload).expect("validated string payload"))
        }
        TAG_BOOLEAN => ValueRef::Boolean(payload[0] == 1),
        TAG_INTEGER => ValueRef::Integer(decode_ordered_i32(
            payload.try_into().expect("validated i32 payload"),
        )),
        TAG_FLOAT => {
            ValueRef::Float(decode_ordered_f32(payload.try_into().expect("validated f32 payload")))
        }
        TAG_UNSIGNED_INTEGER => ValueRef::UnsignedInteger(u64::from_be_bytes(
            payload.try_into().expect("validated u64 payload"),
        )),
        _ => unreachable!("validated tuple value tag"),
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::String(value) => Self::String(value),
            Value::Boolean(value) => Self::Boolean(*value),
            Value::Integer(value) => Self::Integer(*value),
            Value::Float(value) => Self::Float(*value),
            Value::UnsignedInteger(value) => Self::UnsignedInteger(*value),
        }
    }
}

impl<'a> From<ValueRef<'a>> for Value {
    fn from(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::String(value) => Self::String(value.to_owned()),
            ValueRef::Boolean(value) => Self::Boolean(value),
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Float(value) => Self::Float(value),
            ValueRef::UnsignedInteger(value) => Self::UnsignedInteger(value),
        }
    }
}

impl<'a> IntoIterator for TupleRef<'a> {
    type Item = ValueRef<'a>;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, ValueRef<'a>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter().copied()
    }
}

impl<'a> IntoIterator for &'a TupleRef<'a> {
    type Item = ValueRef<'a>;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, ValueRef<'a>>>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter().copied()
    }
}

impl<'a> IntoIterator for &'a EncodedTupleView<'a> {
    type Item = ValueRef<'a>;
    type IntoIter = EncodedTupleViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        EncodedTupleViewIter { bytes: self.bytes, fields: self.values.iter() }
    }
}

/// Iterator over an [`EncodedTupleView`].
pub struct EncodedTupleViewIter<'a> {
    bytes: &'a [u8],
    fields: std::slice::Iter<'a, ValueField>,
}

impl<'a> Iterator for EncodedTupleViewIter<'a> {
    type Item = ValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fields.next().map(|field| value_ref_from_field(self.bytes, field))
    }
}

fn write_values<'a, W, I>(writer: &mut W, values: I) -> io::Result<()>
where
    W: Write,
    I: IntoIterator<Item = ValueRef<'a>>,
    I::IntoIter: ExactSizeIterator,
{
    let values = values.into_iter();
    let value_count = u32::try_from(values.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "tuple value count exceeds u32::MAX")
    })?;
    writer.write_all(&value_count.to_le_bytes())?;

    for value in values {
        write_value_ref(writer, value)?;
    }

    Ok(())
}

fn write_value_ref<W: Write>(writer: &mut W, value: ValueRef<'_>) -> io::Result<()> {
    match value {
        ValueRef::Null => write_tlv_header(writer, TAG_NULL, NULL_LENGTH),
        ValueRef::String(value) => {
            let len = u32::try_from(value.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "string length exceeds u32::MAX")
            })?;
            write_tlv_header(writer, TAG_STRING, len)?;
            writer.write_all(value.as_bytes())
        }
        ValueRef::Boolean(value) => {
            write_tlv_header(writer, TAG_BOOLEAN, BOOL_LENGTH)?;
            writer.write_all(&[value as u8])
        }
        ValueRef::Integer(value) => {
            write_tlv_header(writer, TAG_INTEGER, I32_LENGTH)?;
            writer.write_all(&encode_ordered_i32(value))
        }
        ValueRef::Float(value) => {
            let bytes = encode_ordered_f32(value)?;
            write_tlv_header(writer, TAG_FLOAT, F32_LENGTH)?;
            writer.write_all(&bytes)
        }
        ValueRef::UnsignedInteger(value) => {
            write_tlv_header(writer, TAG_UNSIGNED_INTEGER, U64_LENGTH)?;
            writer.write_all(&value.to_be_bytes())
        }
    }
}

fn write_tlv_header<W: Write>(writer: &mut W, tag: u8, len: u32) -> io::Result<()> {
    writer.write_all(&[tag])?;
    writer.write_all(&len.to_be_bytes())
}

fn read_value<R: Read>(reader: &mut R, tag: u8, len: u32) -> io::Result<Value> {
    match tag {
        TAG_NULL => {
            validate_len(tag, len, NULL_LENGTH)?;
            Ok(Value::Null)
        }
        TAG_STRING => {
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(len as usize).map_err(|source| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    TupleAllocationError::StringBytes { byte_count: len as usize, source },
                )
            })?;
            bytes.resize(len as usize, 0);
            reader.read_exact(&mut bytes)?;
            String::from_utf8(bytes).map(Value::String).map_err(invalid_data)
        }
        TAG_BOOLEAN => {
            validate_len(tag, len, BOOL_LENGTH)?;
            match read_u8(reader)? {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                actual => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid boolean value: {actual}"),
                )),
            }
        }
        TAG_INTEGER => {
            validate_len(tag, len, I32_LENGTH)?;
            let mut bytes = [0; size_of::<i32>()];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Integer(decode_ordered_i32(bytes)))
        }
        TAG_FLOAT => {
            validate_len(tag, len, F32_LENGTH)?;
            let mut bytes = [0; size_of::<f32>()];
            reader.read_exact(&mut bytes)?;
            let value = decode_ordered_f32(bytes);
            validate_float(value)?;
            Ok(Value::Float(value))
        }
        TAG_UNSIGNED_INTEGER => {
            validate_len(tag, len, U64_LENGTH)?;
            let mut bytes = [0; size_of::<u64>()];
            reader.read_exact(&mut bytes)?;
            Ok(Value::UnsignedInteger(u64::from_be_bytes(bytes)))
        }
        actual => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown tuple value tag: {actual}"),
        )),
    }
}

fn validate_value_payload(tag: u8, payload: &[u8]) -> io::Result<()> {
    match tag {
        TAG_NULL => validate_len(tag, payload.len() as u32, NULL_LENGTH),
        TAG_STRING => {
            std::str::from_utf8(payload).map_err(invalid_data)?;
            Ok(())
        }
        TAG_BOOLEAN => {
            validate_len(tag, payload.len() as u32, BOOL_LENGTH)?;
            match payload[0] {
                0 | 1 => Ok(()),
                actual => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid boolean value: {actual}"),
                )),
            }
        }
        TAG_INTEGER => validate_len(tag, payload.len() as u32, I32_LENGTH),
        TAG_FLOAT => {
            validate_len(tag, payload.len() as u32, F32_LENGTH)?;
            validate_float(decode_ordered_f32(payload.try_into().expect("validated f32 payload")))
        }
        TAG_UNSIGNED_INTEGER => validate_len(tag, payload.len() as u32, U64_LENGTH),
        actual => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown tuple value tag: {actual}"),
        )),
    }
}

fn validate_len(tag: u8, actual: u32, expected: u32) -> io::Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid length {actual} for tuple value tag {tag}; expected {expected}"),
        ))
    }
}

fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut bytes = [0];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_u32<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut bytes = [0; size_of::<u32>()];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_value_len<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut bytes = [0; size_of::<u32>()];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn read_u8_from_slice(bytes: &[u8], offset: usize) -> io::Result<(u8, usize)> {
    let value = *bytes.get(offset).ok_or_else(unexpected_eof)?;
    Ok((value, offset + 1))
}

fn read_u32_from_slice(bytes: &[u8], offset: usize) -> io::Result<(u32, usize)> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "offset overflows usize"))?;
    let value = bytes.get(offset..end).ok_or_else(unexpected_eof)?;
    Ok((u32::from_le_bytes(value.try_into().expect("u32 slice has fixed width")), end))
}

fn read_value_len_from_slice(bytes: &[u8], offset: usize) -> io::Result<(u32, usize)> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "offset overflows usize"))?;
    let value = bytes.get(offset..end).ok_or_else(unexpected_eof)?;
    Ok((u32::from_be_bytes(value.try_into().expect("u32 slice has fixed width")), end))
}

fn encode_ordered_i32(value: i32) -> [u8; size_of::<i32>()] {
    ((value as u32) ^ 0x8000_0000).to_be_bytes()
}

fn decode_ordered_i32(bytes: [u8; size_of::<i32>()]) -> i32 {
    (u32::from_be_bytes(bytes) ^ 0x8000_0000) as i32
}

fn encode_ordered_f32(value: f32) -> io::Result<[u8; size_of::<f32>()]> {
    validate_float(value)?;
    let value = if value == 0.0 { 0.0 } else { value };
    let bits = value.to_bits();
    let ordered = if bits & 0x8000_0000 == 0 { bits ^ 0x8000_0000 } else { !bits };
    Ok(ordered.to_be_bytes())
}

fn decode_ordered_f32(bytes: [u8; size_of::<f32>()]) -> f32 {
    let ordered = u32::from_be_bytes(bytes);
    let bits = if ordered & 0x8000_0000 == 0 { !ordered } else { ordered ^ 0x8000_0000 };
    let value = f32::from_bits(bits);
    if value == 0.0 { 0.0 } else { value }
}

fn validate_float(value: f32) -> io::Result<()> {
    if value.is_nan() {
        Err(io::Error::new(io::ErrorKind::InvalidData, "NaN tuple floats are not supported"))
    } else {
        Ok(())
    }
}

#[allow(dead_code)]
struct EncodedValueItem<'a> {
    bytes: &'a [u8],
    field: ValueField,
}

impl EncodedValueItem<'_> {
    fn payload(&self) -> &[u8] {
        &self.bytes[self.field.value_range.clone()]
    }
}

impl PartialEq for EncodedValueItem<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EncodedValueItem<'_> {}

impl PartialOrd for EncodedValueItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EncodedValueItem<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.field.tag == TAG_NULL, other.field.tag == TAG_NULL) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {}
        }

        match self.field.tag.cmp(&other.field.tag) {
            Ordering::Equal => self.payload().cmp(other.payload()),
            ordering => ordering,
        }
    }
}

#[allow(dead_code)]
fn parse_encoded_value_item(bytes: &[u8]) -> io::Result<EncodedValueItem<'_>> {
    let (tag, next_offset) = read_u8_from_slice(bytes, 0)?;
    let (len, value_offset) = read_value_len_from_slice(bytes, next_offset)?;
    let value_len = usize::try_from(len).map_err(invalid_data)?;
    let value_end = value_offset.checked_add(value_len).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "tuple value length overflows usize")
    })?;
    let payload = bytes.get(value_offset..value_end).ok_or_else(unexpected_eof)?;
    validate_value_payload(tag, payload)?;
    if value_end != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes after tuple value payload",
        ));
    }
    Ok(EncodedValueItem { bytes, field: ValueField { tag, value_range: value_offset..value_end } })
}

fn unexpected_eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated tuple payload")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn read(bytes: &[u8]) -> io::Result<Tuple> {
        Tuple::read_from(&mut Cursor::new(bytes))
    }

    fn encoded_value(value: ValueRef<'_>) -> Vec<u8> {
        let mut bytes = Vec::new();
        value.write_to(&mut bytes).unwrap();
        bytes
    }

    fn assert_encoded_value_order(left: ValueRef<'_>, right: ValueRef<'_>, expected: Ordering) {
        let left = encoded_value(left);
        let right = encoded_value(right);
        assert_eq!(
            parse_encoded_value_item(&left)
                .unwrap()
                .cmp(&parse_encoded_value_item(&right).unwrap()),
            expected
        );
    }

    #[test]
    fn mixed_tuple_round_trips() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::String("hello".to_owned()),
            Value::Boolean(true),
            Value::Integer(-42),
            Value::Float(3.25),
            Value::UnsignedInteger(u64::MAX),
        ]);

        let bytes = tuple.to_bytes().unwrap();
        assert_eq!(read(&bytes).unwrap(), tuple);
    }

    #[test]
    fn owned_tuple_reads_from_bytes() {
        let tuple = Tuple::new(vec![Value::Integer(7), Value::String("seven".to_owned())]);

        assert_eq!(Tuple::from_bytes(&tuple.to_bytes().unwrap()).unwrap(), tuple);
    }

    #[test]
    fn owned_tuple_rejects_trailing_bytes() {
        let tuple = Tuple::new(vec![Value::Integer(1)]);
        let mut bytes = tuple.to_bytes().unwrap();
        bytes.push(0);

        let error = Tuple::from_bytes(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn empty_tuple_round_trips() {
        let tuple = Tuple::new(vec![]);

        let bytes = tuple.to_bytes().unwrap();
        assert_eq!(bytes, 0u32.to_le_bytes());
        assert_eq!(read(&bytes).unwrap(), tuple);
    }

    #[test]
    fn preserves_order_and_duplicates() {
        let tuple = Tuple::new(vec![
            Value::Integer(7),
            Value::String("same".to_owned()),
            Value::Integer(7),
            Value::String("same".to_owned()),
        ]);

        assert_eq!(read(&tuple.to_bytes().unwrap()).unwrap(), tuple);
    }

    #[test]
    fn writes_expected_bytes_for_small_tuple() {
        let tuple = Tuple::new(vec![Value::Boolean(false), Value::Integer(258)]);

        let mut expected = Vec::new();
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.push(TAG_BOOLEAN);
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.push(0);
        expected.push(TAG_INTEGER);
        expected.extend_from_slice(&4u32.to_be_bytes());
        expected.extend_from_slice(&encode_ordered_i32(258));

        assert_eq!(tuple.to_bytes().unwrap(), expected);
    }

    #[test]
    fn rejects_unknown_type_tag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(&0u32.to_be_bytes());

        let error = read(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_invalid_fixed_lengths() {
        for tag in [TAG_NULL, TAG_BOOLEAN, TAG_INTEGER, TAG_FLOAT, TAG_UNSIGNED_INTEGER] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&1u32.to_le_bytes());
            bytes.push(tag);
            let invalid_len = if tag == TAG_NULL { 1_u32 } else { 0 };
            bytes.extend_from_slice(&invalid_len.to_be_bytes());

            let error = read(&bytes).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn rejects_invalid_boolean_byte() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(TAG_BOOLEAN);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.push(2);

        let error = read(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_invalid_utf8_string() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(TAG_STRING);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.push(0xff);

        let error = read(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_truncated_stream() {
        let tuple = Tuple::new(vec![Value::String("abc".to_owned())]);
        let bytes = tuple.to_bytes().unwrap();

        let error = read(&bytes[..bytes.len() - 1]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn value_refs_iterate_owned_tuple_without_allocating_values() {
        let tuple = Tuple::new(vec![Value::String("hello".to_owned()), Value::Boolean(false)]);

        let values: Vec<_> = tuple.value_refs().collect();

        assert_eq!(values, vec![ValueRef::String("hello"), ValueRef::Boolean(false)]);
    }

    #[test]
    fn tuple_ref_serializes_like_owned_tuple() {
        let borrowed_values = [
            ValueRef::Null,
            ValueRef::String("hello"),
            ValueRef::Boolean(true),
            ValueRef::Integer(-42),
            ValueRef::Float(3.25),
            ValueRef::UnsignedInteger(u64::MAX),
        ];
        let tuple_ref = TupleRef::new(&borrowed_values);
        let owned_tuple = Tuple::new(borrowed_values.into_iter().map(Value::from).collect());

        assert_eq!(tuple_ref.to_bytes().unwrap(), owned_tuple.to_bytes().unwrap());
    }

    #[test]
    fn value_ref_serializes_single_tlv_item() {
        let mut bytes = Vec::new();
        ValueRef::Integer(258).write_to(&mut bytes).unwrap();

        let mut expected = Vec::new();
        expected.push(TAG_INTEGER);
        expected.extend_from_slice(&4u32.to_be_bytes());
        expected.extend_from_slice(&encode_ordered_i32(258));

        assert_eq!(bytes, expected);
    }

    #[test]
    fn tuple_view_parses_encoded_bytes_without_owning_values() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::String("hello".to_owned()),
            Value::Boolean(true),
            Value::Integer(-42),
            Value::Float(3.25),
            Value::UnsignedInteger(u64::MAX),
        ]);
        let bytes = tuple.to_bytes().unwrap();

        let view = TupleView::parse(&bytes).unwrap();

        assert_eq!(view.len(), 6);
        assert!(!view.is_empty());
        assert_eq!(view.bytes(), bytes);
        assert_eq!(view.values().collect::<Vec<_>>(), tuple.value_refs().collect::<Vec<_>>());
        assert_eq!((&view).into_iter().collect::<Vec<_>>(), tuple.value_refs().collect::<Vec<_>>());
    }

    #[test]
    fn encoded_tuple_view_copies_to_owned_tuple() {
        let tuple = Tuple::new(vec![Value::String("abc".to_owned()), Value::Integer(42)]);
        let bytes = tuple.to_bytes().unwrap();
        let view = EncodedTupleView::parse(&bytes).unwrap();

        assert_eq!(view.to_owned_tuple(), tuple);
    }

    #[test]
    fn tuple_view_rejects_trailing_bytes() {
        let tuple = Tuple::new(vec![Value::Integer(1)]);
        let mut bytes = tuple.to_bytes().unwrap();
        bytes.push(0);

        let error = TupleView::parse(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn tuple_view_rejects_invalid_payloads() {
        let cases = [
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&1u32.to_le_bytes());
                bytes.push(0xff);
                bytes.extend_from_slice(&0u32.to_be_bytes());
                bytes
            },
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&1u32.to_le_bytes());
                bytes.push(TAG_BOOLEAN);
                bytes.extend_from_slice(&1u32.to_be_bytes());
                bytes.push(2);
                bytes
            },
            {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&1u32.to_le_bytes());
                bytes.push(TAG_STRING);
                bytes.extend_from_slice(&1u32.to_be_bytes());
                bytes.push(0xff);
                bytes
            },
        ];

        for bytes in cases {
            let error = TupleView::parse(&bytes).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn tuple_view_rejects_truncated_payload() {
        let tuple = Tuple::new(vec![Value::String("abc".to_owned())]);
        let bytes = tuple.to_bytes().unwrap();

        let error = TupleView::parse(&bytes[..bytes.len() - 1]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn encoded_values_compare_in_logical_order() {
        assert_encoded_value_order(ValueRef::Null, ValueRef::Boolean(false), Ordering::Less);
        assert_encoded_value_order(
            ValueRef::Boolean(false),
            ValueRef::Boolean(true),
            Ordering::Less,
        );
        assert_encoded_value_order(ValueRef::Integer(-1), ValueRef::Integer(0), Ordering::Less);
        assert_encoded_value_order(ValueRef::Integer(0), ValueRef::Integer(1), Ordering::Less);
        assert_encoded_value_order(
            ValueRef::UnsignedInteger(9),
            ValueRef::UnsignedInteger(10),
            Ordering::Less,
        );
        assert_encoded_value_order(ValueRef::Float(-1.5), ValueRef::Float(0.0), Ordering::Less);
        assert_encoded_value_order(ValueRef::Float(0.0), ValueRef::Float(2.25), Ordering::Less);
        assert_encoded_value_order(ValueRef::String("a"), ValueRef::String("aa"), Ordering::Less);
        assert_encoded_value_order(ValueRef::String("aa"), ValueRef::String("b"), Ordering::Less);
    }

    #[test]
    fn fixed_width_payload_bytes_are_canonical_for_ordering() {
        let integers = [-42, -1, 0, 1, 42];
        for pair in integers.windows(2) {
            assert!(encode_ordered_i32(pair[0]) < encode_ordered_i32(pair[1]));
        }

        let unsigned = [0_u64, 1, 42, u64::MAX];
        for pair in unsigned.windows(2) {
            assert!(pair[0].to_be_bytes() < pair[1].to_be_bytes());
        }

        let floats = [-12.5_f32, -0.25, 0.0, 0.5, 9.75];
        for pair in floats.windows(2) {
            assert!(encode_ordered_f32(pair[0]).unwrap() < encode_ordered_f32(pair[1]).unwrap());
        }
    }

    #[test]
    fn rejects_little_endian_value_length_as_malformed_big_endian_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(TAG_BOOLEAN);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(1);

        let error = read(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_nan_float_during_serialization_and_validation() {
        let error = Tuple::new(vec![Value::Float(f32::NAN)]).to_bytes().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(TAG_FLOAT);
        bytes.extend_from_slice(&4u32.to_be_bytes());
        bytes.extend_from_slice(&(f32::NAN.to_bits() ^ 0x8000_0000).to_be_bytes());

        let error = read(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let error = TupleView::parse(&bytes).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
