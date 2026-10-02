//! Count-prefixed tuples of tagged values. The count is little-endian; each
//! field has a one-byte tag, a big-endian u32 length, and a payload. Numeric
//! payloads use big-endian order-preserving encodings for index keys.

use std::io;

use crate::core::error::TupleAllocationError;

const STRING: u8 = 1;
const BOOLEAN: u8 = 2;
const INTEGER: u8 = 3;
const FLOAT: u8 = 4;
const NULL: u8 = 5;
const UNSIGNED_INTEGER: u8 = 6;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    String(String),
    Boolean(bool),
    Integer(i32),
    Float(f32),
    UnsignedInteger(u64),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::String(v) => write!(f, "{v}"),
            Self::Boolean(v) => write!(f, "{v}"),
            Self::Integer(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::UnsignedInteger(v) => write!(f, "{v}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    String(&'a str),
    Boolean(bool),
    Integer(i32),
    Float(f32),
    UnsignedInteger(u64),
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::String(v) => Self::String(v),
            Value::Boolean(v) => Self::Boolean(*v),
            Value::Integer(v) => Self::Integer(*v),
            Value::Float(v) => Self::Float(*v),
            Value::UnsignedInteger(v) => Self::UnsignedInteger(*v),
        }
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::String(v) => Self::String(v.to_owned()),
            ValueRef::Boolean(v) => Self::Boolean(v),
            ValueRef::Integer(v) => Self::Integer(v),
            ValueRef::Float(v) => Self::Float(v),
            ValueRef::UnsignedInteger(v) => Self::UnsignedInteger(v),
        }
    }
}

/// Owned row values.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple(Vec<Value>);

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Self(values)
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn into_values(self) -> Vec<Value> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Ok(Self(TupleView::parse(bytes)?.values.into_iter().map(Value::from).collect()))
    }

    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        encode(self.0.iter().map(ValueRef::from), self.0.len())
    }
}

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Borrowed row values, used to encode without cloning strings.
pub struct TupleRef<'a>(&'a [ValueRef<'a>]);

impl<'a> TupleRef<'a> {
    pub fn new(values: &'a [ValueRef<'a>]) -> Self {
        Self(values)
    }

    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        encode(self.0.iter().copied(), self.0.len())
    }
}

/// Validated encoded row; strings borrow the original byte buffer.
#[derive(Debug)]
pub struct TupleView<'a> {
    values: Vec<ValueRef<'a>>,
}

impl<'a> TupleView<'a> {
    pub fn parse(bytes: &'a [u8]) -> io::Result<Self> {
        let mut input = bytes;
        let count = take(&mut input, 4)?;
        let count = u32::from_le_bytes([count[0], count[1], count[2], count[3]]) as usize;
        // Every field needs at least a tag and length, even NULL.
        if count > input.len() / 5 {
            return Err(eof());
        }
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|source| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                TupleAllocationError::Values { value_count: count, source },
            )
        })?;
        for _ in 0..count {
            let tag = take(&mut input, 1)?[0];
            let len = take(&mut input, 4)?;
            let len = u32::from_be_bytes([len[0], len[1], len[2], len[3]]) as usize;
            let payload = take(&mut input, len)?;
            values.push(decode(tag, payload)?);
        }
        if !input.is_empty() {
            return Err(invalid("trailing bytes after tuple"));
        }
        Ok(Self { values })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> + '_ {
        self.values.iter().copied()
    }
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
    if len > input.len() {
        return Err(eof());
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn eof() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "truncated tuple")
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn decode<'a>(tag: u8, payload: &'a [u8]) -> io::Result<ValueRef<'a>> {
    match (tag, payload) {
        (NULL, []) => Ok(ValueRef::Null),
        (STRING, bytes) => std::str::from_utf8(bytes)
            .map(ValueRef::String)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        (BOOLEAN, [0]) => Ok(ValueRef::Boolean(false)),
        (BOOLEAN, [1]) => Ok(ValueRef::Boolean(true)),
        (INTEGER, [a, b, c, d]) => Ok(ValueRef::Integer(
            (u32::from_be_bytes([*a, *b, *c, *d]) ^ 0x8000_0000).cast_signed(),
        )),
        (FLOAT, [a, b, c, d]) => {
            let ordered = u32::from_be_bytes([*a, *b, *c, *d]);
            let bits = if ordered & 0x8000_0000 == 0 { !ordered } else { ordered ^ 0x8000_0000 };
            let value = f32::from_bits(bits);
            if value.is_nan() {
                return Err(invalid("NaN tuple float"));
            }
            Ok(ValueRef::Float(if value == 0.0 { 0.0 } else { value }))
        }
        (UNSIGNED_INTEGER, [a, b, c, d, e, f, g, h]) => {
            Ok(ValueRef::UnsignedInteger(u64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h])))
        }
        (NULL | BOOLEAN | INTEGER | FLOAT | UNSIGNED_INTEGER, _) => {
            Err(invalid("invalid tuple value length or payload"))
        }
        _ => Err(invalid("unknown tuple value tag")),
    }
}

fn encode<'a>(values: impl Iterator<Item = ValueRef<'a>>, count: usize) -> io::Result<Vec<u8>> {
    let count = u32::try_from(count).map_err(|_count_out_of_range| {
        io::Error::new(io::ErrorKind::InvalidInput, "too many tuple values")
    })?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&count.to_le_bytes());
    for value in values {
        let (tag, payload): (u8, &[u8]);
        // Fixed-size payloads live in this iteration until they are written.
        let mut fixed = [0; 8];
        match value {
            ValueRef::Null => {
                tag = NULL;
                payload = &[];
            }
            ValueRef::String(v) => {
                tag = STRING;
                payload = v.as_bytes();
            }
            ValueRef::Boolean(v) => {
                tag = BOOLEAN;
                fixed[0] = v as u8;
                payload = &fixed[..1];
            }
            ValueRef::Integer(v) => {
                tag = INTEGER;
                fixed[..4].copy_from_slice(&(v.cast_unsigned() ^ 0x8000_0000).to_be_bytes());
                payload = &fixed[..4];
            }
            ValueRef::Float(v) => {
                if v.is_nan() {
                    return Err(invalid("NaN tuple float"));
                }
                tag = FLOAT;
                let bits = if v == 0.0 { 0.0_f32.to_bits() } else { v.to_bits() };
                let ordered = if bits & 0x8000_0000 == 0 { bits ^ 0x8000_0000 } else { !bits };
                fixed[..4].copy_from_slice(&ordered.to_be_bytes());
                payload = &fixed[..4];
            }
            ValueRef::UnsignedInteger(v) => {
                tag = UNSIGNED_INTEGER;
                fixed = v.to_be_bytes();
                payload = &fixed;
            }
        }
        let len = u32::try_from(payload.len()).map_err(|_length_out_of_range| {
            io::Error::new(io::ErrorKind::InvalidInput, "tuple value too large")
        })?;
        bytes.push(tag);
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(payload);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::{Tuple, TupleRef, TupleView, Value, ValueRef};

    #[test]
    fn tuple_round_trips_every_value_type() {
        let tuple = Tuple::new(vec![
            Value::Null,
            Value::String("hello".to_owned()),
            Value::Boolean(true),
            Value::Integer(-42),
            Value::Float(-1.5),
            Value::UnsignedInteger(u64::MAX),
        ]);

        let bytes = tuple.to_bytes().unwrap();
        assert_eq!(Tuple::from_bytes(&bytes).unwrap(), tuple);

        let view = TupleView::parse(&bytes).unwrap();
        assert_eq!(view.len(), tuple.len());
        assert_eq!(
            view.values().collect::<Vec<_>>(),
            vec![
                ValueRef::Null,
                ValueRef::String("hello"),
                ValueRef::Boolean(true),
                ValueRef::Integer(-42),
                ValueRef::Float(-1.5),
                ValueRef::UnsignedInteger(u64::MAX),
            ]
        );
    }

    #[test]
    fn tuple_encoding_uses_documented_byte_order() {
        let values = [
            ValueRef::Null,
            ValueRef::String("hi"),
            ValueRef::Boolean(true),
            ValueRef::Integer(-42),
            ValueRef::Float(-1.0),
            ValueRef::UnsignedInteger(42),
        ];

        assert_eq!(
            TupleRef::new(&values).to_bytes().unwrap(),
            vec![
                6, 0, 0, 0, // field count (little-endian)
                5, 0, 0, 0, 0, // NULL
                1, 0, 0, 0, 2, b'h', b'i', // string
                2, 0, 0, 0, 1, 1, // boolean
                3, 0, 0, 0, 4, 0x7f, 0xff, 0xff, 0xd6, // integer
                4, 0, 0, 0, 4, 0x40, 0x7f, 0xff, 0xff, // float
                6, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 42, // unsigned integer
            ]
        );
    }

    #[test]
    fn tuple_rejects_truncated_and_invalid_data() {
        let truncated = TupleView::parse(&[1, 0, 0, 0]);
        assert_eq!(truncated.unwrap_err().kind(), ErrorKind::UnexpectedEof);

        let unknown_tag = TupleView::parse(&[1, 0, 0, 0, 99, 0, 0, 0, 0]);
        assert_eq!(unknown_tag.unwrap_err().kind(), ErrorKind::InvalidData);

        let trailing_bytes = TupleView::parse(&[0, 0, 0, 0, 0]);
        assert_eq!(trailing_bytes.unwrap_err().kind(), ErrorKind::InvalidData);
    }
}
