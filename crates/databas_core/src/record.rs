use std::{
    io::{Cursor, Read, Write},
    string::FromUtf8Error,
};

use crate::varint::{
    varint_decode, varint_decode_signed, varint_encode, varint_encode_signed, varint_size,
    varint_size_signed,
};

#[derive(Debug)]
pub enum SerializationError {
    InvalidData(String),
    InvalidTag(u8),
    BufferTooSmall { required: usize, available: usize },
    VarIntBufferTooSmall { available: usize },
    InvalidVarInt,
    StringTooLong,
    InvalidSignedInteger(u64),
    IoError(std::io::Error),
}

impl From<std::io::Error> for SerializationError {
    fn from(err: std::io::Error) -> Self {
        SerializationError::IoError(err)
    }
}

impl From<FromUtf8Error> for SerializationError {
    fn from(err: FromUtf8Error) -> Self {
        SerializationError::InvalidData(err.to_string())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    UnsignedInteger(u64),
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

enum Tag {
    Integer,
    Float,
    String,
    Boolean,
    Null,
    UnsignedInteger,
}

impl TryFrom<u8> for Tag {
    type Error = SerializationError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Tag::UnsignedInteger),
            2 => Ok(Tag::Integer),
            3 => Ok(Tag::Float),
            4 => Ok(Tag::String),
            5 => Ok(Tag::Boolean),
            6 => Ok(Tag::Null),
            other => Err(SerializationError::InvalidTag(other)),
        }
    }
}

impl Value {
    fn serialized_size(&self) -> usize {
        let data_size = match self {
            Value::UnsignedInteger(i) => varint_size(*i),
            Value::Integer(i) => varint_size_signed(*i),
            Value::Float(f) => std::mem::size_of_val(f),
            Value::String(s) => varint_size(s.len() as u64) + s.len(),
            Value::Boolean(b) => std::mem::size_of_val(b),
            Value::Null => 0,
        };
        let tag_size = std::mem::size_of::<u8>();
        data_size + tag_size
    }

    fn tag(&self) -> u8 {
        match self {
            Value::UnsignedInteger(_) => 1,
            Value::Integer(_) => 2,
            Value::Float(_) => 3,
            Value::String(_) => 4,
            Value::Boolean(_) => 5,
            Value::Null => 6,
        }
    }

    pub fn serialize(&self, buf: &mut Cursor<&mut [u8]>) -> Result<(), SerializationError> {
        buf.write_all(&self.tag().to_le_bytes())?;
        match self {
            Value::UnsignedInteger(i) => {
                varint_encode(*i as u64, buf)?;
            }
            Value::Integer(i) => {
                varint_encode_signed(*i, buf)?;
            }
            Value::Float(f) => buf.write_all(&f.to_le_bytes())?,
            Value::String(s) => {
                let len: u32 = s.len().try_into().map_err(|_| SerializationError::StringTooLong)?;
                varint_encode(len as u64, buf)?;
                buf.write_all(s.as_bytes())?;
            }
            Value::Boolean(b) => buf.write_all(&[*b as u8])?,
            Value::Null => {}
        }

        Ok(())
    }

    pub fn deserialize(reader: &mut Cursor<&[u8]>) -> Result<Value, SerializationError> {
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf)?;
        let tag = Tag::try_from(tag_buf[0])?;

        match tag {
            Tag::UnsignedInteger => {
                let uint = varint_decode(reader)?;
                Ok(Value::UnsignedInteger(uint))
            }
            Tag::Integer => {
                let int = varint_decode_signed(reader)?;
                Ok(Value::Integer(int))
            }
            Tag::Float => {
                let mut float_buf = [0u8; 8];
                reader.read_exact(&mut float_buf)?;
                let float = f64::from_le_bytes(float_buf);
                Ok(Value::Float(float))
            }
            Tag::String => {
                let len: usize = varint_decode(reader)?
                    .try_into()
                    .map_err(|_| SerializationError::StringTooLong)?;

                let mut str_buf = vec![0u8; len as usize];
                reader.read_exact(&mut str_buf)?;
                let string = String::from_utf8(str_buf)?;
                Ok(Value::String(string))
            }
            Tag::Boolean => {
                let mut bool_buf = [0u8; 1];
                reader.read_exact(&mut bool_buf)?;
                let bool = bool_buf[0] != 0;
                Ok(Value::Boolean(bool))
            }
            Tag::Null => Ok(Value::Null),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Record(Vec<Value>);

impl Record {
    fn serialized_size(&self) -> usize {
        self.0.iter().map(|value| value.serialized_size()).sum()
    }

    pub fn serialize(&self, buffer: &mut Cursor<&mut [u8]>) -> Result<(), SerializationError> {
        let record_size = self.serialized_size();
        varint_encode(record_size as u64, buffer)?;
        for value in &self.0 {
            value.serialize(buffer)?;
        }
        Ok(())
    }

    pub fn deserialize(reader: &mut Cursor<&[u8]>) -> Result<Self, SerializationError> {
        let record_size = varint_decode(reader)?;
        let mut values = Vec::new();
        let mut bytes_read = 0;
        let record_size = record_size as usize;
        while bytes_read < record_size {
            let value = Value::deserialize(reader)?;
            let value_size = value.serialized_size();
            bytes_read += value_size;
            values.push(value);
        }
        Ok(Record(values))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{Record, SerializationError, Value};

    #[test]
    fn test_serialize_deserialize_record() {
        let record = Record(vec![
            Value::UnsignedInteger(1234),
            Value::Integer(-42),
            Value::Integer(500),
            Value::Float(3.1415),
            Value::Boolean(true),
            Value::String("hello world".to_string()),
        ]);

        let mut buffer = [0u8; 64];
        let mut cursor = Cursor::new(&mut buffer[..]);
        record.serialize(&mut cursor).expect("serialization failed");

        let mut cursor = Cursor::new(&buffer[..]);
        let deserialize_record = Record::deserialize(&mut cursor).expect("deserialization failed");
        assert_eq!(record, deserialize_record);
    }

    #[test]
    fn test_serialize_deserialize_multiple_record() {
        let values = vec![
            Value::UnsignedInteger(900),
            Value::Integer(-42),
            Value::Integer(500),
            Value::Float(3.1415),
            Value::Boolean(true),
            Value::String("hello world".to_string()),
        ];

        let mut buffer = [0u8; 128];
        let mut cursor = Cursor::new(&mut buffer[..]);
        for value in &values {
            let record = Record(vec![value.clone()]);
            record.serialize(&mut cursor).expect("serialization failed");
        }

        let mut cursor = Cursor::new(&buffer[..]);
        let mut deserialized_values = Vec::with_capacity(values.len());
        for value in &values {
            let record = Record(vec![value.clone()]);
            let deserialize_record =
                Record::deserialize(&mut cursor).expect("deserialization failed");
            assert_eq!(record, deserialize_record);
            deserialized_values.push(value.clone());
        }
        assert_eq!(values, deserialized_values);
    }

    #[test]
    fn test_serialize_into_buffer_too_small() {
        let record = Record(vec![Value::Integer(42)]);
        let mut buffer = [0u8; 1];
        let result = record.serialize(&mut Cursor::new(&mut buffer[..]));
        assert!(matches!(result, Err(SerializationError::IoError(_))));
    }

    #[test]
    fn test_deserialize_from_empty_buffer() {
        let record = Record::deserialize(&mut Cursor::new(&mut []));
        assert!(matches!(record, Err(SerializationError::IoError(_))));
    }

    #[test]
    fn test_deserialize_invalid_tag() {
        let invalid_tag: u8 = 99;
        let buffer = [invalid_tag; 1];
        let err = Value::deserialize(&mut Cursor::new(&buffer[..])).unwrap_err();
        assert!(matches!(err, SerializationError::InvalidTag(tag) if tag == invalid_tag));
    }

    #[test]
    fn test_serialized_size_value_varint() {
        assert_eq!(Value::UnsignedInteger(100).serialized_size(), 1 + 1);
        assert_eq!(Value::Integer(-200).serialized_size(), 1 + 2);
        assert_eq!(Value::String("abcd".to_string()).serialized_size(), 1 + 1 + 4);
    }

    #[test]
    fn test_serialize_deserialize_signed_integer_record() {
        let record = Record(vec![Value::Integer(-42)]);
        let mut buffer = [0u8; 8];
        let mut cursor = Cursor::new(&mut buffer[..]);
        record.serialize(&mut cursor).unwrap();
        let mut cursor = Cursor::new(&buffer[..]);
        let deserialized_record = Record::deserialize(&mut cursor).unwrap();
        assert_eq!(record, deserialized_record);
    }
}
