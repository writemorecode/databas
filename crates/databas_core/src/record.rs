use std::{
    io::{Read, Write},
    string::FromUtf8Error,
};

#[derive(Debug, PartialEq)]
pub enum SerializationError {
    UnexpectedEof,
    InvalidData(String),
    InvalidTag(u8),
    BufferTooSmall { required: usize, available: usize },
    VarIntBufferTooSmall { available: usize },
    InvalidVarInt,
    StringTooLong,
}

impl From<std::io::Error> for SerializationError {
    fn from(_err: std::io::Error) -> Self {
        SerializationError::UnexpectedEof
    }
}

impl From<FromUtf8Error> for SerializationError {
    fn from(err: FromUtf8Error) -> Self {
        SerializationError::InvalidData(err.to_string())
    }
}

#[derive(Debug, PartialEq)]
pub enum Value {
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
}

impl TryFrom<u8> for Tag {
    type Error = SerializationError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Tag::Integer),
            2 => Ok(Tag::Float),
            3 => Ok(Tag::String),
            4 => Ok(Tag::Boolean),
            5 => Ok(Tag::Null),
            other => Err(SerializationError::InvalidTag(other)),
        }
    }
}

impl Value {
    fn serialized_size(&self) -> usize {
        let data_size = match self {
            Value::Integer(i) => std::mem::size_of_val(i),
            Value::Float(f) => std::mem::size_of_val(f),
            Value::String(s) => std::mem::size_of::<u32>() + s.len(),
            Value::Boolean(b) => std::mem::size_of_val(b),
            Value::Null => 0,
        };
        let tag_size = std::mem::size_of::<u8>();
        data_size + tag_size
    }

    fn tag(&self) -> u8 {
        match self {
            Value::Integer(_) => 1,
            Value::Float(_) => 2,
            Value::String(_) => 3,
            Value::Boolean(_) => 4,
            Value::Null => 5,
        }
    }

    pub fn serialize(&self, mut buf: &mut [u8]) -> Result<(), SerializationError> {
        let required_size = self.serialized_size();
        if required_size > buf.len() {
            return Err(SerializationError::BufferTooSmall {
                required: required_size,
                available: buf.len(),
            });
        }
        buf.write_all(&self.tag().to_le_bytes())?;
        match self {
            Value::Integer(i) => buf.write_all(&i.to_le_bytes())?,
            Value::Float(f) => buf.write_all(&f.to_le_bytes())?,
            Value::String(s) => {
                let len: u32 = s.len().try_into().map_err(|_| SerializationError::StringTooLong)?;
                buf.write_all(&len.to_le_bytes())?;
                buf.write_all(s.as_bytes())?;
            }
            Value::Boolean(b) => buf.write_all(&[*b as u8])?,
            Value::Null => {}
        }

        Ok(())
    }

    pub fn deserialize(reader: &mut &[u8]) -> Result<Value, SerializationError> {
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf)?;
        let tag = Tag::try_from(tag_buf[0])?;

        match tag {
            Tag::Integer => {
                let mut int_buf = [0u8; 8];
                reader.read_exact(&mut int_buf)?;
                let integer = i64::from_le_bytes(int_buf);
                Ok(Value::Integer(integer))
            }
            Tag::Float => {
                let mut float_buf = [0u8; 8];
                reader.read_exact(&mut float_buf)?;
                let float = f64::from_le_bytes(float_buf);
                Ok(Value::Float(float))
            }
            Tag::String => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf);

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
        let data_size: usize = self.0.iter().map(|value| value.serialized_size()).sum();
        std::mem::size_of::<u64>() + data_size
    }

    pub fn serialize(&self, mut buffer: &mut [u8]) -> Result<(), SerializationError> {
        let record_size = self.serialized_size();
        if buffer.len() < record_size {
            return Err(SerializationError::BufferTooSmall {
                required: record_size,
                available: buffer.len(),
            });
        }

        let record_size_bytes = (record_size as u64).to_le_bytes();
        buffer.write_all(&record_size_bytes)?;

        for value in &self.0 {
            let value_size = value.serialized_size();
            value.serialize(buffer)?;
            buffer = &mut buffer[value_size..];
        }

        Ok(())
    }

    pub fn deserialize(reader: &[u8]) -> Result<Self, SerializationError> {
        let mut reader = reader;
        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf)?;
        let record_size = u64::from_le_bytes(size_buf) as usize;

        let mut values = Vec::new();
        let mut bytes_read = 8;

        while bytes_read < record_size {
            let value = Value::deserialize(&mut reader)?;
            let value_size = value.serialized_size();
            bytes_read += value_size;
            values.push(value);
        }
        Ok(Record(values))
    }
}

#[cfg(test)]
mod tests {
    use super::{Record, SerializationError, Value};

    #[test]
    fn test_serialize_deserialize_record() {
        let record = Record(vec![
            Value::Integer(42),
            Value::Float(3.1415),
            Value::Boolean(true),
            Value::String("hello world".to_string()),
        ]);

        let mut buffer = [0u8; 64];
        record.serialize(&mut buffer[..]).expect("serialization failed");

        let deserialize_record = Record::deserialize(&buffer[..]).expect("deserialization failed");
        assert_eq!(record, deserialize_record);
    }

    #[test]
    fn test_serialize_into_buffer_too_small() {
        let record = Record(vec![Value::Integer(42)]);
        let mut buffer = [0u8; 4];
        let result = record.serialize(&mut buffer[..]);
        assert!(matches!(result, Err(SerializationError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_deserialize_from_empty_buffer() {
        assert_eq!(Record::deserialize(&[]), Err(SerializationError::UnexpectedEof));
    }

    #[test]
    fn test_deserialize_invalid_tag() {
        let invalid_tag: u8 = 6;
        let buffer = [invalid_tag; 1];
        assert_eq!(
            Value::deserialize(&mut &buffer[..]),
            Err(SerializationError::InvalidTag(invalid_tag))
        );
    }
}
