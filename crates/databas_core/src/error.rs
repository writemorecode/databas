use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum StorageError {
    SerializationError(SerializationError),
}

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
