//! Databas TCP wire-protocol primitives.
//!
//! The protocol is a versioned, length-prefixed binary protocol. See
//! `docs/wire-protocol.md` for the byte-level specification and compatibility
//! rules. This module exposes stable server error codes; frame codecs remain an
//! implementation detail shared by the client and server.

use std::io::{self, Read, Write};

use thiserror::Error;

use crate::core::Value;

pub(crate) const MAGIC: [u8; 4] = *b"DBAS";
pub(crate) const VERSION: u16 = 1;
pub(crate) const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
const HEADER_LEN: usize = 12;

pub(crate) const STARTUP: u8 = 0x01;
pub(crate) const READY: u8 = 0x02;
pub(crate) const QUERY: u8 = 0x03;
pub(crate) const ROW: u8 = 0x10;
pub(crate) const COMPLETE: u8 = 0x11;
pub(crate) const ERROR: u8 = 0x7f;

pub(crate) const COMPLETE_ROWS: u8 = 0x00;
pub(crate) const COMPLETE_EXPLAIN: u8 = 0x01;
pub(crate) const COMPLETE_ROWS_AFFECTED: u8 = 0x02;
pub(crate) const COMPLETE_SCHEMA_AFFECTED: u8 = 0x03;
pub(crate) const COMPLETE_COMMAND_OK: u8 = 0x04;

/// Stable error code sent by a Databas server.
///
/// Clients should make decisions using this code and treat the accompanying
/// message as a human-readable diagnostic. Unknown codes are retained so a
/// version-1 client can report codes introduced by a newer server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// A frame or protocol state was invalid.
    ProtocolError,
    /// The peer uses an unsupported protocol version.
    UnsupportedProtocolVersion,
    /// The requested database name is not served by this endpoint.
    DatabaseNotFound,
    /// A request payload is invalid.
    InvalidRequest,
    /// SQL could not be parsed.
    SyntaxError,
    /// A schema or data constraint was violated.
    ConstraintViolation,
    /// An argument or referenced database object is invalid.
    InvalidArgument,
    /// A database or protocol resource limit was exceeded.
    LimitExceeded,
    /// Transaction state made the request invalid.
    TransactionError,
    /// SQL planning failed.
    PlanningError,
    /// Query execution failed.
    ExecutionError,
    /// Database storage encountered an I/O failure.
    StorageIoError,
    /// The database or WAL is corrupt.
    DatabaseCorruption,
    /// The server encountered an internal invariant failure.
    InternalError,
    /// A code unknown to this client implementation.
    Unknown(u16),
}

impl ErrorCode {
    /// Returns the numeric representation used on the wire.
    pub const fn as_u16(self) -> u16 {
        match self {
            Self::ProtocolError => 1,
            Self::UnsupportedProtocolVersion => 2,
            Self::DatabaseNotFound => 3,
            Self::InvalidRequest => 4,
            Self::SyntaxError => 100,
            Self::ConstraintViolation => 200,
            Self::InvalidArgument => 201,
            Self::LimitExceeded => 202,
            Self::TransactionError => 203,
            Self::PlanningError => 300,
            Self::ExecutionError => 400,
            Self::StorageIoError => 500,
            Self::DatabaseCorruption => 501,
            Self::InternalError => 599,
            Self::Unknown(code) => code,
        }
    }

    pub(crate) const fn from_u16(code: u16) -> Self {
        match code {
            1 => Self::ProtocolError,
            2 => Self::UnsupportedProtocolVersion,
            3 => Self::DatabaseNotFound,
            4 => Self::InvalidRequest,
            100 => Self::SyntaxError,
            200 => Self::ConstraintViolation,
            201 => Self::InvalidArgument,
            202 => Self::LimitExceeded,
            203 => Self::TransactionError,
            300 => Self::PlanningError,
            400 => Self::ExecutionError,
            500 => Self::StorageIoError,
            501 => Self::DatabaseCorruption,
            599 => Self::InternalError,
            other => Self::Unknown(other),
        }
    }
}

/// Failure while reading or writing protocol data.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// The underlying stream failed.
    #[error("protocol I/O error: {0}")]
    Io(#[from] io::Error),
    /// A frame did not begin with the Databas magic bytes.
    #[error("invalid protocol magic")]
    InvalidMagic,
    /// A peer sent a protocol version this implementation does not support.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    /// A frame payload exceeds the protocol limit.
    #[error("frame payload is {length} bytes; maximum is {MAX_PAYLOAD_LEN}")]
    FrameTooLarge { length: usize },
    /// A frame or message payload is malformed.
    #[error("malformed protocol message: {0}")]
    Malformed(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Frame {
    pub kind: u8,
    pub payload: Vec<u8>,
}

pub(crate) fn read_frame(reader: &mut impl Read) -> Result<Option<Frame>, ProtocolError> {
    let mut header = [0_u8; HEADER_LEN];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    reader.read_exact(&mut header[1..])?;

    if header[..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    if header[7] != 0 {
        return Err(ProtocolError::Malformed("frame flags must be zero"));
    }

    let length = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    if length > MAX_PAYLOAD_LEN {
        return Err(ProtocolError::FrameTooLarge { length });
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload)?;
    Ok(Some(Frame { kind: header[6], payload }))
}

pub(crate) fn write_frame(
    writer: &mut impl Write,
    kind: u8,
    payload: &[u8],
) -> Result<(), ProtocolError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(ProtocolError::FrameTooLarge { length: payload.len() });
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::FrameTooLarge { length: payload.len() })?;
    let mut header = [0_u8; HEADER_LEN];
    header[..4].copy_from_slice(&MAGIC);
    header[4..6].copy_from_slice(&VERSION.to_be_bytes());
    header[6] = kind;
    header[8..12].copy_from_slice(&length.to_be_bytes());
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    Ok(())
}

pub(crate) fn encode_error(code: ErrorCode, message: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + message.len());
    payload.extend_from_slice(&code.as_u16().to_be_bytes());
    payload.extend_from_slice(message.as_bytes());
    payload
}

pub(crate) fn decode_error(payload: &[u8]) -> Result<(ErrorCode, String), ProtocolError> {
    if payload.len() < 2 {
        return Err(ProtocolError::Malformed("error response has no error code"));
    }
    let code = ErrorCode::from_u16(u16::from_be_bytes([payload[0], payload[1]]));
    let message = std::str::from_utf8(&payload[2..])
        .map_err(|_| ProtocolError::Malformed("error message is not UTF-8"))?;
    Ok((code, message.to_owned()))
}

pub(crate) fn encode_row(values: &[Value]) -> Result<Vec<u8>, ProtocolError> {
    let count = u32::try_from(values.len())
        .map_err(|_| ProtocolError::Malformed("row has too many values"))?;
    let mut payload = Vec::new();
    payload.extend_from_slice(&count.to_be_bytes());
    for value in values {
        match value {
            Value::Null => payload.push(0x00),
            Value::String(value) => {
                payload.push(0x01);
                let length = u32::try_from(value.len())
                    .map_err(|_| ProtocolError::Malformed("text value is too large"))?;
                payload.extend_from_slice(&length.to_be_bytes());
                payload.extend_from_slice(value.as_bytes());
            }
            Value::Boolean(value) => {
                payload.push(0x02);
                payload.push(u8::from(*value));
            }
            Value::Integer(value) => {
                payload.push(0x03);
                payload.extend_from_slice(&value.to_be_bytes());
            }
            Value::Float(value) => {
                if value.is_nan() {
                    return Err(ProtocolError::Malformed("NaN float value"));
                }
                payload.push(0x04);
                payload.extend_from_slice(&value.to_bits().to_be_bytes());
            }
            Value::UnsignedInteger(value) => {
                payload.push(0x05);
                payload.extend_from_slice(&value.to_be_bytes());
            }
        }
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(ProtocolError::FrameTooLarge { length: payload.len() });
        }
    }
    Ok(payload)
}

pub(crate) fn decode_row(payload: &[u8]) -> Result<Vec<Value>, ProtocolError> {
    let mut decoder = Decoder::new(payload);
    let count = decoder.u32()? as usize;
    if count > decoder.remaining() {
        return Err(ProtocolError::Malformed("row value count exceeds payload"));
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| ProtocolError::Malformed("cannot allocate row values"))?;
    for _ in 0..count {
        values.push(match decoder.u8()? {
            0x00 => Value::Null,
            0x01 => {
                let length = decoder.u32()? as usize;
                let bytes = decoder.bytes(length)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| ProtocolError::Malformed("text value is not UTF-8"))?;
                Value::String(value.to_owned())
            }
            0x02 => match decoder.u8()? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                _ => return Err(ProtocolError::Malformed("invalid boolean value")),
            },
            0x03 => Value::Integer(i32::from_be_bytes(decoder.array()?)),
            0x04 => {
                let value = f32::from_bits(u32::from_be_bytes(decoder.array()?));
                if value.is_nan() {
                    return Err(ProtocolError::Malformed("NaN float value"));
                }
                Value::Float(value)
            }
            0x05 => Value::UnsignedInteger(u64::from_be_bytes(decoder.array()?)),
            _ => return Err(ProtocolError::Malformed("unknown row value tag")),
        });
    }
    if decoder.remaining() != 0 {
        return Err(ProtocolError::Malformed("row has trailing bytes"));
    }
    Ok(values)
}

struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.input.len() - self.position
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or(ProtocolError::Malformed("message payload is truncated"))?;
        let bytes = &self.input[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        self.bytes(N)?
            .try_into()
            .map_err(|_| ProtocolError::Malformed("message payload is truncated"))
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trips() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, QUERY, b"SELECT 1;").unwrap();

        assert_eq!(
            read_frame(&mut bytes.as_slice()).unwrap(),
            Some(Frame { kind: QUERY, payload: b"SELECT 1;".to_vec() })
        );
    }

    #[test]
    fn row_round_trips_all_value_types() {
        let values = vec![
            Value::Null,
            Value::String("hello".to_owned()),
            Value::Boolean(true),
            Value::Integer(-42),
            Value::Float(1.5),
            Value::UnsignedInteger(u64::MAX),
        ];

        let encoded = encode_row(&values).unwrap();

        assert_eq!(decode_row(&encoded).unwrap(), values);
    }

    #[test]
    fn unknown_error_codes_are_preserved() {
        assert_eq!(ErrorCode::from_u16(777), ErrorCode::Unknown(777));
        assert_eq!(ErrorCode::Unknown(777).as_u16(), 777);
    }
}
