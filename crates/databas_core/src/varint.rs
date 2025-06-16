use std::io::{Cursor, Read, Write};

use crate::record::SerializationError;

pub fn varint_encode(value: u64, buf: &mut Cursor<&mut [u8]>) -> Result<usize, SerializationError> {
    if value <= 0x7F {
        buf.write_all(&[(value & 0x7F) as u8])?;
        return Ok(1);
    }
    let mut value = value;
    let mut n = 0;
    while value != 0 {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.write_all(&[byte])?;
        n += 1;
    }
    Ok(n)
}

pub fn varint_decode(buf: &mut Cursor<&[u8]>) -> Result<u64, SerializationError> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut temp = [0u8; 1];
    for _ in 0..10 {
        buf.read_exact(&mut temp)?;
        let byte = temp[0];
        let value_bits = (byte & 0x7F) as u64;
        result |= value_bits << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(SerializationError::InvalidVarInt)
}

pub fn varint_size(value: u64) -> usize {
    if value <= 0x7F {
        return 1;
    }
    let mut size = 0;
    let mut value = value;
    while value != 0 {
        size += 1;
        value >>= 7;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encode_decode() {
        // Encoding u64 as varint requires 10 bytes
        // SQLite is limited to signed types
        // An i64 varint requires only 9 bytes
        let mut buf = [0u8; 10];
        let values: Vec<u64> = vec![
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            2097152,
            u32::MAX as u64,
            u64::MAX,
        ];
        for value in values {
            let encoded_size = varint_encode(value as u64, &mut Cursor::new(&mut buf[..])).unwrap();
            let decoded = varint_decode(&mut Cursor::new(&buf[..]));
            assert_eq!(Ok(value), decoded);
            assert_eq!(encoded_size, varint_size(value));
        }
    }

    #[test]
    fn test_varint_encode_decode_multiple() {
        let mut buf = [0u8; 128];
        let values =
            [0, 1, 127, 128, 255, 256, 16383, 16384, 2097151, 2097152, u32::MAX as u64, u64::MAX];
        let mut write_cursor = Cursor::new(&mut buf[..]);
        for value in &values {
            varint_encode(*value as u64, &mut write_cursor).unwrap();
        }
        let mut read_cursor = Cursor::new(&buf[..]);
        for value in &values {
            let decoded = varint_decode(&mut read_cursor).unwrap();
            assert_eq!(*value, decoded);
        }
    }
}
