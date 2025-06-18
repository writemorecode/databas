use std::io::{Cursor, Read, Write};

use crate::record::SerializationError;

fn zigzag_encode_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode_i64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

pub fn varint_encode_signed(
    value: i64,
    buf: &mut Cursor<&mut [u8]>,
) -> Result<usize, SerializationError> {
    let zigzag_value = zigzag_encode_i64(value);
    varint_encode(zigzag_value, buf)
}

pub fn varint_decode_signed(buf: &mut Cursor<&[u8]>) -> Result<i64, SerializationError> {
    let value = varint_decode(buf)?;
    let decoded = zigzag_decode_i64(value);
    Ok(decoded)
}

pub fn varint_size_signed(value: i64) -> usize {
    let zigzag_value = zigzag_encode_i64(value);
    varint_size(zigzag_value)
}

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
            let decoded = varint_decode(&mut Cursor::new(&buf[..])).unwrap();
            assert_eq!(value, decoded);
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

    #[test]
    fn test_encode_decode_signed_varint() {
        let values = [
            0_i64,
            -127,
            -128,
            -16383,
            -16384,
            i16::MAX as i64,
            i16::MIN as i64,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ];
        let mut buf = [0u8; 16];
        for value in values {
            let mut write_cursor = Cursor::new(&mut buf[..]);
            varint_encode_signed(value, &mut write_cursor).unwrap();
            let mut read_cursor = Cursor::new(&buf[..]);
            let decoded_value = varint_decode_signed(&mut read_cursor).unwrap();
            assert_eq!(value, decoded_value);
        }
    }
}
