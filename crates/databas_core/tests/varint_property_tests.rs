use databas_core::varint::{
    varint_decode, varint_decode_signed, varint_encode, varint_encode_signed, varint_size,
    varint_size_signed,
};
use proptest::prelude::*;
use std::io::Cursor;
use test_strategy::proptest;

#[proptest]
fn test_unsigned_varint_roundtrip(value: u64) {
    let mut buf = [0u8; 10]; // Maximum size for u64 varint

    // Encode the value
    let mut write_cursor = Cursor::new(&mut buf[..]);
    let encoded_size = varint_encode(value, &mut write_cursor).unwrap();

    // Decode the value
    let mut read_cursor = Cursor::new(&buf[..]);
    let decoded_value = varint_decode(&mut read_cursor).unwrap();

    // Verify roundtrip correctness
    prop_assert_eq!(value, decoded_value);

    // Verify size calculation is correct
    prop_assert_eq!(encoded_size, varint_size(value));
}

#[proptest]
fn test_signed_varint_roundtrip(value: i64) {
    let mut buf = [0u8; 10]; // Maximum size for zigzag-encoded i64 varint

    // Encode the value
    let mut write_cursor = Cursor::new(&mut buf[..]);
    let encoded_size = varint_encode_signed(value, &mut write_cursor).unwrap();

    // Decode the value
    let mut read_cursor = Cursor::new(&buf[..]);
    let decoded_value = varint_decode_signed(&mut read_cursor).unwrap();

    // Verify roundtrip correctness
    prop_assert_eq!(value, decoded_value);

    // Verify size calculation is correct
    prop_assert_eq!(encoded_size, varint_size_signed(value));
}

#[proptest]
fn test_multiple_unsigned_varints_in_sequence(values: Vec<u64>) {
    let mut buf = vec![0u8; values.len() * 10]; // Allocate enough space

    // Encode all values sequentially
    let mut write_cursor = Cursor::new(&mut buf[..]);
    let mut total_encoded_size = 0;
    for &value in &values {
        let size = varint_encode(value, &mut write_cursor).unwrap();
        total_encoded_size += size;
    }

    // Decode all values sequentially
    let mut read_cursor = Cursor::new(&buf[..]);
    let mut decoded_values = Vec::new();
    for _ in 0..values.len() {
        let decoded = varint_decode(&mut read_cursor).unwrap();
        decoded_values.push(decoded);
    }

    // Verify total size calculation
    let expected_total_size: usize = values.iter().map(|&v| varint_size(v)).sum();

    // Verify all values were correctly encoded and decoded
    prop_assert_eq!(values, decoded_values);
    prop_assert_eq!(total_encoded_size, expected_total_size);
}

#[proptest]
fn test_multiple_signed_varints_in_sequence(values: Vec<i64>) {
    let mut buf = vec![0u8; values.len() * 10]; // Allocate enough space

    // Encode all values sequentially
    let mut write_cursor = Cursor::new(&mut buf[..]);
    let mut total_encoded_size = 0;
    for &value in &values {
        let size = varint_encode_signed(value, &mut write_cursor).unwrap();
        total_encoded_size += size;
    }

    // Decode all values sequentially
    let mut read_cursor = Cursor::new(&buf[..]);
    let mut decoded_values = Vec::new();
    for _ in 0..values.len() {
        let decoded = varint_decode_signed(&mut read_cursor).unwrap();
        decoded_values.push(decoded);
    }

    // Verify total size calculation
    let expected_total_size: usize = values.iter().map(|&v| varint_size_signed(v)).sum();

    // Verify all values were correctly encoded and decoded
    prop_assert_eq!(values, decoded_values);
    prop_assert_eq!(total_encoded_size, expected_total_size);
}
