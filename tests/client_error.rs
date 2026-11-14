#![allow(clippy::panic, clippy::unwrap_used, reason = "assertions in integration tests")]

use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use databas::{
    client::{Client, ClientError},
    protocol::ErrorCode,
};

#[test]
fn server_error_exposes_code_and_message_to_downstream_callers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut header = [0; 12];
        stream.read_exact(&mut header).unwrap();
        let payload_length = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut startup = vec![0; payload_length];
        stream.read_exact(&mut startup).unwrap();

        let message = b"requested database is not served";
        let mut payload = 3_u16.to_be_bytes().to_vec();
        payload.extend_from_slice(message);
        let mut response = Vec::with_capacity(12 + payload.len());
        response.extend_from_slice(b"DBAS");
        response.extend_from_slice(&1_u16.to_be_bytes());
        response.push(0x7f);
        response.push(0);
        response.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        response.extend_from_slice(&payload);
        stream.write_all(&response).unwrap();
    });

    let error = Client::connect(address, "missing").unwrap_err();
    let ClientError::Server(server_error) = error else {
        panic!("expected server error");
    };
    assert_eq!(server_error.code(), ErrorCode::DatabaseNotFound);
    assert_eq!(server_error.message(), "requested database is not served");
    server.join().unwrap();
}
