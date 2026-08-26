# Databas wire protocol version 1

Databas uses a synchronous binary protocol over TCP. Version 1 deliberately has
no authentication, encryption, multiplexing, cancellation, prepared
statements, or concurrent requests. Deploy it only on a trusted network (or
behind a separate secure tunnel).

A server process hosts exactly one database file under one logical **database
name**. The name is an opaque UTF-8 identifier and is not a path. It must be
1–255 bytes and cannot contain NUL. The client sends it during startup; a name
mismatch is rejected.

## Connection and sequencing

1. The server opens or creates its database file and performs WAL recovery.
2. Only after recovery succeeds does it bind its TCP listener.
3. The server accepts one connection and reads one `STARTUP` message.
4. It responds with `READY` or `ERROR`.
5. After `READY`, the client sends one `QUERY` at a time. The server sends that
   query's complete response before reading the next query.
6. When the client disconnects, an explicit transaction left open by that
   session is rolled back. The server flushes the database, then accepts the
   next connection.

The implementation does not create connection or query worker threads. The OS
may queue connection attempts in the TCP listen backlog, but the server does
not accept or process the next connection until the current one closes.

## Frame format

Every integer is unsigned big-endian unless stated otherwise.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `DBAS` |
| 4 | 2 | protocol version (`1`) |
| 6 | 1 | message type |
| 7 | 1 | flags (must be zero) |
| 8 | 4 | payload length |
| 12 | N | payload |

The maximum payload is 16 MiB. Unknown message types, nonzero flags, malformed
payloads, and out-of-state messages are protocol errors. There is no request ID
because only one request may be in flight.

## Messages

| Type | Name | Direction | Payload |
|---:|---|---|---|
| `0x01` | `STARTUP` | client → server | UTF-8 database name |
| `0x02` | `READY` | server → client | empty |
| `0x03` | `QUERY` | client → server | one UTF-8 SQL item |
| `0x10` | `ROW` | server → client | typed row, below |
| `0x11` | `COMPLETE` | server → client | completion kind and data |
| `0x7f` | `ERROR` | server → client | error code and message |

A query response is either:

- zero or more `ROW` messages followed by one row `COMPLETE`;
- one non-row `COMPLETE`; or
- one `ERROR`.

An `ERROR` ends only the current query unless it occurs during startup or is a
framing error, in which case the server closes the connection.

### Typed row payload

A row starts with a `u32` value count. Each value then starts with a one-byte
tag and has the following data:

| Tag | SQL/storage value | Data |
|---:|---|---|
| `0x00` | `NULL` | none |
| `0x01` | text | `u32` byte length, then UTF-8 bytes |
| `0x02` | boolean | one byte: `0` or `1` |
| `0x03` | signed integer | `i32`, big-endian two's complement |
| `0x04` | float | IEEE-754 binary32 bits; NaN is invalid |
| `0x05` | unsigned integer | `u64` |

Rows do not include column names because the current executor does not expose a
result-column metadata API. Adding metadata requires a future protocol version
or a backwards-compatible new message type.

### Completion payload

The first byte selects the completion kind:

| Kind | Meaning | Remaining data |
|---:|---|---|
| `0x00` | rows complete | `u64` row count |
| `0x01` | `EXPLAIN` | UTF-8 plan text |
| `0x02` | rows affected | `u64` count |
| `0x03` | schema affected | empty |
| `0x04` | command completed | empty |

### Error payload and codes

An error payload is a `u16` stable code followed by a UTF-8 diagnostic. Clients
should branch on the code, not parse the diagnostic. Unknown codes must be
reported rather than treated as malformed.

| Code | Name | Meaning |
|---:|---|---|
| 1 | `PROTOCOL_ERROR` | invalid frame or protocol state |
| 2 | `UNSUPPORTED_PROTOCOL_VERSION` | unsupported frame version |
| 3 | `DATABASE_NOT_FOUND` | endpoint does not serve the startup name |
| 4 | `INVALID_REQUEST` | invalid request payload |
| 100 | `SYNTAX_ERROR` | SQL parse error |
| 200 | `CONSTRAINT_VIOLATION` | data or schema constraint violation |
| 201 | `INVALID_ARGUMENT` | bad argument or missing referenced object |
| 202 | `LIMIT_EXCEEDED` | database resource/encoding limit |
| 203 | `TRANSACTION_ERROR` | invalid transaction state |
| 300 | `PLANNING_ERROR` | query planning failure |
| 400 | `EXECUTION_ERROR` | expression or operator execution failure |
| 500 | `STORAGE_IO_ERROR` | database/WAL I/O failure |
| 501 | `DATABASE_CORRUPTION` | corrupt database or WAL |
| 599 | `INTERNAL_ERROR` | server invariant failure |

Codes 5–99, 101–199, 204–299, 301–399, 401–499, 502–598, and 600–65535 are
reserved for future use.

## Compatibility

Peers must put their protocol version in every frame. Version 1 rejects any
other version. Within version 1, existing message meanings and error numbers
are stable. New error numbers may be added; clients preserve unknown numbers.
Any incompatible frame, row, or sequencing change requires a new protocol
version.
