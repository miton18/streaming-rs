//! RESP2 encoder and incremental decoder.
//!
//! This module is pure: it performs no I/O and knows nothing about
//! connections. Requests are arrays of bulk strings ([`encode_command`]);
//! replies are decoded by a [`Decoder`] that is fed arbitrary slices of the
//! byte stream and yields complete [`Value`]s as they become available.
//! Bulk strings are binary safe.

use std::fmt;

use bytes::{Buf, Bytes, BytesMut};

/// Default cap on a single bulk string (the protocol allows 512 MiB).
pub const DEFAULT_MAX_BULK: usize = 64 * 1024 * 1024;
/// Default cap on the element count of a single array.
pub const DEFAULT_MAX_ARRAY: usize = 16 * 1024 * 1024;
/// Maximum nesting depth of arrays.
pub const MAX_DEPTH: usize = 16;
/// Maximum length of a line (simple string, error, integer, length header).
const MAX_LINE: usize = 64 * 1024;

/// A decoded RESP2 reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$3\r\nfoo\r\n`
    Bulk(Bytes),
    /// `$-1\r\n` or `*-1\r\n`
    Null,
    /// `*2\r\n...`
    Array(Vec<Value>),
}

/// The byte stream is not valid RESP, or exceeds a configured limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RespError(pub String);

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid RESP: {}", self.0)
    }
}

impl std::error::Error for RespError {}

fn bad<T>(msg: impl Into<String>) -> Result<T, RespError> {
    Err(RespError(msg.into()))
}

/// Append a request (an array of bulk strings) to `out`.
pub fn encode_command<A: AsRef<[u8]>>(args: &[A], out: &mut Vec<u8>) {
    out.push(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        let arg = arg.as_ref();
        out.push(b'$');
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// Append the wire form of `value` to `out` (used by the fake server).
pub fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(n) => {
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Bulk(b) => {
            out.push(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Value::Null => out.extend_from_slice(b"$-1\r\n"),
        Value::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_value(item, out);
            }
        }
    }
}

/// Incremental reply decoder.
///
/// Feed it whatever the socket produced with [`Decoder::feed`] (or write
/// straight into [`Decoder::buffer_mut`]) and call [`Decoder::next_value`]
/// until it returns `Ok(None)`. After an `Err` the stream is desynchronised
/// and the decoder must be discarded.
#[derive(Debug)]
pub struct Decoder {
    buf: BytesMut,
    max_bulk: usize,
    max_array: usize,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    /// A decoder with the default limits.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_BULK, DEFAULT_MAX_ARRAY)
    }

    /// A decoder rejecting bulk strings above `max_bulk` bytes and arrays
    /// above `max_array` elements.
    #[must_use]
    pub fn with_limits(max_bulk: usize, max_array: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            max_bulk,
            max_array,
        }
    }

    /// Append received bytes.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// The raw receive buffer, to read from a socket directly into.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Number of buffered, not yet consumed bytes.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Decode the next complete value, or `Ok(None)` when more bytes are needed.
    ///
    /// # Errors
    /// The stream is not valid RESP or exceeds a limit.
    pub fn next_value(&mut self) -> Result<Option<Value>, RespError> {
        let Some(len) = self.scan(0, 0)? else {
            return Ok(None);
        };
        let frame = self.buf.split_to(len).freeze();
        let (value, used) = self.build(&frame, 0)?;
        debug_assert_eq!(used, len);
        Ok(Some(value))
    }

    /// Validate without allocating: the end offset of the value starting at
    /// `pos`, or `None` when incomplete.
    fn scan(&self, pos: usize, depth: usize) -> Result<Option<usize>, RespError> {
        if depth > MAX_DEPTH {
            return bad("nesting too deep");
        }
        let buf = &self.buf[..];
        let Some(&tag) = buf.get(pos) else {
            return Ok(None);
        };
        let Some((line, next)) = read_line(buf, pos + 1)? else {
            return Ok(None);
        };
        match tag {
            b'+' | b'-' => Ok(Some(next)),
            b':' => {
                parse_int(line)?;
                Ok(Some(next))
            }
            b'$' => {
                let n = parse_int(line)?;
                if n == -1 {
                    return Ok(Some(next));
                }
                let n = usize::try_from(n).or_else(|_| bad("negative bulk length"))?;
                if n > self.max_bulk {
                    return bad(format!("bulk of {n} bytes exceeds limit {}", self.max_bulk));
                }
                let end = next + n;
                if buf.len() < end + 2 {
                    return Ok(None);
                }
                if &buf[end..end + 2] != b"\r\n" {
                    return bad("bulk not terminated by CRLF");
                }
                Ok(Some(end + 2))
            }
            b'*' => {
                let n = parse_int(line)?;
                if n == -1 {
                    return Ok(Some(next));
                }
                let n = usize::try_from(n).or_else(|_| bad("negative array length"))?;
                if n > self.max_array {
                    return bad(format!("array of {n} elements exceeds limit"));
                }
                let mut cur = next;
                for _ in 0..n {
                    match self.scan(cur, depth + 1)? {
                        Some(end) => cur = end,
                        None => return Ok(None),
                    }
                }
                Ok(Some(cur))
            }
            other => bad(format!("unknown type byte 0x{other:02x}")),
        }
    }

    /// Build a value from a frame already validated by [`Self::scan`].
    fn build(&self, src: &Bytes, pos: usize) -> Result<(Value, usize), RespError> {
        let Some((line, next)) = read_line(src, pos + 1)? else {
            return bad("truncated frame");
        };
        match src[pos] {
            b'+' => Ok((Value::Simple(String::from_utf8_lossy(line).into_owned()), next)),
            b'-' => Ok((Value::Error(String::from_utf8_lossy(line).into_owned()), next)),
            b':' => Ok((Value::Integer(parse_int(line)?), next)),
            b'$' => {
                let n = parse_int(line)?;
                if n == -1 {
                    return Ok((Value::Null, next));
                }
                let n = usize::try_from(n).or_else(|_| bad("negative bulk length"))?;
                Ok((Value::Bulk(src.slice(next..next + n)), next + n + 2))
            }
            b'*' => {
                let n = parse_int(line)?;
                if n == -1 {
                    return Ok((Value::Null, next));
                }
                let n = usize::try_from(n).or_else(|_| bad("negative array length"))?;
                let mut items = Vec::with_capacity(n.min(1024));
                let mut cur = next;
                for _ in 0..n {
                    let (item, end) = self.build(src, cur)?;
                    items.push(item);
                    cur = end;
                }
                Ok((Value::Array(items), cur))
            }
            other => bad(format!("unknown type byte 0x{other:02x}")),
        }
    }

    /// Drop `n` buffered bytes (test helper for garbage handling).
    #[doc(hidden)]
    pub fn advance(&mut self, n: usize) {
        self.buf.advance(n);
    }
}

/// Find the CRLF-terminated line starting at `from`.
/// Returns the line without CRLF and the offset after it.
fn read_line(buf: &[u8], from: usize) -> Result<Option<(&[u8], usize)>, RespError> {
    let rest = &buf[from.min(buf.len())..];
    let window = &rest[..rest.len().min(MAX_LINE + 2)];
    match window.windows(2).position(|w| w == b"\r\n") {
        Some(i) => Ok(Some((&rest[..i], from + i + 2))),
        None if rest.len() > MAX_LINE => bad("line too long"),
        None => Ok(None),
    }
}

fn parse_int(line: &[u8]) -> Result<i64, RespError> {
    let digits = line.strip_prefix(b"-").unwrap_or(line);
    if digits.is_empty() || digits.len() > 20 || !digits.iter().all(u8::is_ascii_digit) {
        return bad(format!(
            "bad integer {:?}",
            String::from_utf8_lossy(&line[..line.len().min(32)])
        ));
    }
    // Safe: ASCII digits and an optional minus sign only.
    let text = std::str::from_utf8(line).or_else(|_| bad("bad integer"))?;
    text.parse::<i64>().or_else(|_| bad("integer out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(bytes: &[u8]) -> Value {
        let mut d = Decoder::new();
        d.feed(bytes);
        let v = d.next_value().unwrap().expect("complete");
        assert_eq!(d.buffered(), 0, "all bytes consumed");
        v
    }

    fn bulk(b: &[u8]) -> Value {
        Value::Bulk(Bytes::copy_from_slice(b))
    }

    #[test]
    fn encodes_commands_as_bulk_arrays() {
        let mut out = Vec::new();
        encode_command(&[&b"SET"[..], b"k", b"a\r\nb\0"], &mut out);
        assert_eq!(out, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\na\r\nb\0\r\n");
        let mut out = Vec::new();
        encode_command::<&[u8]>(&[], &mut out);
        assert_eq!(out, b"*0\r\n");
        let mut out = Vec::new();
        encode_command(&[b""], &mut out);
        assert_eq!(out, b"*1\r\n$0\r\n\r\n");
    }

    #[test]
    fn decodes_every_type() {
        assert_eq!(one(b"+OK\r\n"), Value::Simple("OK".into()));
        assert_eq!(one(b"+\r\n"), Value::Simple(String::new()));
        assert_eq!(
            one(b"-WRONGTYPE bad kind\r\n"),
            Value::Error("WRONGTYPE bad kind".into())
        );
        assert_eq!(one(b":0\r\n"), Value::Integer(0));
        assert_eq!(one(b":-17\r\n"), Value::Integer(-17));
        assert_eq!(one(b":9223372036854775807\r\n"), Value::Integer(i64::MAX));
        assert_eq!(one(b":-9223372036854775808\r\n"), Value::Integer(i64::MIN));
        assert_eq!(one(b"$3\r\nfoo\r\n"), bulk(b"foo"));
        assert_eq!(one(b"$0\r\n\r\n"), bulk(b""));
        assert_eq!(one(b"$-1\r\n"), Value::Null);
        assert_eq!(one(b"*-1\r\n"), Value::Null);
        assert_eq!(one(b"*0\r\n"), Value::Array(vec![]));
    }

    #[test]
    fn bulk_is_binary_safe() {
        let payload: Vec<u8> = (0..=255u8).chain(*b"\r\n$3\r\n\0\0").collect();
        let mut wire = format!("${}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        assert_eq!(one(&wire), bulk(&payload));
    }

    #[test]
    fn nested_arrays() {
        let wire = b"*3\r\n$1\r\n0\r\n*2\r\n*2\r\n$1\r\na\r\n:1\r\n$-1\r\n+x\r\n";
        assert_eq!(
            one(wire),
            Value::Array(vec![
                bulk(b"0"),
                Value::Array(vec![Value::Array(vec![bulk(b"a"), Value::Integer(1)]), Value::Null]),
                Value::Simple("x".into()),
            ])
        );
    }

    #[test]
    fn fragmented_at_every_byte_boundary() {
        let mut wire = Vec::new();
        let value = Value::Array(vec![
            bulk(b"17"),
            Value::Array(vec![
                bulk(b"f\r\n1"),
                bulk(b"v\0\r\n"),
                Value::Null,
                Value::Integer(-5),
                Value::Simple("OK".into()),
                Value::Error("ERR x".into()),
                bulk(b""),
            ]),
        ]);
        encode_value(&value, &mut wire);
        // Value followed by a second one, to check boundaries between values.
        encode_value(&Value::Integer(7), &mut wire);

        // Every split point in two chunks.
        for cut in 0..=wire.len() {
            let mut d = Decoder::new();
            d.feed(&wire[..cut]);
            let first = d.next_value().unwrap();
            d.feed(&wire[cut..]);
            let (a, b) = match first {
                Some(a) => (a, d.next_value().unwrap().expect("second")),
                None => {
                    let a = d.next_value().unwrap().expect("first after rest");
                    (a, d.next_value().unwrap().expect("second"))
                }
            };
            assert_eq!(a, value, "cut at {cut}");
            assert_eq!(b, Value::Integer(7));
            assert_eq!(d.next_value().unwrap(), None);
        }

        // One byte at a time.
        let mut d = Decoder::new();
        let mut got = Vec::new();
        for byte in &wire {
            d.feed(std::slice::from_ref(byte));
            while let Some(v) = d.next_value().unwrap() {
                got.push(v);
            }
        }
        assert_eq!(got, vec![value, Value::Integer(7)]);
    }

    #[test]
    fn incomplete_input_yields_none_not_error() {
        for partial in [
            &b""[..],
            b"+OK",
            b"+OK\r",
            b"$3",
            b"$3\r\nfo",
            b"$3\r\nfoo\r",
            b"*2\r\n:1\r\n",
            b"*2\r\n:1\r\n$3\r\nab",
            b"-ERR",
        ] {
            let mut d = Decoder::new();
            d.feed(partial);
            assert_eq!(d.next_value().unwrap(), None, "{partial:?}");
            assert_eq!(d.buffered(), partial.len(), "nothing consumed");
        }
    }

    #[test]
    fn huge_bulk_in_chunks() {
        let payload = vec![0xabu8; 6 * 1024 * 1024];
        let mut wire = format!("${}\r\n", payload.len()).into_bytes();
        wire.extend_from_slice(&payload);
        wire.extend_from_slice(b"\r\n");
        let mut d = Decoder::new();
        let mut result = None;
        for chunk in wire.chunks(16 * 1024) {
            d.feed(chunk);
            if let Some(v) = d.next_value().unwrap() {
                result = Some(v);
            }
        }
        assert_eq!(result, Some(Value::Bulk(Bytes::from(payload))));
    }

    #[test]
    fn many_element_array() {
        let items: Vec<Value> = (0..5000).map(|i| bulk(format!("field-{i}").as_bytes())).collect();
        let value = Value::Array(vec![bulk(b"0"), Value::Array(items)]);
        let mut wire = Vec::new();
        encode_value(&value, &mut wire);
        let mut d = Decoder::new();
        let mut result = None;
        for chunk in wire.chunks(1500) {
            d.feed(chunk);
            if let Some(v) = d.next_value().unwrap() {
                result = Some(v);
            }
        }
        assert_eq!(result, Some(value));
    }

    #[test]
    fn rejects_garbage() {
        for garbage in [
            &b"hello\r\n"[..],
            b"?\r\n",
            b":abc\r\n",
            b":\r\n",
            b":1.5\r\n",
            b":+5\r\n",
            b":99999999999999999999\r\n",
            b"$abc\r\n",
            b"$-2\r\n",
            b"*-2\r\n",
            b"$3\r\nfoobar\r\n",
            b"$3\r\nfooXY",
            b"*1\r\n?\r\n",
        ] {
            let mut d = Decoder::new();
            d.feed(garbage);
            assert!(d.next_value().is_err(), "{garbage:?} must be rejected");
        }
    }

    #[test]
    fn enforces_limits() {
        let mut d = Decoder::with_limits(10, 3);
        d.feed(b"$11\r\n");
        assert!(d.next_value().is_err());
        let mut d = Decoder::with_limits(10, 3);
        d.feed(b"*4\r\n");
        assert!(d.next_value().is_err());
        let mut d = Decoder::with_limits(10, 3);
        d.feed(b"$10\r\n0123456789\r\n");
        assert!(d.next_value().unwrap().is_some());

        // Too deep.
        let mut d = Decoder::new();
        d.feed(&b"*1\r\n".repeat(MAX_DEPTH + 2));
        assert!(d.next_value().is_err());

        // A line that never ends must not buffer forever.
        let mut d = Decoder::new();
        d.feed(b"+");
        d.feed(&vec![b'a'; MAX_LINE + 10]);
        assert!(d.next_value().is_err());
    }

    #[test]
    fn bytes_after_a_value_are_kept() {
        let mut d = Decoder::new();
        d.feed(b":1\r\n:2\r\n:3");
        assert_eq!(d.next_value().unwrap(), Some(Value::Integer(1)));
        assert_eq!(d.next_value().unwrap(), Some(Value::Integer(2)));
        assert_eq!(d.next_value().unwrap(), None);
        d.feed(b"\r\n");
        assert_eq!(d.next_value().unwrap(), Some(Value::Integer(3)));
    }

    #[test]
    fn encode_decode_roundtrip_of_all_kinds() {
        let v = Value::Array(vec![
            Value::Simple("PONG".into()),
            Value::Error("ERR no".into()),
            Value::Integer(i64::MIN),
            bulk(b"\r\n\0"),
            Value::Null,
            Value::Array(vec![]),
        ]);
        let mut wire = Vec::new();
        encode_value(&v, &mut wire);
        assert_eq!(one(&wire), v);
    }
}
