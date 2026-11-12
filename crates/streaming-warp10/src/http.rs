//! A small HTTP/1.1 implementation: just enough to POST a body and read the
//! answer (client side), or to read a request and answer it (the fake ingress).
//!
//! Everything that parses or builds bytes is pure and unit-tested; the two
//! async helpers at the bottom only move bytes over a `futures::io` stream.
//! One request per connection (`Connection: close`), no keep-alive, no
//! redirects, no compression.

use futures::io::{AsyncRead, AsyncReadExt};
use std::io;

/// What can go wrong when building or parsing an HTTP message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HttpError {
    /// The bytes are not a valid HTTP/1.x message.
    #[error("malformed HTTP message: {0}")]
    Malformed(String),
    /// The peer closed the connection before the message was complete.
    #[error("connection closed before the HTTP message was complete")]
    UnexpectedEof,
    /// The header block is larger than the limit.
    #[error("HTTP header block larger than {0} bytes")]
    HeadTooLarge(usize),
}

/// Failure to read a message from a stream.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// I/O error on the stream.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Not a valid message.
    #[error(transparent)]
    Http(#[from] HttpError),
}

/// Size limits applied while reading a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum size of the start line plus headers.
    pub max_head: usize,
    /// Maximum number of body bytes kept; the rest is dropped and the message
    /// flagged `truncated`.
    pub max_body: usize,
}

impl Limits {
    /// Limits for the responses read by the sink: only an excerpt is needed.
    pub const RESPONSE: Limits = Limits {
        max_head: 16 * 1024,
        max_body: 8 * 1024,
    };
    /// Limits for the requests read by the fake ingress.
    pub const REQUEST: Limits = Limits {
        max_head: 16 * 1024,
        max_body: 64 * 1024 * 1024,
    };
}

/// A parsed `http://` or `https://` URL, reduced to what a client needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// `true` for `https`.
    pub tls: bool,
    /// Host name or IP literal (IPv6 without the brackets).
    pub host: String,
    /// Explicit port, if the URL had one.
    pub port: Option<u16>,
    /// Path and query, always starting with `/`.
    pub path: String,
}

impl Endpoint {
    /// Parse a URL. Only `http` and `https`; no user info.
    pub fn parse(url: &str) -> Result<Self, String> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("endpoint `{url}` must start with `http://` or `https://`"));
        };
        let rest = rest.split('#').next().unwrap_or(rest);
        let (authority, path) = match rest.find(['/', '?']) {
            Some(i) if rest.as_bytes()[i] == b'/' => (&rest[..i], rest[i..].to_owned()),
            Some(i) => (&rest[..i], format!("/{}", &rest[i..])),
            None => (rest, "/".to_owned()),
        };
        if authority.contains('@') {
            return Err("endpoint must not contain user info".to_owned());
        }
        if authority.chars().any(|c| c.is_whitespace() || c.is_control())
            || path.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(format!("endpoint `{url}` contains whitespace"));
        }
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, after) = v6
                .split_once(']')
                .ok_or_else(|| format!("endpoint `{url}`: unterminated `[`"))?;
            let port = match after.strip_prefix(':') {
                Some(p) => Some(p),
                None if after.is_empty() => None,
                None => return Err(format!("endpoint `{url}`: bad authority")),
            };
            (host, port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (authority, None),
            }
        };
        if host.is_empty() {
            return Err(format!("endpoint `{url}` has no host"));
        }
        let port = port
            .map(|p| {
                p.parse::<u16>()
                    .map_err(|_| format!("endpoint `{url}`: invalid port `{p}`"))
            })
            .transpose()?;
        Ok(Self {
            tls,
            host: host.to_owned(),
            port,
            path,
        })
    }

    /// The port to connect to: explicit, or 80 / 443.
    #[must_use]
    pub fn effective_port(&self) -> u16 { self.port.unwrap_or(if self.tls { 443 } else { 80 }) }

    /// `host:port` as handed to `NetworkProvider::connect`.
    #[must_use]
    pub fn address(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.effective_port())
        } else {
            format!("{}:{}", self.host, self.effective_port())
        }
    }

    /// Value of the `Host` header (port omitted when it is the default one).
    #[must_use]
    pub fn host_header(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        match self.port {
            Some(p) if p != if self.tls { 443 } else { 80 } => format!("{host}:{p}"),
            _ => host,
        }
    }
}

/// Build a complete request: start line, `Host`, `Content-Length`,
/// `Connection: close`, the given headers, then the body.
///
/// Header names and values containing CR, LF (or a `:` in a name) are refused,
/// so a hostile configuration value cannot inject headers.
pub fn build_request(
    method: &str,
    path: &str,
    host: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<Vec<u8>, HttpError> {
    let clean = |s: &str| !s.contains(['\r', '\n', '\0']);
    if !clean(method) || !clean(path) || !clean(host) || path.contains(' ') || method.contains(' ') {
        return Err(HttpError::Malformed("invalid request line".into()));
    }
    let mut out = Vec::with_capacity(body.len() + 256);
    out.extend_from_slice(format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\n").as_bytes());
    for (name, value) in headers {
        if name.is_empty() || name.contains(':') || !clean(name) || !clean(value) {
            return Err(HttpError::Malformed(format!("invalid header `{name}`")));
        }
        out.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    out.extend_from_slice(format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// Build a response with a text body.
#[must_use]
pub fn build_response(status: u16, body: &str) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        reason_phrase(status),
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

/// Standard reason phrase of the status codes this crate uses.
#[must_use]
pub fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    }
}

/// A parsed HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Status code.
    pub status: u16,
    /// Reason phrase (may be empty).
    pub reason: String,
    /// Headers, in order, names as received.
    pub headers: Vec<(String, String)>,
    /// Body (decoded if chunked), at most `Limits::max_body` bytes.
    pub body: Vec<u8>,
    /// The body was longer than `Limits::max_body` and was cut.
    pub truncated: bool,
}

/// A parsed HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Method, as received.
    pub method: String,
    /// Request target (path and query).
    pub target: String,
    /// Headers, in order, names as received.
    pub headers: Vec<(String, String)>,
    /// Body (decoded if chunked).
    pub body: Vec<u8>,
    /// The body was longer than `Limits::max_body` and was cut.
    pub truncated: bool,
}

/// Case-insensitive header lookup (first match).
#[must_use]
pub fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

struct Head {
    start_line: String,
    headers: Vec<(String, String)>,
    len: usize,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// `Ok(None)`: the header block is not complete yet.
fn parse_head(buf: &[u8], max_head: usize) -> Result<Option<Head>, HttpError> {
    let Some(end) = find_subslice(buf, b"\r\n\r\n") else {
        if buf.len() > max_head {
            return Err(HttpError::HeadTooLarge(max_head));
        }
        return Ok(None);
    };
    if end + 4 > max_head {
        return Err(HttpError::HeadTooLarge(max_head));
    }
    let text = std::str::from_utf8(&buf[..end]).map_err(|_| HttpError::Malformed("non UTF-8 header block".into()))?;
    let mut lines = text.split("\r\n");
    let start_line = lines.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| HttpError::Malformed(format!("bad header line `{line}`")))?;
        if name.is_empty() || name.contains(char::is_whitespace) {
            return Err(HttpError::Malformed(format!("bad header name `{name}`")));
        }
        headers.push((name.to_owned(), value.trim().to_owned()));
    }
    Ok(Some(Head {
        start_line,
        headers,
        len: end + 4,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    None,
    Length(usize),
    Chunked,
    UntilClose,
}

fn framing(headers: &[(String, String)], default: Framing) -> Result<Framing, HttpError> {
    if let Some(te) = header(headers, "transfer-encoding") {
        return if te
            .split(',')
            .next_back()
            .is_some_and(|last| last.trim().eq_ignore_ascii_case("chunked"))
        {
            Ok(Framing::Chunked)
        } else {
            Err(HttpError::Malformed(format!("unsupported transfer-encoding `{te}`")))
        };
    }
    let mut length = None;
    for (n, v) in headers {
        if n.eq_ignore_ascii_case("content-length") {
            let parsed: usize = v
                .parse()
                .map_err(|_| HttpError::Malformed(format!("bad content-length `{v}`")))?;
            match length {
                Some(previous) if previous != parsed => {
                    return Err(HttpError::Malformed("conflicting content-length".into()));
                }
                _ => length = Some(parsed),
            }
        }
    }
    Ok(length.map_or(default, Framing::Length))
}

struct Body {
    data: Vec<u8>,
    truncated: bool,
}

/// `Ok(None)`: more bytes needed.
fn decode_body(buf: &[u8], framing: Framing, eof: bool, max_body: usize) -> Result<Option<Body>, HttpError> {
    match framing {
        Framing::None => Ok(Some(Body {
            data: Vec::new(),
            truncated: false,
        })),
        Framing::Length(n) => {
            let want = n.min(max_body);
            if buf.len() >= want {
                Ok(Some(Body {
                    data: buf[..want].to_vec(),
                    truncated: n > max_body,
                }))
            } else if eof {
                Err(HttpError::UnexpectedEof)
            } else {
                Ok(None)
            }
        }
        Framing::UntilClose => {
            if buf.len() > max_body {
                Ok(Some(Body {
                    data: buf[..max_body].to_vec(),
                    truncated: true,
                }))
            } else if eof {
                Ok(Some(Body {
                    data: buf.to_vec(),
                    truncated: false,
                }))
            } else {
                Ok(None)
            }
        }
        Framing::Chunked => decode_chunked(buf, eof, max_body),
    }
}

fn decode_chunked(buf: &[u8], eof: bool, max_body: usize) -> Result<Option<Body>, HttpError> {
    let incomplete = || {
        if eof { Err(HttpError::UnexpectedEof) } else { Ok(None) }
    };
    let mut pos = 0;
    let mut data = Vec::new();
    loop {
        let Some(line_end) = find_subslice(&buf[pos..], b"\r\n") else {
            return incomplete();
        };
        let line = std::str::from_utf8(&buf[pos..pos + line_end])
            .map_err(|_| HttpError::Malformed("non UTF-8 chunk size".into()))?;
        let size_text = line.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| HttpError::Malformed(format!("bad chunk size `{size_text}`")))?;
        pos += line_end + 2;
        if size == 0 {
            // Trailers, then the final empty line.
            loop {
                let Some(t_end) = find_subslice(&buf[pos..], b"\r\n") else {
                    return incomplete();
                };
                let empty = t_end == 0;
                pos += t_end + 2;
                if empty {
                    return Ok(Some(Body { data, truncated: false }));
                }
            }
        }
        let room = max_body - data.len();
        let available = buf.len() - pos;
        if size > room {
            // Enough to fill the cap: stop here, the rest is not needed.
            if available >= room {
                data.extend_from_slice(&buf[pos..pos + room]);
                return Ok(Some(Body { data, truncated: true }));
            }
            return incomplete();
        }
        if available < size + 2 {
            return incomplete();
        }
        data.extend_from_slice(&buf[pos..pos + size]);
        if &buf[pos + size..pos + size + 2] != b"\r\n" {
            return Err(HttpError::Malformed("chunk not followed by CRLF".into()));
        }
        pos += size + 2;
    }
}

/// Parse a response from the bytes received so far. `eof` tells that the peer
/// closed the connection: nothing more will come.
///
/// `Ok(None)` means "feed me more bytes".
pub fn parse_response(buf: &[u8], eof: bool, limits: Limits) -> Result<Option<Response>, HttpError> {
    let Some(head) = parse_head(buf, limits.max_head)? else {
        return if eof { Err(HttpError::UnexpectedEof) } else { Ok(None) };
    };
    let mut parts = head.start_line.splitn(3, ' ');
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(HttpError::Malformed(format!("bad status line `{}`", head.start_line)));
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .filter(|s| (100..1000).contains(s))
        .ok_or_else(|| HttpError::Malformed(format!("bad status line `{}`", head.start_line)))?;
    let reason = parts.next().unwrap_or_default().to_owned();
    let default = if (100..200).contains(&status) || status == 204 || status == 304 {
        Framing::None
    } else {
        Framing::UntilClose
    };
    let framing = if default == Framing::None {
        Framing::None
    } else {
        framing(&head.headers, default)?
    };
    Ok(
        decode_body(&buf[head.len..], framing, eof, limits.max_body)?.map(|body| Response {
            status,
            reason,
            headers: head.headers,
            body: body.data,
            truncated: body.truncated,
        }),
    )
}

/// Parse a request from the bytes received so far (see [`parse_response`]).
pub fn parse_request(buf: &[u8], eof: bool, limits: Limits) -> Result<Option<Request>, HttpError> {
    let Some(head) = parse_head(buf, limits.max_head)? else {
        return if eof { Err(HttpError::UnexpectedEof) } else { Ok(None) };
    };
    let mut parts = head.start_line.split(' ');
    let (method, target, version) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(m), Some(t), Some(v), None) if !m.is_empty() && !t.is_empty() => (m, t, v),
        _ => {
            return Err(HttpError::Malformed(format!("bad request line `{}`", head.start_line)));
        }
    };
    if !version.starts_with("HTTP/1.") {
        return Err(HttpError::Malformed(format!("bad version `{version}`")));
    }
    let framing = framing(&head.headers, Framing::None)?;
    Ok(
        decode_body(&buf[head.len..], framing, eof, limits.max_body)?.map(|body| Request {
            method: method.to_owned(),
            target: target.to_owned(),
            headers: head.headers,
            body: body.data,
            truncated: body.truncated,
        }),
    )
}

async fn read_message<R, T>(
    reader: &mut R,
    parse: impl Fn(&[u8], bool) -> Result<Option<T>, HttpError>,
) -> Result<T, ReadError>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = reader.read(&mut chunk).await?;
        let eof = n == 0;
        buf.extend_from_slice(&chunk[..n]);
        if let Some(message) = parse(&buf, eof)? {
            return Ok(message);
        }
        if eof {
            return Err(HttpError::UnexpectedEof.into());
        }
    }
}

/// Read one response from a stream.
pub async fn read_response<R: AsyncRead + Unpin>(reader: &mut R, limits: Limits) -> Result<Response, ReadError> {
    read_message(reader, |buf, eof| parse_response(buf, eof, limits)).await
}

/// Read one request from a stream.
pub async fn read_request<R: AsyncRead + Unpin>(reader: &mut R, limits: Limits) -> Result<Request, ReadError> {
    read_message(reader, |buf, eof| parse_request(buf, eof, limits)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Limits = Limits {
        max_head: 1024,
        max_body: 64,
    };

    fn resp(bytes: &[u8]) -> Response { parse_response(bytes, true, L).unwrap().unwrap() }

    #[test]
    fn endpoint_parsing() {
        let e = Endpoint::parse("http://h.example:8080/api/v0/update?x=1").unwrap();
        assert!(!e.tls);
        assert_eq!(e.host, "h.example");
        assert_eq!(e.port, Some(8080));
        assert_eq!(e.path, "/api/v0/update?x=1");
        assert_eq!(e.address(), "h.example:8080");
        assert_eq!(e.host_header(), "h.example:8080");

        let e = Endpoint::parse("https://warp.example").unwrap();
        assert!(e.tls);
        assert_eq!(e.path, "/");
        assert_eq!(e.address(), "warp.example:443");
        assert_eq!(e.host_header(), "warp.example");

        let e = Endpoint::parse("http://[::1]:9/x").unwrap();
        assert_eq!(e.host, "::1");
        assert_eq!(e.address(), "[::1]:9");
        assert_eq!(e.host_header(), "[::1]:9");

        let e = Endpoint::parse("http://[::1]/x").unwrap();
        assert_eq!((e.host.as_str(), e.port), ("::1", None));

        let e = Endpoint::parse("http://h?a=b").unwrap();
        assert_eq!(e.path, "/?a=b");
        assert_eq!(Endpoint::parse("http://h:80/p").unwrap().host_header(), "h");

        for bad in [
            "ftp://h/",
            "h/x",
            "http://",
            "http://:80/",
            "http://u:p@h/",
            "http://h:99999/",
            "http://h:x/",
            "http://h /x",
            "http://[::1/x",
            "http://[::1]x/",
            "http://h/a\u{1}b",
            "http://h/a b",
        ] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn request_building() {
        let req = build_request(
            "POST",
            "/api/v0/update",
            "h:1",
            &[("X-Warp10-Token", "t"), ("Content-Type", "text/plain")],
            b"abc",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(req).unwrap(),
            "POST /api/v0/update HTTP/1.1\r\nHost: h:1\r\nX-Warp10-Token: t\r\nContent-Type: text/plain\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc"
        );
        assert!(build_request("POST", "/", "h", &[("X", "a\r\nEvil: 1")], b"").is_err());
        assert!(build_request("POST", "/", "h", &[("X:", "a")], b"").is_err());
        assert!(build_request("POST", "/a b", "h", &[], b"").is_err());
    }

    #[test]
    fn request_roundtrip_through_the_parser() {
        let raw = build_request("POST", "/p", "h", &[("X-K", "v")], b"hello").unwrap();
        let r = parse_request(&raw, false, L).unwrap().unwrap();
        assert_eq!((r.method.as_str(), r.target.as_str()), ("POST", "/p"));
        assert_eq!(header(&r.headers, "x-k"), Some("v"));
        assert_eq!(r.body, b"hello");
    }

    #[test]
    fn content_length_response() {
        let r = resp(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\nX: y\r\n\r\nbusy");
        assert_eq!(r.status, 503);
        assert_eq!(r.reason, "Service Unavailable");
        assert_eq!(r.body, b"busy");
        assert!(!r.truncated);
        assert_eq!(header(&r.headers, "x"), Some("y"));
    }

    #[test]
    fn fragmented_input_at_every_split_point() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        for cut in 0..raw.len() {
            assert_eq!(parse_response(&raw[..cut], false, L).unwrap(), None, "cut at {cut}");
        }
        assert_eq!(resp(raw).body, b"hello");
        // a closed connection in the middle is an error
        assert_eq!(
            parse_response(&raw[..raw.len() - 1], true, L),
            Err(HttpError::UnexpectedEof)
        );
        assert_eq!(parse_response(b"HTT", true, L), Err(HttpError::UnexpectedEof));
    }

    #[test]
    fn chunked_response() {
        let raw: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nTrailer: x\r\n\r\n";
        let r = resp(raw);
        assert_eq!(r.body, b"hello world");
        for cut in 0..raw.len() {
            assert_eq!(parse_response(&raw[..cut], false, L).unwrap(), None);
        }
        // Content-Length is ignored when chunked
        let both = b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n";
        assert_eq!(resp(both).body, b"hi");
        // bad chunk framing
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", false, L).is_err());
        assert!(
            parse_response(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhiXX0\r\n\r\n",
                false,
                L
            )
            .is_err()
        );
        assert!(parse_response(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n", false, L).is_err());
    }

    #[test]
    fn missing_length_reads_until_close() {
        let raw: &[u8] = b"HTTP/1.1 500 Oops\r\nConnection: close\r\n\r\nsome body";
        assert_eq!(parse_response(raw, false, L).unwrap(), None);
        let r = parse_response(raw, true, L).unwrap().unwrap();
        assert_eq!(r.status, 500);
        assert_eq!(r.body, b"some body");
    }

    #[test]
    fn no_body_statuses() {
        let r = parse_response(b"HTTP/1.1 204 No Content\r\n\r\n", false, L)
            .unwrap()
            .unwrap();
        assert_eq!((r.status, r.body.len()), (204, 0));
        let r = resp(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        assert!(r.body.is_empty());
    }

    #[test]
    fn body_is_capped() {
        let mut raw = b"HTTP/1.1 500 X\r\nContent-Length: 1000\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'a', 100));
        let r = parse_response(&raw, false, L).unwrap().unwrap();
        assert_eq!(r.body.len(), 64);
        assert!(r.truncated);

        let mut chunked = b"HTTP/1.1 500 X\r\nTransfer-Encoding: chunked\r\n\r\n3e8\r\n".to_vec();
        chunked.extend(std::iter::repeat_n(b'b', 100));
        let r = parse_response(&chunked, false, L).unwrap().unwrap();
        assert_eq!(r.body.len(), 64);
        assert!(r.truncated);

        let mut close = b"HTTP/1.1 500 X\r\n\r\n".to_vec();
        close.extend(std::iter::repeat_n(b'c', 100));
        let r = parse_response(&close, false, L).unwrap().unwrap();
        assert!(r.truncated);
    }

    #[test]
    fn limits_and_reason_phrases() {
        assert_eq!((Limits::RESPONSE.max_head, Limits::RESPONSE.max_body), (16_384, 8_192));
        assert_eq!(
            (Limits::REQUEST.max_head, Limits::REQUEST.max_body),
            (16_384, 67_108_864)
        );
        for (status, phrase) in [
            (200, "OK"),
            (400, "Bad Request"),
            (401, "Unauthorized"),
            (404, "Not Found"),
            (405, "Method Not Allowed"),
            (413, "Payload Too Large"),
            (500, "Internal Server Error"),
            (502, "Bad Gateway"),
            (503, "Service Unavailable"),
            (504, "Gateway Timeout"),
            (418, "Status"),
        ] {
            assert_eq!(reason_phrase(status), phrase);
        }
    }

    #[test]
    fn request_lines_refuse_control_characters_anywhere() {
        let ok = build_request("POST", "/p", "h", &[], b"");
        assert!(ok.is_ok());
        for (method, path, host) in [
            ("PO\rST", "/p", "h"),
            ("POST", "/p\n", "h"),
            ("POST", "/p", "h\0"),
            ("POST", "/a b", "h"),
            ("PO ST", "/p", "h"),
        ] {
            assert!(
                build_request(method, path, host, &[], b"").is_err(),
                "{method:?} {path:?} {host:?}"
            );
        }
    }

    #[test]
    fn repeated_equal_content_lengths_are_accepted() {
        let r = resp(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\ncontent-length: 2\r\n\r\nhi");
        assert_eq!(r.body, b"hi");
    }

    #[test]
    fn a_chunk_overflowing_the_room_left_is_cut_there() {
        // 40 + 40 bytes against a cap of 64: the second chunk only gets 24.
        let mut raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n28\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'x', 40));
        raw.extend(b"\r\n28\r\n");
        raw.extend(std::iter::repeat_n(b'y', 40));
        raw.extend(b"\r\n0\r\n\r\n");
        let r = resp(&raw);
        assert_eq!((r.body.len(), r.truncated), (64, true));
        assert_eq!(&r.body[38..42], b"xxyy");
    }

    #[test]
    fn request_lines_need_a_method_and_a_target() {
        for bad in [
            &b"GET  HTTP/1.1\r\nHost: h\r\n\r\n"[..],
            b" /x HTTP/1.1\r\nHost: h\r\n\r\n",
        ] {
            assert!(
                parse_request(bad, false, L).is_err(),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn head_limit_is_inclusive() {
        let head = |len: usize| {
            // "HTTP/1.1 204 X\r\nX: " + padding + "\r\n\r\n" is `len` bytes.
            let prefix = b"HTTP/1.1 204 X\r\nX: ";
            let mut raw = prefix.to_vec();
            raw.extend(std::iter::repeat_n(b'a', len - prefix.len() - 4));
            raw.extend(b"\r\n\r\n");
            raw
        };
        assert!(parse_response(&head(1024), false, L).unwrap().is_some());
        assert_eq!(
            parse_response(&head(1025), false, L),
            Err(HttpError::HeadTooLarge(1024))
        );
        // An incomplete head of exactly the limit may still complete.
        assert_eq!(parse_response(&[b'a'; 1024], false, L), Ok(None));
    }

    #[test]
    fn bodies_of_exactly_the_cap_are_not_truncated() {
        let mut raw = b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n".to_vec();
        raw.extend(std::iter::repeat_n(b'a', 64));
        let r = resp(&raw);
        assert_eq!((r.body.len(), r.truncated), (64, false));

        // Until close: 64 bytes may be the whole body, keep reading.
        let mut close = b"HTTP/1.1 500 X\r\n\r\n".to_vec();
        close.extend(std::iter::repeat_n(b'c', 64));
        assert_eq!(parse_response(&close, false, L).unwrap(), None);
        let r = resp(&close);
        assert_eq!((r.body.len(), r.truncated), (64, false));

        // Chunked: one chunk filling the cap exactly, then a second one.
        let mut chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n20\r\n".to_vec();
        chunked.extend(std::iter::repeat_n(b'x', 32));
        chunked.extend(b"\r\n20\r\n");
        chunked.extend(std::iter::repeat_n(b'y', 32));
        chunked.extend(b"\r\n0\r\nA: 1\r\nB: 2\r\n\r\n");
        let r = resp(&chunked);
        assert_eq!((r.body.len(), r.truncated), (64, false));
        assert_eq!(&r.body[30..34], b"xxyy");
    }

    #[test]
    fn malformed_responses() {
        for bad in [
            &b"garbage\r\n\r\n"[..],
            b"HTTP/1.1 abc OK\r\n\r\n",
            b"HTTP/1.1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nno colon here\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n bad: header\r\n\r\n",
        ] {
            assert!(
                matches!(parse_response(bad, true, L), Err(HttpError::Malformed(_))),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
        assert_eq!(
            parse_response(&[b'a'; 2000], false, L),
            Err(HttpError::HeadTooLarge(1024))
        );
    }

    #[test]
    fn request_parsing_edge_cases() {
        let r = parse_request(b"GET /x HTTP/1.1\r\nHost: h\r\n\r\n", false, L)
            .unwrap()
            .unwrap();
        assert!(r.body.is_empty());
        let chunked = b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
        assert_eq!(parse_request(chunked, false, L).unwrap().unwrap().body, b"abc");
        assert!(parse_request(b"GET /x\r\n\r\n", false, L).is_err());
        assert!(parse_request(b"GET /x HTTP/2\r\n\r\n", false, L).is_err());
    }

    #[test]
    fn built_response_parses_back() {
        let r = resp(&build_response(401, "no token"));
        assert_eq!((r.status, r.reason.as_str()), (401, "Unauthorized"));
        assert_eq!(r.body, b"no token");
    }

    #[test]
    fn async_readers_handle_one_byte_reads() {
        struct OneByte<'a>(&'a [u8]);
        impl AsyncRead for OneByte<'_> {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut [u8],
            ) -> std::task::Poll<io::Result<usize>> {
                if self.0.is_empty() || buf.is_empty() {
                    return std::task::Poll::Ready(Ok(0));
                }
                buf[0] = self.0[0];
                self.0 = &self.0[1..];
                std::task::Poll::Ready(Ok(1))
            }
        }
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n";
        let mut reader = OneByte(raw);
        let r = futures::executor::block_on(read_response(&mut reader, L)).unwrap();
        assert_eq!(r.body, b"ok");

        let mut truncated = OneByte(&raw[..20]);
        let err = futures::executor::block_on(read_response(&mut truncated, L)).unwrap_err();
        assert!(matches!(err, ReadError::Http(HttpError::UnexpectedEof)));
    }
}
