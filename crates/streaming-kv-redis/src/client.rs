//! The pooled RESP client and its [`Kv`] implementation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    FutureExt,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};
#[cfg(feature = "tls")]
use futures_rustls::{TlsConnector, rustls::pki_types::ServerName};
use moonpool_core::{NetworkProvider, Providers, TimeProvider};
use parking_lot::Mutex;
use streaming_core::{Kv, KvError};
use tokio::sync::Semaphore;

#[cfg(feature = "tls")]
use crate::config::config_error;
#[cfg(not(feature = "tls"))]
use crate::config::tls_unavailable;
use crate::{
    config::{MAX_KEY_BYTES, RedisConfig},
    resp::{Decoder, Value, encode_command},
};

/// Object-safe alias for the byte streams a connection runs over.
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Soft cap on the bytes of values sent by one `HSET` command (Materia KV
/// limits a transaction to 10 MB).
const MAX_BYTES_PER_COMMAND: usize = 8 * 1024 * 1024;
const READ_CHUNK: usize = 32 * 1024;

/// One established, authenticated connection.
struct Conn {
    io: Box<dyn Io>,
    decoder: Decoder,
    out: Vec<u8>,
    scratch: Vec<u8>,
}

impl Conn {
    fn new(io: Box<dyn Io>) -> Self {
        Self {
            io,
            decoder: Decoder::new(),
            out: Vec::new(),
            scratch: vec![0; READ_CHUNK],
        }
    }

    /// Send one request and read its reply. Any `Err` means the connection
    /// must be discarded (I/O failure or protocol desync).
    async fn call(&mut self, args: &[&[u8]]) -> Result<Value, String> {
        self.out.clear();
        encode_command(args, &mut self.out);
        self.io
            .write_all(&self.out)
            .await
            .map_err(|e| format!("write failed: {e}"))?;
        self.io.flush().await.map_err(|e| format!("flush failed: {e}"))?;
        loop {
            if let Some(value) = self.decoder.next_value().map_err(|e| e.to_string())? {
                if self.decoder.buffered() > 0 {
                    return Err("unsolicited bytes after reply".to_owned());
                }
                return Ok(value);
            }
            let n = self
                .io
                .read(&mut self.scratch)
                .await
                .map_err(|e| format!("read failed: {e}"))?;
            if n == 0 {
                return Err("connection closed by server".to_owned());
            }
            self.decoder.feed(&self.scratch[..n]);
        }
    }

    /// Cheap liveness check of an idle connection: a poll that finds data,
    /// EOF or an error means the server did not leave it quiet.
    fn looks_alive(&mut self) -> bool {
        let mut byte = [0u8; 1];
        self.io.read(&mut byte).now_or_never().is_none() && self.decoder.buffered() == 0
    }
}

#[cfg(feature = "tls")]
type TlsSetup = std::sync::Arc<futures_rustls::rustls::ClientConfig>;
#[cfg(not(feature = "tls"))]
type TlsSetup = ();

/// A [`Kv`] backed by a Redis-protocol server (Redis, Materia KV, ...).
///
/// All I/O and timing goes through the `P: Providers` bundle, so the same
/// client runs under Moonpool simulation. Connections are opened lazily, one
/// command is in flight per connection, and any I/O error, timeout or
/// protocol desync drops the connection and yields `KvError::Unavailable`
/// (outcome unknown, never retried here). A server error reply
/// (`-WRONGTYPE`, `-ERR`) is `KvError::Protocol` and keeps the connection.
///
/// Share it behind an `Arc`.
pub struct RedisKv<P: Providers> {
    providers: P,
    config: RedisConfig,
    permits: Semaphore,
    idle: Mutex<Vec<Conn>>,
    tls: Result<Option<TlsSetup>, String>,
}

impl<P: Providers> std::fmt::Debug for RedisKv<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisKv")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn unavailable(msg: impl Into<String>) -> KvError {
    KvError::Unavailable(msg.into())
}

fn protocol(msg: impl Into<String>) -> KvError {
    KvError::Protocol(msg.into())
}

impl<P: Providers> RedisKv<P> {
    /// Create a client. Nothing is connected until the first command.
    ///
    /// Zero limits in `config` are raised to 1. With `tls: true` and no `tls`
    /// cargo feature (or an unusable trust configuration), every command
    /// fails with a configuration error.
    #[must_use]
    pub fn new(providers: P, mut config: RedisConfig) -> Self {
        config.connections = config.connections.max(1);
        config.scan_count = config.scan_count.max(1);
        config.max_fields_per_command = config.max_fields_per_command.max(1);
        config.max_keys_per_del = config.max_keys_per_del.max(1);
        let tls = Self::tls_setup(&config);
        Self {
            providers,
            permits: Semaphore::new(config.connections),
            idle: Mutex::new(Vec::new()),
            tls,
            config,
        }
    }

    #[cfg(feature = "tls")]
    fn tls_setup(config: &RedisConfig) -> Result<Option<TlsSetup>, String> {
        if config.tls {
            crate::tls::client_config(&config.tls_extra_roots_der).map(Some)
        } else {
            Ok(None)
        }
    }

    #[cfg(not(feature = "tls"))]
    fn tls_setup(config: &RedisConfig) -> Result<Option<TlsSetup>, String> {
        if config.tls {
            Err(match tls_unavailable() {
                KvError::Protocol(m) => m,
                other => other.to_string(),
            })
        } else {
            Ok(None)
        }
    }

    /// `PING`.
    ///
    /// # Errors
    /// `Unavailable` when the server cannot be reached, `Protocol` otherwise.
    pub async fn ping(&self) -> Result<(), KvError> {
        self.exec(&[b"PING"], |v| match v {
            Value::Simple(s) if s == "PONG" => Ok(()),
            other => Err(format!("unexpected PING reply {other:?}")),
        })
        .await
    }

    /// The configuration in effect (limits already normalised).
    #[must_use]
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    // -- connection management ------------------------------------------

    async fn open(&self) -> Result<Conn, KvError> {
        if let Err(msg) = &self.tls {
            return Err(protocol(msg.clone()));
        }
        let stream = self
            .providers
            .network()
            .connect(&self.config.addr)
            .await
            .map_err(|e| unavailable(format!("connect to {} failed: {e}", self.config.addr)))?;
        let io: Box<dyn Io> = match &self.tls {
            Err(msg) => return Err(protocol(msg.clone())),
            Ok(None) => Box::new(stream),
            Ok(Some(setup)) => self.wrap_tls(setup, stream).await?,
        };
        let mut conn = Conn::new(io);
        let auth: Option<Vec<&[u8]>> = match (&self.config.username, &self.config.password) {
            (None, None) => None,
            (None, Some(p)) => Some(vec![b"AUTH", p.as_bytes()]),
            (Some(u), p) => Some(vec![b"AUTH", u.as_bytes(), p.as_deref().unwrap_or("").as_bytes()]),
        };
        if let Some(args) = auth {
            match conn.call(&args).await {
                Ok(Value::Simple(s)) if s == "OK" => {}
                Ok(Value::Error(msg)) => {
                    return Err(protocol(format!("AUTH rejected: {msg}")));
                }
                Ok(other) => return Err(protocol(format!("unexpected AUTH reply {other:?}"))),
                Err(e) => return Err(unavailable(format!("AUTH failed: {e}"))),
            }
        }
        Ok(conn)
    }

    #[cfg(feature = "tls")]
    async fn wrap_tls(
        &self,
        setup: &TlsSetup,
        stream: <P::Network as NetworkProvider>::TcpStream,
    ) -> Result<Box<dyn Io>, KvError> {
        let name = ServerName::try_from(self.config.host().to_owned())
            .map_err(|e| config_error(format!("invalid TLS server name: {e}")))?;
        let tls = TlsConnector::from(setup.clone())
            .connect(name, stream)
            .await
            .map_err(|e| unavailable(format!("TLS handshake failed: {e}")))?;
        Ok(Box::new(tls))
    }

    #[cfg(not(feature = "tls"))]
    #[allow(clippy::unused_async)]
    async fn wrap_tls(
        &self,
        _setup: &TlsSetup,
        _stream: <P::Network as NetworkProvider>::TcpStream,
    ) -> Result<Box<dyn Io>, KvError> {
        Err(tls_unavailable())
    }

    /// Run one command on a pooled connection and parse its reply.
    ///
    /// `parse` maps the reply; failing there means the server answered
    /// something we do not understand, so the connection is dropped.
    async fn exec<T>(&self, args: &[&[u8]], parse: impl FnOnce(Value) -> Result<T, String>) -> Result<T, KvError> {
        let time = self.providers.time();
        let cmd_timeout = self.config.command_timeout;

        let _permit = match time.timeout(cmd_timeout, self.permits.acquire()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(unavailable("connection pool closed")),
            Err(_) => return Err(unavailable("timed out waiting for a free connection")),
        };

        // Prefer an idle connection that still looks healthy.
        let mut conn = loop {
            let candidate = self.idle.lock().pop();
            match candidate {
                Some(mut c) => {
                    if c.looks_alive() {
                        break c;
                    }
                    tracing::debug!("dropping stale idle connection");
                }
                None => {
                    break match time.timeout(self.config.connect_timeout, self.open()).await {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(unavailable(format!(
                                "connecting to {} timed out after {:?}",
                                self.config.addr, self.config.connect_timeout
                            )));
                        }
                    };
                }
            }
        };

        let reply = match time.timeout(cmd_timeout, conn.call(args)).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(e)) => return Err(unavailable(e)),
            Err(_) => {
                return Err(unavailable(format!("command timed out after {cmd_timeout:?}")));
            }
        };
        // A well-formed exchange: the connection stays usable.
        if let Value::Error(msg) = reply {
            self.idle.lock().push(conn);
            return Err(protocol(msg));
        }
        match parse(reply) {
            Ok(value) => {
                self.idle.lock().push(conn);
                Ok(value)
            }
            Err(msg) => Err(protocol(msg)),
        }
    }

    // -- validation -------------------------------------------------------

    fn check_key(key: &str) -> Result<(), KvError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(KvError::TooLarge {
                size: key.len(),
                limit: MAX_KEY_BYTES,
            });
        }
        Ok(())
    }

    fn check_value(&self, value: &[u8]) -> Result<(), KvError> {
        if value.len() > self.config.max_value_bytes {
            return Err(KvError::TooLarge {
                size: value.len(),
                limit: self.config.max_value_bytes,
            });
        }
        Ok(())
    }
}

// -- reply parsers ---------------------------------------------------------

fn expect_ok(v: Value) -> Result<(), String> {
    match v {
        Value::Simple(s) if s == "OK" => Ok(()),
        other => Err(format!("expected +OK, got {other:?}")),
    }
}

fn expect_int(v: Value) -> Result<i64, String> {
    match v {
        Value::Integer(n) => Ok(n),
        other => Err(format!("expected an integer, got {other:?}")),
    }
}

fn expect_opt_bulk(v: Value) -> Result<Option<Vec<u8>>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Bulk(b) => Ok(Some(b.to_vec())),
        other => Err(format!("expected a bulk string, got {other:?}")),
    }
}

type ScanPage = (Bytes, Vec<(Bytes, Bytes)>);

fn expect_scan(v: Value) -> Result<ScanPage, String> {
    let Value::Array(mut parts) = v else {
        return Err("HSCAN: expected an array".to_owned());
    };
    if parts.len() != 2 {
        return Err(format!("HSCAN: expected 2 elements, got {}", parts.len()));
    }
    let flat = parts.pop();
    let cursor = parts.pop();
    let (Some(Value::Bulk(cursor)), Some(Value::Array(flat))) = (cursor, flat) else {
        return Err("HSCAN: malformed reply".to_owned());
    };
    if flat.len() % 2 != 0 {
        return Err("HSCAN: odd number of field/value elements".to_owned());
    }
    let mut pairs = Vec::with_capacity(flat.len() / 2);
    let mut it = flat.into_iter();
    while let (Some(Value::Bulk(f)), Some(Value::Bulk(v))) = (it.next(), it.next()) {
        pairs.push((f, v));
    }
    Ok((cursor, pairs))
}

// -- Kv --------------------------------------------------------------------

#[async_trait]
impl<P: Providers> Kv for RedisKv<P> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        Self::check_key(key)?;
        self.exec(&[b"GET", key.as_bytes()], expect_opt_bulk).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> {
        Self::check_key(key)?;
        self.check_value(&value)?;
        self.exec(&[b"SET", key.as_bytes(), &value], expect_ok).await
    }

    /// Splits into several `DEL` commands above `max_keys_per_del`; such a
    /// call is then not atomic as a whole.
    async fn del(&self, keys: Vec<String>) -> Result<(), KvError> {
        for key in &keys {
            Self::check_key(key)?;
        }
        for chunk in keys.chunks(self.config.max_keys_per_del) {
            let mut args: Vec<&[u8]> = Vec::with_capacity(chunk.len() + 1);
            args.push(b"DEL");
            args.extend(chunk.iter().map(String::as_bytes));
            self.exec(&args, expect_int).await?;
        }
        Ok(())
    }

    async fn incr(&self, key: &str) -> Result<i64, KvError> {
        Self::check_key(key)?;
        self.exec(&[b"INCR", key.as_bytes()], expect_int).await
    }

    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
        Self::check_key(key)?;
        self.check_value(&value)?;
        // The boolean is exactly Redis' integer reply; anything else is a
        // protocol error, never guessed.
        self.exec(&[b"HSETNX", key.as_bytes(), field.as_bytes(), &value], |v| match v {
            Value::Integer(1) => Ok(true),
            Value::Integer(0) => Ok(false),
            other => Err(format!("HSETNX: unexpected reply {other:?}")),
        })
        .await
    }

    /// One `HSET` (atomic) up to `max_fields_per_command` fields (and a few
    /// MiB of payload); beyond that, several sequential `HSET`s, so the call
    /// is then not atomic as a whole and may be partially applied on error.
    async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
        Self::check_key(key)?;
        for (_, value) in &entries {
            self.check_value(value)?;
        }
        let max_fields = self.config.max_fields_per_command;
        let mut start = 0;
        while start < entries.len() {
            let mut end = start;
            let mut bytes = 0usize;
            while end < entries.len() && end - start < max_fields {
                let size = entries[end].0.len() + entries[end].1.len();
                if end > start && bytes + size > MAX_BYTES_PER_COMMAND {
                    break;
                }
                bytes += size;
                end += 1;
            }
            let mut args: Vec<&[u8]> = Vec::with_capacity(2 * (end - start) + 2);
            args.push(b"HSET");
            args.push(key.as_bytes());
            for (field, value) in &entries[start..end] {
                args.push(field.as_bytes());
                args.push(value);
            }
            self.exec(&args, expect_int).await?;
            start = end;
        }
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> {
        Self::check_key(key)?;
        self.exec(&[b"HGET", key.as_bytes(), field.as_bytes()], expect_opt_bulk)
            .await
    }

    /// Pages through `HSCAN` (`scan_count` per page), merging pages by field.
    /// The result is sorted by field. Like any `HSCAN` loop it is not a
    /// point-in-time snapshot when the hash changes concurrently: a field
    /// present for the whole scan is always returned, others may or may not.
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError> {
        Self::check_key(key)?;
        let count = self.config.scan_count.to_string();
        let mut cursor: Vec<u8> = b"0".to_vec();
        let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        loop {
            let (next, pairs) = self
                .exec(
                    &[b"HSCAN", key.as_bytes(), &cursor, b"COUNT", count.as_bytes()],
                    expect_scan,
                )
                .await?;
            for (field, value) in pairs {
                let field = String::from_utf8(field.to_vec())
                    .map_err(|_| protocol(format!("hash `{key}` has a non UTF-8 field name")))?;
                merged.insert(field, value.to_vec());
            }
            if next.as_ref() == b"0" {
                break;
            }
            if next.is_empty() || next.len() > 20 || !next.iter().all(u8::is_ascii_digit) {
                return Err(protocol("HSCAN returned a malformed cursor"));
            }
            cursor = next.to_vec();
        }
        Ok(merged.into_iter().collect())
    }

    /// Splits into several `HDEL` commands above `max_fields_per_command`;
    /// such a call is then not atomic as a whole.
    async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError> {
        Self::check_key(key)?;
        for chunk in fields.chunks(self.config.max_fields_per_command) {
            let mut args: Vec<&[u8]> = Vec::with_capacity(chunk.len() + 2);
            args.push(b"HDEL");
            args.push(key.as_bytes());
            args.extend(chunk.iter().map(String::as_bytes));
            self.exec(&args, expect_int).await?;
        }
        Ok(())
    }
}
