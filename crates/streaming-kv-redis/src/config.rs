//! Client configuration and URL parsing.

use std::{fmt, time::Duration};
use streaming_core::KvError;

/// Largest key Materia KV accepts.
pub const MAX_KEY_BYTES: usize = 8 * 1024;

/// Configuration of a [`crate::RedisKv`].
///
/// Zero values of the numeric limits are treated as `1` (and a zero
/// `max_value_bytes` as "nothing may be stored"), they never disable a limit.
#[derive(Clone)]
pub struct RedisConfig {
    /// `host:port` (IPv6 hosts in brackets: `[::1]:6379`).
    pub addr: String,
    /// ACL user name for `AUTH`.
    pub username: Option<String>,
    /// `AUTH` password, or the Materia KV token.
    pub password: Option<String>,
    /// Wrap the connection in TLS. Requires the `tls` cargo feature.
    pub tls: bool,
    /// Extra trust anchors (DER encoded certificates) added to the built-in
    /// web PKI roots. Only used with `tls`; meant for private CAs and tests.
    pub tls_extra_roots_der: Vec<Vec<u8>>,
    /// Maximum number of connections (also the maximum number of concurrent
    /// commands). Connections are opened lazily.
    pub connections: usize,
    /// Bound on waiting for a free connection, and separately on one
    /// request/response exchange.
    pub command_timeout: Duration,
    /// Bound on TCP connect + TLS handshake + `AUTH`.
    pub connect_timeout: Duration,
    /// Values larger than this are refused with `KvError::TooLarge` before
    /// anything is sent (Materia KV: 5 MiB).
    pub max_value_bytes: usize,
    /// `COUNT` hint of each `HSCAN` page used by `hgetall`.
    pub scan_count: usize,
    /// `HSET`/`HDEL` calls with more fields are split into several commands
    /// (and are then not atomic as a whole).
    pub max_fields_per_command: usize,
    /// `DEL` calls with more keys are split into several commands
    /// (Materia KV: 100).
    pub max_keys_per_del: usize,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:6379".to_owned(),
            username: None,
            password: None,
            tls: false,
            tls_extra_roots_der: Vec::new(),
            connections: 4,
            command_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            max_value_bytes: 5 * 1024 * 1024,
            scan_count: 500,
            max_fields_per_command: 1000,
            max_keys_per_del: 100,
        }
    }
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("addr", &self.addr)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .field("connections", &self.connections)
            .field("command_timeout", &self.command_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_value_bytes", &self.max_value_bytes)
            .field("scan_count", &self.scan_count)
            .field("max_fields_per_command", &self.max_fields_per_command)
            .field("max_keys_per_del", &self.max_keys_per_del)
            .finish_non_exhaustive()
    }
}

pub(crate) fn config_error(msg: impl fmt::Display) -> KvError {
    KvError::Protocol(format!("configuration error: {msg}"))
}

impl RedisConfig {
    /// Parse `redis://[user:pass@]host[:port][/0]` or `rediss://...` (TLS).
    ///
    /// The password-only form `redis://:token@host:port` is supported. User
    /// and password are percent-decoded. A lone userinfo without `:`
    /// (`redis://token@host`) is ambiguous and rejected. Only database `0`
    /// exists (no `SELECT` is sent). Other settings keep their defaults.
    ///
    /// # Errors
    /// `KvError::Protocol` describing the configuration problem.
    pub fn from_url(url: &str) -> Result<Self, KvError> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("redis://") {
            (false, rest)
        } else if let Some(rest) = url.strip_prefix("rediss://") {
            (true, rest)
        } else {
            return Err(config_error(
                "url must start with redis:// or rediss:// (scheme is case sensitive)",
            ));
        };
        if tls && !cfg!(feature = "tls") {
            return Err(tls_unavailable());
        }
        let rest = rest.split(['?', '#']).next().unwrap_or("");
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        if !(path.is_empty() || path == "0") {
            return Err(config_error(format!(
                "database `{path}` is not supported, only database 0"
            )));
        }
        let (userinfo, hostport) = match authority.rfind('@') {
            Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
            None => (None, authority),
        };
        let (username, password) = match userinfo {
            None => (None, None),
            Some(info) => match info.split_once(':') {
                None => {
                    return Err(config_error("userinfo must be `user:password` or `:password`"));
                }
                Some((user, pass)) => (
                    (!user.is_empty()).then(|| percent_decode(user)).transpose()?,
                    Some(percent_decode(pass)?),
                ),
            },
        };
        let addr = parse_host_port(hostport)?;
        Ok(Self {
            addr,
            username,
            password,
            tls,
            ..Self::default()
        })
    }

    /// Host part of [`Self::addr`], without brackets or port (TLS server name).
    #[must_use]
    pub fn host(&self) -> &str { host_of(&self.addr) }
}

pub(crate) fn tls_unavailable() -> KvError {
    config_error(
        "TLS requested (rediss:// or tls = true) but streaming-kv-redis was built without the `tls` cargo feature",
    )
}

pub(crate) fn host_of(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    addr.rsplit_once(':').map_or(addr, |(host, _)| host)
}

fn parse_host_port(hostport: &str) -> Result<String, KvError> {
    if hostport.is_empty() {
        return Err(config_error("url has no host"));
    }
    let (host, port) = if let Some(rest) = hostport.strip_prefix('[') {
        let Some((host, after)) = rest.split_once(']') else {
            return Err(config_error("unterminated `[` in host"));
        };
        let port = match after {
            "" => None,
            _ => Some(
                after
                    .strip_prefix(':')
                    .ok_or_else(|| config_error("garbage after IPv6 host"))?,
            ),
        };
        (format!("[{host}]"), port)
    } else {
        match hostport.rsplit_once(':') {
            Some((host, port)) => (host.to_owned(), Some(port)),
            None => (hostport.to_owned(), None),
        }
    };
    if host.is_empty() || host == "[]" {
        return Err(config_error("url has no host"));
    }
    let port = match port {
        None => 6379,
        Some(p) => p
            .parse::<u16>()
            .ok()
            .filter(|p| *p != 0)
            .ok_or_else(|| config_error(format!("invalid port `{p}`")))?,
    };
    Ok(format!("{host}:{port}"))
}

fn percent_decode(s: &str) -> Result<String, KvError> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| config_error("invalid percent escape in userinfo"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| config_error("userinfo is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_urls() {
        let c = RedisConfig::from_url("redis://localhost").unwrap();
        assert_eq!(c.addr, "localhost:6379");
        assert!(!c.tls && c.username.is_none() && c.password.is_none());

        let c = RedisConfig::from_url("redis://10.0.0.1:7000/0").unwrap();
        assert_eq!(c.addr, "10.0.0.1:7000");
    }

    #[test]
    fn credentials() {
        let c = RedisConfig::from_url("redis://bob:s3cret@db.internal:6380").unwrap();
        assert_eq!(c.username.as_deref(), Some("bob"));
        assert_eq!(c.password.as_deref(), Some("s3cret"));
        assert_eq!(c.addr, "db.internal:6380");

        // Password only, the Materia KV token form.
        let c = RedisConfig::from_url("redis://:tok%40en%3A1@kv.example:6379").unwrap();
        assert_eq!(c.username, None);
        assert_eq!(c.password.as_deref(), Some("tok@en:1"));

        // `@` inside the password, last one separates the host.
        let c = RedisConfig::from_url("redis://u:p@ss@h:1").unwrap();
        assert_eq!(c.password.as_deref(), Some("p@ss"));
        assert_eq!(c.addr, "h:1");

        // Empty password is still "set".
        let c = RedisConfig::from_url("redis://u:@h").unwrap();
        assert_eq!(c.password.as_deref(), Some(""));
    }

    #[test]
    fn ipv6() {
        let c = RedisConfig::from_url("redis://:pw@[::1]:6380").unwrap();
        assert_eq!(c.addr, "[::1]:6380");
        assert_eq!(c.host(), "::1");
        let c = RedisConfig::from_url("redis://[fe80::1]").unwrap();
        assert_eq!(c.addr, "[fe80::1]:6379");
    }

    #[test]
    fn rejects_bad_urls() {
        for url in [
            "http://h",
            "redis:/h",
            "redis://",
            "redis://:pw@",
            "redis://h:0",
            "redis://h:99999",
            "redis://h:abc",
            "redis://h/3",
            "redis://token@h",
            "redis://u:%zz@h",
            "redis://[::1",
            "redis://[]:6379",
            "redis://[]",
            "redis://:6379",
            "",
        ] {
            assert!(RedisConfig::from_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn rediss_needs_the_tls_feature() {
        let r = RedisConfig::from_url("rediss://:tok@kv.example:6379");
        if cfg!(feature = "tls") {
            let c = r.unwrap();
            assert!(c.tls);
            assert_eq!(c.host(), "kv.example");
        } else {
            let KvError::Protocol(msg) = r.unwrap_err() else {
                panic!("expected a configuration error")
            };
            assert!(msg.contains("`tls` cargo feature"), "{msg}");
        }
    }

    #[test]
    fn debug_hides_the_password() {
        let c = RedisConfig {
            password: Some("topsecret".into()),
            ..RedisConfig::default()
        };
        let shown = format!("{c:?}");
        assert!(!shown.contains("topsecret"), "{shown}");
        assert!(shown.starts_with("RedisConfig {"), "{shown}");
        assert!(shown.contains("password: Some(\"<redacted>\")"), "{shown}");
        assert!(shown.contains("addr: \"127.0.0.1:6379\""), "{shown}");
    }
}
