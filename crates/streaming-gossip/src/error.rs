//! Error types.

/// Errors returned by the public API.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum GossipError {
    /// The configuration is inconsistent.
    #[error("invalid gossip config: {0}")]
    InvalidConfig(String),
    /// Binding the listen address failed.
    #[error("cannot bind {addr}: {reason}")]
    Bind {
        /// Address that failed.
        addr: String,
        /// Underlying error.
        reason: String,
    },
    /// Key is empty or longer than the limit.
    #[error("key length {len} not within 1..={max}")]
    KeyTooLong {
        /// Actual length.
        len: usize,
        /// Limit.
        max: usize,
    },
    /// Value longer than the limit.
    #[error("value length {len} exceeds {max}")]
    ValueTooLong {
        /// Actual length.
        len: usize,
        /// Limit.
        max: usize,
    },
    /// Publishing one more key would exceed the per-node key limit.
    #[error("too many keys (limit {max})")]
    TooManyKeys {
        /// Limit.
        max: usize,
    },
}

/// Errors of the wire layer (framing / decoding). Internal, but public so that
/// tests and tools can match on it.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// I/O error on the stream (includes EOF).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Frame length above the configured limit.
    #[error("frame of {len} bytes exceeds limit {max}")]
    FrameTooLarge {
        /// Announced length.
        len: usize,
        /// Limit.
        max: usize,
    },
    /// Checksum mismatch (corruption).
    #[error("frame checksum mismatch")]
    Checksum,
    /// Unknown protocol version byte.
    #[error("unsupported protocol version {0}")]
    Version(u8),
    /// Payload did not decode.
    #[error("malformed payload: {0}")]
    Malformed(String),
    /// Payload decoded but violates protocol rules.
    #[error("protocol violation: {0}")]
    Protocol(String),
}
