//! Wire format.
//!
//! One gossip exchange uses one TCP connection carrying three frames:
//! `Syn` (initiator -> responder), `SynAck` (responder -> initiator), `Ack`
//! (initiator -> responder). A responder that does not belong to the
//! initiator's cluster answers `BadCluster` instead of `SynAck`.
//!
//! Frame layout (all integers big-endian):
//!
//! ```text
//! +----------------+----------------+-----------------------------------+
//! | len: u32       | crc32: u32     | payload: len bytes                |
//! +----------------+----------------+-----------------------------------+
//! payload = [PROTOCOL_VERSION: u8] ++ postcard(Message)
//! ```
//!
//! * `len` is checked against the configured `max_frame_bytes` *before*
//!   anything is allocated; larger frames are rejected and the connection is
//!   dropped.
//! * `crc32` (IEEE, as in zlib) covers the whole payload; a mismatch drops the
//!   frame (the network can flip bits).
//! * The postcard payload must be consumed exactly (no trailing bytes).
//!
//! postcard encodes integers as LEB128 varints, strings and sequences as a
//! varint length followed by the elements, `Option` as a one byte tag, `bool`
//! as one byte and enum variants as a varint discriminant: the `*_size`
//! functions below compute the exact encoded sizes so that deltas can be
//! filled up to a byte budget without serialising twice.

use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use serde::{Deserialize, Serialize};

use crate::error::WireError;

/// Protocol version byte, first byte of every payload.
pub const PROTOCOL_VERSION: u8 = 1;
/// Frame header length (len + crc).
pub const FRAME_HEADER_LEN: usize = 8;
/// Absolute upper bound of `max_frame_bytes`, whatever the configuration.
pub const ABSOLUTE_MAX_FRAME: usize = 64 * 1024 * 1024;
/// Max varint length of a u64.
pub const MAX_VARINT_U64: usize = 10;
/// Max varint length of a usize (collection lengths).
pub const MAX_VARINT_USIZE: usize = 10;
/// Upper bound of the per message overhead besides digest / delta / cluster id
/// (version byte, discriminant, cluster id length).
pub const MESSAGE_OVERHEAD: usize = 1 + 5 + MAX_VARINT_USIZE;

/// Per node summary sent in SYN / SYN-ACK.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestEntry {
    /// Node id.
    pub node_id: String,
    /// Generation known for that node.
    pub generation: u64,
    /// Heartbeat known for that node.
    pub heartbeat: u64,
    /// Highest version known for that node (every version <= it is known).
    pub max_version: u64,
    /// Highest version of that node below which deletions may have been
    /// garbage-collected (see `state` module docs).
    pub last_gc_version: u64,
}

/// What a node knows about the cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    /// One entry per known node, including the sender itself.
    pub entries: Vec<DigestEntry>,
}

/// One key change. `value == None` is a tombstone (deletion).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyUpdate {
    /// Key.
    pub key: String,
    /// New value, `None` when deleted.
    pub value: Option<String>,
    /// Version assigned by the owning node.
    pub version: u64,
}

/// Changes of one node's state.
///
/// Updates are sorted by increasing version and cover every version in
/// `(from_version, to_version]` of the owner (a prefix when truncated by the
/// byte budget). With `reset`, the receiver replaces everything it knows about
/// `(node_id, generation)` by this content (tombstones are omitted and
/// accounted for by `last_gc_version`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDelta {
    /// Node id.
    pub node_id: String,
    /// Generation of the content.
    pub generation: u64,
    /// Advertise address of the node.
    pub addr: String,
    /// Latest heartbeat known by the sender.
    pub heartbeat: u64,
    /// Full replacement of the receiver's knowledge.
    pub reset: bool,
    /// Versions strictly above this one are covered.
    pub from_version: u64,
    /// Versions up to and including this one are covered.
    pub to_version: u64,
    /// For resets: receiver's new `last_gc_version`.
    pub last_gc_version: u64,
    /// The key updates, by increasing version.
    pub updates: Vec<KeyUpdate>,
}

/// A set of node deltas.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delta {
    /// Node deltas.
    pub nodes: Vec<NodeDelta>,
}

/// Messages of the 3-way exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Initiator: cluster id + its digest.
    Syn {
        /// Cluster id of the initiator.
        cluster_id: String,
        /// Initiator's digest.
        digest: Digest,
    },
    /// Responder: its digest + what the initiator is missing.
    SynAck {
        /// Responder's digest.
        digest: Digest,
        /// Delta for the initiator.
        delta: Delta,
    },
    /// Initiator: what the responder is missing.
    Ack {
        /// Delta for the responder.
        delta: Delta,
    },
    /// Responder belongs to another cluster.
    BadCluster {
        /// Responder's cluster id.
        cluster_id: String,
    },
}

/// LEB128 length of `v`.
#[must_use]
pub const fn varint_len(v: u64) -> usize {
    let bits = 64 - v.leading_zeros() as usize;
    if bits == 0 { 1 } else { bits.div_ceil(7) }
}

/// Encoded size of a string.
#[must_use]
pub fn string_size(s: &str) -> usize {
    varint_len(s.len() as u64) + s.len()
}

/// Encoded size of a key update.
#[must_use]
pub fn key_update_size(key: &str, value: Option<&str>, version: u64) -> usize {
    string_size(key) + 1 + value.map_or(0, string_size) + varint_len(version)
}

/// Encoded size of a node delta whose `updates` vector has `updates_len`
/// elements, excluding the elements themselves.
#[must_use]
pub fn node_header_size(nd: &NodeDelta, updates_len: usize) -> usize {
    string_size(&nd.node_id)
        + varint_len(nd.generation)
        + string_size(&nd.addr)
        + varint_len(nd.heartbeat)
        + 1
        + varint_len(nd.from_version)
        + varint_len(nd.to_version)
        + varint_len(nd.last_gc_version)
        + varint_len(updates_len as u64)
}

/// Exact encoded size of a node delta.
#[must_use]
pub fn node_delta_size(nd: &NodeDelta) -> usize {
    node_header_size(nd, nd.updates.len())
        + nd.updates
            .iter()
            .map(|u| key_update_size(&u.key, u.value.as_deref(), u.version))
            .sum::<usize>()
}

/// Exact encoded size of a delta.
#[must_use]
pub fn delta_size(d: &Delta) -> usize {
    varint_len(d.nodes.len() as u64) + d.nodes.iter().map(node_delta_size).sum::<usize>()
}

/// Exact encoded size of a digest entry.
#[must_use]
pub fn digest_entry_size(e: &DigestEntry) -> usize {
    string_size(&e.node_id)
        + varint_len(e.generation)
        + varint_len(e.heartbeat)
        + varint_len(e.max_version)
        + varint_len(e.last_gc_version)
}

/// Exact encoded size of a digest.
#[must_use]
pub fn digest_size(d: &Digest) -> usize {
    varint_len(d.entries.len() as u64) + d.entries.iter().map(digest_entry_size).sum::<usize>()
}

/// CRC-32 (IEEE 802.3, reflected, as zlib/PNG).
#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            table[i] = c;
            i += 1;
        }
        table
    };
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = TABLE[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Encode a message into a complete frame (header + payload).
///
/// # Errors
/// [`WireError::Malformed`] if serialisation fails (cannot happen for these types).
pub fn encode_frame(msg: &Message) -> Result<Vec<u8>, WireError> {
    let mut payload = vec![PROTOCOL_VERSION];
    payload = postcard::to_extend(msg, payload).map_err(|e| WireError::Malformed(e.to_string()))?;
    let len = u32::try_from(payload.len()).map_err(|_| WireError::FrameTooLarge {
        len: payload.len(),
        max: u32::MAX as usize,
    })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&crc32(&payload).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Parse a frame header, returning `(payload_len, crc)`.
///
/// # Errors
/// [`WireError::FrameTooLarge`] when the announced length exceeds `max`.
pub fn decode_header(
    header: [u8; FRAME_HEADER_LEN],
    max: usize,
) -> Result<(usize, u32), WireError> {
    let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    if len > max {
        return Err(WireError::FrameTooLarge { len, max });
    }
    if len == 0 {
        return Err(WireError::Malformed("empty payload".into()));
    }
    Ok((len, crc))
}

/// Verify and decode a payload.
///
/// # Errors
/// Checksum, version or decoding errors.
pub fn decode_payload(payload: &[u8], crc: u32) -> Result<Message, WireError> {
    if crc32(payload) != crc {
        return Err(WireError::Checksum);
    }
    let (&version, body) = payload
        .split_first()
        .ok_or_else(|| WireError::Malformed("empty payload".into()))?;
    if version != PROTOCOL_VERSION {
        return Err(WireError::Version(version));
    }
    let (msg, rest) = postcard::take_from_bytes::<Message>(body)
        .map_err(|e| WireError::Malformed(e.to_string()))?;
    if !rest.is_empty() {
        return Err(WireError::Malformed(format!(
            "{} trailing bytes",
            rest.len()
        )));
    }
    Ok(msg)
}

/// Decode a complete frame held in memory.
///
/// # Errors
/// Any framing / decoding error, including a length mismatch.
pub fn decode_frame(frame: &[u8], max: usize) -> Result<Message, WireError> {
    let header: [u8; FRAME_HEADER_LEN] = frame
        .get(..FRAME_HEADER_LEN)
        .and_then(|h| h.try_into().ok())
        .ok_or_else(|| WireError::Malformed("short frame".into()))?;
    let (len, crc) = decode_header(header, max)?;
    let payload = &frame[FRAME_HEADER_LEN..];
    if payload.len() != len {
        return Err(WireError::Malformed(format!(
            "length {len} announced, {} present",
            payload.len()
        )));
    }
    decode_payload(payload, crc)
}

/// Write one frame; returns the number of bytes written.
///
/// # Errors
/// I/O errors.
pub async fn write_frame<W: AsyncWrite + Unpin + Send>(
    w: &mut W,
    msg: &Message,
) -> Result<usize, WireError> {
    let frame = encode_frame(msg)?;
    w.write_all(&frame).await?;
    w.flush().await?;
    Ok(frame.len())
}

/// Read one frame; returns the message and the number of bytes read.
///
/// # Errors
/// I/O errors (EOF included), oversize frames, corruption, malformed payload.
pub async fn read_frame<R: AsyncRead + Unpin + Send>(
    r: &mut R,
    max: usize,
) -> Result<(Message, usize), WireError> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut header).await?;
    let (len, crc) = decode_header(header, max)?;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    let msg = decode_payload(&payload, crc)?;
    Ok((msg, FRAME_HEADER_LEN + len))
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
