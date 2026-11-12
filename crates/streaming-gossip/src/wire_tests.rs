#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

fn sample_delta() -> Delta {
    Delta {
        nodes: vec![
            NodeDelta {
                node_id: "node-1".into(),
                generation: 1_700_000_000_123,
                addr: "10.0.1.1:7000".into(),
                heartbeat: 300,
                reset: true,
                from_version: 0,
                to_version: 129,
                last_gc_version: 17,
                updates: vec![
                    KeyUpdate {
                        key: "addr".into(),
                        value: Some("10.0.1.1:9000".into()),
                        version: 3,
                    },
                    KeyUpdate {
                        key: "gone".into(),
                        value: None,
                        version: 128,
                    },
                    KeyUpdate {
                        key: "k".repeat(200),
                        value: Some("é".repeat(100)),
                        version: 129,
                    },
                ],
            },
            NodeDelta {
                node_id: "n2".into(),
                generation: 0,
                addr: String::new(),
                heartbeat: u64::MAX,
                reset: false,
                from_version: 5,
                to_version: 5,
                last_gc_version: 0,
                updates: vec![],
            },
        ],
    }
}

fn sample_digest() -> Digest {
    Digest {
        entries: vec![
            DigestEntry {
                node_id: "a".into(),
                generation: 1,
                heartbeat: 2,
                max_version: 3,
                last_gc_version: 0,
            },
            DigestEntry {
                node_id: "x".repeat(130),
                generation: u64::MAX,
                heartbeat: 1 << 40,
                max_version: 127,
                last_gc_version: 128,
            },
        ],
    }
}

fn all_messages() -> Vec<Message> {
    vec![
        Message::Syn {
            cluster_id: "prod".into(),
            digest: sample_digest(),
        },
        Message::SynAck {
            digest: sample_digest(),
            delta: sample_delta(),
        },
        Message::Ack { delta: sample_delta() },
        Message::Ack {
            delta: Delta::default(),
        },
        Message::BadCluster {
            cluster_id: "other".into(),
        },
    ]
}

#[test]
fn crc32_known_vector() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
}

#[test]
fn varint_len_matches_leb128() {
    for (v, l) in [(0u64, 1), (127, 1), (128, 2), (16_383, 2), (16_384, 3), (u64::MAX, 10)] {
        assert_eq!(varint_len(v), l, "{v}");
        let enc = postcard::to_allocvec(&v).unwrap();
        assert_eq!(enc.len(), l);
    }
}

#[test]
fn size_functions_are_exact() {
    let d = sample_delta();
    assert_eq!(delta_size(&d), postcard::to_allocvec(&d).unwrap().len());
    for nd in &d.nodes {
        assert_eq!(node_delta_size(nd), postcard::to_allocvec(nd).unwrap().len());
    }
    let g = sample_digest();
    assert_eq!(digest_size(&g), postcard::to_allocvec(&g).unwrap().len());
    assert_eq!(
        delta_size(&Delta::default()),
        postcard::to_allocvec(&Delta::default()).unwrap().len()
    );
}

#[test]
fn frames_round_trip() {
    for msg in all_messages() {
        let frame = encode_frame(&msg).unwrap();
        assert_eq!(decode_frame(&frame, 1 << 20).unwrap(), msg);
    }
}

#[test]
fn malformed_frames_are_rejected() {
    let msg = &all_messages()[1];
    let frame = encode_frame(msg).unwrap();
    let max = 1 << 20;

    // Oversized announced length, rejected from the header alone.
    let mut big = frame.clone();
    big[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(decode_frame(&big, max), Err(WireError::FrameTooLarge { .. })));
    let small_limit = frame.len() - FRAME_HEADER_LEN - 1;
    assert!(matches!(
        decode_frame(&frame, small_limit),
        Err(WireError::FrameTooLarge { .. })
    ));

    // Every single bit flip in the payload is caught (by the CRC).
    for i in FRAME_HEADER_LEN..frame.len() {
        for bit in 0..8 {
            let mut f = frame.clone();
            f[i] ^= 1 << bit;
            assert!(decode_frame(&f, max).is_err(), "flip at byte {i} bit {bit} undetected");
        }
    }
    // Flip in the CRC itself.
    let mut f = frame.clone();
    f[5] ^= 0x10;
    assert!(matches!(decode_frame(&f, max), Err(WireError::Checksum)));

    // Truncated / extended.
    assert!(decode_frame(&frame[..frame.len() - 1], max).is_err());
    assert!(decode_frame(&frame[..3], max).is_err());

    // Wrong protocol version (with a valid CRC).
    let mut payload = frame[FRAME_HEADER_LEN..].to_vec();
    payload[0] = 99;
    assert!(matches!(
        decode_payload(&payload, crc32(&payload)),
        Err(WireError::Version(99))
    ));

    // Trailing bytes after a valid message (with a valid CRC).
    let mut payload = frame[FRAME_HEADER_LEN..].to_vec();
    payload.push(0);
    assert!(matches!(
        decode_payload(&payload, crc32(&payload)),
        Err(WireError::Malformed(_))
    ));

    // Garbage with a valid CRC.
    let garbage = vec![PROTOCOL_VERSION, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
    assert!(matches!(
        decode_payload(&garbage, crc32(&garbage)),
        Err(WireError::Malformed(_))
    ));
    // Huge collection length with nothing behind it: no huge allocation, an error.
    let huge = vec![PROTOCOL_VERSION, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
    assert!(decode_payload(&huge, crc32(&huge)).is_err());
    // Empty payload.
    let mut empty = vec![0u8; FRAME_HEADER_LEN];
    empty[4..].copy_from_slice(&crc32(&[]).to_be_bytes());
    assert!(decode_frame(&empty, max).is_err());
}

#[test]
fn async_framing_over_a_byte_stream() {
    futures::executor::block_on(async {
        let mut buf = futures::io::Cursor::new(Vec::new());
        let mut total = 0;
        for m in all_messages() {
            total += write_frame(&mut buf, &m).await.unwrap();
        }
        assert_eq!(total, buf.get_ref().len());
        buf.set_position(0);
        for m in all_messages() {
            let (got, _) = read_frame(&mut buf, 1 << 20).await.unwrap();
            assert_eq!(got, m);
        }
        assert!(matches!(read_frame(&mut buf, 1 << 20).await, Err(WireError::Io(_))));

        // Oversized frame rejected before reading the payload.
        let frame = encode_frame(&all_messages()[1]).unwrap();
        let mut r = futures::io::Cursor::new(frame);
        assert!(matches!(
            read_frame(&mut r, 16).await,
            Err(WireError::FrameTooLarge { .. })
        ));
        assert_eq!(r.position(), FRAME_HEADER_LEN as u64);
    });
}
