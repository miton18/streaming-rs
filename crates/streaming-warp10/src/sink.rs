//! [`Warp10Writer`]: the `SinkConnector`.

use crate::{
    config::Warp10Config,
    gts::{Gts, encode_line},
    http::{Limits, Response, build_request, read_response},
};
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "tls")]
use futures_rustls::{TlsConnector, rustls::pki_types::ServerName};
use moonpool_core::{NetworkProvider, Providers, TimeProvider};
use streaming_core::{Emit, OpError, SinkConnector, SinkError, ops::downcast};

/// Longest excerpt of an error body put in a [`SinkError`], in characters.
const EXCERPT_CHARS: usize = 200;

/// Writes [`Gts`] points to Warp 10 through its `/api/v0/update` endpoint.
///
/// See the crate documentation for the idempotence argument.
pub struct Warp10Writer<P: Providers> {
    providers: P,
    config: Warp10Config,
    #[cfg(feature = "tls")]
    tls: Option<std::sync::Arc<futures_rustls::rustls::ClientConfig>>,
}

impl<P: Providers> std::fmt::Debug for Warp10Writer<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Warp10Writer")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<P: Providers> Warp10Writer<P> {
    /// Build a sink. Fails when the configuration is invalid, in particular
    /// for an `https://` endpoint in a build without the `tls` feature.
    pub fn new(providers: P, config: Warp10Config) -> Result<Self, OpError> {
        config.validate()?;
        #[cfg(feature = "tls")]
        let tls = if config.endpoint.tls {
            Some(
                crate::tls::client_config(&config.tls_extra_roots_der)
                    .map_err(|e| OpError::Config(format!("TLS: {e}")))?,
            )
        } else {
            None
        };
        Ok(Self {
            providers,
            config,
            #[cfg(feature = "tls")]
            tls,
        })
    }

    /// The configuration in use.
    #[must_use]
    pub fn config(&self) -> &Warp10Config { &self.config }

    /// Encode a batch and cut it into request bodies of at most
    /// `max_batch_bytes` (a single point larger than that travels alone).
    /// Nothing is sent: a batch with an unusable element fails here, whole.
    fn bodies(&self, batch: &[Emit]) -> Result<Vec<String>, SinkError> {
        let mut bodies = Vec::new();
        let mut current = String::new();
        let mut line = String::new();
        for emit in batch {
            let gts = downcast::<Gts>(&emit.value).map_err(|_| {
                SinkError(format!(
                    "warp10: element {} is not a Gts (expected `{}`)",
                    emit.id,
                    std::any::type_name::<Gts>()
                ))
            })?;
            line.clear();
            encode_line(gts, self.config.time_unit, &mut line)
                .map_err(|e| SinkError(format!("warp10: element {}: {e}", emit.id)))?;
            line.push('\n');
            if !current.is_empty() && current.len() + line.len() > self.config.max_batch_bytes {
                bodies.push(std::mem::take(&mut current));
            }
            current.push_str(&line);
        }
        if !current.is_empty() {
            bodies.push(current);
        }
        Ok(bodies)
    }

    /// One request: connect, send, read the answer. No timeout here.
    async fn exchange(&self, request: &[u8]) -> Result<Response, String> {
        let address = self
            .config
            .connect_address
            .clone()
            .unwrap_or_else(|| self.config.endpoint.address());
        let stream = self
            .providers
            .network()
            .connect(&address)
            .await
            .map_err(|e| format!("connect to {address} failed: {e}"))?;

        #[cfg(feature = "tls")]
        if let Some(tls) = &self.tls {
            let name = ServerName::try_from(self.config.endpoint.host.clone())
                .map_err(|e| format!("invalid TLS server name: {e}"))?;
            let mut stream = TlsConnector::from(tls.clone())
                .connect(name, stream)
                .await
                .map_err(|e| format!("TLS handshake with {address} failed: {e}"))?;
            return round_trip(&mut stream, request, &address).await;
        }

        let mut stream = stream;
        round_trip(&mut stream, request, &address).await
    }

    async fn post(&self, body: &str) -> Result<(), SinkError> {
        let endpoint = &self.config.endpoint;
        let request = build_request(
            "POST",
            &endpoint.path,
            &endpoint.host_header(),
            &[("X-Warp10-Token", &self.config.token), ("Content-Type", "text/plain")],
            body.as_bytes(),
        )
        .map_err(|e| SinkError(format!("warp10: cannot build the request: {e}")))?;

        let target = format!("{}{}", endpoint.host_header(), endpoint.path);
        let outcome = self
            .providers
            .time()
            .timeout(self.config.timeout, self.exchange(&request))
            .await;
        let response = match outcome {
            Err(_) => {
                return Err(SinkError(format!(
                    "warp10: POST {target} timed out after {:?}",
                    self.config.timeout
                )));
            }
            Ok(Err(e)) => return Err(SinkError(format!("warp10: POST {target}: {e}"))),
            Ok(Ok(response)) => response,
        };
        if response.status == 200 {
            return Ok(());
        }
        Err(SinkError(format!(
            "warp10: POST {target} returned HTTP {} {}: {}",
            response.status,
            response.reason,
            excerpt(&response.body, &self.config.token)
        )))
    }
}

async fn round_trip<S>(stream: &mut S, request: &[u8], address: &str) -> Result<Response, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(request)
        .await
        .map_err(|e| format!("sending the request to {address} failed: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("sending the request to {address} failed: {e}"))?;
    read_response(stream, Limits::RESPONSE)
        .await
        .map_err(|e| format!("reading the response from {address} failed: {e}"))
}

/// A short, single-line, printable excerpt of an error body, with the token
/// masked should the server echo it.
fn excerpt(body: &[u8], token: &str) -> String {
    let text = String::from_utf8_lossy(body);
    let text = if token.is_empty() {
        text.into_owned()
    } else {
        text.replace(token, "***")
    };
    let mut out: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(EXCERPT_CHARS)
        .collect();
    if text.chars().count() > EXCERPT_CHARS {
        out.push_str("...");
    }
    let out = out.trim().to_owned();
    if out.is_empty() { "<empty body>".to_owned() } else { out }
}

#[async_trait]
impl<P: Providers> SinkConnector for Warp10Writer<P> {
    async fn write(&self, batch: &[Emit]) -> Result<(), SinkError> {
        // Encode everything first: an element that cannot be sent fails the
        // batch before any request leaves.
        for body in self.bodies(batch)? {
            self.post(&body).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gts::TimeUnit;
    use moonpool_core::TokioProviders;
    use streaming_core::{EmitId, Origin, Position, SliceId, datum};

    pub(crate) fn emit(n: u64, value: streaming_core::Datum) -> Emit {
        let origin = Origin::source(Position(0, n, 0));
        let slice = SliceId::Source {
            node: streaming_core::NodeId::new("n").unwrap(),
            partition: 0,
        };
        Emit {
            id: EmitId::new(&slice, &origin),
            origin,
            ts: 0,
            key: None,
            value,
        }
    }

    fn sink(max: usize, unit: TimeUnit) -> Warp10Writer<TokioProviders> {
        let mut config = Warp10Config::new("http://127.0.0.1:1/api/v0/update", "tok").unwrap();
        config.max_batch_bytes = max;
        config.time_unit = unit;
        Warp10Writer::new(TokioProviders::new(), config).unwrap()
    }

    #[test]
    fn bodies_are_split_at_the_limit_and_keep_order() {
        let points: Vec<Emit> = (0..5)
            .map(|i| emit(i, datum(Gts::new("m").at(i64::try_from(i).unwrap()).value(1))))
            .collect();
        // "0// m{} 1\n" is 10 bytes
        let one = sink(1_000, TimeUnit::Ms).bodies(&points).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].lines().count(), 5);

        let split = sink(25, TimeUnit::Ms).bodies(&points).unwrap();
        assert_eq!(split.iter().map(|b| b.lines().count()).collect::<Vec<_>>(), [2, 2, 1]);
        assert!(split.iter().all(|b| b.len() <= 25 && b.ends_with('\n')));
        assert_eq!(split.concat(), one[0]);

        // a point bigger than the limit still goes, alone
        let tiny = sink(3, TimeUnit::Ms).bodies(&points).unwrap();
        assert_eq!(tiny.len(), 5);
        assert!(sink(10, TimeUnit::Ms).bodies(&[]).unwrap().is_empty());
    }

    #[test]
    fn a_body_may_reach_the_limit_exactly() {
        let points: Vec<Emit> = (0..2)
            .map(|i| emit(i, datum(Gts::new("m").at(i64::try_from(i).unwrap()).value(1))))
            .collect();
        // Two 10-byte lines fit in a 20-byte body.
        let bodies = sink(20, TimeUnit::Ms).bodies(&points).unwrap();
        assert_eq!(bodies.len(), 1);
        let bodies = sink(19, TimeUnit::Ms).bodies(&points).unwrap();
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn debug_forms_hide_the_token() {
        let s = sink(10, TimeUnit::Ms);
        let debug = format!("{s:?}");
        assert!(debug.starts_with("Warp10Writer {"), "{debug}");
        assert!(debug.contains("<redacted>") && !debug.contains("tok\""), "{debug}");
    }

    #[test]
    fn a_foreign_datum_fails_the_whole_batch() {
        let batch = vec![
            emit(0, datum(Gts::new("m").value(1))),
            emit(1, datum(String::from("not a gts"))),
        ];
        let err = sink(100, TimeUnit::Ms).bodies(&batch).unwrap_err();
        assert!(err.0.contains("not a Gts"), "{err}");
    }

    #[test]
    fn an_unencodable_point_fails_the_whole_batch() {
        let batch = vec![
            emit(0, datum(Gts::new("m").value(1))),
            emit(1, datum(Gts::new("m").value(f64::NAN))),
        ];
        let err = sink(100, TimeUnit::Ms).bodies(&batch).unwrap_err();
        assert!(err.0.contains("non-finite"), "{err}");
    }

    #[test]
    fn excerpts_are_bounded_single_line_and_token_free() {
        let long = "x".repeat(1000);
        let e = excerpt(long.as_bytes(), "tok");
        assert!(e.chars().count() <= EXCERPT_CHARS + 3 && e.ends_with("..."));
        let exact = "y".repeat(EXCERPT_CHARS);
        assert_eq!(excerpt(exact.as_bytes(), "tok"), exact);
        assert_eq!(excerpt(b"a\nb\r\nc", "tok"), "a b  c");
        assert_eq!(excerpt(b"bad SECRET here", "SECRET"), "bad *** here");
        assert_eq!(excerpt(b"", "tok"), "<empty body>");
        assert!(excerpt(&[0xff, 0xfe, b'a'], "tok").ends_with('a'));
    }
}
