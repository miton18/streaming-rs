//! Node configurations and their validation.

use crate::sequence::SequenceLayout;
use serde::Deserialize;
use serde_json::Value;
use streaming_core::{OpContext, OpError};

/// How a `pulsar://` / `pulsar+ssl://` service URL was understood.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceUrl {
    /// Whether the connection is TLS (`pulsar+ssl://`).
    pub tls: bool,
    /// Host name or address, without brackets for IPv6.
    pub host: String,
    /// TCP port (6650 / 6651 when the URL omits it).
    pub port: u16,
}

impl ServiceUrl {
    /// Parse `pulsar://host[:port]` or `pulsar+ssl://host[:port]`. A single
    /// host only: multi-host service URLs are rejected.
    pub fn parse(url: &str) -> Result<Self, String> {
        let url = url.trim();
        let (tls, rest) = if let Some(rest) = strip_prefix_ci(url, "pulsar+ssl://") {
            (true, rest)
        } else if let Some(rest) = strip_prefix_ci(url, "pulsar://") {
            (false, rest)
        } else {
            return Err(format!(
                "serviceUrl `{url}` must start with `pulsar://` or `pulsar+ssl://`"
            ));
        };
        let authority = rest.trim_end_matches('/');
        if authority.contains(',') {
            return Err(format!(
                "serviceUrl `{url}`: multi-host service URLs are not supported, give one host"
            ));
        }
        if authority.is_empty() || authority.contains('/') || authority.contains('@') {
            return Err(format!("serviceUrl `{url}`: expected `scheme://host[:port]`"));
        }
        let default_port = if tls { 6651 } else { 6650 };
        let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
            let (host, after) = v6
                .split_once(']')
                .ok_or_else(|| format!("serviceUrl `{url}`: unterminated IPv6 address"))?;
            let port = match after.strip_prefix(':') {
                Some(port) => parse_port(url, port)?,
                None if after.is_empty() => default_port,
                None => return Err(format!("serviceUrl `{url}`: garbage after IPv6 address")),
            };
            (host.to_owned(), port)
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host.to_owned(), parse_port(url, port)?),
                None => (authority.to_owned(), default_port),
            }
        };
        if host.is_empty() {
            return Err(format!("serviceUrl `{url}`: empty host"));
        }
        Ok(Self { tls, host, port })
    }

    /// `host:port` as the moonpool network provider dials it.
    #[must_use]
    pub fn addr(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    (s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)).then(|| &s[prefix.len()..])
}

fn parse_port(url: &str, port: &str) -> Result<u16, String> {
    port.parse()
        .map_err(|_| format!("serviceUrl `{url}`: invalid port `{port}`"))
}

/// Connection settings shared by the source and the sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionSettings {
    /// Where to connect.
    pub url: ServiceUrl,
    /// JWT for the `token` authentication method, already resolved.
    pub token: Option<String>,
    /// PEM file holding the CA(s) to trust instead of the system store
    /// (TLS only).
    pub tls_trust_certs_file: Option<String>,
}

/// Which timestamp becomes the record's event time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventTimeMode {
    /// The broker-side publish time (always present).
    #[default]
    Publish,
    /// The producer-set event time, falling back to the publish time when a
    /// message carries none.
    Event,
}

/// Where a brand-new subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialPosition {
    /// The oldest retained message (the default: a new graph reads the topic
    /// from its beginning).
    #[default]
    Earliest,
    /// Only messages published after the subscription is created.
    Latest,
}

fn default_receiver_queue_size() -> usize {
    1000
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct RawSource {
    service_url: String,
    topic: String,
    subscription: String,
    #[serde(default)]
    token: Option<Value>,
    #[serde(default)]
    tls_trust_certs_file: Option<String>,
    #[serde(default)]
    event_time: EventTimeMode,
    #[serde(default = "default_receiver_queue_size")]
    receiver_queue_size: usize,
    #[serde(default)]
    initial_position: InitialPosition,
}

/// Validated configuration of `std/pulsar-source@1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConfig {
    /// Connection.
    pub connection: ConnectionSettings,
    /// Topic, `persistent://tenant/namespace/name` (partitioned or not).
    pub topic: String,
    /// Durable subscription name.
    pub subscription: String,
    /// Event-time extraction.
    pub event_time: EventTimeMode,
    /// Consumer receiver queue (flow permits).
    pub receiver_queue_size: usize,
    /// Start position of a subscription that does not exist yet.
    pub initial_position: InitialPosition,
}

/// Opt-in broker-side deduplication settings of the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DedupConfig {
    /// Bits given to the three components of the origin position, most
    /// significant first.
    pub position_bits: [u8; 3],
    /// Bits given to the index of an element among the consecutive elements
    /// sharing one origin position.
    #[serde(default)]
    pub index_bits: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
struct RawSink {
    service_url: String,
    topic: String,
    #[serde(default)]
    token: Option<Value>,
    #[serde(default)]
    tls_trust_certs_file: Option<String>,
    #[serde(default)]
    producer_name: Option<String>,
    #[serde(default)]
    broker_dedup: Option<DedupConfig>,
    #[serde(default)]
    send_timeout_ms: Option<u64>,
}

/// Validated configuration of `std/pulsar-sink@1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConfig {
    /// Connection.
    pub connection: ConnectionSettings,
    /// Destination topic (partitioned or not).
    pub topic: String,
    /// Base producer name; the slice id is appended to it.
    pub producer_name: Option<String>,
    /// Broker-side deduplication (effectively-once towards Pulsar).
    pub dedup: Option<SequenceLayout>,
    /// Per-message send timeout, `0` disables it.
    pub send_timeout_ms: u64,
}

fn parse<T: serde::de::DeserializeOwned>(config: &Value) -> Result<T, OpError> {
    serde_json::from_value(config.clone()).map_err(|e| OpError::Config(e.to_string()))
}

fn check_topic(topic: &str) -> Result<(), OpError> {
    let rest = topic
        .strip_prefix("persistent://")
        .or_else(|| topic.strip_prefix("non-persistent://"))
        .ok_or_else(|| {
            OpError::Config(format!(
                "topic `{topic}` must be a full name `persistent://tenant/namespace/name`"
            ))
        })?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(OpError::Config(format!(
            "topic `{topic}` must be `persistent://tenant/namespace/name`"
        )));
    }
    Ok(())
}

fn connection<P: moonpool_core::Providers>(
    context: &OpContext<P>,
    service_url: &str,
    token: Option<&Value>,
    tls_trust_certs_file: Option<String>,
) -> Result<ConnectionSettings, OpError> {
    let url = ServiceUrl::parse(service_url).map_err(OpError::Config)?;
    let token = match token {
        None | Some(Value::Null) => None,
        Some(value) => Some(context.secret_or_literal(value)?),
    };
    if tls_trust_certs_file.is_some() && !url.tls {
        return Err(OpError::Config(
            "tlsTrustCertsFile only applies to a `pulsar+ssl://` serviceUrl".into(),
        ));
    }
    Ok(ConnectionSettings {
        url,
        token,
        tls_trust_certs_file,
    })
}

impl SourceConfig {
    /// Parse and validate a node configuration, resolving secrets.
    pub fn from_json<P: moonpool_core::Providers>(context: &OpContext<P>, config: &Value) -> Result<Self, OpError> {
        let raw: RawSource = parse(config)?;
        check_topic(&raw.topic)?;
        if raw.subscription.trim().is_empty() {
            return Err(OpError::Config("subscription must not be empty".into()));
        }
        if raw.receiver_queue_size == 0 {
            return Err(OpError::Config("receiverQueueSize must be positive".into()));
        }
        Ok(Self {
            connection: connection(context, &raw.service_url, raw.token.as_ref(), raw.tls_trust_certs_file)?,
            topic: raw.topic,
            subscription: raw.subscription,
            event_time: raw.event_time,
            receiver_queue_size: raw.receiver_queue_size,
            initial_position: raw.initial_position,
        })
    }
}

impl SinkConfig {
    /// Parse and validate a node configuration, resolving secrets.
    pub fn from_json<P: moonpool_core::Providers>(context: &OpContext<P>, config: &Value) -> Result<Self, OpError> {
        let raw: RawSink = parse(config)?;
        check_topic(&raw.topic)?;
        if let Some(name) = &raw.producer_name
            && name.trim().is_empty()
        {
            return Err(OpError::Config("producerName must not be empty".into()));
        }
        let dedup = match raw.broker_dedup {
            None => None,
            Some(dedup) => {
                if raw.producer_name.is_none() {
                    return Err(OpError::Config(
                        "brokerDedup needs a stable producerName: the broker keys its \
                         deduplication state by producer name"
                            .into(),
                    ));
                }
                Some(SequenceLayout::new(dedup.position_bits, dedup.index_bits).map_err(OpError::Config)?)
            }
        };
        Ok(Self {
            connection: connection(context, &raw.service_url, raw.token.as_ref(), raw.tls_trust_certs_file)?,
            topic: raw.topic,
            producer_name: raw.producer_name,
            dedup,
            send_timeout_ms: raw.send_timeout_ms.unwrap_or(30_000),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_urls() {
        let plain = ServiceUrl::parse("pulsar://broker.example:6650").unwrap();
        assert_eq!(
            plain,
            ServiceUrl {
                tls: false,
                host: "broker.example".into(),
                port: 6650
            }
        );
        assert_eq!(plain.addr(), "broker.example:6650");
        let tls = ServiceUrl::parse("pulsar+ssl://broker.example").unwrap();
        assert!(tls.tls);
        assert_eq!(tls.port, 6651);
        assert_eq!(ServiceUrl::parse("pulsar://h/").unwrap().port, 6650);
        assert_eq!(ServiceUrl::parse("PULSAR://h:1").unwrap().port, 1);
        let v6 = ServiceUrl::parse("pulsar://[::1]:7000").unwrap();
        assert_eq!(v6.host, "::1");
        assert_eq!(v6.addr(), "[::1]:7000");
        assert_eq!(ServiceUrl::parse("pulsar://[::1]").unwrap().port, 6650);
        for bad in [
            "http://h:6650",
            "pulsar://",
            "pulsar://h1:6650,h2:6650",
            "pulsar://h:port",
            "pulsar://h:6650/path",
            "pulsar://[::1",
            "pulsar://[::1]x",
            "pulsar://user@h",
            "pulsar://h/x",
        ] {
            assert!(ServiceUrl::parse(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn topic_names() {
        assert!(check_topic("persistent://public/default/orders").is_ok());
        assert!(check_topic("non-persistent://public/default/orders").is_ok());
        assert!(check_topic("orders").is_err());
        assert!(check_topic("persistent://public/orders").is_err());
        assert!(check_topic("persistent://public//orders").is_err());
    }
}
