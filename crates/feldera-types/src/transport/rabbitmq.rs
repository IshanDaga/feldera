//! Configuration for the RabbitMQ transport connector.
//!
//! The connector speaks **AMQP 1.0**, the OASIS/ISO standard protocol that
//! RabbitMQ exposes as a first-class transport since RabbitMQ 3.8 (and as a core
//! protocol since RabbitMQ 4.0).  AMQP 1.0 addresses queues, streams, and
//! exchanges uniformly:
//!
//! * a queue or stream is reached at the address `/queues/{name}`;
//! * an exchange is reached at the address `/exchanges/{exchange}/{routing-key}`.
//!
//! Message headers travel as AMQP *application-properties* and the routing key
//! travels as the message *subject*, so both can be used to filter the messages
//! that an input connector ingests.
//!
//! The configuration is intentionally structured (rather than a free-form option
//! bag) so that the web console and Python SDK can present a typed form, much
//! like the Kafka connector.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

const fn default_true() -> bool {
    true
}

const fn default_connection_timeout_secs() -> u64 {
    30
}

const fn default_initialization_timeout_secs() -> u64 {
    30
}

const fn default_retry_interval_secs() -> u64 {
    5
}

const fn default_prefetch() -> u32 {
    100
}

/// TLS/SSL options for an `amqps://` connection to a RabbitMQ broker.
///
/// When [`RabbitMqConnectionConfig::tls`] is set (or the connection URL uses the
/// `amqps` scheme), the connector establishes a TLS session using these options.
/// When the struct is left at its defaults, the broker certificate is validated
/// against the system root store with hostname verification enabled.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
pub struct RabbitMqTlsConfig {
    /// Path to a PEM file with one or more additional CA certificates trusted to
    /// sign the broker certificate.
    ///
    /// The certificates are added to the system root store rather than replacing
    /// it.  Use this to trust a private certificate authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_pem_path: Option<String>,

    /// PEM-encoded CA certificate(s), provided inline instead of via
    /// [`Self::ca_cert_pem_path`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_cert_pem: Option<String>,

    /// Path to a PEM file with the client certificate chain, used for mutual
    /// TLS.  Requires [`Self::client_key_pem_path`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_cert_pem_path: Option<String>,

    /// Path to a PEM file with the PKCS#8 client private key, used for mutual
    /// TLS.  Requires [`Self::client_cert_pem_path`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key_pem_path: Option<String>,

    /// Server name used for SNI and certificate hostname verification.
    ///
    /// Defaults to the host of the connection URL.  Override it when connecting
    /// through an IP address or a tunnel whose name differs from the certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,

    /// Disable certificate-chain and hostname verification.
    ///
    /// **Insecure.**  Intended only for testing against a broker with a
    /// self-signed certificate.  Never enable this in production.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

/// How to reach the RabbitMQ broker and authenticate to it.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RabbitMqConnectionConfig {
    /// Connection URL, e.g. `amqp://localhost:5672` or `amqps://host:5671`.
    ///
    /// The `amqps` scheme enables TLS.  Credentials embedded in the URL
    /// (`amqp://user:pass@host`) are used for SASL PLAIN authentication; prefer
    /// the dedicated [`Self::username`]/[`Self::password`] fields so that secrets
    /// can be supplied through Feldera secret references.
    pub url: String,

    /// SASL PLAIN username.  Ignored when the URL already carries credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// SASL PLAIN password.  Ignored when the URL already carries credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// AMQP virtual host to open the connection on.
    ///
    /// RabbitMQ selects the virtual host from the SASL hostname; when set, this
    /// value is appended to the broker host as `host/vhost`.  Defaults to the
    /// broker default (`/`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_host: Option<String>,

    /// TLS/SSL options.
    ///
    /// When omitted, TLS is used only if the URL scheme is `amqps`, with default
    /// (system root store) verification.  Set this to customize trust anchors,
    /// supply a client certificate, or relax verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<RabbitMqTlsConfig>,

    /// AMQP container id reported to the broker.  Defaults to an auto-generated
    /// unique id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_id: Option<String>,

    /// Maximum time, in seconds, to wait for the connection (and TLS handshake)
    /// to be established.  Must be at least 1.
    #[serde(default = "default_connection_timeout_secs")]
    pub connection_timeout_secs: u64,
}

/// Where a stream consumer begins reading.
///
/// Applies only to [`RabbitMqInputSource::Stream`]; RabbitMQ streams are
/// non-destructive logs, so the connector must declare a starting position.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum StreamOffset {
    /// Start at the first message retained by the stream.
    First,
    /// Start just past the last message; only new messages are delivered.
    #[default]
    Next,
    /// Start at the last message currently in the stream.
    Last,
    /// Start at an absolute stream offset (zero-based message index).
    Offset(u64),
    /// Start at the first message stored at or after the given Unix timestamp
    /// (milliseconds since the epoch).
    Timestamp(i64),
}

/// The RabbitMQ object an input connector consumes from.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RabbitMqInputSource {
    /// Consume from a classic or quorum queue at address `/queues/{name}`.
    Queue {
        /// Queue name.
        name: String,
    },
    /// Consume from a stream at address `/queues/{name}`, starting at `offset`.
    Stream {
        /// Stream name.
        name: String,
        /// Position at which to start reading.
        #[serde(default)]
        offset: StreamOffset,
    },
    /// Consume the messages an exchange routes to a queue.
    ///
    /// AMQP 1.0 receivers always attach to a queue, so the queue named here must
    /// already exist and be bound to `exchange` (typically with `routing_key`).
    /// Use the [`Self::routing_keys`](RabbitMqInputConfig::routing_keys) and
    /// [`headers`](RabbitMqInputConfig::headers) filters to narrow which routed
    /// messages are ingested.
    Exchange {
        /// Source exchange name, kept for documentation and validation.
        exchange: String,
        /// Existing queue bound to `exchange` to attach the receiver to.
        queue: String,
    },
}

/// The RabbitMQ object an output connector publishes to.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RabbitMqOutputTarget {
    /// Publish directly to a queue or stream at address `/queues/{name}`.
    Queue {
        /// Destination queue or stream name.
        name: String,
    },
    /// Publish to an exchange at address `/exchanges/{exchange}/{routing_key}`.
    ///
    /// The routing key is used by direct and topic exchanges to select bindings;
    /// for a headers exchange leave it empty and rely on
    /// [`headers`](RabbitMqOutputConfig::headers).
    Exchange {
        /// Destination exchange name.
        exchange: String,
        /// Default routing key applied to every message.  A Kafka-style
        /// key/value format (e.g. Debezium) overrides this per message with the
        /// record key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        routing_key: Option<String>,
    },
}

/// Configuration for reading data from RabbitMQ with an input connector.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RabbitMqInputConfig {
    /// Broker connection and authentication options.
    pub connection: RabbitMqConnectionConfig,

    /// Queue, stream, or exchange to consume from.
    pub source: RabbitMqInputSource,

    /// Ingest only messages whose routing key (the AMQP message *subject*)
    /// equals one of these values.  An empty list disables routing-key
    /// filtering.  Filtering is performed by the connector after delivery.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routing_keys: Vec<String>,

    /// Ingest only messages whose application-property headers contain all of
    /// these key/value pairs.  An empty map disables header filtering.
    /// Filtering is performed by the connector after delivery.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,

    /// Receiver link credit: the maximum number of unacknowledged messages the
    /// broker may have in flight.  Higher values increase throughput at the cost
    /// of memory.  Must be at least 1.
    #[serde(default = "default_prefetch")]
    pub prefetch: u32,

    /// Delay, in seconds, between reconnect attempts after a connection error.
    /// Must be at least 1.
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,
}

/// Configuration for writing data to RabbitMQ with an output connector.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RabbitMqOutputConfig {
    /// Broker connection and authentication options.
    pub connection: RabbitMqConnectionConfig,

    /// Queue, stream, or exchange to publish to.
    pub target: RabbitMqOutputTarget,

    /// Application-property headers added to every published message.
    ///
    /// A Kafka-style key/value format may add further per-message headers, which
    /// are merged with these.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,

    /// Mark published messages as durable so that the broker persists them to
    /// disk.  Enabled by default.
    #[serde(default = "default_true")]
    pub durable: bool,

    /// Maximum time, in seconds, to wait for the connection to be established
    /// during startup.  Must be at least 1.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u64,
}

impl RabbitMqConnectionConfig {
    /// Returns the SASL credentials to authenticate with, if any.
    ///
    /// Credentials embedded in the URL take precedence over the dedicated
    /// fields, matching the behavior documented on [`Self::username`].
    pub fn credentials(&self) -> Option<(String, String)> {
        if let Ok(url) = url::Url::parse(&self.url) {
            let user = url.username();
            if !user.is_empty() {
                if let Some(password) = url.password() {
                    return Some((user.to_string(), password.to_string()));
                }
            }
        }
        match (&self.username, &self.password) {
            (Some(user), Some(password)) => Some((user.clone(), password.clone())),
            _ => None,
        }
    }

    /// Returns true when the connection must use TLS, i.e. the URL uses the
    /// `amqps` scheme or explicit TLS options were supplied.
    pub fn uses_tls(&self) -> bool {
        self.tls.is_some()
            || url::Url::parse(&self.url)
                .map(|url| url.scheme().eq_ignore_ascii_case("amqps"))
                .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn input_config_defaults() {
        let config: RabbitMqInputConfig = serde_json::from_value(json!({
            "connection": {"url": "amqp://localhost:5672"},
            "source": {"kind": "queue", "name": "events"}
        }))
        .unwrap();

        assert_eq!(config.connection.url, "amqp://localhost:5672");
        assert!(matches!(config.source, RabbitMqInputSource::Queue { name } if name == "events"));
        assert_eq!(config.prefetch, default_prefetch());
        assert_eq!(config.retry_interval_secs, default_retry_interval_secs());
        assert!(config.routing_keys.is_empty());
        assert!(config.headers.is_empty());
        assert!(!config.connection.uses_tls());
    }

    #[test]
    fn input_stream_offset_round_trips() {
        let config: RabbitMqInputConfig = serde_json::from_value(json!({
            "connection": {"url": "amqps://broker:5671", "username": "u", "password": "p"},
            "source": {"kind": "stream", "name": "log", "offset": {"offset": 42}},
            "routing_keys": ["a", "b"],
            "headers": {"region": "eu"}
        }))
        .unwrap();

        match &config.source {
            RabbitMqInputSource::Stream { name, offset } => {
                assert_eq!(name, "log");
                assert_eq!(*offset, StreamOffset::Offset(42));
            }
            other => panic!("unexpected source: {other:?}"),
        }
        assert!(config.connection.uses_tls());
        assert_eq!(
            config.connection.credentials(),
            Some(("u".to_string(), "p".to_string()))
        );
        assert_eq!(config.routing_keys, vec!["a".to_string(), "b".to_string()]);

        // The config survives a serialize/deserialize round trip.
        let reparsed: RabbitMqInputConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(reparsed, config);
    }

    #[test]
    fn url_credentials_take_precedence() {
        let config = RabbitMqConnectionConfig {
            url: "amqp://urluser:urlpass@localhost:5672".to_string(),
            username: Some("fielduser".to_string()),
            password: Some("fieldpass".to_string()),
            virtual_host: None,
            tls: None,
            container_id: None,
            connection_timeout_secs: default_connection_timeout_secs(),
        };
        assert_eq!(
            config.credentials(),
            Some(("urluser".to_string(), "urlpass".to_string()))
        );
    }

    #[test]
    fn output_exchange_round_trips() {
        let config: RabbitMqOutputConfig = serde_json::from_value(json!({
            "connection": {"url": "amqp://localhost:5672"},
            "target": {"kind": "exchange", "exchange": "amq.topic", "routing_key": "orders.created"},
            "headers": {"source": "feldera"}
        }))
        .unwrap();

        match &config.target {
            RabbitMqOutputTarget::Exchange {
                exchange,
                routing_key,
            } => {
                assert_eq!(exchange, "amq.topic");
                assert_eq!(routing_key.as_deref(), Some("orders.created"));
            }
            other => panic!("unexpected target: {other:?}"),
        }
        assert!(config.durable);

        let reparsed: RabbitMqOutputConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(reparsed, config);
    }
}
