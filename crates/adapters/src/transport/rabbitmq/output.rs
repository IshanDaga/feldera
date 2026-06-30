//! RabbitMQ (AMQP 1.0) output adapter.
//!
//! The adapter attaches an AMQP 1.0 sender link to a queue or exchange and
//! publishes each encoded record as a message.  When publishing to an exchange,
//! the routing key travels as the message *subject* (RabbitMQ maps the AMQP 1.0
//! subject to the classic routing key), so it can be set per record from a
//! key/value format such as Debezium.  Static headers and per-record headers are
//! carried as AMQP *application-properties*.
//!
//! The [`OutputEndpoint`] interface is synchronous, so the adapter drives the
//! asynchronous `fe2o3-amqp` client with [`TOKIO`]`.block_on`.

use super::connection::connect;
use anyhow::{Context, Result as AnyResult, anyhow};
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{
    ApplicationProperties, Data, Header, Message, Properties,
};
use fe2o3_amqp::types::primitives::{Binary, SimpleValue};
use fe2o3_amqp::{Sender, Session};
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputEndpoint};
use feldera_types::transport::rabbitmq::{RabbitMqOutputConfig, RabbitMqOutputTarget};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::{info_span, span::EnteredSpan};

/// Live AMQP connection state, created on [`OutputEndpoint::connect`].
///
/// The connection and session handles are not accessed directly after setup, but
/// they must be kept alive: dropping them tears down the AMQP engine task and
/// closes the link the sender publishes on.
struct AmqpState {
    #[allow(dead_code)]
    connection: ConnectionHandle<()>,
    #[allow(dead_code)]
    session: SessionHandle<()>,
    sender: Sender,
}

pub struct RabbitMqOutputEndpoint {
    config: RabbitMqOutputConfig,
    state: Option<AmqpState>,
}

impl RabbitMqOutputEndpoint {
    pub fn new(config: RabbitMqOutputConfig) -> AnyResult<Self> {
        if config.initialization_timeout_secs == 0 {
            return Err(anyhow!(
                "invalid RabbitMQ output configuration: 'initialization_timeout_secs' must be at least 1"
            ));
        }
        if config.connection.connection_timeout_secs == 0 {
            return Err(anyhow!(
                "invalid RabbitMQ output configuration: 'connection_timeout_secs' must be at least 1"
            ));
        }
        Ok(Self {
            config,
            state: None,
        })
    }

    fn span(&self) -> EnteredSpan {
        info_span!(
            "rabbitmq_output",
            url = %self.config.connection.url,
            target = %target_address(&self.config.target),
        )
        .entered()
    }

    /// Publishes a single message with the given payload, routing key, and
    /// headers.
    fn publish(
        &mut self,
        payload: &[u8],
        routing_key: Option<&str>,
        extra_headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let durable = self.config.durable;
        let static_headers = self.config.headers.clone();
        let subject = routing_key
            .map(str::to_string)
            .or_else(|| default_routing_key(&self.config.target));

        let state = self
            .state
            .as_mut()
            .ok_or_else(|| anyhow!("RabbitMQ output: publish called before connect"))?;

        let message = build_message(payload, subject, durable, &static_headers, extra_headers);

        TOKIO.block_on(async {
            let outcome = state
                .sender
                .send(message)
                .await
                .context("failed to publish message to RabbitMQ")?;
            outcome
                .accepted_or_else(|state| anyhow!("RabbitMQ broker rejected message: {state:?}"))?;
            Ok::<_, anyhow::Error>(())
        })
    }
}

impl OutputEndpoint for RabbitMqOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let _guard = self.span();
        let config = self.config.clone();
        let target = target_address(&config.target);
        let init_timeout = Duration::from_secs(config.initialization_timeout_secs.max(1));

        let state = TOKIO.block_on(async move {
            tokio::time::timeout(init_timeout, async {
                let mut connection = connect(&config.connection).await?;
                let mut session = Session::begin(&mut connection)
                    .await
                    .context("failed to begin AMQP session")?;
                let sender = Sender::attach(
                    &mut session,
                    format!("feldera-{}", uuid::Uuid::now_v7()),
                    target.clone(),
                )
                .await
                .with_context(|| format!("failed to attach sender to '{target}'"))?;
                Ok::<_, anyhow::Error>(AmqpState {
                    connection,
                    session,
                    sender,
                })
            })
            .await
            .map_err(|_| anyhow!("timed out after {init_timeout:?} connecting to RabbitMQ"))?
        })?;

        self.state = Some(state);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = self.span();
        self.publish(buffer, None, &[])
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = self.span();
        // For an exchange target, the record key becomes the routing key.
        let routing_key = key.map(|key| String::from_utf8_lossy(key).into_owned());
        self.publish(val.unwrap_or(&[]), routing_key.as_deref(), headers)
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

/// Computes the AMQP 1.0 target address for an output target.
///
/// An exchange is addressed without its routing key so that the key can be set
/// per message through the message subject.
fn target_address(target: &RabbitMqOutputTarget) -> String {
    match target {
        RabbitMqOutputTarget::Queue { name } => format!("/queues/{name}"),
        RabbitMqOutputTarget::Exchange { exchange, .. } => format!("/exchanges/{exchange}"),
    }
}

/// The default routing key (message subject) for a target, if any.
fn default_routing_key(target: &RabbitMqOutputTarget) -> Option<String> {
    match target {
        RabbitMqOutputTarget::Queue { .. } => None,
        RabbitMqOutputTarget::Exchange { routing_key, .. } => routing_key.clone(),
    }
}

fn build_message(
    payload: &[u8],
    subject: Option<String>,
    durable: bool,
    static_headers: &BTreeMap<String, String>,
    extra_headers: &[(&str, Option<&[u8]>)],
) -> Message<Data> {
    let mut builder = Message::builder().header(Header::builder().durable(durable).build());

    if let Some(subject) = subject {
        builder = builder.properties(Properties::builder().subject(subject).build());
    }

    if !static_headers.is_empty() || !extra_headers.is_empty() {
        let mut properties = ApplicationProperties::builder();
        for (key, value) in static_headers {
            properties = properties.insert(key.clone(), SimpleValue::String(value.clone()));
        }
        for (key, value) in extra_headers {
            let value = value
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .unwrap_or_default();
            properties = properties.insert((*key).to_string(), SimpleValue::String(value));
        }
        builder = builder.application_properties(properties.build());
    }

    builder.data(Data(Binary::from(payload.to_vec()))).build()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn target_addresses() {
        assert_eq!(
            target_address(&RabbitMqOutputTarget::Queue {
                name: "orders".to_string()
            }),
            "/queues/orders"
        );
        assert_eq!(
            target_address(&RabbitMqOutputTarget::Exchange {
                exchange: "amq.topic".to_string(),
                routing_key: Some("a.b".to_string())
            }),
            "/exchanges/amq.topic"
        );
    }

    #[test]
    fn default_routing_keys() {
        assert_eq!(
            default_routing_key(&RabbitMqOutputTarget::Queue {
                name: "q".to_string()
            }),
            None
        );
        assert_eq!(
            default_routing_key(&RabbitMqOutputTarget::Exchange {
                exchange: "e".to_string(),
                routing_key: Some("rk".to_string())
            }),
            Some("rk".to_string())
        );
    }

    #[test]
    fn message_carries_payload_and_subject() {
        let headers = BTreeMap::from([("h".to_string(), "v".to_string())]);
        let message = build_message(b"hello", Some("rk".to_string()), true, &headers, &[]);

        let Data(bytes) = &message.body;
        assert_eq!(&bytes[..], b"hello");
        assert_eq!(
            message.properties.as_ref().unwrap().subject.as_deref(),
            Some("rk")
        );
        assert!(message.header.as_ref().unwrap().durable);
        assert!(message.application_properties.is_some());
    }
}
