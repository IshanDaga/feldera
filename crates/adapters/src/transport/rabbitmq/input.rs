//! RabbitMQ (AMQP 1.0) input adapter.
//!
//! The adapter attaches an AMQP 1.0 receiver link to a queue or stream and feeds
//! the delivered message payloads to a [`Parser`].  It is not fault tolerant: on
//! a connection error it reconnects after `retry_interval_secs` and resumes
//! reading new deliveries.  Routing-key and header filters are applied after
//! delivery so that the adapter works against any RabbitMQ version regardless of
//! its server-side filtering support.

use super::connection::connect;
use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, PipelineState, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand, NonFtInputReaderCommand},
};
use anyhow::{Context, Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::types::messaging::{Body, Source};
use fe2o3_amqp::types::primitives::{SimpleValue, Symbol, Timestamp, Value};
use fe2o3_amqp::{Receiver, Session};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::rabbitmq::{RabbitMqInputConfig, RabbitMqInputSource, StreamOffset},
};
use serde_amqp::described::Described;
use serde_amqp::descriptor::Descriptor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info_span};

/// RabbitMQ stream offset filter name understood by the broker.
const STREAM_OFFSET_FILTER: &str = "rabbitmq:stream-offset-spec";

pub struct RabbitMqInputEndpoint {
    config: Arc<RabbitMqInputConfig>,
}

impl RabbitMqInputEndpoint {
    pub fn new(config: RabbitMqInputConfig) -> AnyResult<Self> {
        if config.prefetch == 0 {
            return Err(anyhow!(
                "invalid RabbitMQ input configuration: 'prefetch' must be at least 1"
            ));
        }
        if config.retry_interval_secs == 0 {
            return Err(anyhow!(
                "invalid RabbitMQ input configuration: 'retry_interval_secs' must be at least 1"
            ));
        }
        if config.connection.connection_timeout_secs == 0 {
            return Err(anyhow!(
                "invalid RabbitMQ input configuration: 'connection_timeout_secs' must be at least 1"
            ));
        }
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for RabbitMqInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for RabbitMqInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(RabbitMqReader::new(
            self.config.clone(),
            consumer,
            parser,
        )))
    }
}

struct RabbitMqReader {
    command_sender: UnboundedSender<NonFtInputReaderCommand>,
}

impl RabbitMqReader {
    fn new(
        config: Arc<RabbitMqInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> Self {
        let (command_sender, command_receiver) = unbounded_channel();
        let span = info_span!("rabbitmq_input", url = %config.connection.url);

        thread::Builder::new()
            .name("rabbitmq-input-tokio-wrapper".to_string())
            .spawn(move || {
                let error_consumer = consumer.clone();
                TOKIO.block_on(async move {
                    worker_task(config, consumer, parser, command_receiver)
                        .instrument(span)
                        .await
                        .unwrap_or_else(|e| {
                            error_consumer.error(true, e, Some("rabbitmq-input"))
                        });
                });
            })
            .expect("failed to spawn RabbitMQ input tokio wrapper thread");

        Self { command_sender }
    }
}

impl InputReader for RabbitMqReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        if let Some(command) = command.as_nonft() {
            let _ = self.command_sender.send(command);
        }
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

/// The control loop: starts and stops the background read task in response to
/// the controller's pause/resume/queue commands.
async fn worker_task(
    config: Arc<RabbitMqInputConfig>,
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    mut command_receiver: UnboundedReceiver<NonFtInputReaderCommand>,
) -> AnyResult<()> {
    let queue = Arc::new(InputQueue::new(consumer.clone()));
    let mut reader: Option<(CancellationToken, JoinHandle<()>)> = None;

    while let Some(command) = command_receiver.recv().await {
        match command {
            NonFtInputReaderCommand::Queue => queue.queue(),
            NonFtInputReaderCommand::Transition(PipelineState::Running) => {
                if reader.is_none() {
                    let cancel = CancellationToken::new();
                    let handle = TOKIO.spawn(read_loop(
                        config.clone(),
                        consumer.clone(),
                        parser.fork(),
                        queue.clone(),
                        cancel.clone(),
                    ));
                    reader = Some((cancel, handle));
                }
            }
            NonFtInputReaderCommand::Transition(PipelineState::Paused) => {
                if let Some((cancel, handle)) = reader.take() {
                    cancel.cancel();
                    let _ = handle.await;
                }
            }
            NonFtInputReaderCommand::Transition(PipelineState::Terminated) => break,
        }
    }

    if let Some((cancel, handle)) = reader.take() {
        cancel.cancel();
        let _ = handle.await;
    }
    Ok(())
}

/// Reconnect loop: keeps a receiver attached, retrying after errors until the
/// task is cancelled.
async fn read_loop(
    config: Arc<RabbitMqInputConfig>,
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    queue: Arc<InputQueue<()>>,
    cancel: CancellationToken,
) {
    let retry_interval = Duration::from_secs(config.retry_interval_secs.max(1));
    let mut parser = parser;

    while !cancel.is_cancelled() {
        match consume(&config, parser.as_mut(), &queue, &cancel).await {
            Ok(()) => return,
            Err(error) => {
                consumer.error(
                    false,
                    error.context(format!(
                        "RabbitMQ input error, reconnecting in {retry_interval:?}"
                    )),
                    Some("rabbitmq-input"),
                );
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(retry_interval) => {}
                }
            }
        }
    }
}

/// Connects, attaches a receiver, and consumes deliveries until cancelled or an
/// error occurs.
async fn consume(
    config: &RabbitMqInputConfig,
    parser: &mut dyn Parser,
    queue: &InputQueue<()>,
    cancel: &CancellationToken,
) -> AnyResult<()> {
    let mut connection = connect(&config.connection).await?;
    let mut session = Session::begin(&mut connection)
        .await
        .context("failed to begin AMQP session")?;

    let source = build_source(&config.source);
    let link_name = format!("feldera-{}", uuid::Uuid::now_v7());
    let mut receiver = Receiver::builder()
        .name(link_name)
        .source(source)
        .credit_mode(CreditMode::Auto(config.prefetch))
        .attach(&mut session)
        .await
        .with_context(|| format!("failed to attach receiver to {}", source_address(&config.source)))?;

    let result = consume_loop(config, parser, queue, cancel, &mut receiver).await;

    // Best-effort cleanup; ignore shutdown errors since we are tearing down.
    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    result
}

async fn consume_loop(
    config: &RabbitMqInputConfig,
    parser: &mut dyn Parser,
    queue: &InputQueue<()>,
    cancel: &CancellationToken,
    receiver: &mut Receiver,
) -> AnyResult<()> {
    loop {
        let delivery = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            result = receiver.recv::<Body<Value>>() => {
                result.context("failed to receive AMQP message")?
            }
        };

        if message_passes_filter(config, &delivery) {
            let payload = body_to_bytes(delivery.body());
            queue.push(parser.parse(&payload, None), Utc::now());
        } else {
            debug!("RabbitMQ message filtered out by routing key or header match");
        }

        // Acknowledge every delivery, including filtered ones, so the broker does
        // not redeliver them.
        receiver
            .accept(&delivery)
            .await
            .context("failed to acknowledge AMQP message")?;
    }
}

/// Builds the AMQP source, including the stream offset filter for streams.
fn build_source(source: &RabbitMqInputSource) -> Source {
    let address = source_address(source);
    let builder = Source::builder().address(address);
    let builder = match source {
        RabbitMqInputSource::Stream { offset, .. } => builder
            .add_to_filter(Symbol::from(STREAM_OFFSET_FILTER), stream_offset_filter(offset)),
        RabbitMqInputSource::Queue { .. } | RabbitMqInputSource::Exchange { .. } => builder,
    };
    builder.build()
}

/// Computes the AMQP 1.0 address for an input source.  Queues, streams, and
/// exchange-bound queues are all reached through `/queues/{name}`.
fn source_address(source: &RabbitMqInputSource) -> String {
    match source {
        RabbitMqInputSource::Queue { name } | RabbitMqInputSource::Stream { name, .. } => {
            format!("/queues/{name}")
        }
        RabbitMqInputSource::Exchange { queue, .. } => format!("/queues/{queue}"),
    }
}

fn stream_offset_filter(offset: &StreamOffset) -> Described<Value> {
    let value = match offset {
        StreamOffset::First => Value::String("first".to_string()),
        StreamOffset::Next => Value::String("next".to_string()),
        StreamOffset::Last => Value::String("last".to_string()),
        StreamOffset::Offset(n) => Value::Ulong(*n),
        StreamOffset::Timestamp(ts) => Value::Timestamp(Timestamp::from_milliseconds(*ts)),
    };
    Described {
        descriptor: Descriptor::Name(Symbol::from(STREAM_OFFSET_FILTER)),
        value,
    }
}

/// Returns true if the message satisfies the configured routing-key and header
/// filters.  Empty filters accept everything.
fn message_passes_filter(
    config: &RabbitMqInputConfig,
    delivery: &fe2o3_amqp::link::delivery::Delivery<Body<Value>>,
) -> bool {
    let message = delivery.message();

    if !config.routing_keys.is_empty() {
        let subject = message
            .properties
            .as_ref()
            .and_then(|properties| properties.subject.as_deref());
        match subject {
            Some(subject) if config.routing_keys.iter().any(|key| key == subject) => {}
            _ => return false,
        }
    }

    if !config.headers.is_empty() {
        let Some(application_properties) = message.application_properties.as_ref() else {
            return false;
        };
        for (key, expected) in &config.headers {
            match application_properties.get(key.as_str()) {
                Some(value) if simple_value_to_string(value).as_deref() == Some(expected) => {}
                _ => return false,
            }
        }
    }

    true
}

/// Extracts the raw payload bytes from a received message body.
///
/// RabbitMQ delivers payloads as one or more AMQP *data* sections; a string
/// *value* body is also supported for convenience.  Other body shapes yield an
/// empty payload.
fn body_to_bytes(body: &Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => {
            let mut payload = Vec::new();
            for data in batch.iter() {
                payload.extend_from_slice(data.0.as_ref());
            }
            payload
        }
        Body::Value(value) => match &value.0 {
            Value::Binary(bytes) => bytes.to_vec(),
            Value::String(string) => string.clone().into_bytes(),
            _ => Vec::new(),
        },
        Body::Sequence(_) | Body::Empty => Vec::new(),
    }
}

/// Renders a header value as a string for equality filtering.  Only scalar
/// values can match a configured string filter.
fn simple_value_to_string(value: &SimpleValue) -> Option<String> {
    match value {
        SimpleValue::String(string) => Some(string.clone()),
        SimpleValue::Symbol(symbol) => Some(symbol.0.clone()),
        SimpleValue::Binary(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        SimpleValue::Bool(value) => Some(value.to_string()),
        SimpleValue::Ubyte(value) => Some(value.to_string()),
        SimpleValue::Ushort(value) => Some(value.to_string()),
        SimpleValue::Uint(value) => Some(value.to_string()),
        SimpleValue::Ulong(value) => Some(value.to_string()),
        SimpleValue::Byte(value) => Some(value.to_string()),
        SimpleValue::Short(value) => Some(value.to_string()),
        SimpleValue::Int(value) => Some(value.to_string()),
        SimpleValue::Long(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn addresses_use_queue_path() {
        assert_eq!(
            source_address(&RabbitMqInputSource::Queue {
                name: "events".to_string()
            }),
            "/queues/events"
        );
        assert_eq!(
            source_address(&RabbitMqInputSource::Stream {
                name: "log".to_string(),
                offset: StreamOffset::First
            }),
            "/queues/log"
        );
        assert_eq!(
            source_address(&RabbitMqInputSource::Exchange {
                exchange: "amq.topic".to_string(),
                queue: "bound_q".to_string()
            }),
            "/queues/bound_q"
        );
    }

    #[test]
    fn stream_offset_filter_values() {
        assert!(matches!(
            stream_offset_filter(&StreamOffset::First).value,
            Value::String(ref s) if s == "first"
        ));
        assert!(matches!(
            stream_offset_filter(&StreamOffset::Offset(7)).value,
            Value::Ulong(7)
        ));
        assert!(matches!(
            stream_offset_filter(&StreamOffset::Timestamp(1000)).value,
            Value::Timestamp(_)
        ));
    }
}
