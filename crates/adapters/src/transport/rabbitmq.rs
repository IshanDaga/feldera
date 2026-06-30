//! Transport adapter for RabbitMQ over AMQP 1.0.
//!
//! RabbitMQ exposes queues, streams, and exchanges through the AMQP 1.0 protocol
//! (a first-class transport since RabbitMQ 3.8 and a core protocol since
//! RabbitMQ 4.0).  This module provides an [input](input::RabbitMqInputEndpoint)
//! adapter that consumes deliveries from a queue, stream, or exchange-bound
//! queue, and an [output](output::RabbitMqOutputEndpoint) adapter that publishes
//! to a queue or exchange.  Both support TLS (`amqps://`), routing keys, and
//! application-property headers.

mod connection;
mod input;
mod output;

#[cfg(all(test, feature = "rabbitmq-test"))]
mod test;

pub use input::RabbitMqInputEndpoint;
pub use output::RabbitMqOutputEndpoint;
