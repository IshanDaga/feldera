//! Live test for the RabbitMQ input connector.
//!
//! This test is gated behind the `rabbitmq-test` feature because it needs a
//! running RabbitMQ broker with the AMQP 1.0 protocol enabled (RabbitMQ 3.13
//! with the `rabbitmq_amqp1_0` plugin, or RabbitMQ 4.0+ where AMQP 1.0 is a core
//! protocol).
//!
//! Because AMQP 1.0 receivers and senders attach to existing queues, the test
//! queue must be created in advance, for example:
//!
//! ```text
//! rabbitmqadmin declare queue name=feldera_test durable=true
//! ```
//!
//! The test publishes records straight through the `fe2o3-amqp` client (not the
//! Feldera output connector, which lives in a separate change) and then reads
//! them back through the input connector, asserting that the data survives.
//!
//! Configuration is taken from the environment:
//!
//! * `RABBITMQ_URL` — broker URL (default `amqp://guest:guest@localhost:5672`).
//! * `RABBITMQ_TEST_QUEUE` — pre-declared queue name (default `feldera_test`).
//!
//! Run with:
//!
//! ```text
//! cargo test -p dbsp_adapters --features rabbitmq-test rabbitmq
//! ```

use super::connection::connect;
use crate::test::{TestStruct, init_test_logger, mock_input_pipeline, wait_for_output_unordered};
use csv::WriterBuilder;
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::types::messaging::{Data, Message};
use fe2o3_amqp::types::primitives::Binary;
use fe2o3_amqp::{Sender, Session};
use feldera_types::program_schema::Relation;
use feldera_types::transport::rabbitmq::RabbitMqConnectionConfig;
use serde_json::json;
use std::env;

fn broker_url() -> String {
    env::var("RABBITMQ_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_string())
}

fn test_queue() -> String {
    env::var("RABBITMQ_TEST_QUEUE").unwrap_or_else(|_| "feldera_test".to_string())
}

/// Encodes each record as a single header-less CSV row.
fn to_csv_rows(data: &[TestStruct]) -> Vec<Vec<u8>> {
    data.iter()
        .map(|record| {
            let mut writer = WriterBuilder::new().has_headers(false).from_writer(Vec::new());
            writer.serialize(record).unwrap();
            writer.into_inner().unwrap()
        })
        .collect()
}

/// Publishes the given payloads to `queue` using the raw AMQP 1.0 client.
///
/// This keeps the input test independent of the output connector: it exercises
/// only the shared connection path plus a plain sender.
fn publish_rows(connection_config: &RabbitMqConnectionConfig, queue: &str, rows: Vec<Vec<u8>>) {
    TOKIO.block_on(async {
        let mut connection = connect(connection_config).await.unwrap();
        let mut session = Session::begin(&mut connection).await.unwrap();
        let mut sender = Sender::attach(&mut session, "feldera-test-sender", format!("/queues/{queue}"))
            .await
            .unwrap();

        for row in rows {
            let message = Message::builder().data(Data(Binary::from(row))).build();
            let outcome = sender.send(message).await.unwrap();
            outcome
                .accepted_or_else(|state| panic!("broker rejected test message: {state:?}"))
                .unwrap();
        }

        sender.close().await.unwrap();
        session.end().await.unwrap();
        connection.close().await.unwrap();
    });
}

/// Publishes records straight to a queue, then reads them back through the input
/// connector, asserting that the data survives the trip.
#[test]
fn test_rabbitmq_queue_input() {
    init_test_logger();

    let url = broker_url();
    let queue = test_queue();

    let data: Vec<TestStruct> = (0..10)
        .map(|id| TestStruct {
            id,
            b: id % 2 == 0,
            i: Some(id as i64),
            s: format!("record-{id}"),
        })
        .collect();

    // Publish via the raw AMQP client.
    let connection_config: RabbitMqConnectionConfig =
        serde_json::from_value(json!({ "url": url })).unwrap();
    publish_rows(&connection_config, &queue, to_csv_rows(&data));

    // Consume via the input connector.
    let input_config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "rabbitmq_input",
            "config": {
                "connection": {"url": url},
                "source": {"kind": "queue", "name": queue},
            },
        },
        "format": {"name": "csv"},
    }))
    .unwrap();

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(input_config, Relation::empty()).unwrap();

    endpoint.extend();
    wait_for_output_unordered(&zset, &[data], || endpoint.queue(false));
    endpoint.disconnect();
}
