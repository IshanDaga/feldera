//! Live round-trip test for the RabbitMQ connector.
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

use crate::test::{TestStruct, init_test_logger, mock_input_pipeline, wait_for_output_unordered};
use feldera_adapterlib::transport::OutputEndpoint;
use crate::transport::rabbitmq::RabbitMqOutputEndpoint;
use csv::WriterBuilder;
use feldera_types::program_schema::Relation;
use feldera_types::transport::rabbitmq::RabbitMqOutputConfig;
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

/// Publishes records through the output connector, then reads them back through
/// the input connector, asserting that the data survives the round trip.
#[test]
fn test_rabbitmq_queue_roundtrip() {
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

    // Publish via the output connector.
    let output_config: RabbitMqOutputConfig = serde_json::from_value(json!({
        "connection": {"url": url},
        "target": {"kind": "queue", "name": queue},
    }))
    .unwrap();

    let mut output = RabbitMqOutputEndpoint::new(output_config).unwrap();
    output
        .connect(Box::new(|fatal, error, _tag| {
            panic!("unexpected async error (fatal={fatal}): {error}")
        }))
        .unwrap();
    for row in to_csv_rows(&data) {
        output.push_buffer(&row).unwrap();
    }
    drop(output);

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
