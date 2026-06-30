# RabbitMQ output connector

Feldera can publish a stream of changes to a SQL view to RabbitMQ with the
`rabbitmq_output` connector.

The connector speaks **AMQP 1.0** (a first-class RabbitMQ transport since
RabbitMQ 3.8 and a core protocol since RabbitMQ 4.0) and can publish directly to
a queue or to an exchange with a routing key. Message headers travel as AMQP
*application-properties* and the routing key travels as the message *subject*.

## RabbitMQ Output Connector Configuration

| Property                       | Type    | Default | Description |
|--------------------------------|---------|---------|-------------|
| `connection` (required)        | object  |         | Broker connection and authentication options, identical to the [input connector](/connectors/sources/rabbitmq#connection-options). |
| `target` (required)            | object  |         | The queue or exchange to publish to. See [Target options](#target-options). |
| `headers`                      | object  |         | Application-property headers added to every published message. A key/value format may add further per-message headers, which are merged with these. |
| `durable`                      | boolean | true    | Mark published messages as durable so the broker persists them to disk. |
| `initialization_timeout_secs`  | seconds | 30      | Maximum time to wait for the connection to be established during startup. |

The `connection` object — including the [TLS/SSL
options](/connectors/sources/rabbitmq#tls-options) — is documented with the
input connector.

### Target options

The `target` object selects where to publish. Its `kind` field is one of `queue`
or `exchange`.

A **queue** is addressed at `/queues/{name}`:

| Property         | Type   | Description |
|------------------|--------|-------------|
| `kind`           | string | `"queue"`. |
| `name`           | string | Destination queue or stream name. |

An **exchange** is addressed at `/exchanges/{exchange}`, with the routing key
carried as the message subject:

| Property         | Type   | Description |
|------------------|--------|-------------|
| `kind`           | string | `"exchange"`. |
| `exchange`       | string | Destination exchange name. |
| `routing_key`    | string | Default routing key applied to every message. A Kafka-style key/value format (e.g. Debezium) overrides this per message with the record key. For a headers exchange, leave it empty and rely on `headers`. |

## Example usage

Publish a view to a RabbitMQ queue named `enriched-sales` as newline-delimited
JSON:

```sql
CREATE MATERIALIZED VIEW enriched_sales
WITH (
    'connectors' = '[{
        "transport": {
            "name": "rabbitmq_output",
            "config": {
                "connection": { "url": "amqp://example.com:5672" },
                "target": { "kind": "queue", "name": "enriched-sales" }
            }
        },
        "format": { "name": "json", "config": { "update_format": "raw" } }
    }]'
) AS SELECT * FROM sales;
```

Publish to a topic exchange with a routing key and a static header, over TLS:

```sql
CREATE MATERIALIZED VIEW orders_out
WITH (
    'connectors' = '[{
        "transport": {
            "name": "rabbitmq_output",
            "config": {
                "connection": {
                    "url": "amqps://broker.example.com:5671",
                    "username": "${secret:rabbitmq_user}",
                    "password": "${secret:rabbitmq_password}"
                },
                "target": {
                    "kind": "exchange",
                    "exchange": "orders",
                    "routing_key": "orders.created"
                },
                "headers": { "source": "feldera" }
            }
        },
        "format": { "name": "json", "config": { "update_format": "raw" } }
    }]'
) AS SELECT * FROM orders;
```

## Additional resources

* [RabbitMQ AMQP 1.0 documentation](https://www.rabbitmq.com/docs/amqp)
* [RabbitMQ input connector](/connectors/sources/rabbitmq)
