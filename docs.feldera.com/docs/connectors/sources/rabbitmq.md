# RabbitMQ input connector

Feldera can consume a stream of changes to a SQL table from RabbitMQ with the
`rabbitmq_input` connector.

The connector speaks **AMQP 1.0**, the standard protocol that RabbitMQ exposes as
a first-class transport since RabbitMQ 3.8 (via the `rabbitmq_amqp1_0` plugin)
and as a core protocol since RabbitMQ 4.0. Over AMQP 1.0 the connector consumes
from queues, streams, and exchange-bound queues uniformly.

The connector is not [fault tolerant](/pipelines/fault-tolerance): on a
connection error it reconnects and resumes reading new deliveries.

## RabbitMQ Input Connector Configuration

| Property                | Type    | Default | Description |
|-------------------------|---------|---------|-------------|
| `connection` (required) | object  |         | Broker connection and authentication options. See [Connection options](#connection-options). |
| `source` (required)     | object  |         | The queue, stream, or exchange to consume from. See [Source options](#source-options). |
| `routing_keys`          | string list |     | Ingest only messages whose routing key (the AMQP message *subject*) equals one of these values. An empty list disables routing-key filtering. |
| `headers`               | object  |         | Ingest only messages whose application-property headers contain all of these key/value pairs. An empty object disables header filtering. |
| `prefetch`              | integer | 100     | Receiver link credit: the maximum number of unacknowledged messages the broker may have in flight. |
| `retry_interval_secs`   | seconds | 5       | Delay between reconnect attempts after a connection error. |

Routing-key and header filters are applied by the connector after delivery, so
they work against any RabbitMQ version regardless of its server-side filtering
support. Filtered-out messages are acknowledged so the broker does not redeliver
them.

### Connection options

The `connection` object describes how to reach and authenticate to the broker:

| Property                  | Type    | Default | Description |
|---------------------------|---------|---------|-------------|
| `url` (required)          | string  |         | Connection URL, e.g. `amqp://localhost:5672` or `amqps://host:5671`. The `amqps` scheme enables TLS. |
| `username`                | string  |         | SASL PLAIN username. Ignored when the URL already carries credentials. Prefer this field (with a [secret reference](/connectors/secret-references)) over embedding the password in the URL. |
| `password`                | string  |         | SASL PLAIN password. |
| `virtual_host`            | string  | `/`     | AMQP virtual host to open the connection on. |
| `tls`                     | object  |         | TLS/SSL options. See [TLS options](#tls-options). |
| `container_id`            | string  |         | AMQP container id reported to the broker. Defaults to an auto-generated unique id. |
| `connection_timeout_secs` | seconds | 30      | Maximum time to wait for the connection (and TLS handshake) to be established. |

### TLS options

When the `url` uses the `amqps` scheme, or the `tls` object is present, the
connector establishes a TLS session. With default options the broker certificate
is validated against the system root store with hostname verification enabled.

| Property                | Type    | Default | Description |
|-------------------------|---------|---------|-------------|
| `ca_cert_pem_path`      | string  |         | Path to a PEM file with additional trusted CA certificates. Added to the system root store. |
| `ca_cert_pem`           | string  |         | Inline PEM-encoded CA certificate(s); an alternative to `ca_cert_pem_path`. |
| `client_cert_pem_path`  | string  |         | Path to a PEM file with the client certificate chain, for mutual TLS. |
| `client_key_pem_path`   | string  |         | Path to a PEM file with the PKCS#8 client private key, for mutual TLS. |
| `domain`                | string  |         | Server name used for SNI and certificate hostname verification. Defaults to the URL host; set it when connecting through an IP address. |
| `accept_invalid_certs`  | boolean | false   | Disable certificate verification. **Insecure** — for testing only. |

### Source options

The `source` object selects what to consume from. Its `kind` field is one of
`queue`, `stream`, or `exchange`.

A **queue** is reached at the AMQP address `/queues/{name}`:

| Property         | Type   | Description |
|------------------|--------|-------------|
| `kind`           | string | `"queue"`. |
| `name`           | string | Queue name. |

A **stream** is reached at `/queues/{name}` with a starting offset:

| Property         | Type   | Description |
|------------------|--------|-------------|
| `kind`           | string | `"stream"`. |
| `name`           | string | Stream name. |
| `offset`         | variant | Where to start reading: `"first"`, `"next"` (default), `"last"`, `{"offset": <n>}` for an absolute offset, or `{"timestamp": <ms>}` for a Unix timestamp in milliseconds. |

An **exchange** is consumed through a queue bound to it. AMQP 1.0 receivers
always attach to a queue, so the queue must already exist and be bound to the
exchange:

| Property         | Type   | Description |
|------------------|--------|-------------|
| `kind`           | string | `"exchange"`. |
| `exchange`       | string | Source exchange name. |
| `queue`          | string | Existing queue bound to the exchange to attach the receiver to. |

## Example usage

Create a table backed by a RabbitMQ queue named `book-fair-sales` on a broker at
`example.com:5672`, consuming newline-delimited JSON:

```sql
CREATE TABLE book_fair_sales (
    sid BIGINT,
    pid BIGINT,
    sold_at TIMESTAMP,
    price DECIMAL(8, 2)
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "rabbitmq_input",
            "config": {
                "connection": { "url": "amqp://example.com:5672" },
                "source": { "kind": "queue", "name": "book-fair-sales" }
            }
        },
        "format": {
            "name": "json",
            "config": { "update_format": "raw", "array": false }
        }
    }]'
);
```

Consume a RabbitMQ **stream** from the beginning, over TLS with SASL PLAIN
credentials supplied through [secret references](/connectors/secret-references):

```sql
CREATE TABLE events (
    id BIGINT,
    payload VARCHAR
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "rabbitmq_input",
            "config": {
                "connection": {
                    "url": "amqps://broker.example.com:5671",
                    "username": "${secret:rabbitmq_user}",
                    "password": "${secret:rabbitmq_password}"
                },
                "source": { "kind": "stream", "name": "events", "offset": "first" }
            }
        },
        "format": { "name": "json", "config": { "update_format": "raw" } }
    }]'
);
```

Consume only the messages an exchange routes with selected routing keys and
headers, through a bound queue:

```sql
CREATE TABLE orders (
    order_id BIGINT,
    region VARCHAR
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "rabbitmq_input",
            "config": {
                "connection": { "url": "amqp://example.com:5672" },
                "source": {
                    "kind": "exchange",
                    "exchange": "orders",
                    "queue": "orders-eu"
                },
                "routing_keys": ["orders.created", "orders.updated"],
                "headers": { "region": "eu" }
            }
        },
        "format": { "name": "json", "config": { "update_format": "raw" } }
    }]'
);
```

## Additional resources

* [RabbitMQ AMQP 1.0 documentation](https://www.rabbitmq.com/docs/amqp)
* [RabbitMQ streams](https://www.rabbitmq.com/docs/streams)
