# RabbitMQ AMQP 1.0 connector — frontend end-to-end proof

Full-platform e2e test of the `rabbitmq_input` / `rabbitmq_output` connectors,
driven through the **Feldera web console** with **headless Chromium (Playwright)**
against a live **RabbitMQ 4.2.5** broker.

The platform was built and run entirely from this branch's source, so the running
pipeline executes **this branch's `dbsp_adapters`** (the AMQP 1.0 connector) and
**this branch's SQL compiler** (connector validation + config generation):

- SQL compiler jar built from source (`sql-to-dbsp-compiler`)
- `pipeline-manager` (debug) with embedded Postgres, serving the real web console
- Pipelines compiled against the local crates (`dbsp-override-path=.`)

## Bug found and fixed during this test

The UI test surfaced a real defect: `rabbitmq_input`/`rabbitmq_output` were not
classified as input/output connectors in
`crates/pipeline-manager/src/db/types/program.rs`. A table using `rabbitmq_input`
failed to compile with *"expected an input variant but got an output variant"*,
making the connector unusable in real pipelines. Fixed (commit
`pipeline-manager: classify rabbitmq_input/output connector direction`), after
which the pipeline compiles and runs cleanly.

## Pipeline under test

```sql
CREATE TABLE ui_in (id BIGINT) WITH ('connectors' = '[{
  "name": "rmq-in",
  "transport": { "name": "rabbitmq_input", "config": {
    "host": "127.0.0.1", "port": 5672, "username": "guest", "password": "guest",
    "queue": "feldera_ui_in", "offset": { "policy": "first" } } },
  "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
}]');

CREATE MATERIALIZED VIEW ui_out WITH ('connectors' = '[{
  "name": "rmq-out",
  "transport": { "name": "rabbitmq_output", "config": {
    "host": "127.0.0.1", "port": 5672, "username": "guest", "password": "guest",
    "exchange": "feldera_ui_ex", "routing_key": "ui.results" } },
  "format": { "name": "json", "config": { "update_format": "insert_delete", "array": false } }
}]') AS SELECT id FROM ui_in;
```

Data flow exercised:

```
RabbitMQ stream feldera_ui_in --(rabbitmq_input)--> table ui_in
    --> view ui_out --(rabbitmq_output)--> exchange feldera_ui_ex / ui.results --> queue feldera_ui_out
```

## Evidence

| File | Shows |
|------|-------|
| `01-pipeline-running.png` | Pipeline **RUNNING** in the console with both `rabbitmq_input` and `rabbitmq_output` connectors in the SQL. |
| `02-inbound-5rows.png` | **Inbound**: Ad-Hoc query `SELECT * FROM ui_out` returns the 5 rows (id 100–104) ingested from the RabbitMQ stream. |
| `03-inbound-8rows-after-live-publish.png` | **Live inbound**: after publishing 3 more rows (200–202) to the RabbitMQ stream, the same query now returns **8 rows** — live data flowing through the connector. |
| `04-outbound-rabbitmq-payloads.png` | **Outbound**: RabbitMQ management UI for queue `feldera_ui_out` — Exchange `feldera_ui_ex`, Routing Key `ui.results`, delivery_mode 2 (persistent), Payload `{"insert":{"id":100}}` … published by `rabbitmq_output`. |
| `frontend-e2e-flow.webm` | Screen recording of the console flow: RUNNING pipeline → Ad-Hoc query (5 rows) → live publish → 8 rows. |

### Independently verified (CLI, same run)

Inbound (materialized view via ad-hoc API):
```
{"id":100} {"id":101} {"id":102} {"id":103} {"id":104}
```
Outbound (queue `feldera_ui_out`, published by the pipeline):
```
{"insert":{"id":100}} {"insert":{"id":101}} {"insert":{"id":102}} {"insert":{"id":103}} {"insert":{"id":104}}
```

## Reproduce

1. Build the SQL compiler jar: `sql-to-dbsp-compiler/build.sh` (needs Maven + JDK 21).
2. Run the manager (LLVM bin on PATH so the pipeline's `bindgen` finds `stddef.h`):
   `PATH=<llvm-bin>:$PATH target/debug/pipeline-manager --db-connection-string=postgres-embed --compilation-profile=dev`
3. Pre-create broker entities (stream `feldera_ui_in`, topic exchange `feldera_ui_ex`,
   classic queue `feldera_ui_out` bound with key `ui.results`) via the management API.
4. Create the pipeline above in the console, Start it, publish JSON `{"id":N}` to
   `feldera_ui_in`, and observe `ui_out` in Ad-Hoc Queries and `feldera_ui_out` in the broker.
