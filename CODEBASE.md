# foxwatch — Codebase Reference

> This document fills gaps not covered by README.md, ARCHITECTURE.md, or FLINK.md.
> Read those first for architecture overview, concurrency model, and Flink deployment.
> This file is optimized for hialt-recall RAG retrieval.

---

## What does foxwatch do? (one-line answer)

foxwatch subscribes to MQTT topics from Home Assistant, deserializes IoT device payloads
into typed Rust structs, publishes each message to a Kafka topic keyed by device_id, and
ships structured CLEF logs to Seq. It does not persist data — it is a stateless ingest
and routing pipeline.

---

## What Kafka topics does foxwatch use?

| Topic | Direction | Producer | Consumer |
|---|---|---|---|
| `foxwatch-telemetry` | Sink | foxwatch (Rust) | Apache Flink, any downstream consumer |
| `foxwatch-analytics` | Sink | Apache Flink | downstream analytics consumers |

foxwatch (Rust) writes only to `foxwatch-telemetry`. It never reads from Kafka.
Flink reads `foxwatch-telemetry` and writes `foxwatch-analytics`.

Kafka bootstrap server (in-cluster): `bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`

Kafka runs as Strimzi on the same k3s cluster. The Kafka instance is shared with BitByBit.

---

## What is the MQTT topic structure?

Subscription wildcard: `ha/#`

Per-device topic pattern: `ha/<device_type>/<device_id>/state`

The `device_id` field in TelemetryMessage is extracted by splitting the topic on `/`
and taking index 2 (zero-based). A topic of `ha/lights/family_room_greenie/state`
yields `device_id = "family_room_greenie"`.

---

## What devices publish to foxwatch?

14 live devices across four categories:

- **Smart bulbs** — publish state, brightness (0–255 float), color (RGB array), color_temp
- **Smart switches** — publish state only
- **Flood cameras** — publish recording status and motion state
- **Air purifier** — publishes environmental readings

All devices are managed by Home Assistant and publish via Mosquitto MQTT broker.

---

## Data model — TelemetryMessage fields

Defined in `src/telemetry.rs`. Every ingested MQTT message becomes one TelemetryMessage.

| Field | Type | Notes |
|---|---|---|
| `id` | `Uuid` (v4) | Generated at ingestion time |
| `topic` | `String` | Full MQTT topic, e.g. `ha/lights/family_room_greenie/state` |
| `device_id` | `String` | Extracted from topic |
| `state` | `DeviceState` | Parsed from payload, serialized as tagged enum in Kafka JSON |
| `timestamp` | `DateTime<Utc>` | Ingestion timestamp, not device clock |
| `payload` | `Option<HaPayload>` | Typed device payload; None if deserialization fails |
| `raw_payload` | `String` | Original JSON string, always preserved |

---

## Data model — HaPayload fields

HaPayload uses `serde(default)` and `Option<>` on every field so any device type
deserializes without error. Missing fields become None and are omitted from Kafka
output via `skip_serializing_if = "Option::is_none"`.

| Field | Type | Populated by |
|---|---|---|
| `state` | `Option<String>` | All devices |
| `brightness` | `Option<f64>` | Smart bulbs |
| `color` | `Option<HaColor>` | Smart bulbs — RGB array [u8; 3] |
| `color_temp` | `Option<f64>` | Smart bulbs — Kelvin value |

---

## Data model — DeviceState enum and Kafka serialization

DeviceState is a Rust tagged enum. In Kafka JSON it serializes as a map with the
variant name as the key and null as the value (standard serde tagged enum format).

| Rust variant | Kafka JSON representation |
|---|---|
| `DeviceState::On` | `{"On": null}` |
| `DeviceState::Off` | `{"Off": null}` |
| `DeviceState::Unavailable` | `{"Unavailable": null}` |
| `DeviceState::Unknown(s)` | `{"Unknown": "..."}` |

Important for Flink SQL: The production tables.sql reads state as a plain STRING
(not MAP<STRING, STRING>). The Unavailable detection in dropout_summary.sql uses
WHERE state = 'Unavailable' as a direct string comparison, not MAP_KEYS(state)[1].
The FLINK.md MAP approach is an earlier design; production uses STRING.

---

## Flink production vs FLINK.md discrepancies

Two differences exist between FLINK.md and the actual production SQL in flink/jobs/:

### State field type

FLINK.md documents state as MAP<STRING, STRING>.
Production tables.sql defines state as STRING.
Production dropout_summary.sql filters with WHERE state = 'Unavailable'.

### Window time attribute

FLINK.md example uses TUMBLE(event_time, INTERVAL '5' MINUTE) — event-time windowing.
Production dropout_summary.sql uses TUMBLE(PROCTIME(), INTERVAL '5' MINUTE) — processing-time windowing.
PROCTIME() does not require a WATERMARK and is simpler to operate in a home lab context.

The files in flink/jobs/ are the authoritative production definitions.

---

## What does the dropout_summary job do?

Defined in flink/jobs/dropout_summary.sql.

For every 5-minute processing-time window, counts the number of Unavailable state
transitions per device and emits one row per device that had at least one dropout.
Results are written to the foxwatch-analytics Kafka topic.

Output schema: device_id STRING, dropout_count BIGINT, window_start TIMESTAMP(3)

A correlated spike in dropout_count across multiple devices in the same window
indicates a WAP or upstream network event rather than an individual device fault.

Flink consumer group for foxwatch-telemetry: flink-foxwatch-production

---

## Source file responsibilities

| File | What it does |
|---|---|
| `src/main.rs` | Tokio runtime, rumqttc event loop, Semaphore(64) cap, task spawning |
| `src/config.rs` | Config::from_env() — loads all env vars, fails fast on missing required vars |
| `src/ingestion.rs` | process_payload() — extract_device_id, deserialize, validate, publish, log |
| `src/telemetry.rs` | TelemetryMessage, HaPayload, HaColor, DeviceState structs and impls |
| `src/kafka_producer.rs` | Thin async wrapper over rdkafka::FutureProducer |
| `src/seq_logger.rs` | Background Tokio task, mpsc receiver, CLEF batch HTTP shipper, 1s flush |
| `src/lib.rs` | Re-exports modules for integration test access |
| `flink/jobs/tables.sql` | Production Flink table DDL — source (foxwatch-telemetry) and sink (foxwatch-analytics) |
| `flink/jobs/dropout_summary.sql` | Production dropout detection INSERT job |

---

## Test suites

| File | Count | What is tested |
|---|---|---|
| `tests/ingestion_tests.rs` | 21 | extract_device_id(), parse_ha_state() |
| `tests/telemetry_tests.rs` | 20 | HaPayload serde round-trips, per-device-type deserialization |
| `tests/mqtt_integration_tests.rs` | 7 | End-to-end MQTT message flow (integration) |

Total: 48 tests. All run on cargo test. CI runs on every branch push.

---

## Concurrency model detail

ARCHITECTURE.md covers the core model. Additional detail for hialt-recall:

The event loop uses rumqttc in non-blocking eventloop.poll(), not blocking await.
A tokio::sync::Semaphore with 64 permits caps concurrent ingestion tasks to prevent
resource exhaustion on the HP EliteDesk home lab hardware.
The Semaphore permit is acquired before tokio::spawn and released when the task completes.
The Seq logger mpsc channel sender is cloned per task — no shared mutable logger state.

---

## Configuration — full variable list

Loaded by Config::from_env() in src/config.rs.
MQTT client ID format: {CONFIG_ID}-{HOSTNAME}

| Variable | Required | Default | Description |
|---|---|---|---|
| `MQTT_HOST` | Yes | — | MQTT broker IP or hostname |
| `MQTT_PORT` | No | `1883` | MQTT broker port |
| `MQTT_TOPIC` | No | `ha/#` | Subscription pattern |
| `KAFKA_BOOTSTRAP` | Yes | — | Kafka bootstrap server host:port |
| `KAFKA_TOPIC` | No | `foxwatch-telemetry` | Target Kafka topic |
| `SEQ_URL` | Yes | — | Seq HTTP ingestion endpoint (port 5341) |
| `RUST_LOG` | No | `info` | Tracing filter: info or debug |
| `CONFIG_ID` | No | — | Prefix for MQTT client ID |

Missing required variables cause immediate startup failure with a descriptive error.

---

## Kubernetes namespaces and overlays

| Namespace | Overlay | Differences from base |
|---|---|---|
| `staging` | `k8s/staging/` | RUST_LOG=debug, staging MQTT client ID |
| `production` | `k8s/production/` | Production namespace, production client ID |
| `flink` | `k8s/flink/` | FlinkDeployment CRD, session cluster, init container for connector JARs |

Base manifests: k8s/base/foxwatch-configmap.yaml, k8s/base/foxwatch-deployment.yaml

---

## Does foxwatch use a database?

No. foxwatch writes to Kafka only. It has no MongoDB, PostgreSQL, or any other database
connection. Downstream Kafka consumers are responsible for persistence.

---

## How does foxwatch relate to BitByBit?

Both run on the same single-node k3s cluster.
Both use the same Strimzi Kafka instance (bitbybit-kafka-kafka-bootstrap).
Both ship structured logs to the same Seq instance.
They do not share a database or any direct API connection.
foxwatch produces to foxwatch-telemetry; BitByBit has its own Kafka topics.
BitByBit is a Spring Boot / React full-stack app; foxwatch is a Rust ingest service.
