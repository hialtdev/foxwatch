# foxwatch

A high-performance IoT telemetry ingestion and observability pipeline written in Rust.

[![Rust CI](https://github.com/hialtdev/foxwatch/actions/workflows/ci.yml/badge.svg)](https://github.com/hialtdev/foxwatch/actions/workflows/ci.yml)

---

## Overview

foxwatch is an async Rust service that ingests real-time sensor telemetry from a home IoT fleet over MQTT, validates and deserializes structured payloads, publishes to Apache Kafka for downstream consumption, and ships structured log events to a Seq observability server.

The architecture mirrors production robotics and autonomous systems data pipelines — the same pattern used to ingest, process, and route multimodal sensor data from fleets of remote devices operating in constrained network environments.

**Real sensor fleet:** foxwatch ingests live telemetry from 14 devices including smart bulbs, switches, flood cameras, and an air purifier — all publishing state, brightness, color, recording status, and environmental readings over MQTT.

---

## Portfolio

| Project | Stack | Link |
|---|---|---|
| **foxwatch** | Rust · Tokio · Kafka · k3s | this repo |
| **BitByBit** | Spring Boot · React/Vite · Kafka · MongoDB · k3s | [bitbybit.hialt.dev](https://bitbybit.hialt.dev) |

BitByBit is a production-deployed full-stack application running on the same k3s cluster as foxwatch — Spring Boot backend, React/TypeScript frontend, Strimzi Kafka, MongoDB Atlas, structured logging to Seq, JWT/OAuth2 security, and Cloudflare tunnel ingress.

---

## Architecture

```
IoT Devices (14 sensors)
    └── Home Assistant
            └── Mosquitto MQTT Broker (ha/#)
                    └── foxwatch (Rust / Tokio)
                            ├── validate()          — typed domain logic on TelemetryMessage
                            ├── HaPayload serde     — device-typed structured deserialization
                            ├── Kafka producer      — keyed by device_id, partition-ordered
                            │       └── foxwatch-telemetry topic (Apache Kafka / Strimzi)
                            └── Seq logger          — CLEF structured events over HTTP
                                    └── seq-service (structured log server)
```

All components run on a single-node k3s Kubernetes cluster with staging and production namespaces, mirroring a managed cloud Kubernetes deployment.

---

## Technical Stack

| Layer | Technology |
|---|---|
| Language | Rust (2021 edition) |
| Async runtime | Tokio |
| MQTT client | rumqttc (async) |
| Serialization | serde + serde_json |
| Kafka producer | rdkafka (librdkafka) |
| Structured logging | Custom CLEF/HTTP → Seq |
| Kubernetes | k3s (single-node), kubectl, kustomize |
| CI/CD | GitHub Actions (3 pipelines) |
| Container | Docker multi-stage build |

---

## Key Design Decisions

### Ownership-safe async task spawning
MQTT payloads arrive as reference-counted `Bytes`. On each publish event, `topic` and `payload` are cloned/converted to owned types, then moved into a `tokio::spawn` task via a `move` closure. This gives each task full ownership of its data — no shared mutable state, no locks, no lifetime entanglement with the event loop.

```rust
tokio::spawn(async move {
    ingestion::process_payload(topic, payload, producer, seq).await;
});
```

### Smart Data Type pattern
Domain logic lives in `impl` blocks on the struct itself. `TelemetryMessage` knows how to validate itself — no separate service or manager class holds its logic.

```rust
impl TelemetryMessage {
    pub fn validate(&self) -> Result<(), ValidationError> { ... }
}
```

### Device-typed payload deserialization
Each device class publishes a different JSON schema. `HaPayload` uses `serde(default)` and `Option<>` fields so any device payload deserializes correctly without error — missing fields become `None`. `skip_serializing_if = "Option::is_none"` keeps Kafka messages clean — only populated fields appear.

```rust
// One line replaces try/catch in Java
let ha_payload: Option<HaPayload> = serde_json::from_str(&raw).ok();
```

### Partition-ordered delivery by device
Each `TelemetryMessage` is keyed by `device_id` when published to Kafka. All messages from the same device land on the same partition, preserving temporal order per device — the same pattern used for robot endpoint data in production observability systems.

### Abstracted serialization
`TelemetryMessage::serialize()` encapsulates the serialization format. Swapping from JSON to a binary format (`postcard`, `bincode`) requires changing one function body — all call sites remain unchanged.

### Non-blocking Seq shipping
A background Tokio task receives structured log events over an `mpsc` channel and batches them to Seq every second. The hot path (MQTT ingestion → Kafka publish) never blocks on log I/O.

---

## Project Structure

```
foxwatch/
├── src/
│   ├── main.rs           — Tokio runtime, MQTT event loop, task spawning
│   ├── config.rs         — Environment-based configuration (12-factor)
│   ├── ingestion.rs      — Payload parsing, validation, pipeline routing
│   ├── telemetry.rs      — TelemetryMessage, HaPayload, HaColor, DeviceState
│   ├── kafka_producer.rs — rdkafka FutureProducer wrapper
│   ├── seq_logger.rs     — Async CLEF batch shipper
│   └── lib.rs            — Public module exports for integration tests
├── tests/
│   ├── ingestion_tests.rs — extract_device_id, parse_ha_state — 21 tests
│   └── telemetry_tests.rs — HaPayload serde, round-trips, device types — 20 tests
├── k8s/
│   ├── base/             — Deployment, ConfigMap, kustomization
│   ├── staging/          — RUST_LOG=debug, staging client ID
│   └── production/       — Production namespace overlay
├── .github/workflows/
│   ├── ci.yml            — Format + lint + test on all branches
│   ├── deploy-staging.yml — Full gate + Docker build on staging branch
│   └── deploy-production.yml — Release tests + Docker build on main
└── Dockerfile            — Multi-stage build, debian:bookworm-slim runtime
```

---

## CI/CD Pipeline

Three-stage GitHub Actions pipeline:

**CI** (`ci.yml`) — triggers on every branch push:
1. `cargo fmt --check` — style gate
2. `cargo clippy -D warnings` — lint gate
3. `cargo build`
4. `cargo test` — 48 tests across 3 suites

**Staging deploy** (`deploy-staging.yml`) — triggers on `staging` branch:
- Full quality gate + Docker image build

**Production deploy** (`deploy-production.yml`) — triggers on `main`:
- Release-mode tests + Docker image build

---

## Running Locally

```bash
# Clone and build
git clone https://github.com/hialtdev/foxwatch.git
cd foxwatch
cargo build

# Run against a local MQTT broker
RUST_LOG=info \
MQTT_HOST=localhost \
MQTT_PORT=1883 \
MQTT_TOPIC="ha/#" \
cargo run

# Run tests
cargo test
```

**System dependencies** required for rdkafka (librdkafka cmake build):
```bash
sudo apt-get install -y cmake make gcc g++ libssl-dev libcurl4-openssl-dev pkg-config
```

---

## Kubernetes Deployment

```bash
# Build and import image into k3s
docker build -t foxwatch:latest .
docker save foxwatch:latest | sudo k3s ctr images import -

# Deploy to staging
kubectl apply -k k8s/staging/
kubectl rollout status deployment/foxwatch-deployment -n staging

# Tail logs
kubectl logs -n staging -l app=foxwatch -f
```

Configuration is fully externalized via ConfigMap:

| Variable | Description |
|---|---|
| `MQTT_HOST` | MQTT broker address |
| `MQTT_PORT` | MQTT broker port (default 1883) |
| `MQTT_TOPIC` | Topic subscription pattern (default `ha/#`) |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap server address |
| `KAFKA_TOPIC` | Target Kafka topic |
| `SEQ_URL` | Seq ingestion endpoint |
| `RUST_LOG` | Log verbosity (info / debug) |

---

## Live Data Sample

Real telemetry message from `family_room_greenie` smart bulb — Kafka payload:

```json
{
  "id": "025030d1-a244-464b-81fb-5aa633fcaa5d",
  "topic": "ha/lights/family_room_greenie/state",
  "device_id": "family_room_greenie",
  "state": "On",
  "timestamp": "2026-04-23T23:22:34.824903588Z",
  "payload": {
    "state": "ON",
    "brightness": 21.0,
    "color": [255, 137, 14],
    "color_temp": 500.0
  },
  "raw_payload": "{\"state\": \"ON\", \"brightness\": 21, \"color\": [255, 137, 14], \"color_temp\": 500}"
}
```

---

## Relation to Foxglove's Technical Stack

Foxglove ingests multimodal sensor data from robot fleets — IMU, camera, LiDAR — over network-constrained links, routes it through streaming pipelines, and makes it queryable for replay and analysis.

foxwatch applies the same architectural pattern to a real IoT sensor fleet:

| Foxglove requirement | foxwatch implementation |
|---|---|
| Ingest sensor data from remote devices | MQTT subscriber consuming `ha/#` from 14 live devices |
| Handle heterogeneous sensor schemas | `HaPayload` with `Option<>` fields, `skip_serializing_if` |
| Partition-ordered delivery per device | Kafka key = `device_id` |
| Low-latency async pipeline | Tokio + `tokio::spawn` per message, no blocking I/O |
| Binary serialization path | Abstracted behind `TelemetryMessage::serialize()` — postcard/bincode ready |
| Structured observability | CLEF events to Seq with device_id, topic, partition, offset |
| Managed Kubernetes deployment | k3s with staging/production namespaces, kustomize overlays |
| Systems programming experience | Rust ownership model — zero shared mutable state across async tasks |

---

## Author

Robert Glasser — [hialt.dev](https://hialt.dev)
