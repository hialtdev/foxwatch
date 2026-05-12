# foxwatch — Stream Analytics with Apache Flink

This document covers the Apache Flink layer added to the foxwatch pipeline — a stateful stream processing engine that consumes the `foxwatch-telemetry` Kafka topic and performs windowed anomaly detection on the live IoT device fleet.

---

## Architecture

```
IoT Devices (14 sensors)
    └── Home Assistant
            └── Mosquitto MQTT Broker
                    └── foxwatch (Rust / Tokio)
                            └── foxwatch-telemetry (Kafka / Strimzi)
                                    └── Apache Flink (k3s)
                                            ├── Windowed dropout detection
                                            ├── Per-device unavailability counts
                                            └── foxwatch-analytics (Kafka sink)
```

Flink sits entirely downstream of the existing foxwatch pipeline. The Rust ingest service is unchanged — Flink is a pure Kafka consumer that adds stateful analysis without touching the hot path.

---

## Infrastructure

**Flink Kubernetes Operator** manages the Flink cluster lifecycle via a `FlinkDeployment` CRD. Deployed on the same single-node k3s cluster as the rest of the foxwatch stack.

| Component | Detail |
|---|---|
| Flink version | 1.19.3 |
| Deployment mode | Session cluster (standalone) |
| JobManager | 1 replica, 1024m RAM, 0.25 CPU |
| TaskManager | 1 replica, 1024m RAM, 0.25 CPU, 2 task slots |
| Operator | flink-kubernetes-operator 1.9.0 via Helm |
| Cert manager | cert-manager 1.17.2 (operator dependency) |

### Connector JARs

The base `flink:1.19` image does not include Kafka or JSON connectors. An init container seeds `/opt/flink/lib` before the main container starts:

- `flink-sql-connector-kafka-3.3.0-1.19.jar`
- `flink-json-1.19.3.jar`

The init container uses the same `flink:1.19` image so it can copy the base Flink JARs into the shared volume before adding the connectors — mounting an `emptyDir` directly over `/opt/flink/lib` would otherwise wipe the existing classpath.

See `k8s/flink/foxwatch-session-cluster.yaml` for the full `FlinkDeployment` manifest.

---

## Kafka Topic Schema

Flink reads from `foxwatch-telemetry` using the following SQL table definition:

```sql
CREATE TABLE foxwatch_telemetry (
  id          STRING,
  device_id   STRING,
  state       MAP<STRING, STRING>,
  `timestamp` STRING,
  event_time  AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss'),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'foxwatch-telemetry',
  'properties.bootstrap.servers' = 'bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'properties.group.id'          = 'flink-foxwatch-sql',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json'
);
```

### State field mapping

`DeviceState` is serialized from Rust as a tagged enum. In Kafka JSON it appears as a map with the variant name as the key:

| Rust variant | Kafka JSON | Flink MAP_KEYS(state)[1] |
|---|---|---|
| `DeviceState::On` | `{"On": null}` | `"On"` |
| `DeviceState::Off` | `{"Off": null}` | `"Off"` |
| `DeviceState::Unavailable` | `{"Unavailable": null}` | `"Unavailable"` |
| `DeviceState::Unknown(s)` | `{"Unknown": "..."}` | `"Unknown"` |

---

## Running Jobs

### Dropout summary (persistent background job)

Counts `Unavailable` transitions per device in 5-minute tumbling windows and writes results to the `foxwatch-analytics` Kafka topic.

```sql
CREATE TABLE dropout_summary (
  device_id     STRING,
  dropout_count BIGINT,
  window_start  TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'foxwatch-analytics',
  'properties.bootstrap.servers' = 'bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'format'                       = 'json'
);

INSERT INTO dropout_summary
SELECT
  device_id,
  COUNT(*) AS dropout_count,
  TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start
FROM foxwatch_telemetry
WHERE state = 'Unavailable'
GROUP BY device_id, TUMBLE(event_time, INTERVAL '5' MINUTE);
```

Each closed window emits one row per device that had at least one unavailability event in that period. Correlated dropouts across multiple devices in the same window indicate a WAP or upstream network event rather than an individual device fault.

### Ad-hoc queries

The session cluster supports interactive Flink SQL via:

```bash
kubectl get pods -n flink
kubectl exec -it -n flink <jobmanager-pod> -- /opt/flink/bin/sql-client.sh
kubectl exec -it -n flink foxwatch-session-cluster-5dd96b78dd-xgfnz -- /opt/flink/bin/sql-client.sh
```

Useful queries:

```sql
-- Live event stream
SELECT device_id, MAP_KEYS(state)[1] AS device_state, event_time
FROM foxwatch_telemetry;

-- Hourly dropout count per device
SELECT
  device_id,
  COUNT(*) AS dropout_count,
  TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start
FROM foxwatch_telemetry
WHERE MAP_KEYS(state)[1] = 'Unavailable'
GROUP BY device_id, TUMBLE(event_time, INTERVAL '1' HOUR);
```

---

## Flink Dashboard

The Flink web UI is available via port-forward:

```bash
kubectl port-forward svc/foxwatch-session-cluster-rest 8081:8081 -n flink
```

Then open `http://localhost:8081`. The dashboard shows running jobs, task slot utilization, records processed per second, and the dataflow graph for each job (Kafka source → windowed aggregation → Kafka sink).

---

## Deployment

```bash
# Install cert-manager (Flink operator dependency)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
kubectl wait --for=condition=ready pod -l app=cert-manager -n cert-manager --timeout=90s

# Add Flink operator Helm repo and install
helm repo add flink-operator-helm https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.9.0/
helm repo update
helm install flink-kubernetes-operator flink-operator-helm/flink-kubernetes-operator \
  --namespace flink \
  --create-namespace

# Deploy session cluster (includes init container for connector JARs)
kubectl apply -f k8s/flink/foxwatch-session-cluster.yaml

# Verify
kubectl get pods -n flink
```

---

## Relation to Foxglove's Technical Stack

The Flink layer directly addresses the analytic workload requirements in Foxglove's engineering scope:

| Foxglove requirement | Flink implementation |
|---|---|
| Low-latency analytics over live sensor data | Flink streaming SQL consuming Kafka in real time |
| Windowed aggregation over time-series telemetry | Tumbling window dropout counts per device per interval |
| Anomaly detection on device fleet | Correlated unavailability detection across device group |
| Separation of ingest and analytic concerns | Flink is a pure downstream consumer — Rust hot path unchanged |
| Queryable replay over recorded data | `scan.startup.mode` = `earliest-offset` replays full topic history |

The combination of foxwatch (Rust ingest) + Flink (stream analytics) + Kafka (durable ordered log) mirrors the sensor data architecture described in Foxglove's infrastructure — multimodal device data ingested at the edge, routed through a durable message bus, and made queryable for both live monitoring and historical analysis.
