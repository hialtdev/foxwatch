# Foxwatch: High-Concurrency MQTT Bridge

## 1. Overview
Foxwatch is a Rust-based ingestion engine that bridges local IoT telemetry (MQTT) to enterprise-grade observability (Seq) and streaming (Kafka).

## 2. Core Execution Model
- **Primary Loop**: Uses `rumqttc` in a non-blocking `eventloop.poll()` pattern.
- **State Management**: Orchestrates a `KafkaProducer` and `SeqLogger` through the `ingestion` module.
- **Concurrency**: Employs a `tokio::sync::Semaphore` to cap concurrent ingestion tasks at 64, preventing resource exhaustion on home lab hardware.
- **Logging**: Implements structured logging via `tracing` and `SeqLogger` to ship CLEF-formatted logs to a Seq sink.

## 3. Data Pipeline
1. **Source**: MQTT topic subscription (managed after `ConnAck`).
2. **Transfer**: `tokio::spawn` offloads payload processing to `ingestion::process_payload`.
3. **Sinks**: Dispatches to Kafka for long-term streaming and Seq for immediate visibility.

## 4. Dependencies
- **Tokio**: Multi-threaded async runtime.
- **rdkafka**: C-backed Kafka client for high-performance delivery.
- **rumqttc**: Robust async MQTT driver with automatic reconnection.