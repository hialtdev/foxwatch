// src/kafka_producer.rs
// Wraps rdkafka's FutureProducer.
// Receives an owned TelemetryMessage, serializes it to JSON bytes,
// and publishes to the foxwatch-telemetry topic.
//
// OWNERSHIP NOTE: we take ownership of the message here — the caller
// moves it in and we consume it. This is intentional: once a message
// is handed to Kafka it belongs to the producer pipeline, not the caller.

use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

use crate::telemetry::TelemetryMessage;

/// A thin wrapper around rdkafka's FutureProducer.
/// Clone is cheap — FutureProducer is Arc-backed internally.
#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaProducer {
    /// Creates a new producer connected to the given bootstrap servers.
    /// Called once at startup in main.rs and then cloned into each task.
    pub fn new(bootstrap_servers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            // How long to retry delivery before giving up
            .set("message.timeout.ms", "5000")
            // Required for idempotent delivery (no duplicate messages)
            .set("enable.idempotence", "true")
            .create()
            .expect("Failed to create Kafka producer — check bootstrap address");

        info!("Kafka producer connected to {bootstrap_servers} → topic={topic}");

        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    /// Serializes the message to JSON and publishes it.
    /// Uses the device_id as the Kafka partition key so all messages
    /// from the same device land on the same partition — preserving order.
    /// This is the same pattern Foxglove uses for robot endpoint data.
    pub async fn publish(&self, message: TelemetryMessage) {
        // Serialize to JSON bytes — returns Result, we handle the error below
        let payload = match serde_json::to_vec(&message) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize message {}: {e}", message.id);
                return;
            }
        };

        // device_id is the partition key — owned String, lives for this call
        let key = message.device_id.clone();

        // FutureRecord borrows payload and key — they must outlive the send call.
        // .await on the future blocks until Kafka acknowledges receipt.
        let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                info!(
                    "→ Kafka [{topic}] partition={partition} offset={offset} device={key}",
                    topic = self.topic,
                );
            }
            Err((e, _)) => {
                error!("Kafka delivery failed for device={key}: {e}");
            }
        }
    }
}
