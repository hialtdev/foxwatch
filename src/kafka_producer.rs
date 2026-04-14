// src/kafka_producer.rs

use crate::seq_logger::SeqLogger;
use crate::telemetry::TelemetryMessage;
use log::{error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaProducer {
    pub fn new(bootstrap_servers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("enable.idempotence", "true")
            .create()
            .expect("Failed to create Kafka producer");

        info!("Kafka producer connected to {bootstrap_servers} → topic={topic}");

        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    /// Takes ownership of message and ships to Kafka.
    /// Ships delivery confirmation to Seq on success.
    pub async fn publish(&self, message: TelemetryMessage, seq: SeqLogger) {
        let payload = match serde_json::to_vec(&message) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize message {}: {e}", message.id);
                seq.error(&format!("Serialization failed for {}: {e}", message.id));
                return;
            }
        };

        let key = message.device_id.clone();
        let device_id = message.device_id.clone();

        let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                info!(
                    "→ Kafka [{topic}] partition={partition} offset={offset} device={key}",
                    topic = self.topic,
                );
                // Ship delivery confirmation to Seq with full context
                seq.kafka_delivered(&device_id, partition, offset);
            }
            Err((e, _)) => {
                error!("Kafka delivery failed for device={key}: {e}");
                seq.error(&format!("Kafka delivery failed for {key}: {e}"));
            }
        }
    }
}
