// src/ingestion.rs
// Receives owned data from main's event loop.
// Parses → validates → logs → publishes to Kafka.

use crate::kafka_producer::KafkaProducer;
use crate::telemetry::{DeviceState, TelemetryMessage};
use log::{error, info, warn};

/// Takes ownership of topic and payload from the MQTT event loop.
/// Also receives a cloned KafkaProducer — cheap because it's Arc-backed.
pub async fn process_payload(topic: String, payload: Vec<u8>, producer: KafkaProducer) {
    // Parse raw bytes to UTF-8 — borrow payload, no copy needed
    let raw = match std::str::from_utf8(&payload) {
        Ok(s) => s.to_string(),
        Err(e) => {
            error!("Non-UTF8 payload on {topic}: {e}");
            return;
        }
    };

    let device_id = extract_device_id(&topic).unwrap_or_else(|| "unknown".to_string());
    let state = parse_ha_state(&raw);

    let message = TelemetryMessage::new(topic.clone(), device_id, state, raw);

    match message.validate() {
        Ok(()) => {
            info!(
                "✓ [{id}] {topic} → {state:?}",
                id = &message.id.to_string()[..8],
                topic = message.topic,
                state = message.state,
            );
            // Move ownership of message into the producer.
            // After this line, message is gone — Kafka owns it.
            producer.publish(message).await;
        }
        Err(e) => {
            warn!("✗ Validation failed for {topic}: {e}");
        }
    }
}

// &str borrow — we only read the topic, no ownership needed
fn extract_device_id(topic: &str) -> Option<String> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() >= 3 {
        Some(parts[2].to_string())
    } else {
        None
    }
}

fn parse_ha_state(raw: &str) -> DeviceState {
    match raw.trim() {
        "ON" => return DeviceState::On,
        "OFF" => return DeviceState::Off,
        "unavailable" => return DeviceState::Unavailable,
        _ => {}
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(s) = v.get("state").and_then(|s| s.as_str()) {
            return match s {
                "ON" => DeviceState::On,
                "OFF" => DeviceState::Off,
                other => DeviceState::Unknown(other.to_string()),
            };
        }
    }
    DeviceState::Unknown(raw.to_string())
}
