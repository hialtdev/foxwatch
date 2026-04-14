// src/ingestion.rs
// Receives owned data from main's event loop.
// Parses → validates → logs → publishes to Kafka → ships to Seq.

use crate::kafka_producer::KafkaProducer;
use crate::seq_logger::SeqLogger;
use crate::telemetry::{DeviceState, TelemetryMessage};
use log::{error, info, warn};

pub async fn process_payload(
    topic: String,
    payload: Vec<u8>,
    producer: KafkaProducer,
    seq: SeqLogger,
) {
    let raw = match std::str::from_utf8(&payload) {
        Ok(s) => s.to_string(),
        Err(e) => {
            error!("Non-UTF8 payload on {topic}: {e}");
            seq.error(&format!("Non-UTF8 payload on {topic}: {e}"));
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

            // Ship to Seq before moving message into Kafka
            // (Kafka publish consumes ownership of message)
            seq.telemetry_received(&message.topic, &message.device_id, &message.id.to_string());

            producer.publish(message, seq).await;
        }
        Err(e) => {
            warn!("✗ Validation failed for {topic}: {e}");
            seq.error(&format!("Validation failed for {topic}: {e}"));
        }
    }
}


pub fn extract_device_id(topic: &str) -> Option<String> {
    // .filter(|s| !s.is_empty()) handles leading/trailing/double slashes
    let parts: Vec<&str> = topic.split('/')
        .filter(|s| !s.is_empty())
        .collect();

    match parts.as_slice() {
        // Case 1: The "Deep" or "Standard" HA topic
        // Pattern: [prefix, type, id, ...everything else]
        // Example: "homeassistant/binary_sensor/motion_sensor/state" -> "motion_sensor"
        [prefix, _type, id, ..] if *prefix == "homeassistant" => {
            Some(id.to_string())
        }

        // Case 2: The "Short" topic
        // Example: "homeassistant/status" -> "status"
        [prefix, id] if *prefix == "homeassistant" => {
            Some(id.to_string())
        }

        _ => None,
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
