// src/ingestion.rs
// Receives owned data from main's event loop.
// Parses → validates → logs → publishes to Kafka → ships to Seq.
//
// Serde flow:
//   raw bytes → UTF-8 String → attempt HaPayload deserialization
//   Success: structured payload with brightness, color, etc.
//   Failure: plain string (e.g. "76", "sleep") → payload is None

use crate::kafka_producer::KafkaProducer;
use crate::seq_logger::SeqLogger;
use crate::telemetry::{DeviceState, HaPayload, TelemetryMessage};
use log::{error, info, warn};

pub async fn process_payload(
    topic: String,
    payload: Vec<u8>,
    producer: KafkaProducer,
    seq: SeqLogger,
) {
    // Step 1 — raw bytes to UTF-8 string
    // &payload is a borrow — we read the bytes without taking ownership
    let raw = match std::str::from_utf8(&payload) {
        Ok(s) => s.to_string(),
        Err(e) => {
            error!("Non-UTF8 payload on {topic}: {e}");
            seq.error(&format!("Non-UTF8 payload on {topic}: {e}"));
            return;
        }
    };

    // Step 2 — attempt structured deserialization into HaPayload
    // serde_json::from_str returns Result<HaPayload, Error>
    // .ok() converts that to Option<HaPayload>:
    //   Ok(payload)  → Some(payload)   (valid JSON matching our struct)
    //   Err(_)       → None            (plain string like "76" or "sleep")
    let ha_payload: Option<HaPayload> = serde_json::from_str(&raw).ok();

    // Step 3 — extract DeviceState
    // If we have a structured payload, read state from it.
    // Otherwise fall back to parsing the raw string directly.
    let state = match &ha_payload {
        Some(p) => match p.state.as_deref() {
            // as_deref() converts Option<String> to Option<&str>
            // so we can match on string slices without cloning
            Some(s) => parse_ha_state(s),
            None => parse_ha_state(&raw),
        },
        None => parse_ha_state(&raw),
    };

    // Step 4 — extract device_id from topic path
    let device_id = extract_device_id(&topic).unwrap_or_else(|| "unknown".to_string());

    // Step 5 — construct the pipeline envelope
    // TelemetryMessage::new now takes Option<HaPayload>
    let message = TelemetryMessage::new(
        topic.clone(),
        device_id,
        state,
        ha_payload, // moved in — Option<HaPayload>
        raw,        // moved in — original string preserved for audit
    );

    // Step 6 — validate and route
    match message.validate() {
        Ok(()) => {
            info!(
                "✓ [{id}] {topic} → {state:?} brightness={brightness:?}",
                id = &message.id.to_string()[..8],
                topic = message.topic,
                state = message.state,
                brightness = message.payload.as_ref().and_then(|p| p.brightness),
            );

            seq.telemetry_received(&message.topic, &message.device_id, &message.id.to_string());

            producer.publish(message, seq).await;
        }
        Err(e) => {
            warn!("✗ Validation failed for {topic}: {e}");
            seq.error(&format!("Validation failed for {topic}: {e}"));
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

pub fn extract_device_id(topic: &str) -> Option<String> {
    let parts: Vec<&str> = topic.split('/').filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        [_prefix, _type, id, ..] => Some(id.to_string()),
        _ => None,
    }
}

pub fn parse_ha_state(raw: &str) -> DeviceState {
    // Normalize to uppercase so "on", "ON", "On" all match
    match raw.trim().to_uppercase().as_str() {
        "ON" => return DeviceState::On,
        "OFF" => return DeviceState::Off,
        "UNAVAILABLE" => return DeviceState::Unavailable,
        _ => {}
    }
    // Try JSON — some devices send {"state":"ON",...}
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(s) = v.get("state").and_then(|s| s.as_str()) {
            return match s.to_uppercase().as_str() {
                "ON" => DeviceState::On,
                "OFF" => DeviceState::Off,
                "UNAVAILABLE" => DeviceState::Unavailable,
                other => DeviceState::Unknown(other.to_string()),
            };
        }
    }
    DeviceState::Unknown(raw.to_string())
}
