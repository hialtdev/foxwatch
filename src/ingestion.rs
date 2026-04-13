// src/ingestion.rs
// Receives owned data from main's event loop.
// Parses, validates, and logs. Week 2 adds Kafka publish here.

use log::{info, warn, error};
use crate::telemetry::{TelemetryMessage, DeviceState};

// Takes ownership of topic (String) and payload (Vec<u8>).
// Called from tokio::spawn — must own its data, not borrow it,
// because the spawned task may outlive the calling stack frame.
pub async fn process_payload(topic: String, payload: Vec<u8>) {
    // Parse raw bytes to UTF-8. &payload borrows — no copy needed.
    let raw = match std::str::from_utf8(&payload) {
        Ok(s)  => s.to_string(),
        Err(e) => {
            error!("Non-UTF8 payload on {topic}: {e}");
            return;
        }
    };

    // Extract device_id from topic path: "homeassistant/light/bulb_01/state"
    // → device_id = "bulb_01"
    let device_id = extract_device_id(&topic).unwrap_or_else(|| "unknown".to_string());

    // Parse the HA state payload ("ON", "OFF", or JSON with state field)
    let state = parse_ha_state(&raw);

    let message = TelemetryMessage::new(topic.clone(), device_id, state, raw);

    match message.validate() {
        Ok(()) => {
            info!(
                "✓ [{id}] {topic} → {state:?}",
                id    = &message.id.to_string()[..8],  // short ID for readability
                topic = message.topic,
                state = message.state,
            );
            // Week 2: kafka_producer::publish(message).await;
        }
        Err(e) => {
            warn!("✗ Validation failed for {topic}: {e}");
        }
    }
}

// &str borrow — we only read the topic, don't need ownership.
// Returns Option: None if the topic doesn't have enough segments.
fn extract_device_id(topic: &str) -> Option<String> {
    // "homeassistant/light/bulb_01/state" → ["homeassistant","light","bulb_01","state"]
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() >= 3 {
        Some(parts[2].to_string())
    } else {
        None
    }
}

// Parses HA state strings. HA publishes either "ON"/"OFF" directly,
// or a JSON object like {"state":"ON","brightness":128}.
fn parse_ha_state(raw: &str) -> DeviceState {
    // Try simple string match first
    match raw.trim() {
        "ON"          => return DeviceState::On,
        "OFF"         => return DeviceState::Off,
        "unavailable" => return DeviceState::Unavailable,
        _             => {}
    }
    // Try JSON — HA sometimes sends {"state":"ON",...}
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
        if let Some(s) = v.get("state").and_then(|s| s.as_str()) {
            return match s {
                "ON"  => DeviceState::On,
                "OFF" => DeviceState::Off,
                other => DeviceState::Unknown(other.to_string()),
            };
        }
    }
    DeviceState::Unknown(raw.to_string())
}