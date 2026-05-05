// src/ingestion.rs
// Receives owned data from main's event loop.
// Parses → validates → logs → publishes to Kafka → ships to Seq.

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
    let raw = match std::str::from_utf8(&payload) {
        Ok(s) => s.to_string(),
        Err(e) => {
            error!("Non-UTF8 payload on {topic}: {e}");
            seq.error(&format!("Non-UTF8 payload on {topic}: {e}"));
            return;
        }
    };

    let ha_payload: Option<HaPayload> = serde_json::from_str(&raw).ok();

    let state = match &ha_payload {
        Some(p) => match p.state.as_deref() {
            Some(s) => parse_ha_state(s),
            None    => parse_ha_state(&raw),
        },
        None => parse_ha_state(&raw),
    };

    let device_id = extract_device_id(&topic).unwrap_or_else(|| "unknown".to_string());
    let message   = TelemetryMessage::new(
        topic.clone(),
        device_id,
        state,
        ha_payload,
        raw,
    );

    match message.validate() {
        Ok(()) => {
            info!(
                "✓ [{id}] {topic} → {state:?} brightness={brightness:?}",
                id         = &message.id.to_string()[..8],
                topic      = message.topic,
                state      = message.state,
                brightness = message.payload.as_ref().and_then(|p| p.brightness),
            );

            // Fire WAP dropout detector for unavailable transitions
            // before moving message into Kafka (publish consumes ownership)
            if matches!(message.state, DeviceState::Unavailable) {
                seq.device_unavailable(&message.device_id, &message.topic);
            } else {
                seq.telemetry_received(
                    &message.topic,
                    &message.device_id,
                    &message.id.to_string(),
                );
            }

            producer.publish(message, seq).await;
        }
        Err(e) => {
            warn!("✗ Validation failed for {topic}: {e}");
            seq.error(&format!("Validation failed for {topic}: {e}"));
        }
    }
}

pub fn extract_device_id(topic: &str) -> Option<String> {
    let parts: Vec<&str> = topic.split('/').filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        [_prefix, _type, id, ..] => Some(id.to_string()),
        _ => None,
    }
}

pub fn parse_ha_state(input: &str) -> DeviceState {
    let trimmed = input.trim();

    // 1. Direct match for simple strings (Fast Path)
    match trimmed.to_lowercase().as_str() {
        "on" => return DeviceState::On,
        "off" => return DeviceState::Off,
        "unavailable" => return DeviceState::Unavailable,
        _ => {}
    }

    // 2. JSON Extraction (The "Truth" Path)
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(s) = v.get("state").and_then(|s| s.as_str()) {
            return match s.to_lowercase().as_str() {
                "on" => DeviceState::On,
                "off" => DeviceState::Off,
                "unavailable" => DeviceState::Unavailable,
                other => DeviceState::Unknown(other.to_string()),
            };
        }

        // 3. All-Unavailable check (for your Floodcams)
        if let Some(obj) = v.as_object() {
            let string_values: Vec<&str> = obj.values().filter_map(|v| v.as_str()).collect();
            if !string_values.is_empty() && string_values.iter().all(|s| s.eq_ignore_ascii_case("unavailable")) {
                return DeviceState::Unavailable;
            }
        }
    }

    // 4. Fallback only if no "state" key was found
    if trimmed.len() > 20 {
        DeviceState::Unknown("complex_payload".to_string())
    } else {
        DeviceState::Unknown(trimmed.to_string())
    }
}