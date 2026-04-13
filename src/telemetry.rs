// src/telemetry.rs
// Smart Data Type pattern: domain logic lives in impl blocks on the struct.
// No separate TelemetryService class. The data knows how to validate itself.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// #[derive] macros auto-generate trait impls — similar to Lombok in Java.
// Serialize/Deserialize: serde handles JSON today, binary tomorrow.
// Debug: lets us use {:?} in log messages.
// Clone: lets callers duplicate a message if needed (e.g. fan-out to Kafka + Seq)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub id: Uuid,
    pub topic: String,
    pub device_id: String,
    pub state: DeviceState,
    pub timestamp: DateTime<Utc>,
    pub raw_payload: String, // preserve original for audit / replay
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeviceState {
    On,
    Off,
    Unavailable,
    Unknown(String), // captures any new state HA adds — forward-compatible
}

// ValidationError is a simple enum — no exceptions in Rust.
// Functions return Result<T, E> and callers must handle both arms.
#[derive(Debug)]
pub enum ValidationError {
    EmptyTopic,
    EmptyDeviceId,
    FutureTimestamp,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::EmptyTopic => write!(f, "topic is empty"),
            ValidationError::EmptyDeviceId => write!(f, "device_id is empty"),
            ValidationError::FutureTimestamp => write!(f, "timestamp is in the future"),
        }
    }
}

impl TelemetryMessage {
    // Constructor — takes owned Strings (caller moves in, we own them).
    // No getters/setters needed; Rust's field visibility handles access.
    pub fn new(topic: String, device_id: String, state: DeviceState, raw_payload: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            device_id,
            state,
            timestamp: Utc::now(),
            raw_payload,
        }
    }

    // &self = immutable borrow — we inspect but don't consume the message.
    // Returns Result: Ok(()) means valid, Err carries the reason.
    // Callers use `?` operator to propagate errors — like checked exceptions
    // but enforced at compile time, not runtime.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.topic.is_empty() {
            return Err(ValidationError::EmptyTopic);
        }
        if self.device_id.is_empty() {
            return Err(ValidationError::EmptyDeviceId);
        }
        if self.timestamp > Utc::now() {
            return Err(ValidationError::FutureTimestamp);
        }
        Ok(())
    }

    // Abstracted serialization — today JSON, tomorrow bincode/postcard.
    // Swapping the body here is the only change needed for binary formats.
    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

// ── Unit tests live alongside the code in Rust — no separate test/ tree ──
#[cfg(test)]
mod tests {
    use super::*;

    fn make_message() -> TelemetryMessage {
        TelemetryMessage::new(
            "homeassistant/light/bulb_01/state".to_string(),
            "bulb_01".to_string(),
            DeviceState::On,
            r#"{"state":"ON"}"#.to_string(),
        )
    }

    #[test]
    fn valid_message_passes() {
        assert!(make_message().validate().is_ok());
    }

    #[test]
    fn empty_topic_fails() {
        let mut msg = make_message();
        msg.topic = String::new();
        assert!(msg.validate().is_err());
    }

    #[test]
    fn serializes_to_json() {
        let bytes = make_message().serialize().unwrap();
        assert!(!bytes.is_empty());
        // Round-trip: deserialize back and check id survives
        let back: TelemetryMessage = serde_json::from_slice(&bytes).unwrap();
        assert!(!back.id.to_string().is_empty());
    }
}
