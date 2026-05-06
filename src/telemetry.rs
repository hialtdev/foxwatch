// src/telemetry.rs
// Smart Data Type pattern — domain logic lives in impl blocks.
// HaPayload models the real JSON your devices publish over MQTT.
// TelemetryMessage wraps it with pipeline metadata (id, topic, timestamp).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── HaPayload ────────────────────────────────────────────────────────────────
// Models the actual JSON payloads your HA devices publish.
// All fields are Option<> because different device types send different fields:
//   - Bulbs send state + brightness + color + color_temp
//   - Switches send state only
//   - VeSync air purifier sends mode, speed, filter separately
//   - Floodcams send floodlight + recording + flip etc
//
// serde(default) means missing fields deserialize to None instead of erroring.
// serde(rename_all = "lowercase") handles "state", "brightness" etc.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct HaPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub brightness: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<HaColor>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub color_temp: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub floodlight: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub flip: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub night_vision: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<f64>,
}
// HA publishes color as a JSON array: [255, 107, 91]
// We deserialize it as a struct with r/g/b fields for clarity in Seq/Kafka.
#[derive(Debug, Clone)]
pub struct HaColor {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

// Custom deserializer for HaColor — HA sends [r, g, b] array, not {r,g,b} object.
// This implements the Visitor pattern Rust uses for custom serde logic.
impl<'de> Deserialize<'de> for HaColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialize as a sequence (JSON array)
        let arr: Vec<serde_json::Value> = Vec::deserialize(deserializer)?;

        // Extract each element — default to 0 if missing or null
        let r = arr
            .first()
            .and_then(|v| v.as_f64())
            .map(|f| f as u8)
            .unwrap_or(0);
        let g = arr
            .get(1)
            .and_then(|v| v.as_f64())
            .map(|f| f as u8)
            .unwrap_or(0);
        let b = arr
            .get(2)
            .and_then(|v| v.as_f64())
            .map(|f| f as u8)
            .unwrap_or(0);

        Ok(HaColor { r, g, b })
    }
}

// Override Serialize so HaColor goes back out as [r, g, b] array
// keeping the wire format consistent with what HA sends
impl Serialize for HaColor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element(&self.r)?;
        seq.serialize_element(&self.g)?;
        seq.serialize_element(&self.b)?;
        seq.end()
    }
}

// ── DeviceState ──────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum DeviceState {
    On,
    Off,
    Unavailable,
    Unknown(String),
}

// ── ValidationError ──────────────────────────────────────────────────────────
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

// ── TelemetryMessage ─────────────────────────────────────────────────────────
// Pipeline envelope — wraps the parsed HaPayload with metadata.
// This is what gets serialized to Kafka and shipped to Seq.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub id: Uuid,
    pub topic: String,
    pub device_id: String,
    pub state: DeviceState,
    pub timestamp: DateTime<Utc>,

    // Structured payload — Some() for JSON devices, None for plain string devices
    // (VeSync topics like ha/vesync/perry_air/filter send "76" not JSON)
    pub payload: Option<HaPayload>,

    // Always preserved for audit / replay / debugging
    pub raw_payload: String,
}

impl TelemetryMessage {
    pub fn new(
        topic: String,
        device_id: String,
        state: DeviceState,
        payload: Option<HaPayload>,
        raw_payload: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            device_id,
            state,
            timestamp: Utc::now(),
            payload,
            raw_payload,
        }
    }

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

    #[allow(dead_code)]
    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    fn make_message() -> TelemetryMessage {
        let payload = HaPayload {
            state: Some("on".to_string()),
            brightness: Some(255.0),
            color: Some(HaColor {
                r: 255,
                g: 107,
                b: 91,
            }),
            color_temp: Some(0.0),
            ..Default::default()
        };
        TelemetryMessage::new(
            "ha/lights/family_room_giraffe/state".to_string(),
            "family_room_giraffe".to_string(),
            DeviceState::On,
            Some(payload),
            r#"{"state":"on","brightness":255,"color":[255,107,91],"color_temp":0}"#.to_string(),
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
        let back: TelemetryMessage = serde_json::from_slice(&bytes).unwrap();
        assert!(!back.id.to_string().is_empty());
    }

    #[test]
    fn deserializes_rich_bulb_payload() {
        let raw = r#"{"state":"on","brightness":255,"color":[255,107,91],"color_temp":0}"#;
        let payload: HaPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(payload.state, Some("on".to_string()));
        assert_eq!(payload.brightness, Some(255.0));
        let color = payload.color.unwrap();
        assert_eq!(color.r, 255);
        assert_eq!(color.g, 107);
        assert_eq!(color.b, 91);
    }

    #[test]
    fn deserializes_null_fields_as_none() {
        // Front porch bulb — unavailable with null fields
        let raw = r#"{"state":"unavailable","brightness":null,"color":null,"color_temp":null}"#;
        let payload: HaPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(payload.state, Some("unavailable".to_string()));
        assert!(payload.brightness.is_none());
        assert!(payload.color.is_none());
    }

    #[test]
    fn deserializes_switch_payload() {
        let raw = r#"{"state":"OFF"}"#;
        let payload: HaPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(payload.state, Some("OFF".to_string()));
        assert!(payload.brightness.is_none());
        assert!(payload.color.is_none());
    }

    #[test]
    fn color_round_trips_as_array() {
        let color = HaColor {
            r: 255,
            g: 107,
            b: 91,
        };
        let json = serde_json::to_string(&color).unwrap();
        assert_eq!(json, "[255,107,91]");
        let back: HaColor = serde_json::from_str(&json).unwrap();
        assert_eq!(back.r, 255);
        assert_eq!(back.g, 107);
        assert_eq!(back.b, 91);
    }
}
