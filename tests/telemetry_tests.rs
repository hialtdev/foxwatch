// tests/telemetry_tests.rs
// Integration tests for TelemetryMessage, HaPayload, HaColor, and DeviceState.
// These test the serde layer — serialization, deserialization, and round-trips.
// Run with: cargo test
// Run just this suite: cargo test --test telemetry_tests

use foxwatch::telemetry::{DeviceState, HaColor, HaPayload, TelemetryMessage};

// ── Test helpers ─────────────────────────────────────────────────────────────

fn bulb_payload() -> HaPayload {
    HaPayload {
        state: Some("on".to_string()),
        brightness: Some(255.0),
        color: Some(HaColor {
            r: 255,
            g: 107,
            b: 91,
        }),
        color_temp: Some(0.0),
        ..Default::default()
    }
}

fn unavailable_payload() -> HaPayload {
    HaPayload {
        state: Some("unavailable".to_string()),
        brightness: None,
        color: None,
        color_temp: None,
        ..Default::default()
    }
}

fn switch_payload() -> HaPayload {
    HaPayload {
        state: Some("OFF".to_string()),
        ..Default::default()
    }
}

fn floodcam_payload() -> HaPayload {
    HaPayload {
        floodlight: Some("on".to_string()),
        recording: Some("on".to_string()),
        flip: Some("off".to_string()),
        watermark: Some("on".to_string()),
        ..Default::default()
    }
}

fn make_bulb_message() -> TelemetryMessage {
    TelemetryMessage::new(
        "ha/lights/family_room_giraffe/state".to_string(),
        "family_room_giraffe".to_string(),
        DeviceState::On,
        Some(bulb_payload()),
        r#"{"state":"on","brightness":255,"color":[255,107,91],"color_temp":0}"#.to_string(),
    )
}

// ── TelemetryMessage validation tests ────────────────────────────────────────

#[test]
fn valid_bulb_message_passes_validation() {
    assert!(make_bulb_message().validate().is_ok());
}

#[test]
fn empty_topic_fails_validation() {
    let mut msg = make_bulb_message();
    msg.topic = String::new();
    assert!(msg.validate().is_err());
}

#[test]
fn empty_device_id_fails_validation() {
    let mut msg = make_bulb_message();
    msg.device_id = String::new();
    assert!(msg.validate().is_err());
}

// ── HaPayload deserialization tests ──────────────────────────────────────────

#[test]
fn deserializes_full_bulb_payload() {
    let raw = r#"{"state":"on","brightness":255,"color":[255,107,91],"color_temp":0}"#;
    let payload: HaPayload = serde_json::from_str(raw).unwrap();

    assert_eq!(payload.state, Some("on".to_string()));
    assert_eq!(payload.brightness, Some(255.0));
    assert_eq!(payload.color_temp, Some(0.0));

    let color = payload.color.unwrap();
    assert_eq!(color.r, 255);
    assert_eq!(color.g, 107);
    assert_eq!(color.b, 91);
}

#[test]
fn deserializes_null_fields_as_none() {
    // Front porch bulb — unavailable with null brightness/color
    let raw = r#"{"state":"unavailable","brightness":null,"color":null,"color_temp":null}"#;
    let payload: HaPayload = serde_json::from_str(raw).unwrap();

    assert_eq!(payload.state, Some("unavailable".to_string()));
    assert!(payload.brightness.is_none());
    assert!(payload.color.is_none());
    assert!(payload.color_temp.is_none());
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
fn deserializes_floodcam_payload() {
    let raw = r#"{"floodlight":"on","brightness":255,"recording":"on","flip":"off","watermark":"on","night_vision":"0"}"#;
    let payload: HaPayload = serde_json::from_str(raw).unwrap();

    assert_eq!(payload.floodlight, Some("on".to_string()));
    assert_eq!(payload.recording, Some("on".to_string()));
    assert_eq!(payload.flip, Some("off".to_string()));
    // No "state" key in floodcam payload
    assert!(payload.state.is_none());
}

#[test]
fn plain_string_deserializes_to_none() {
    // VeSync filter topic sends "76" — not JSON, should fail gracefully
    let result: Option<HaPayload> = serde_json::from_str("76").ok();
    assert!(result.is_none());
}

#[test]
fn plain_on_string_deserializes_to_none() {
    // Simple "ON" string — not JSON
    let result: Option<HaPayload> = serde_json::from_str("ON").ok();
    assert!(result.is_none());
}

// ── HaColor serde tests ───────────────────────────────────────────────────────

#[test]
fn color_serializes_as_array() {
    let color = HaColor {
        r: 255,
        g: 107,
        b: 91,
    };
    let json = serde_json::to_string(&color).unwrap();
    assert_eq!(json, "[255,107,91]");
}

#[test]
fn color_deserializes_from_array() {
    let json = "[255,107,91]";
    let color: HaColor = serde_json::from_str(json).unwrap();
    assert_eq!(color.r, 255);
    assert_eq!(color.g, 107);
    assert_eq!(color.b, 91);
}

#[test]
fn color_round_trips() {
    let original = HaColor {
        r: 128,
        g: 64,
        b: 32,
    };
    let json = serde_json::to_string(&original).unwrap();
    let restored: HaColor = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.r, original.r);
    assert_eq!(restored.g, original.g);
    assert_eq!(restored.b, original.b);
}

#[test]
fn color_handles_null_array_elements() {
    // Some HA bulbs send null color components when unavailable
    let json = "[null,null,null]";
    let color: HaColor = serde_json::from_str(json).unwrap();
    // Should default to 0 for null elements
    assert_eq!(color.r, 0);
    assert_eq!(color.g, 0);
    assert_eq!(color.b, 0);
}

// ── TelemetryMessage serialization round-trip tests ──────────────────────────

#[test]
fn bulb_message_round_trips() {
    let original = make_bulb_message();
    let bytes = original.serialize().unwrap();
    let restored: TelemetryMessage = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(original.topic, restored.topic);
    assert_eq!(original.device_id, restored.device_id);
    assert_eq!(original.raw_payload, restored.raw_payload);

    // Payload survives round-trip
    let orig_color = original.payload.as_ref().unwrap().color.as_ref().unwrap();
    let rest_color = restored.payload.as_ref().unwrap().color.as_ref().unwrap();
    assert_eq!(orig_color.r, rest_color.r);
    assert_eq!(orig_color.g, rest_color.g);
    assert_eq!(orig_color.b, rest_color.b);
}

#[test]
fn message_with_no_payload_round_trips() {
    // VeSync filter — plain string payload, no structured HaPayload
    let msg = TelemetryMessage::new(
        "ha/vesync/perry_air/filter".to_string(),
        "perry_air".to_string(),
        DeviceState::Unknown("76".to_string()),
        None, // no structured payload
        "76".to_string(),
    );
    let bytes = msg.serialize().unwrap();
    let restored: TelemetryMessage = serde_json::from_slice(&bytes).unwrap();
    assert!(restored.payload.is_none());
    assert_eq!(restored.raw_payload, "76");
}

#[test]
fn kafka_json_contains_brightness() {
    // Verify Kafka payload includes brightness for Foxglove-style queries
    let msg = make_bulb_message();
    let bytes = msg.serialize().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let brightness = json["payload"]["brightness"].as_f64();
    assert_eq!(brightness, Some(255.0));
}

#[test]
fn kafka_json_contains_color_array() {
    let msg = make_bulb_message();
    let bytes = msg.serialize().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let color = &json["payload"]["color"];
    assert_eq!(color[0], 255);
    assert_eq!(color[1], 107);
    assert_eq!(color[2], 91);
}

#[test]
fn unavailable_message_has_no_brightness() {
    let msg = TelemetryMessage::new(
        "ha/lights/front_porch_south/state".to_string(),
        "front_porch_south".to_string(),
        DeviceState::Unavailable,
        Some(unavailable_payload()),
        r#"{"state":"unavailable","brightness":null,"color":null,"color_temp":null}"#.to_string(),
    );
    let bytes = msg.serialize().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert!(json["payload"]["brightness"].is_null());
}

#[test]
fn switch_message_has_no_color() {
    let msg = TelemetryMessage::new(
        "ha/switches/bedroom_socket_1/state".to_string(),
        "bedroom_socket_1".to_string(),
        DeviceState::Off,
        Some(switch_payload()),
        r#"{"state":"OFF"}"#.to_string(),
    );
    let bytes = msg.serialize().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert!(json["payload"]["color"].is_null());
}
#[test]
fn floodcam_message_has_no_state() {
    let msg = TelemetryMessage::new(
        "ha/floodcam/rear/state".to_string(),
        "rear".to_string(),
        DeviceState::Unknown("floodlight:on".to_string()),
        Some(floodcam_payload()),
        r#"{"floodlight":"on","recording":"on","flip":"off","watermark":"on"}"#.to_string(),
    );
    let bytes = msg.serialize().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["payload"]["state"].is_null());
    assert_eq!(json["payload"]["floodlight"], "on");
}