// tests/ingestion_tests.rs
// Integration tests — run with: cargo test
// These import from the foxwatch crate via lib.rs
// Real topic format: ha/lights/device_name/state (not homeassistant/)

use foxwatch::ingestion::{extract_device_id, parse_ha_state};
use foxwatch::telemetry::DeviceState;

// ── extract_device_id tests ──────────────────────────────────────────────────

#[test]
fn test_extract_light_device_id() {
    assert_eq!(
        extract_device_id("ha/lights/family_room_giraffe/state"),
        Some("family_room_giraffe".to_string())
    );
}

#[test]
fn test_extract_vesync_device_id() {
    assert_eq!(
        extract_device_id("ha/vesync/perry_air/state"),
        Some("perry_air".to_string())
    );
}

#[test]
fn test_extract_floodcam_device_id() {
    assert_eq!(
        extract_device_id("ha/floodcam/rear/state"),
        Some("rear".to_string())
    );
}

#[test]
fn test_extract_switch_device_id() {
    assert_eq!(
        extract_device_id("ha/switches/bedroom_socket_1/state"),
        Some("bedroom_socket_1".to_string())
    );
}

#[test]
fn test_extract_short_topic_returns_none() {
    // "ha/lights" only has 2 parts — not enough for a device_id
    assert_eq!(extract_device_id("ha/lights"), None);
}

#[test]
fn test_extract_empty_topic_returns_none() {
    assert_eq!(extract_device_id(""), None);
}

#[test]
fn test_extract_single_segment_returns_none() {
    assert_eq!(extract_device_id("invalid"), None);
}

#[test]
fn test_extract_deep_topic() {
    // ha/vesync/perry_air/speed/state — device_id is still parts[2]
    assert_eq!(
        extract_device_id("ha/vesync/perry_air/speed/state"),
        Some("perry_air".to_string())
    );
}

// ── parse_ha_state tests ─────────────────────────────────────────────────────

#[test]
fn test_parse_simple_on_uppercase() {
    assert!(matches!(parse_ha_state("ON"), DeviceState::On));
}

#[test]
fn test_parse_simple_off_uppercase() {
    assert!(matches!(parse_ha_state("OFF"), DeviceState::Off));
}

#[test]
fn test_parse_lowercase_on() {
    // Real HA payload — family_room_froggy sends lowercase "on"
    assert!(matches!(parse_ha_state("on"), DeviceState::On));
}

#[test]
fn test_parse_lowercase_off() {
    assert!(matches!(parse_ha_state("off"), DeviceState::Off));
}

#[test]
fn test_parse_unavailable() {
    assert!(matches!(parse_ha_state("unavailable"), DeviceState::Unavailable));
}

#[test]
fn test_parse_rich_json_on() {
    // Real bulb payload with brightness and color
    let raw = r#"{"state":"ON","brightness":255,"color":[255,107,91],"color_temp":0}"#;
    assert!(matches!(parse_ha_state(raw), DeviceState::On));
}

#[test]
fn test_parse_rich_json_off() {
    let raw = r#"{"state":"OFF","brightness":0,"color":[0,0,0],"color_temp":0}"#;
    assert!(matches!(parse_ha_state(raw), DeviceState::Off));
}

#[test]
fn test_parse_rich_json_unavailable() {
    // Front porch bulb — null fields
    let raw = r#"{"state":"unavailable","brightness":null,"color":null,"color_temp":null}"#;
    assert!(matches!(parse_ha_state(raw), DeviceState::Unavailable));
}

#[test]
fn test_parse_rich_json_unknown_state() {
    let raw = r#"{"state":"on","brightness":255}"#;
    // lowercase "on" in JSON state field
    assert!(matches!(parse_ha_state(raw), DeviceState::On));
}

#[test]
fn test_parse_vesync_filter_number() {
    // ha/vesync/perry_air/filter sends "76" — numeric string
    assert!(matches!(parse_ha_state("76"), DeviceState::Unknown(_)));
}

#[test]
fn test_parse_vesync_mode_string() {
    // ha/vesync/perry_air/mode sends "sleep"
    assert!(matches!(parse_ha_state("sleep"), DeviceState::Unknown(_)));
}

#[test]
fn test_parse_floodcam_no_state_key() {
    // ha/floodcam/rear/state — has "floodlight" key not "state"
    let raw = r#"{"floodlight":"on","brightness":255,"recording":"on","flip":"off"}"#;
    assert!(matches!(parse_ha_state(raw), DeviceState::Unknown(_)));
}

#[test]
fn test_parse_switch_off() {
    // ha/switches/bedroom_socket_1/state
    let raw = r#"{"state":"OFF"}"#;
    assert!(matches!(parse_ha_state(raw), DeviceState::Off));
}