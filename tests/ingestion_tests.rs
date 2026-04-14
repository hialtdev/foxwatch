// tests/ingestion_tests.rs
use foxwatch::ingestion::extract_device_id;

#[test]
fn test_extract_device_id_standard_ha() {
    let topic = "homeassistant/light/kitchen_bulb/state";
    // Currently, your code takes parts[2], which is 'kitchen_bulb'
    assert_eq!(extract_device_id(topic), Some("kitchen_bulb".to_string()));
}

#[test]
fn test_extract_device_id_short_topic() {
    let _topic = "homeassistant/status";
    // This is currently failing in your logs (returning None/unknown)
    // because parts.len() is only 2.
    // assert_eq!(extract_device_id(topic), None);
}

#[test]
fn test_extract_device_id_empty() {
    assert_eq!(extract_device_id(""), None);
}

#[test]
fn test_extract_device_id_edge_cases() {
    // 1. Standard HA topic
    assert_eq!(
        extract_device_id("homeassistant/light/bulb_01/state"),
        Some("bulb_01".to_string())
    );

    // 2. The "Short" topic (your current logs show this returns 'unknown')
    // Let's decide: if it's 'homeassistant/status', should the ID be 'status'?
    assert_eq!(
        extract_device_id("homeassistant/status"),
        Some("status".to_string())
    );

    // 3. Garbage input
    assert_eq!(extract_device_id("invalid"), None);
}

#[test]
fn test_extract_device_id_suite() {
    // Standard
    assert_eq!(
        extract_device_id("homeassistant/light/bulb_01/state"),
        Some("bulb_01".to_string())
    );

    // Short
    assert_eq!(
        extract_device_id("homeassistant/status"),
        Some("status".to_string())
    );

    // Deep (Attic motion sensor)
    assert_eq!(
        extract_device_id("homeassistant/binary_sensor/attic_motion/state"),
        Some("attic_motion".to_string())
    );

    // Leading slash safety
    assert_eq!(
        extract_device_id("/homeassistant/status/"),
        Some("status".to_string())
    );
}
// docker exec -it mosquitto mosquitto_pub -h localhost \
// -t "homeassistant/light/test_bulb/state" -m "ON"
