// tests/mqtt_integration_tests.rs
// Pipeline traffic generator: publishes MQTT messages that flow through foxwatch.
// Observe results in Kafka UI, Seq, and Flink UI.
//
// Prerequisites:
//   - foxwatch running in k3s (staging or production namespace)
//   - MQTT broker at localhost:1883
//
// Run with:
//   cargo test --test mqtt_integration_tests -- --nocapture

use rumqttc::{Client, MqttOptions, QoS};
use std::time::Duration;

const MQTT_HOST: &str = "localhost";
const MQTT_PORT: u16 = 1883;
const KAFKA_BOOTSTRAP: &str = "localhost:9092";
const KAFKA_TOPIC: &str = "foxwatch-telemetry";


const TEST_BULB_TOPIC: &str    = "ha/lights/test_bulb_alpha/state";
const TEST_BULB_DEVICE: &str   = "test_bulb_alpha";

const TEST_SOCKET_TOPIC: &str  = "ha/switches/test_socket_beta/state";
const TEST_SOCKET_DEVICE: &str = "test_socket_beta";

const TEST_WAP_DEVICES: [(&str, &str); 3] = [
    ("ha/lights/test_wap_device_1/state", "test_wap_device_1"),
    ("ha/lights/test_wap_device_2/state", "test_wap_device_2"),
    ("ha/lights/test_wap_device_3/state", "test_wap_device_3"),
];

const TEST_VESYNC_DEVICE: &str = "test_purifier_gamma";
const TEST_VESYNC_MODE_TOPIC: &str   = "ha/vesync/test_purifier_gamma/mode";
const TEST_VESYNC_SPEED_TOPIC: &str  = "ha/vesync/test_purifier_gamma/speed";
const TEST_VESYNC_FILTER_TOPIC: &str = "ha/vesync/test_purifier_gamma/filter";
// ── Helpers ───────────────────────────────────────────────────────────────────

fn mqtt_client(client_id: &str) -> Client {
    let mut opts = MqttOptions::new(client_id, MQTT_HOST, MQTT_PORT);
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut connection) = Client::new(opts, 10);
    std::thread::spawn(move || {
        for _ in connection.iter() {}
    });
    client
}

// ── Scenario 1: Bulb state cycle ─────────────────────────────────────────────
// Publishes ON → OFF → unavailable for TEST_BULB_TOPIC.

#[tokio::test]
async fn inject_bulb_state_cycle() {
    let topic = TEST_BULB_TOPIC;
    let device_id = TEST_BULB_DEVICE;

    let client = mqtt_client("foxwatch-test-bulb");

    // ON
    println!("Inject bulb ON → topic={topic} device_id={device_id}");
    client.publish(
        topic, QoS::AtLeastOnce, false,
        r#"{"state":"ON","brightness":255,"color":[255,107,91],"color_temp":500}"#,
    ).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // OFF
    println!("Inject bulb OFF → topic={topic} device_id={device_id}");
    client.publish(
        topic, QoS::AtLeastOnce, false,
        r#"{"state":"OFF","brightness":0,"color":[0,0,0],"color_temp":0}"#,
    ).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Unavailable
    println!("Inject bulb unavailable → topic={topic} device_id={device_id}");
    client.publish(
        topic, QoS::AtLeastOnce, false,
        "unavailable",
    ).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
}

// ── Scenario 2: Socket false positive ────────────────────────────────────────
// Publishes OFF → unavailable for TEST_SOCKET_TOPIC.

#[tokio::test]
async fn inject_socket_false_positive() {
    let topic = TEST_SOCKET_TOPIC;
    let device_id = TEST_SOCKET_DEVICE;

    let client = mqtt_client("foxwatch-test-socket");

    // OFF — intentional
    println!("Inject socket OFF → topic={topic} device_id={device_id}");
    client.publish(topic, QoS::AtLeastOnce, false, r#"{"state":"OFF"}"#).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Unavailable — expected false positive after intentional off
    println!("Inject socket unavailable → topic={topic} device_id={device_id}");
    client.publish(topic, QoS::AtLeastOnce, false, "unavailable").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
}

// ── Scenario 3: Correlated WAP dropout ───────────────────────────────────────
// Three family room bulbs go Unavailable within seconds of each other.
// This is the WAP dropout signature — should fire the dropout detector.

#[tokio::test]
async fn inject_correlated_wap_dropout() {
    let client = mqtt_client("foxwatch-test-wap");

    // Fire all three unavailable within 6 seconds
    for (topic, device_id) in TEST_WAP_DEVICES {
        println!("Inject WAP unavailable → topic={topic} device_id={device_id}");
        client.publish(&*topic, QoS::AtLeastOnce, false, "unavailable").unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ── Scenario 4: VeSync purifier ───────────────────────────────────────────────
// Plain string payloads, no JSON, no state field.

#[tokio::test]
async fn inject_vesync_payloads() {
    let client = mqtt_client("foxwatch-test-vesync");

    println!("Inject VeSync mode → topic={TEST_VESYNC_MODE_TOPIC} device_id={TEST_VESYNC_DEVICE} payload=auto");
    client.publish(TEST_VESYNC_MODE_TOPIC,   QoS::AtLeastOnce, false, "auto").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    println!("Inject VeSync speed → topic={TEST_VESYNC_SPEED_TOPIC} device_id={TEST_VESYNC_DEVICE} payload=3");
    client.publish(TEST_VESYNC_SPEED_TOPIC,  QoS::AtLeastOnce, false, "3").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    println!("Inject VeSync filter → topic={TEST_VESYNC_FILTER_TOPIC} device_id={TEST_VESYNC_DEVICE} payload=76");
    client.publish(TEST_VESYNC_FILTER_TOPIC, QoS::AtLeastOnce, false, "76").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
}