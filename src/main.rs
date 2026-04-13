// src/main.rs — foxwatch entry point
// Ownership philosophy: MQTT payloads are cloned once on receipt, then
// ownership is moved into the processor. No shared mutable state.

mod telemetry;
mod ingestion;

use std::time::Duration;
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use log::{info, warn, error};

// #[tokio::main] transforms async fn main() into a Tokio runtime.
// This is the async equivalent of Spring Boot's application context startup.
#[tokio::main]
async fn main() {
    // env_logger reads RUST_LOG env var: RUST_LOG=info cargo run
    env_logger::init();
    info!("foxwatch starting — connecting to MQTT broker");

    // ── MQTT connection setup ────────────────────────────────────────────────
    // Change host/port to match your local Mosquitto broker.
    // Your HA MQTT bridge already publishes here.
    let mut mqttoptions = MqttOptions::new("foxwatch-client", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    // AsyncClient returns a (client, eventloop) pair.
    // client    → we use to subscribe (can be cloned and sent across tasks)
    // eventloop → we poll; it owns the TCP connection, cannot be cloned
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Subscribe to the Home Assistant state topic.
    // "#" is the MQTT wildcard — matches all subtopics.
    // Adjust to your actual HA topic, e.g. "homeassistant/light/#"
    client
        .subscribe("homeassistant/#", QoS::AtLeastOnce)
        .await
        .expect("Failed to subscribe — is the broker running?");

    info!("Subscribed to homeassistant/#  — waiting for telemetry");

    // ── Event loop ───────────────────────────────────────────────────────────
    // We poll the eventloop in a loop. Each .poll() yields one MQTT event.
    // OWNERSHIP NOTE: Publish payloads arrive as Bytes (a reference-counted
    // smart pointer). We call .to_vec() to take owned bytes, then pass
    // ownership into process_payload(). This means zero shared mutable state
    // between the network layer and the processor — safe to spawn as a task.
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic   = publish.topic.clone();    // String — cheap clone
                let payload = publish.payload.to_vec(); // owned Vec<u8>

                // tokio::spawn moves ownership into an async task.
                // The 'move' closure captures topic + payload by value,
                // so the task is fully self-contained — no lifetime entanglement
                // with the event loop. This is the Rust equivalent of submitting
                // a Runnable to a Java ExecutorService.
                tokio::spawn(async move {
                    ingestion::process_payload(topic, payload).await;
                });
            }
            Ok(_) => {}  // ConnAck, SubAck, PingResp, etc — ignore for now
            Err(e) => {
                error!("MQTT connection error: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}