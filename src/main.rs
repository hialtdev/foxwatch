// src/main.rs — foxwatch entry point

mod config;
mod ingestion;
mod kafka_producer;
mod telemetry;

use kafka_producer::KafkaProducer;
use log::{error, info};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();

    let cfg = config::Config::from_env();

    info!(
        "foxwatch starting — MQTT={}:{} Kafka={}",
        cfg.mqtt_host, cfg.mqtt_port, cfg.kafka_bootstrap
    );

    // ── Kafka producer ───────────────────────────────────────────────────────
    // Created once at startup, then cloned into each spawned task.
    // KafkaProducer::clone() is cheap — rdkafka uses Arc internally,
    // so cloning just increments a reference count, no connection overhead.
    let producer = KafkaProducer::new(&cfg.kafka_bootstrap, &cfg.kafka_topic);

    // ── MQTT setup ───────────────────────────────────────────────────────────
    let mut mqttoptions = MqttOptions::new(&cfg.client_id, &cfg.mqtt_host, cfg.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client
        .subscribe(&cfg.mqtt_topic, QoS::AtLeastOnce)
        .await
        .expect("Failed to subscribe — is the broker running?");

    info!("Subscribed to {} — waiting for telemetry", cfg.mqtt_topic);

    // ── Event loop ───────────────────────────────────────────────────────────
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic = publish.topic.clone();
                let payload = publish.payload.to_vec();

                // Clone the producer for this task — Arc refcount bump only,
                // no new TCP connection. Each task owns its own handle.
                let producer = producer.clone();

                tokio::spawn(async move {
                    ingestion::process_payload(topic, payload, producer).await;
                });
            }
            Ok(_) => {}
            Err(e) => {
                error!("MQTT connection error: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
