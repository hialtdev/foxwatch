// src/main.rs — foxwatch entry point

mod config;
mod ingestion;
mod kafka_producer;
mod seq_logger;
mod telemetry;

use kafka_producer::KafkaProducer;
use log::{error, info};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use seq_logger::SeqLogger;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();

    let cfg = config::Config::from_env();

    info!(
        "foxwatch starting — MQTT={}:{} Kafka={} Seq={}",
        cfg.mqtt_host, cfg.mqtt_port, cfg.kafka_bootstrap, cfg.seq_url
    );

    // ── Seq structured logger ────────────────────────────────────────────────
    // Spawns a background task that batches CLEF events to Seq every second.
    // Clone is cheap — just an mpsc sender clone.
    let seq = SeqLogger::start(cfg.seq_url.clone());
    seq.info("foxwatch started — pipeline initializing");

    // ── Kafka producer ───────────────────────────────────────────────────────
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
    seq.info(&format!(
        "Subscribed to {} — pipeline ready",
        cfg.mqtt_topic
    ));

    // ── Event loop ───────────────────────────────────────────────────────────
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let topic = publish.topic.clone();
                let payload = publish.payload.to_vec();
                let producer = producer.clone();
                let seq = seq.clone();

                tokio::spawn(async move {
                    ingestion::process_payload(topic, payload, producer, seq).await;
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
