// src/main.rs

use foxwatch::config;
use foxwatch::ingestion;
use foxwatch::kafka_producer::KafkaProducer;
use foxwatch::seq_logger::SeqLogger;

use log::{error, info};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
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
    // Use the K8s pod hostname to ensure unique client IDs per replica
    let pod_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "foxwatch-local".to_string());
    let unique_client_id = format!("{}-{}", cfg.client_id, pod_name);

    let mut mqttoptions = MqttOptions::new(unique_client_id, &cfg.mqtt_host, cfg.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // We move the subscribe inside the loop or handle the error gracefully
    // so that if the connection drops, we don't just crash on .expect()
    match client.subscribe(&cfg.mqtt_topic, QoS::AtLeastOnce).await {
        Ok(_) => {
            info!("Subscribed to {} — waiting for telemetry", cfg.mqtt_topic);
        }
        Err(e) => {
            error!("Initial subscription failed: {e}");
        }
    }

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
                // Simple backoff: sleep for 10 seconds before retrying
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue; // Force the event loop to try again
            }
        }
    }
}
