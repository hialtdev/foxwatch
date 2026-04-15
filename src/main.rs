use foxwatch::config;
use foxwatch::ingestion;
use foxwatch::kafka_producer::KafkaProducer;
use foxwatch::seq_logger::SeqLogger;

use log::{error, info};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() {
    env_logger::init();

    let cfg = config::Config::from_env();

    info!(
        "foxwatch starting — MQTT={}:{} Kafka={} Seq={}",
        cfg.mqtt_host, cfg.mqtt_port, cfg.kafka_bootstrap, cfg.seq_url
    );

    let seq = SeqLogger::start(cfg.seq_url.clone());
    seq.info("foxwatch started — pipeline initializing");

    let producer = KafkaProducer::new(&cfg.kafka_bootstrap, &cfg.kafka_topic);

    let pod_name = std::env::var("HOSTNAME").unwrap_or_else(|_| "foxwatch-local".to_string());
    let unique_client_id = format!("{}-{}", cfg.client_id, pod_name);

    let mut mqttoptions = MqttOptions::new(unique_client_id, &cfg.mqtt_host, cfg.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    mqttoptions.set_clean_session(true);

    // ✅ Increased channel capacity
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);

    // ✅ Concurrency cap for ingestion tasks
    let semaphore = Arc::new(Semaphore::new(64));

    // ✅ No subscribe() call here — wait for ConnAck

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("MQTT connected — subscribing to {}", cfg.mqtt_topic);
                if let Err(e) = client.subscribe(&cfg.mqtt_topic, QoS::AtLeastOnce).await {
                    error!("Subscription failed: {e}");
                }
            }

            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                let producer = producer.clone();
                let seq = seq.clone();
                let topic = publish.topic.clone();
                let payload = publish.payload.to_vec();

                tokio::spawn(async move {
                    ingestion::process_payload(topic, payload, producer, seq).await;
                    drop(permit);
                });
            }

            Ok(Event::Incoming(Packet::SubAck(ack))) => {
                info!("Subscribed — SubAck pkid={}", ack.pkid);
            }

            Ok(Event::Incoming(Packet::PingResp)) => {
                log::trace!("PingResp received — keep-alive healthy");
            }

            Ok(_) => {}

            // ✅ No sleep — poll immediately to let rumqttc drive reconnect
            Err(e) => {
                error!("MQTT connection error: {e}");
            }
        }
    }
}
