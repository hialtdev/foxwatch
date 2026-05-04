// src/seq_logger.rs
// Ships structured log events to Seq in CLEF (Compact Log Event Format).
// Background Tokio task batches events every second — hot path never blocks.

use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

#[derive(Serialize, Clone)]
struct ClefEvent {
    #[serde(rename = "@t")]
    timestamp: String,
    #[serde(rename = "@mt")]
    message_template: String,
    #[serde(rename = "@l")]
    level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kafka_partition: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kafka_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropout_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropout_devices: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wap_suspect: Option<String>,
    application: String,
}

impl ClefEvent {
    fn new(level: &str, template: &str) -> Self {
        Self {
            timestamp:        Utc::now().to_rfc3339(),
            message_template: template.to_string(),
            level:            level.to_string(),
            topic:            None,
            device_id:        None,
            message_id:       None,
            kafka_partition:  None,
            kafka_offset:     None,
            dropout_count:    None,
            dropout_devices:  None,
            wap_suspect:      None,
            application:      "foxwatch".to_string(),
        }
    }
}

/// Messages sent to the background shipper task
enum ShipperMessage {
    Event(ClefEvent),
    /// Notify the shipper that a device just went unavailable
    /// The shipper tracks these and fires a WAP dropout alert
    /// if multiple devices drop within the window
    DeviceUnavailable { device_id: String, topic: String },
}

#[derive(Clone)]
pub struct SeqLogger {
    sender: mpsc::Sender<ShipperMessage>,
}

impl SeqLogger {
    pub fn start(seq_url: String) -> Self {
        let (tx, mut rx) = mpsc::channel::<ShipperMessage>(1000);
        let client       = Client::new();
        let ingest_url   = format!("{}/api/events/raw?clef", seq_url.trim_end_matches('/'));

        tokio::spawn(async move {
            let mut ticker      = interval(Duration::from_secs(1));
            let mut batch: Vec<String> = Vec::new();

            // Track recent unavailable events for WAP dropout detection
            // (device_id, timestamp)
            let mut recent_unavailable: Vec<(String, std::time::Instant)> = Vec::new();

            // Known WAP device groups — add more as you map your network
            let greyhound_down_devices = vec![
                "family_room_froggy".to_string(),
                "family_room_giraffe".to_string(),
                "family_room_greenie".to_string(),
            ];

            // How many devices must drop within the window to trigger WAP alert
            let dropout_threshold = 2;
            let dropout_window    = std::time::Duration::from_secs(30);

            loop {
                tokio::select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            ShipperMessage::Event(event) => {
                                if let Ok(json) = serde_json::to_string(&event) {
                                    batch.push(json);
                                }
                            }
                            ShipperMessage::DeviceUnavailable { device_id, topic } => {
                                let now = std::time::Instant::now();

                                // Fire individual device warning
                                let mut event = ClefEvent::new(
                                    "Warning",
                                    "Device unavailable: {device_id} on {topic}",
                                );
                                event.device_id = Some(device_id.clone());
                                event.topic     = Some(topic.clone());
                                if let Ok(json) = serde_json::to_string(&event) {
                                    batch.push(json);
                                }

                                // Track for WAP dropout detection
                                recent_unavailable.push((device_id.clone(), now));

                                // Purge events outside the window
                                recent_unavailable.retain(|(_, t)| t.elapsed() < dropout_window);

                                // Check how many greyhound_down devices have dropped recently
                                let greyhound_drops: Vec<String> = recent_unavailable
                                    .iter()
                                    .filter(|(d, _)| greyhound_down_devices.contains(d))
                                    .map(|(d, _)| d.clone())
                                    .collect();

                                // Deduplicate — same device dropping twice shouldn't double-count
                                let mut unique_drops = greyhound_drops.clone();
                                unique_drops.dedup();

                                if unique_drops.len() >= dropout_threshold {
                                    // WAP dropout detected — fire Error level event
                                    let mut alert = ClefEvent::new(
                                        "Error",
                                        "WAP dropout detected: {dropout_count} devices on greyhound_down went unavailable within {window}s — suspect WAP reboot or interference",
                                    );
                                    alert.dropout_count   = Some(unique_drops.len());
                                    alert.dropout_devices = Some(unique_drops.join(", "));
                                    alert.wap_suspect     = Some("greyhound_down".to_string());

                                    if let Ok(json) = serde_json::to_string(&alert) {
                                        batch.push(json);
                                    }

                                    // Clear the window after firing so we don't spam
                                    recent_unavailable.retain(|(d, _)| !greyhound_down_devices.contains(d));
                                }
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            let body = batch.join("\n");
                            batch.clear();
                            let _ = client
                                .post(&ingest_url)
                                .header("Content-Type", "application/vnd.serilog.clef")
                                .body(body)
                                .send()
                                .await;
                        }
                    }
                }
            }
        });

        Self { sender: tx }
    }

    pub fn info(&self, template: &str) {
        let event = ClefEvent::new("Information", template);
        let _ = self.sender.try_send(ShipperMessage::Event(event));
    }

    pub fn telemetry_received(&self, topic: &str, device_id: &str, message_id: &str) {
        let mut event = ClefEvent::new(
            "Information",
            "Telemetry received from {device_id} on {topic}",
        );
        event.topic      = Some(topic.to_string());
        event.device_id  = Some(device_id.to_string());
        event.message_id = Some(message_id.to_string());
        let _ = self.sender.try_send(ShipperMessage::Event(event));
    }

    pub fn kafka_delivered(&self, device_id: &str, partition: i32, offset: i64) {
        let mut event = ClefEvent::new(
            "Information",
            "Kafka delivery confirmed for {device_id} partition={kafka_partition} offset={kafka_offset}",
        );
        event.device_id      = Some(device_id.to_string());
        event.kafka_partition = Some(partition);
        event.kafka_offset    = Some(offset);
        let _ = self.sender.try_send(ShipperMessage::Event(event));
    }

    /// Call when a device transitions to Unavailable state.
    /// Triggers WAP dropout detection in the background shipper.
    pub fn device_unavailable(&self, device_id: &str, topic: &str) {
        let _ = self.sender.try_send(ShipperMessage::DeviceUnavailable {
            device_id: device_id.to_string(),
            topic:     topic.to_string(),
        });
    }

    pub fn error(&self, template: &str) {
        let event = ClefEvent::new("Error", template);
        let _ = self.sender.try_send(ShipperMessage::Event(event));
    }
}