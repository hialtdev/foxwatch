// src/seq_logger.rs
// Ships structured log events to Seq in CLEF (Compact Log Event Format).
// CLEF is newline-delimited JSON — one JSON object per log event.
// This is the same wire format bitbybit's log4j2 HTTP appender uses.
//
// Architecture: a background Tokio task receives log events over an
// mpsc channel and batches them to Seq every second. This keeps the
// hot path (ingestion) non-blocking — logging never stalls message processing.

use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

/// A single CLEF log event — maps directly to Seq's ingestion schema.
/// @t = timestamp, @mt = message template, @l = level, rest = properties.
#[derive(Serialize)]
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
    application: String,
}

impl ClefEvent {
    fn new(level: &str, template: &str) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            message_template: template.to_string(),
            level: level.to_string(),
            topic: None,
            device_id: None,
            message_id: None,
            kafka_partition: None,
            kafka_offset: None,
            application: "foxwatch".to_string(),
        }
    }
}

/// Handle for sending log events to the background shipper task.
/// Clone is cheap — it's just an mpsc sender.
#[derive(Clone)]
pub struct SeqLogger {
    sender: mpsc::Sender<ClefEvent>,
}

impl SeqLogger {
    /// Spawns the background shipping task and returns a handle.
    /// Call once at startup in main.rs.
    pub fn start(seq_url: String) -> Self {
        let (tx, mut rx) = mpsc::channel::<ClefEvent>(1000);
        let client = Client::new();
        let ingest_url = format!("{}/api/events/raw?clef", seq_url.trim_end_matches('/'));

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            let mut batch: Vec<String> = Vec::new();

            loop {
                tokio::select! {
                    // Drain all pending events into the batch
                    Some(event) = rx.recv() => {
                        if let Ok(json) = serde_json::to_string(&event) {
                            batch.push(json);
                        }
                    }
                    // Every second, ship whatever we have
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

    /// Log a simple info message
    pub fn info(&self, template: &str) {
        let event = ClefEvent::new("Information", template);
        let _ = self.sender.try_send(event);
    }

    /// Log a telemetry ingestion event with full context
    pub fn telemetry_received(&self, topic: &str, device_id: &str, message_id: &str) {
        let mut event = ClefEvent::new(
            "Information",
            "Telemetry received from {device_id} on {topic}",
        );
        event.topic = Some(topic.to_string());
        event.device_id = Some(device_id.to_string());
        event.message_id = Some(message_id.to_string());
        let _ = self.sender.try_send(event);
    }

    /// Log a Kafka delivery confirmation
    pub fn kafka_delivered(&self, device_id: &str, partition: i32, offset: i64) {
        let mut event = ClefEvent::new(
            "Information",
            "Kafka delivery confirmed for {device_id} partition={kafka_partition} offset={kafka_offset}",
        );
        event.device_id = Some(device_id.to_string());
        event.kafka_partition = Some(partition);
        event.kafka_offset = Some(offset);
        let _ = self.sender.try_send(event);
    }

    /// Log an error
    pub fn error(&self, template: &str) {
        let event = ClefEvent::new("Error", template);
        let _ = self.sender.try_send(event);
    }
}
