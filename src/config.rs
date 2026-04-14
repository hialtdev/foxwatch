// src/config.rs
pub struct Config {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub client_id: String,
    pub kafka_bootstrap: String,
    pub kafka_topic: String,
    pub seq_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            mqtt_host: std::env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string()),
            mqtt_port: std::env::var("MQTT_PORT")
                .unwrap_or_else(|_| "1883".to_string())
                .parse()
                .expect("MQTT_PORT must be a valid port number"),
            mqtt_topic: std::env::var("MQTT_TOPIC")
                .unwrap_or_else(|_| "homeassistant/#".to_string()),
            client_id: std::env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "foxwatch".to_string()),
            kafka_bootstrap: std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| {
                "bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092".to_string()
            }),
            kafka_topic: std::env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "foxwatch-telemetry".to_string()),
            // Internal k8s DNS — port 5341 is Seq's CLEF ingestion port
            seq_url: std::env::var("SEQ_URL").unwrap_or_else(|_| {
                "http://seq-service.default.svc.cluster.local:5341".to_string()
            }),
        }
    }
}
