-- Production dropout detector
-- Counts Unavailable transitions per device in 5-minute PROCTIME windows
-- Sink: foxwatch-analytics Kafka topic
-- Consumer group: flink-foxwatch-production

INSERT INTO dropout_summary
SELECT
  device_id,
  COUNT(*) AS dropout_count,
  TUMBLE_START(PROCTIME(), INTERVAL '5' MINUTE) AS window_start
FROM foxwatch_telemetry
WHERE state = 'Unavailable'
GROUP BY device_id, TUMBLE(PROCTIME(), INTERVAL '5' MINUTE);
