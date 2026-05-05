-- Source table
CREATE TABLE foxwatch_telemetry (
    device_id    STRING,
    state        STRING,
    `timestamp`  STRING,
    event_time   AS TO_TIMESTAMP(SUBSTR(REPLACE(`timestamp`, 'Z', ''), 1, 19), 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic'     = 'foxwatch-telemetry',
    'properties.bootstrap.servers' = 'bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092',
    'properties.group.id' = 'flink-foxwatch-production',
    'scan.startup.mode' = 'latest-offset',
    'format'    = 'json'
);

-- Sink table
CREATE TABLE dropout_summary (
  device_id     STRING,
  dropout_count BIGINT,
  window_start  TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'foxwatch-analytics',
  'properties.bootstrap.servers' = 'bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092',
  'format'                       = 'json'
);
