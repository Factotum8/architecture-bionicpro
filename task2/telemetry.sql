CREATE TABLE telemetry_raw
(
  event_time   DateTime,
  user_id      String,
  prosthesis_id String,
  sensor_type  LowCardinality(String),
  metric       LowCardinality(String),
  value        Float64,
  ts_ingested  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, prosthesis_id, event_time);