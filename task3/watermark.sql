CREATE TABLE etl_watermark
(
  pipeline   LowCardinality(String),
  max_time   DateTime,
  updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pipeline);