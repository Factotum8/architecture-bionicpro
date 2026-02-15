-- clickhouse/init/001_crm_cdc.sql

-- 1) KafkaEngine table (читает из топика Debezium "crm.public.clients")
CREATE TABLE IF NOT EXISTS crm_clients_kafka
(
  id String,
  user_id String,
  email String,
  full_name String,
  prosthesis_id String,
  model String,
  region String,
  updated_at DateTime,
  op LowCardinality(String),
  ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'crm.public.clients',
  kafka_group_name = 'ch_crm_clients_consumer',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- 2) Dimension table в CH
CREATE TABLE IF NOT EXISTS crm_clients_dim
(
  id String,
  user_id String,
  email String,
  full_name String,
  prosthesis_id String,
  model String,
  region String,
  updated_at DateTime,
  is_deleted UInt8,
  ts_ms UInt64
)
ENGINE = ReplacingMergeTree(ts_ms)
ORDER BY (user_id, prosthesis_id, id);

-- 3) MV: Kafka -> dim
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_crm_clients_dim
TO crm_clients_dim
AS
SELECT
  id,
  user_id,
  email,
  full_name,
  prosthesis_id,
  model,
  region,
  updated_at,
  if(op = 'd', 1, 0) AS is_deleted,
  ts_ms
FROM crm_clients_kafka;