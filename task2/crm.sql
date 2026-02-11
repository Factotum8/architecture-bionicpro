CREATE TABLE crm_clients
(
  user_id       String,
  crm_client_id String,
  full_name     String,
  email         String,
  prosthesis_id String,
  model         String,
  region        LowCardinality(String),
  updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, prosthesis_id);