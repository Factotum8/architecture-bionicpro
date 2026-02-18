CREATE TABLE reports_user_daily
(
  day           Date,
  user_id       String,
  prosthesis_id String,

  steps_sum     UInt64,
  active_minutes UInt32,
  avg_load      Float64,
  max_load      Float64,

  errors_count  UInt32,
  battery_min   Float64,
  battery_avg   Float64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (user_id, prosthesis_id, day);