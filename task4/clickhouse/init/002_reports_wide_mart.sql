INSERT INTO reports_user_daily_v2
SELECT
  r.day,
  r.user_id,
  r.prosthesis_id,
  r.steps_sum,
  r.active_minutes,
  r.avg_load,
  r.max_load,
  r.errors_count,
  r.battery_min,
  r.battery_avg,
  c.full_name,
  c.email,
  c.region,
  c.model
FROM reports_user_daily AS r
ANY LEFT JOIN
(
  SELECT
    user_id,
    prosthesis_id,
    argMax(full_name, ts_ms) AS full_name,
    argMax(email, ts_ms) AS email,
    argMax(region, ts_ms) AS region,
    argMax(model, ts_ms) AS model
  FROM crm_clients_dim
  WHERE is_deleted = 0
  GROUP BY user_id, prosthesis_id
) AS c
USING (user_id, prosthesis_id);