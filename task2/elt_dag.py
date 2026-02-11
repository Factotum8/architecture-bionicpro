from __future__ import annotations

from datetime import timedelta, datetime, timezone, date
from typing import Iterable, Any

import requests
import psycopg2
import psycopg2.extras
import clickhouse_connect

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable


DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "reports_etl"


def _utc(dt: datetime) -> datetime:
    """Ensure timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_clickhouse_client():
    host = Variable.get("CH_HOST")
    port = int(Variable.get("CH_PORT", default_var="8123"))
    user = Variable.get("CH_USER", default_var="default")
    password = Variable.get("CH_PASSWORD", default_var="")
    database = Variable.get("CH_DATABASE", default_var="default")

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )


def _chunked(items: list[dict[str, Any]], chunk_size: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _daterange_days(start_dt: datetime, end_dt: datetime) -> list[date]:
    """All days touched by [start_dt, end_dt)."""
    s = start_dt.date()
    e = (end_dt - timedelta(seconds=1)).date()
    days = []
    cur = s
    while cur <= e:
        days.append(cur)
        cur = cur + timedelta(days=1)
    return days


with DAG(
    dag_id=DAG_ID,
    start_date=days_ago(1),
    schedule="0 * * * *",  # каждый час
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reports", "etl"],
) as dag:

    @task
    def extract_crm(data_interval_start=None, data_interval_end=None) -> list[dict[str, Any]]:
        """
        Тянем клиентов из CRM за окно (updated_since/updated_until).
        Ожидаем, что CRM отдаёт JSON массив объектов:
        {
          "user_id": "...",
          "crm_client_id": "...",
          "full_name": "...",
          "email": "...",
          "prosthesis_id": "...",
          "model": "...",
          "region": "...",
          "updated_at": "2026-02-11T10:15:00Z"
        }
        """
        base_url = Variable.get("CRM_BASE_URL")
        token = Variable.get("CRM_TOKEN", default_var="")
        timeout = int(Variable.get("CRM_TIMEOUT_SECONDS", default_var="30"))

        start = _utc(data_interval_start)
        end = _utc(data_interval_end)

        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # пример URL: GET /clients?updated_since=...&updated_until=...
        url = f"{base_url.rstrip('/')}/clients"
        params = {
            "updated_since": start.isoformat().replace("+00:00", "Z"),
            "updated_until": end.isoformat().replace("+00:00", "Z"),
        }

        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list):
            raise ValueError(f"CRM returned non-list payload: {type(data)}")
        return data

    @task
    def load_crm_to_clickhouse(clients: list[dict[str, Any]]) -> int:
        """
        Грузим в ClickHouse таблицу crm_clients (ReplacingMergeTree(updated_at)).
        """
        if not clients:
            return 0

        ch = _get_clickhouse_client()

        # Нормализуем поля и типы под схему
        rows = []
        for c in clients:
            rows.append({
                "user_id": str(c.get("user_id", "")),
                "crm_client_id": str(c.get("crm_client_id", "")),
                "full_name": str(c.get("full_name", "")),
                "email": str(c.get("email", "")),
                "prosthesis_id": str(c.get("prosthesis_id", "")),
                "model": str(c.get("model", "")),
                "region": str(c.get("region", "")),
                "updated_at": str(c.get("updated_at", "")),  # ClickHouse понимает DateTime из ISO8601
            })

        batch_size = int(Variable.get("CH_BATCH_SIZE", default_var="20000"))
        inserted = 0

        # clickhouse-connect: insert_dicts(table, data, column_names=...)
        cols = ["user_id", "crm_client_id", "full_name", "email", "prosthesis_id", "model", "region", "updated_at"]
        for chunk in _chunked(rows, batch_size):
            ch.insert_dicts("crm_clients", chunk, column_names=cols)
            inserted += len(chunk)

        return inserted

    @task
    def extract_telemetry(data_interval_start=None, data_interval_end=None) -> list[dict[str, Any]]:
        """
        Тянем телеметрию из Postgres за окно.
        Ожидаем таблицу (пример):
          telemetry_events(
            event_time timestamptz,
            user_id text,
            prosthesis_id text,
            sensor_type text,
            metric text,
            value double precision
          )
        """
        dsn = Variable.get("PG_DSN")
        fetch_size = int(Variable.get("PG_FETCH_SIZE", default_var="50000"))

        start = _utc(data_interval_start)
        end = _utc(data_interval_end)

        sql = """
        SELECT
            event_time,
            user_id,
            prosthesis_id,
            sensor_type,
            metric,
            value
        FROM telemetry_events
        WHERE event_time >= %s AND event_time < %s
        ORDER BY event_time
        """

        events: list[dict[str, Any]] = []

        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor(name="telemetry_cursor", cursor_factory=psycopg2.extras.dictCursor) as cur:
                cur.itersize = fetch_size
                cur.execute(sql, (start, end))
                for row in cur:
                    events.append({
                        "event_time": row["event_time"].astimezone(timezone.utc).replace(tzinfo=None),  # CH DateTime
                        "user_id": row["user_id"],
                        "prosthesis_id": row["prosthesis_id"],
                        "sensor_type": row["sensor_type"],
                        "metric": row["metric"],
                        "value": float(row["value"]) if row["value"] is not None else None,
                    })
        finally:
            conn.close()

        return events

    @task
    def load_telemetry_to_clickhouse(events: list[dict[str, Any]]) -> int:
        """
        Грузим в ClickHouse таблицу telemetry_raw.
        """
        if not events:
            return 0

        ch = _get_clickhouse_client()
        batch_size = int(Variable.get("CH_BATCH_SIZE", default_var="20000"))

        cols = ["event_time", "user_id", "prosthesis_id", "sensor_type", "metric", "value"]
        inserted = 0
        for chunk in _chunked(events, batch_size):
            ch.insert_dicts("telemetry_raw", chunk, column_names=cols)
            inserted += len(chunk)

        return inserted

    @task
    def build_mart_daily(data_interval_start=None, data_interval_end=None) -> str:
        """
        Строим витрину reports_user_daily за затронутые дни.
        1) Удаляем старые агрегаты за дни окна (идемпотентность)
        2) Вставляем новые агрегаты из telemetry_raw
        """
        ch = _get_clickhouse_client()
        start = _utc(data_interval_start)
        end = _utc(data_interval_end)

        days = _daterange_days(start, end)
        if not days:
            return "no_days"

        day_list = ",".join([f"toDate('{d.isoformat()}')" for d in days])
        delete_sql = f"""
        ALTER TABLE reports_user_daily
        DELETE WHERE day IN ({day_list})
        """
        ch.command(delete_sql)

        insert_sql = f"""
        INSERT INTO reports_user_daily
        SELECT
            toDate(event_time) AS day,
            user_id,
            prosthesis_id,

            sumIf(toUInt64OrZero(value), metric = 'steps') AS steps_sum,
            toUInt32(countIf(metric = 'active' AND value > 0)) AS active_minutes,

            avgIf(value, metric = 'load') AS avg_load,
            maxIf(value, metric = 'load') AS max_load,

            toUInt32(countIf(metric = 'error' AND value > 0)) AS errors_count,

            minIf(value, metric = 'battery') AS battery_min,
            avgIf(value, metric = 'battery') AS battery_avg
        FROM telemetry_raw
        WHERE event_time >= toDateTime('{start.replace(tzinfo=None).isoformat(sep=" ")}')
          AND event_time <  toDateTime('{end.replace(tzinfo=None).isoformat(sep=" ")}')
        GROUP BY
            day, user_id, prosthesis_id
        """
        ch.command(insert_sql)

        return f"mart_built_days={len(days)}"

    @task
    def update_watermark(data_interval_end=None) -> str:
        """
        Пишем max_time = конец обработанного окна.
        """
        ch = _get_clickhouse_client()
        end = _utc(data_interval_end).replace(tzinfo=None)

        sql = """
        INSERT INTO etl_watermark (pipeline, max_time)
        VALUES (%(pipeline)s, %(max_time)s)
        """
        ch.command(sql, parameters={"pipeline": DAG_ID, "max_time": end})
        return end.isoformat()

    crm_clients = extract_crm()
    crm_loaded = load_crm_to_clickhouse(crm_clients)

    telemetry_events = extract_telemetry()
    telemetry_loaded = load_telemetry_to_clickhouse(telemetry_events)

    mart_status = build_mart_daily()
    wm = update_watermark()

    crm_loaded >> telemetry_loaded >> mart_status >> wm