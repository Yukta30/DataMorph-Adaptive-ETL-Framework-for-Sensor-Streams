import json
import os
import psycopg2
from psycopg2.extras import execute_batch, Json
from typing import List, Dict, Any
import requests

def get_pg_conn():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "sensors")
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)

def ensure_table():
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            data JSONB NOT NULL,
            UNIQUE (ts, device_id)
        );
        """)
        conn.commit()

def upsert_records(records: List[Dict[str, Any]]):
    sql = """
    INSERT INTO sensor_readings (ts, device_id, temperature, humidity, data)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (ts, device_id) DO UPDATE SET
      temperature = EXCLUDED.temperature,
      humidity = EXCLUDED.humidity,
      data = EXCLUDED.data
    """
    vals = [
        (r["ts"], r["device_id"], r.get("temperature"), r.get("humidity"), Json(r.get("data", {})))
        for r in records
    ]
    with get_pg_conn() as conn, conn.cursor() as cur:
        execute_batch(cur, sql, vals, page_size=200)
        conn.commit()

def alert(msg: str):
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        print(f"[ALERT] {msg}")
        return
    try:
        requests.post(url, json={"text": msg}, timeout=5)
    except Exception as e:
        print(f"[ALERT-FAIL] {e}: {msg}")
