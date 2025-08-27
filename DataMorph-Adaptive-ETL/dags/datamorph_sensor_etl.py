from datetime import datetime, timedelta
import os, json, time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from kafka import KafkaConsumer
import pandas as pd

from app.validation import validate_record
from app.transform import enrich, to_dataframe
from app.io_utils import ensure_table, upsert_records, alert

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensors")

default_args = {
    "owner": "Yukta Vajpayee",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="datamorph_sensor_etl",
    description="Adaptive ETL for sensor streams with schema-drift tolerance",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    def consume_batch(**context):
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="datamorph-etl",
        )
        batch = []
        t_end = time.time() + 5
        for msg in consumer:
            batch.append(msg.value)
            if time.time() > t_end or len(batch) >= 500:
                break
        consumer.close()
        if not batch:
            # nothing to do this run
            raise AirflowSkipException("No messages consumed this interval.")
        return batch

    def validate_transform(**context):
        raw_batch = context["ti"].xcom_pull(task_ids="consume")
        cleaned = []
        warnings = 0
        for rec in raw_batch:
            core, extra, warn = validate_record(rec)
            if core.get("ts") is None or not core.get("device_id"):
                warnings += 1
                continue
            core = enrich(core)
            cleaned.append({
                "ts": core["ts"],
                "device_id": core["device_id"],
                "temperature": core.get("temperature"),
                "humidity": core.get("humidity"),
                "data": {**extra, "derived": {k:v for k,v in core.items() if k not in {"ts","device_id","temperature","humidity"}}},
            })
            warnings += len(warn)
        if warnings:
            alert(f"[DataMorph] {warnings} warnings in batch of {len(raw_batch)}")
        if not cleaned:
            raise AirflowSkipException("All records filtered out.")
        # Convert timestamps to ISO strings for JSON-safe XCom
        for r in cleaned:
            if hasattr(r["ts"], "isoformat"):
                r["ts"] = r["ts"].isoformat()
        return cleaned

    def load(**context):
        ensure_table()
        rows = context["ti"].xcom_pull(task_ids="validate_transform")
        # Convert ts back to datetime accepted by psycopg2
        for r in rows:
            r["ts"] = pd.to_datetime(r["ts"])
        upsert_records(rows)

    consume = PythonOperator(task_id="consume", python_callable=consume_batch, provide_context=True)
    validate_t = PythonOperator(task_id="validate_transform", python_callable=validate_transform, provide_context=True)
    load_t = PythonOperator(task_id="load", python_callable=load, provide_context=True)

    consume >> validate_t >> load_t
