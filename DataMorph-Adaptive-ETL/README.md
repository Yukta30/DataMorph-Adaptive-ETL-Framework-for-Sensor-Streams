# DataMorph – Adaptive ETL Framework for Sensor Streams

**Tech**: Python, Apache Airflow, PostgreSQL, Kafka, Pandas, Docker

A compact, production-ish demo I (Yukta Vajpayee) can drop straight into GitHub to showcase a streaming ETL: Kafka → Airflow (batching consumer) → Pandas validation/transform → PostgreSQL.  
It’s personal (hello, future reviewers 👋) but pragmatic—schema drift is handled via dynamic validators and JSONB fallback, with retries and optional Slack alerts.

## Why this exists
- **Adaptive schema**: sensor payloads change; we validate available fields and gracefully store extras in `data` (JSONB).
- **Fault tolerance**: Airflow task retries, idempotent upserts, and optional Slack alerts on failure.
- **Portable**: single `docker compose up` launches Kafka, Postgres, and Airflow (standalone) with a demo producer.

## Quickstart
```bash
# 1) Clone & run
docker compose up -d --build

# 2) Open Airflow in your browser
#    http://localhost:8080  (username: airflow, password: airflow)

# 3) Start sending demo sensor data
docker compose exec airflow bash -lc "python /opt/airflow/producer/sensor_producer.py"

# 4) Trigger the DAG
#    In the UI, trigger DAG: `datamorph_sensor_etl`
#    Or via CLI:
docker compose exec airflow bash -lc "airflow dags trigger datamorph_sensor_etl"
```

## Personal touch
I built this to mirror the kind of streaming ETL I worked on/like working on—small, resilient, and easy to read. Feel free to fork, tweak, and ping me with ideas.

## Services
- **Kafka** (with Zookeeper) for ingestion of synthetic sensor events (topic: `sensors`).
- **Airflow** (standalone) runs a timed DAG that consumes, validates, transforms, and loads to Postgres.
- **PostgreSQL** stores both curated columns and a `JSONB` dump of any extra/unknown fields.

## Dynamic validation: how it adapts
- Known fields are validated/coerced (e.g., `temperature`, `humidity`, `device_id`, `ts`).
- Unknown fields are kept in `data` (JSONB) so nothing is lost when devices change firmware or emit new attributes.
- You can tweak validation rules in `app/validation.py` and add new projections in `app/transform.py`.

## Optional alerts (Slack)
Set an env var in `docker-compose.yml`:
```yaml
SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```
Failures or schema anomalies will send a compact message. If unset, it logs instead.

## Database
Default connection:
- Host: `localhost`
- Port: `5433`
- User: `airflow`
- Password: `airflow`
- DB: `sensors`

Table created automatically by the DAG:
```sql
CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    data JSONB NOT NULL,
    UNIQUE (ts, device_id)
);
```

## Project tree
```
DataMorph-Adaptive-ETL/
├─ docker-compose.yml
├─ airflow/
│  ├─ Dockerfile
│  └─ requirements.txt
├─ dags/
│  └─ datamorph_sensor_etl.py
├─ app/
│  ├─ validation.py
│  ├─ transform.py
│  └─ io_utils.py
├─ producer/
│  └─ sensor_producer.py
└─ README.md
```

---

**Author**: Yukta Vajpayee  
**License**: MIT
