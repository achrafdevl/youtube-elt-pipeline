# dags/update_db_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import json
import glob
import psycopg2
from psycopg2.extras import execute_values

# ==========
# Config
# ==========
DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
DB_USER = os.getenv("PGUSER", "airflow")
DB_PASS = os.getenv("PGPASSWORD", "airflow")

RAW_DIR = os.getenv("JSON_OUTPUT_PATH", "./data/raw")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "update_db",
    default_args=default_args,
    description="Charge les JSON dans staging.video_raw et met à jour core.videos",
    schedule_interval="30 2 * * *",   # tous les jours à 2h30 (après produce_JSON)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "postgres", "etl"],
)

# ==========
# Helpers
# ==========
def _connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def load_to_staging(**_):
    """Insère les dernières données JSON dans staging.video_raw"""
    conn = _connect()
    cur = conn.cursor()

    # Assurez-vous que la table existe
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE TABLE IF NOT EXISTS staging.video_raw (
        id SERIAL PRIMARY KEY,
        video_id TEXT,
        title TEXT,
        description TEXT,
        published_at TIMESTAMP,
        channel_id TEXT,
        raw_json JSONB,
        load_ts TIMESTAMP DEFAULT NOW()
    );
    """)

    files = sorted(glob.glob(f"{RAW_DIR}/*.json"))
    if not files:
        raise ValueError("Aucun fichier JSON trouvé dans le dossier raw")

    # On ne prend que le plus récent
    latest = files[-1]
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for item in data.get("items", []):
        snippet = item["snippet"]
        rows.append((
            item["id"]["videoId"],
            snippet.get("title"),
            snippet.get("description"),
            snippet.get("publishedAt"),
            snippet.get("channelId"),
            json.dumps(item)
        ))

    execute_values(cur,
        """INSERT INTO staging.video_raw
           (video_id, title, description, published_at, channel_id, raw_json)
           VALUES %s
           ON CONFLICT DO NOTHING
        """,
        rows
    )

    conn.commit()
    cur.close()
    conn.close()
    return f"{len(rows)} lignes insérées depuis {latest}"

def transform_core(**_):
    """Transforme et alimente core.videos"""
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS core;
    CREATE TABLE IF NOT EXISTS core.videos (
        video_id TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        published_date DATE,
        channel_id TEXT,
        last_update TIMESTAMP DEFAULT NOW()
    );
    """)

    # Upsert depuis staging
    cur.execute("""
    INSERT INTO core.videos (video_id, title, description, published_date, channel_id)
    SELECT
        video_id,
        title,
        description,
        published_at::date,
        channel_id
    FROM staging.video_raw
    ON CONFLICT (video_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        published_date = EXCLUDED.published_date,
        channel_id = EXCLUDED.channel_id,
        last_update = NOW();
    """)

    conn.commit()
    cur.close()
    conn.close()
    return "Core.videos mis à jour"

# ==========
# Tasks
# ==========
staging_task = PythonOperator(
    task_id="load_staging",
    python_callable=load_to_staging,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_core",
    python_callable=transform_core,
    dag=dag,
)

staging_task >> transform_task
