# dags/update_db_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import os
import psycopg2
from airflow.utils import timezone

from src.db_tasks import load_to_staging, transform_core

# Pour charger les variables d'environnement depuis .env
from dotenv import load_dotenv

# ========== Config ==========

# Chemins
RAW_DIR = os.getenv("JSON_OUTPUT_PATH", "./data/raw")
SODA_PROJECT_DIR = os.getenv("SODA_PROJECT_DIR", "/usr/local/airflow/soda")
SODA_EXEC = os.getenv("SODA_EXEC", "soda")  # Si soda est dans le PATH, sinon mettre le chemin complet

# Variables DB
DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
DB_USER = os.getenv("PGUSER", "airflow")
DB_PASS = os.getenv("PGPASSWORD", "airflow")

# Flags
DISABLE_DB = os.getenv("DISABLE_DB", "0") == "1"
ENABLE_SODA = os.getenv("ENABLE_SODA", "1") == "1"

# Charger le .env du projet Soda
load_dotenv(os.path.join(SODA_PROJECT_DIR, ".env"))

# ========== DAG args ==========
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "update_db",
    default_args=default_args,
    description="Charge les JSON dans staging.video_raw et met à jour core.videos",
    schedule="30 2 * * *",
    start_date=timezone.utcnow() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "postgres", "etl"],
)

# ========== Helpers ==========
def _connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# ========== Tasks ==========
if DISABLE_DB:
    def _noop_load(**_):
        return "DB disabled via DISABLE_DB=1; skipping load_to_staging"

    def _noop_transform(**_):
        return "DB disabled via DISABLE_DB=1; skipping transform_core"

    staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=_noop_load,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_core",
        python_callable=_noop_transform,
        dag=dag,
    )
else:
    staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=lambda **_: load_to_staging(),
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id="transform_core",
        python_callable=lambda **_: transform_core(),
        dag=dag,
    )

# ========== Soda Task ==========
if ENABLE_SODA:
    def _run_soda_scan(**_):
        """
        Run Soda scan on the specified data source.
        Make sure the data source name matches what is in warehouse.yml
        """
        import subprocess

        DATA_SOURCE_NAME = "youtube_dw"

        cmd = [
            SODA_EXEC,
            "scan",
            "-d", DATA_SOURCE_NAME,
            f"{SODA_PROJECT_DIR}/warehouse.yml",
            f"{SODA_PROJECT_DIR}/checks.yml",
        ]

        try:
            subprocess.run(cmd, check=True)
            return "Soda scan completed"
        except subprocess.CalledProcessError as e:
            # Pour ne pas casser le DAG si Soda échoue
            print(f"Soda scan failed: {e}")
            return "Soda scan failed"

    soda_task = PythonOperator(
        task_id="soda_scan",
        python_callable=_run_soda_scan,
        dag=dag,
    )

    staging_task >> transform_task >> soda_task
else:
    staging_task >> transform_task
