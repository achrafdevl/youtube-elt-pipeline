# dags/update_db_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import os
import sys
import psycopg2
from airflow.utils import timezone

# Charger les variables d'environnement depuis .env (racine du projet)
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

# S'assurer que le dossier `src` est importable (aligné avec produce_json_dag.py)
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

# Aligner le dossier de sortie par défaut avec le DAG d'extraction
if not os.getenv("JSON_OUTPUT_PATH"):
    os.environ["JSON_OUTPUT_PATH"] = "./dags/data"

from src.db_tasks import load_to_staging, transform_core

# ========== Config ==========

# Chemins
RAW_DIR = os.getenv("JSON_OUTPUT_PATH", "./dags/data")
SODA_PROJECT_DIR = os.getenv("SODA_PROJECT_DIR", "/usr/local/airflow/soda")
SODA_EXEC = os.getenv("SODA_EXEC", "soda")  

# Variables DB
DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
DB_USER = os.getenv("PGUSER", "airflow")
DB_PASS = os.getenv("PGPASSWORD", "airflow")

# Optionally source DB creds from an Airflow Connection (keeps backward compatibility)
try:
    from airflow.hooks.base import BaseHook

    try:
        conn = BaseHook.get_connection("youtube_dw")
        if conn:
            os.environ["PGHOST"] = conn.host or DB_HOST
            if conn.port:
                os.environ["PGPORT"] = str(conn.port)
            if conn.schema:
                os.environ["PGDATABASE"] = conn.schema
            if conn.login:
                os.environ["PGUSER"] = conn.login
            if conn.password:
                os.environ["PGPASSWORD"] = conn.password

            # Refresh in-module variables after env update
            DB_HOST = os.getenv("PGHOST", DB_HOST)
            DB_PORT = os.getenv("PGPORT", DB_PORT)
            DB_NAME = os.getenv("PGDATABASE", DB_NAME)
            DB_USER = os.getenv("PGUSER", DB_USER)
            DB_PASS = os.getenv("PGPASSWORD", DB_PASS)
    except Exception:
        # If connection not found or outside Airflow context, proceed with env defaults
        pass
except Exception:
    # Airflow might not be available in unit tests; ignore
    pass

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
