# dags/data_quality_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
import psycopg2
import sys


# =======================
# Config / Variables
# =======================
SODA_PROJECT_DIR = os.getenv("SODA_PROJECT_DIR", "/usr/local/airflow/soda")
ENABLE_SODA = os.getenv("ENABLE_SODA", "1") == "1"
DATA_SOURCE_NAME = "youtube_dw"  # doit correspondre à votre warehouse.yml

# Définir le chemin vers l'exécutable soda
SODA_EXECUTABLE = os.path.join(SODA_PROJECT_DIR, "soda")  # Linux/macOS
# Pour Windows, utilisez:
# SODA_EXECUTABLE = os.path.join(SODA_PROJECT_DIR, "Scripts", "soda.exe")

# =======================
# DAG Arguments
# =======================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Validation de la qualité des données avec Soda Core",
    schedule="45 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "quality", "soda"],
)

# =======================
# Task Functions
# =======================
def run_soda_scan(**context):
    if not ENABLE_SODA:
        return "Soda Core désactivé via ENABLE_SODA=0"

    try:
        cmd = [
            sys.executable,
            "-m",
            "soda",
            "scan",
            "-d", DATA_SOURCE_NAME,
            "-c", os.path.join(SODA_PROJECT_DIR, "warehouse.yml"),
            os.path.join(SODA_PROJECT_DIR, "checks.yml"),
        ]


        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=SODA_PROJECT_DIR
        )

        print(f"Soda scan output:\n{result.stdout}")
        return f"Soda scan completed successfully:\n{result.stdout}"

    except subprocess.CalledProcessError as e:
        error_msg = f"Soda scan failed:\n{e.stderr or e.stdout}"
        print(error_msg)
        raise Exception(error_msg)

def validate_data_completeness(**context):
    """
    Validation manuelle de la complétude des données
    """
    DB_HOST = os.getenv("PGHOST", "postgres")
    DB_PORT = os.getenv("PGPORT", "5432")
    DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
    DB_USER = os.getenv("PGUSER", "airflow")
    DB_PASS = os.getenv("PGPASSWORD", "airflow")
    
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    
    checks = []
    
    # 1. Vérifier que les tables existent
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema IN ('staging', 'core') 
        AND table_name IN ('video_raw', 'videos')
    """)
    table_count = cur.fetchone()[0]
    checks.append(f"Tables existantes: {table_count}/2")
    
    # 2. Vérifier le nombre de vidéos dans staging
    cur.execute("SELECT COUNT(*) FROM staging.video_raw")
    staging_count = cur.fetchone()[0]
    checks.append(f"Vidéos en staging: {staging_count}")
    
    # 3. Vérifier le nombre de vidéos dans core
    cur.execute("SELECT COUNT(*) FROM core.videos")
    core_count = cur.fetchone()[0]
    checks.append(f"Vidéos en core: {core_count}")
    
    # 4. Vérifier les données manquantes
    cur.execute("SELECT COUNT(*) FROM staging.video_raw WHERE video_id IS NULL")
    null_ids = cur.fetchone()[0]
    checks.append(f"IDs vidéo manquants: {null_ids}")
    
    # 5. Vérifier les durées invalides
    cur.execute("SELECT COUNT(*) FROM staging.video_raw WHERE duration_seconds < 0")
    invalid_durations = cur.fetchone()[0]
    checks.append(f"Durées invalides: {invalid_durations}")
    
    cur.close()
    conn.close()
    
    result = "\n".join(checks)
    print(f"Validation de qualité:\n{result}")
    return result

# =======================
# Tasks
# =======================
manual_validation_task = PythonOperator(
    task_id="validate_data_completeness",
    python_callable=validate_data_completeness,
    dag=dag,
)

if ENABLE_SODA:
    soda_task = PythonOperator(
        task_id="run_soda_scan",
        python_callable=run_soda_scan,
        dag=dag,
    )
    manual_validation_task >> soda_task
