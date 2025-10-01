# dags/produce_json_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv, find_dotenv

# Ensure `src` is importable when running inside Airflow
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from youtube_requests_extractor import run_flow
import json

# Charger les variables d'environnement depuis un fichier .env si présent
# Ne pas écraser les variables déjà définies par l'environnement/Airflow
load_dotenv(find_dotenv(), override=False)

# =======================
# Variables / Config
# =======================
CHANNEL_HANDLE = os.getenv("TARGET_CHANNEL_HANDLE", "@MrBeast")  # Handle plutôt que ID
API_KEY = os.getenv("YOUTUBE_API_KEY")
OUTPUT_DIR = os.getenv("JSON_OUTPUT_PATH", "./dags/data")

# =======================
# DAG Arguments
# =======================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="produce_JSON",
    default_args=default_args,
    description="Extraction automatique des vidéos YouTube en JSON",
    schedule="0 2 * * *",  # tous les jours à 2h du matin
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "extract", "json"],
)

# =======================
# Task Function
# =======================
def fetch_and_save(**context):
    if not API_KEY:
        raise ValueError("La variable d'environnement YOUTUBE_API_KEY doit être définie.")
    saved_path = run_flow(api_key=API_KEY, channel_handle=CHANNEL_HANDLE, out_dir=OUTPUT_DIR)
    # Charger un petit résumé pour affichage dans l'UI Airflow (éviter XCom volumineux)
    try:
        with open(saved_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        summary = {
            "saved_path": saved_path,
            "channel_handle": data.get("channel_handle"),
            "total_videos": data.get("total_videos"),
            "extraction_date": data.get("extraction_date"),
        }
        return summary
    except Exception:
        # En cas d'échec de lecture, on renvoie au moins le chemin
        return {"saved_path": saved_path}

# =======================
# PythonOperator
# =======================
fetch_task = PythonOperator(
    task_id="fetch_youtube_videos",
    python_callable=fetch_and_save,
    dag=dag,
)