# dags/produce_json_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from src.youtube_client import YouTubeClient

# =======================
#! Variables / Config
# =======================
CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
API_KEY = os.getenv("YOUTUBE_API_KEY")
OUTPUT_DIR = os.getenv("JSON_OUTPUT_PATH", "./data/raw")
MAX_VIDEOS = int(os.getenv("MAX_VIDEOS", "500"))

# =======================
#! DAG Arguments
# =======================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'produce_JSON',
    default_args=default_args,
    description='Extraction automatique des vidéos YouTube en JSON',
    schedule='0 2 * * *',  # tous les jours à 2h du matin
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['youtube', 'extract', 'json']
)

# =======================
#! Task Function
# =======================
def fetch_and_save(**kwargs):
    if not CHANNEL_ID or not API_KEY:
        raise ValueError("TARGET_CHANNEL_ID et YOUTUBE_API_KEY doivent être définis dans les variables Airflow ou .env")
    client = YouTubeClient(api_key=API_KEY)
    saved_file = client.fetch_and_save_channel(channel_id=CHANNEL_ID, out_dir=OUTPUT_DIR, max_videos=MAX_VIDEOS)
    return saved_file

# =======================
#! PythonOperator
# =======================
fetch_task = PythonOperator(
    task_id='fetch_youtube_videos',
    python_callable=fetch_and_save,
    dag=dag,
)

fetch_task
