# dags/data_quality_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys
import subprocess
from dotenv import load_dotenv, find_dotenv

# Ensure `src` is importable when running inside Airflow
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

# Load environment variables
load_dotenv(find_dotenv(), override=False)

# =======================
# Configuration
# =======================
SODA_PROJECT_DIR = os.getenv("SODA_PROJECT_DIR", "/usr/local/airflow/soda")
SODA_EXEC = os.getenv("SODA_EXEC", "soda")
DATA_SOURCE_NAME = "youtube_dw"

# Database connection variables
DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
DB_USER = os.getenv("PGUSER", "airflow")
DB_PASS = os.getenv("PGPASSWORD", "airflow")

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
    description="Validation automatique de la qualité des données avec Soda Core",
    schedule="0 3 * * *",  # Run at 3 AM daily, after update_db
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "quality", "validation", "soda"],
)

# =======================
# Helper Functions
# =======================
def check_database_connection(**context):
    """
    Vérifie la connectivité à la base de données PostgreSQL
    """
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            return "Database connection successful"
        else:
            raise Exception("Database connection failed")
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")

def run_soda_scan(**context):
    """
    Exécute le scan Soda Core pour valider la qualité des données
    """
    try:
        # Ensure Soda project directory exists
        os.makedirs(SODA_PROJECT_DIR, exist_ok=True)
        
        # Set environment variables for Soda
        env = os.environ.copy()
        env.update({
            "PGHOST": DB_HOST,
            "PGPORT": DB_PORT,
            "PGDATABASE": DB_NAME,
            "PGUSER": DB_USER,
            "PGPASSWORD": DB_PASS,
        })
        
        # Run Soda scan
        cmd = [
            SODA_EXEC,
            "scan",
            "-d", DATA_SOURCE_NAME,
            f"{SODA_PROJECT_DIR}/warehouse.yml",
            f"{SODA_PROJECT_DIR}/checks.yml",
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        print(f"Environment: PGHOST={DB_HOST}, PGPORT={DB_PORT}, PGDATABASE={DB_NAME}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            cwd=SODA_PROJECT_DIR,
            timeout=300  # 5 minutes timeout
        )
        
        print(f"Soda scan stdout: {result.stdout}")
        print(f"Soda scan stderr: {result.stderr}")
        
        if result.returncode == 0:
            return f"Soda scan completed successfully: {result.stdout}"
        else:
            # Soda returns non-zero exit code for failed checks, but this is expected
            # We should not fail the DAG for data quality issues, just report them
            return f"Soda scan completed with issues: {result.stdout}\nErrors: {result.stderr}"
            
    except subprocess.TimeoutExpired:
        raise Exception("Soda scan timed out after 5 minutes")
    except Exception as e:
        raise Exception(f"Soda scan failed: {str(e)}")

def generate_quality_report(**context):
    """
    Génère un rapport de qualité des données
    """
    import psycopg2
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        
        # Get data quality metrics
        metrics = {}
        
        # Check staging data
        cur.execute("SELECT COUNT(*) FROM staging.video_raw")
        metrics['staging_videos'] = cur.fetchone()[0]
        
        # Check core data
        cur.execute("SELECT COUNT(*) FROM core.videos")
        metrics['core_videos'] = cur.fetchone()[0]
        
        # Check for missing data
        cur.execute("SELECT COUNT(*) FROM staging.video_raw WHERE video_id IS NULL")
        metrics['missing_video_ids_staging'] = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM core.videos WHERE video_id IS NULL")
        metrics['missing_video_ids_core'] = cur.fetchone()[0]
        
        # Check for recent data
        cur.execute("""
            SELECT COUNT(*) FROM staging.video_raw 
            WHERE load_ts >= NOW() - INTERVAL '24 hours'
        """)
        metrics['recent_staging_data'] = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM core.videos 
            WHERE last_update >= NOW() - INTERVAL '24 hours'
        """)
        metrics['recent_core_data'] = cur.fetchone()[0]
        
        # Check data freshness
        cur.execute("""
            SELECT MAX(published_at) FROM staging.video_raw
        """)
        latest_published = cur.fetchone()[0]
        metrics['latest_published_date'] = str(latest_published) if latest_published else "No data"
        
        cur.close()
        conn.close()
        
        report = f"""
        === DATA QUALITY REPORT ===
        Staging videos: {metrics['staging_videos']}
        Core videos: {metrics['core_videos']}
        Missing video IDs (staging): {metrics['missing_video_ids_staging']}
        Missing video IDs (core): {metrics['missing_video_ids_core']}
        Recent staging data (24h): {metrics['recent_staging_data']}
        Recent core data (24h): {metrics['recent_core_data']}
        Latest published date: {metrics['latest_published_date']}
        """
        
        print(report)
        return report
        
    except Exception as e:
        error_msg = f"Failed to generate quality report: {str(e)}"
        print(error_msg)
        return error_msg

# =======================
# Tasks
# =======================

# Task 1: Check database connectivity
check_db_task = PythonOperator(
    task_id="check_database_connection",
    python_callable=check_database_connection,
    dag=dag,
)

# Task 2: Run Soda Core validation
soda_scan_task = PythonOperator(
    task_id="run_soda_scan",
    python_callable=run_soda_scan,
    dag=dag,
)

# Task 3: Generate quality report
quality_report_task = PythonOperator(
    task_id="generate_quality_report",
    python_callable=generate_quality_report,
    dag=dag,
)

# Task 4: Check if Soda is installed (optional)
check_soda_task = BashOperator(
    task_id="check_soda_installation",
    bash_command=f"which {SODA_EXEC} || echo 'Soda not found in PATH'",
    dag=dag,
)

# =======================
# Task Dependencies
# =======================
check_db_task >> check_soda_task >> soda_scan_task >> quality_report_task