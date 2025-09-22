import os
import json
import glob
import psycopg2
from psycopg2.extras import execute_values

DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "youtube_dw")
DB_USER = os.getenv("PGUSER", "airflow")
DB_PASS = os.getenv("PGPASSWORD", "airflow")

RAW_DIR = os.getenv("JSON_OUTPUT_PATH", "./data/raw")

def _connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def load_to_staging():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE TABLE IF NOT EXISTS staging.video_raw (
        id SERIAL PRIMARY KEY,
        video_id TEXT UNIQUE NOT NULL,
        title TEXT,
        description TEXT,
        published_at TIMESTAMP,
        channel_id TEXT,
        like_count BIGINT CHECK (like_count >= 0),
        view_count BIGINT CHECK (view_count >= 0),
        comment_count BIGINT CHECK (comment_count >= 0),
        duration_iso TEXT,
        duration_seconds INT CHECK (duration_seconds >= 0),
        duration_readable TEXT,
        raw_json JSONB,
        load_ts TIMESTAMP DEFAULT NOW()
    );
    
    -- Index pour améliorer les performances
    CREATE INDEX IF NOT EXISTS idx_staging_video_id ON staging.video_raw(video_id);
    CREATE INDEX IF NOT EXISTS idx_staging_channel_id ON staging.video_raw(channel_id);
    CREATE INDEX IF NOT EXISTS idx_staging_published_at ON staging.video_raw(published_at);
    CREATE INDEX IF NOT EXISTS idx_staging_load_ts ON staging.video_raw(load_ts);
    """)
    files = sorted(glob.glob(f"{RAW_DIR}/*.json"))
    if not files:
        raise ValueError("Aucun fichier JSON trouvé dans le dossier raw")
    latest = files[-1]
    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)
    videos = data.get("videos", [])
    if not videos:
        raise ValueError(f"Aucune vidéo dans le fichier {latest}")
    rows = []
    for v in videos:
        rows.append((
            v.get("video_id"),
            v.get("title"),
            v.get("description"),
            v.get("published_at"),
            v.get("channel_id"),
            int(v.get("like_count", 0)) if v.get("like_count") is not None else None,
            int(v.get("view_count", 0)) if v.get("view_count") is not None else None,
            int(v.get("comment_count", 0)) if v.get("comment_count") is not None else None,
            v.get("duration_iso"),
            int(v.get("duration_seconds", 0)) if v.get("duration_seconds") is not None else None,
            v.get("duration_readable"),
            json.dumps(v)
        ))
    execute_values(cur,
        """INSERT INTO staging.video_raw
           (video_id, title, description, published_at, channel_id,
            like_count, view_count, comment_count, duration_iso, duration_seconds, duration_readable, raw_json)
           VALUES %s
           ON CONFLICT (video_id) DO UPDATE SET
             title = EXCLUDED.title,
             description = EXCLUDED.description,
             published_at = EXCLUDED.published_at,
             channel_id = EXCLUDED.channel_id,
             like_count = EXCLUDED.like_count,
             view_count = EXCLUDED.view_count,
             comment_count = EXCLUDED.comment_count,
             duration_iso = EXCLUDED.duration_iso,
             duration_seconds = EXCLUDED.duration_seconds,
             duration_readable = EXCLUDED.duration_readable,
             raw_json = EXCLUDED.raw_json
        """,
        rows
    )
    conn.commit()
    cur.close()
    conn.close()
    return f"{len(rows)} lignes insérées depuis {latest}"

def transform_core():
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
        like_count BIGINT CHECK (like_count >= 0),
        view_count BIGINT CHECK (view_count >= 0),
        comment_count BIGINT CHECK (comment_count >= 0),
        duration_seconds INT CHECK (duration_seconds >= 0),
        last_update TIMESTAMP DEFAULT NOW()
    );
    
    -- Index pour améliorer les performances
    CREATE INDEX IF NOT EXISTS idx_core_channel_id ON core.videos(channel_id);
    CREATE INDEX IF NOT EXISTS idx_core_published_date ON core.videos(published_date);
    CREATE INDEX IF NOT EXISTS idx_core_like_count ON core.videos(like_count);
    CREATE INDEX IF NOT EXISTS idx_core_view_count ON core.videos(view_count);
    CREATE INDEX IF NOT EXISTS idx_core_last_update ON core.videos(last_update);
    
    -- Index composite pour les requêtes fréquentes
    CREATE INDEX IF NOT EXISTS idx_core_channel_published ON core.videos(channel_id, published_date);
    """)
    cur.execute("""
    INSERT INTO core.videos (
        video_id, title, description, published_date, channel_id,
        like_count, view_count, comment_count, duration_seconds
    )
    SELECT
        s.video_id,
        s.title,
        s.description,
        s.published_at::date,
        s.channel_id,
        s.like_count,
        s.view_count,
        s.comment_count,
        s.duration_seconds
    FROM staging.video_raw s
    ON CONFLICT (video_id)
    DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        published_date = EXCLUDED.published_date,
        channel_id = EXCLUDED.channel_id,
        like_count = EXCLUDED.like_count,
        view_count = EXCLUDED.view_count,
        comment_count = EXCLUDED.comment_count,
        duration_seconds = EXCLUDED.duration_seconds,
        last_update = NOW();
    """)
    conn.commit()
    cur.close()
    conn.close()
    return "Core.videos mis à jour"