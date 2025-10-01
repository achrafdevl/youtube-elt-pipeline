import os
import sys
import traceback
import psycopg2
from dotenv import load_dotenv, find_dotenv


def test_postgres_connection() -> bool:
    try:
        # Load environment variables from .env if present
        load_dotenv(find_dotenv())

        # Clear ALL PG environment variables that might contain bad encoding
        pg_vars_to_clear = [
            "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR",
            "PGDATABASE", "PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGCLIENTENCODING"
        ]
        for var in pg_vars_to_clear:
            if var in os.environ:
                del os.environ[var]

        # Set explicit encoding
        os.environ["PGCLIENTENCODING"] = "UTF8"
        
        # Use hardcoded connection parameters to avoid any environment variable issues
        dbname = "postgres"
        user = "postgres"
        password = "postgres"
        host = "localhost"
        port = "5433"

        print(f"Tentative de connexion à {host}:{port}")
        print(f"Base de données: {dbname}")
        print(f"Utilisateur: {user}")

        # Simple connection attempt with explicit encoding
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            options="-c client_encoding=UTF8"
        )

        cursor = conn.cursor()
        # Show current encodings for diagnostics
        cursor.execute("SHOW client_encoding;")
        current_client_encoding = cursor.fetchone()[0]
        cursor.execute("SHOW server_encoding;")
        current_server_encoding = cursor.fetchone()[0]

        # Try to get version normally
        cursor.execute("SELECT version();")
        version_value = cursor.fetchone()[0]

        # Also get raw bytes form to debug if needed
        cursor.execute("SELECT encode(version()::bytea, 'escape');")
        version_bytes_escaped = cursor.fetchone()[0]

        print("Connexion réussie !")
        print(f"Client encoding: {current_client_encoding} | Server encoding: {current_server_encoding}")
        print(f"Version PostgreSQL: {version_value}")
        print(f"Version bytes (escaped): {version_bytes_escaped}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_postgres_connection()


