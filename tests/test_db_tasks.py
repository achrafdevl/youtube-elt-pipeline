# tests/test_db_tasks.py
import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from src.db_tasks import load_to_staging, transform_core


class TestLoadToStaging:
    """Tests pour la fonction load_to_staging"""
    
    @patch('src.db_tasks.psycopg2.connect')
    @patch('src.db_tasks.glob.glob')
    @patch('builtins.open', create=True)
    def test_load_to_staging_success(self, mock_open, mock_glob, mock_connect):
        """Test de chargement en staging réussi"""
        # Mock des données
        mock_data = {
            "videos": [
                {
                    "video_id": "test_video_1",
                    "title": "Test Video 1",
                    "description": "Test Description 1",
                    "published_at": "2025-01-01T00:00:00Z",
                    "channel_id": "UC123456789",
                    "like_count": "1000",
                    "view_count": "10000",
                    "comment_count": "100",
                    "duration_iso": "PT2M30S",
                    "duration_seconds": 150,
                    "duration_readable": "2:30"
                }
            ]
        }
        
        # Mock des fichiers
        mock_glob.return_value = ["/tmp/test.json"]
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(mock_data)
        
        # Mock de la base de données
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        
        result = load_to_staging()
        
        assert "1 lignes insérées" in result
        mock_cur.execute.assert_called()
        mock_conn.commit.assert_called_once()
    
    @patch('src.db_tasks.glob.glob')
    def test_load_to_staging_no_files(self, mock_glob):
        """Test de chargement sans fichiers JSON"""
        mock_glob.return_value = []
        
        with pytest.raises(ValueError, match="Aucun fichier JSON trouvé"):
            load_to_staging()
    
    @patch('src.db_tasks.psycopg2.connect')
    @patch('src.db_tasks.glob.glob')
    @patch('builtins.open', create=True)
    def test_load_to_staging_no_videos(self, mock_open, mock_glob, mock_connect):
        """Test de chargement sans vidéos dans le fichier"""
        mock_data = {"videos": []}
        mock_glob.return_value = ["/tmp/test.json"]
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(mock_data)
        
        with pytest.raises(ValueError, match="Aucune vidéo dans le fichier"):
            load_to_staging()


class TestTransformCore:
    """Tests pour la fonction transform_core"""
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_transform_core_success(self, mock_connect):
        """Test de transformation vers core réussi"""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        
        result = transform_core()
        
        assert result == "Core.videos mis à jour"
        assert mock_cur.execute.call_count == 2  # CREATE + INSERT
        mock_conn.commit.assert_called_once()
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_transform_core_database_error(self, mock_connect):
        """Test de transformation avec erreur de base de données"""
        mock_connect.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            transform_core()


class TestDatabaseConnection:
    """Tests pour la connexion à la base de données"""
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_connection_success(self, mock_connect):
        """Test de connexion réussie"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        from src.db_tasks import _connect
        conn = _connect()
        
        assert conn == mock_conn
        mock_connect.assert_called_once_with(
            host="postgres", port="5432",
            dbname="youtube_dw", user="airflow", password="airflow"
        )
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_connection_with_env_vars(self, mock_connect):
        """Test de connexion avec variables d'environnement"""
        with patch.dict(os.environ, {
            'PGHOST': 'localhost',
            'PGPORT': '5433',
            'PGDATABASE': 'test_db',
            'PGUSER': 'test_user',
            'PGPASSWORD': 'test_pass'
        }):
            from src.db_tasks import _connect
            _connect()
            
            mock_connect.assert_called_once_with(
                host="localhost", port="5433",
                dbname="test_db", user="test_user", password="test_pass"
            )


class TestDataValidation:
    """Tests pour la validation des données"""
    
    def test_data_type_conversion(self):
        """Test de conversion des types de données"""
        # Test des conversions de chaînes vers entiers
        test_data = {
            "like_count": "1000",
            "view_count": "10000",
            "comment_count": "100",
            "duration_seconds": "150"
        }
        
        # Simulation de la logique de conversion
        like_count = int(test_data["like_count"]) if test_data["like_count"] is not None else None
        view_count = int(test_data["view_count"]) if test_data["view_count"] is not None else None
        comment_count = int(test_data["comment_count"]) if test_data["comment_count"] is not None else None
        duration_seconds = int(test_data["duration_seconds"]) if test_data["duration_seconds"] is not None else None
        
        assert like_count == 1000
        assert view_count == 10000
        assert comment_count == 100
        assert duration_seconds == 150
    
    def test_data_type_conversion_none_values(self):
        """Test de conversion avec valeurs None"""
        test_data = {
            "like_count": None,
            "view_count": None,
            "comment_count": None,
            "duration_seconds": None
        }
        
        like_count = int(test_data["like_count"]) if test_data["like_count"] is not None else None
        view_count = int(test_data["view_count"]) if test_data["view_count"] is not None else None
        comment_count = int(test_data["comment_count"]) if test_data["comment_count"] is not None else None
        duration_seconds = int(test_data["duration_seconds"]) if test_data["duration_seconds"] is not None else None
        
        assert like_count is None
        assert view_count is None
        assert comment_count is None
        assert duration_seconds is None
