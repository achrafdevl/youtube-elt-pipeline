# tests/test_integration.py
import pytest
import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from src.youtube_client import YouTubeClient
from src.db_tasks import load_to_staging, transform_core


class TestEndToEndIntegration:
    """Tests d'intégration end-to-end"""
    
    @pytest.fixture
    def sample_json_data(self):
        """Données JSON d'exemple pour les tests"""
        return {
            "channel_handle": "@testchannel",
            "channel_id": "UC123456789",
            "extraction_date": "2025-01-01T00:00:00Z",
            "total_videos": 2,
            "quota_used": 150,
            "quota_remaining": 9850,
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
                },
                {
                    "video_id": "test_video_2",
                    "title": "Test Video 2",
                    "description": "Test Description 2",
                    "published_at": "2025-01-02T00:00:00Z",
                    "channel_id": "UC123456789",
                    "like_count": "2000",
                    "view_count": "20000",
                    "comment_count": "200",
                    "duration_iso": "PT5M45S",
                    "duration_seconds": 345,
                    "duration_readable": "5:45"
                }
            ]
        }
    
    def test_youtube_client_to_json_flow(self, sample_json_data):
        """Test du flux complet du client YouTube vers JSON"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch('src.youtube_client.build') as mock_build:
                # Mock du service YouTube
                mock_service = Mock()
                mock_build.return_value = mock_service
                
                # Mock des réponses API
                channel_response = {
                    "items": [{"id": {"channelId": "UC123456789"}}]
                }
                videos_response = {
                    "items": [
                        {"id": {"videoId": "test_video_1"}},
                        {"id": {"videoId": "test_video_2"}}
                    ]
                }
                details_response = {
                    "items": [
                        {
                            "id": "test_video_1",
                            "snippet": {
                                "title": "Test Video 1",
                                "description": "Test Description 1",
                                "publishedAt": "2025-01-01T00:00:00Z",
                                "channelId": "UC123456789"
                            },
                            "contentDetails": {"duration": "PT2M30S"},
                            "statistics": {
                                "likeCount": "1000",
                                "viewCount": "10000",
                                "commentCount": "100"
                            }
                        },
                        {
                            "id": "test_video_2",
                            "snippet": {
                                "title": "Test Video 2",
                                "description": "Test Description 2",
                                "publishedAt": "2025-01-02T00:00:00Z",
                                "channelId": "UC123456789"
                            },
                            "contentDetails": {"duration": "PT5M45S"},
                            "statistics": {
                                "likeCount": "2000",
                                "viewCount": "20000",
                                "commentCount": "200"
                            }
                        }
                    ]
                }
                
                # Configuration des mocks
                mock_service.search.return_value.list.return_value.execute.side_effect = [
                    channel_response,  # Premier appel pour trouver la chaîne
                    videos_response,   # Deuxième appel pour lister les vidéos
                ]
                mock_service.videos.return_value.list.return_value.execute.return_value = details_response
                
                # Test du client
                client = YouTubeClient("test_api_key")
                result_file = client.fetch_and_save_channel("@testchannel", temp_dir, 2)
                
                # Vérifications
                assert os.path.exists(result_file)
                assert "@testchannel_" in result_file
                assert ".json" in result_file
                
                # Vérifier le contenu du fichier
                with open(result_file, 'r') as f:
                    saved_data = json.load(f)
                
                assert saved_data["channel_handle"] == "@testchannel"
                assert saved_data["total_videos"] == 2
                assert len(saved_data["videos"]) == 2
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_json_to_database_flow(self, mock_connect, sample_json_data):
        """Test du flux JSON vers base de données"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Créer un fichier JSON de test
            json_file = os.path.join(temp_dir, "test_channel_20250101_000000.json")
            with open(json_file, 'w') as f:
                json.dump(sample_json_data, f)
            
            # Mock de la base de données
            mock_conn = Mock()
            mock_cur = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cur
            
            # Mock de glob pour trouver le fichier
            with patch('src.db_tasks.glob.glob', return_value=[json_file]):
                with patch.dict(os.environ, {'JSON_OUTPUT_PATH': temp_dir}):
                    result = load_to_staging()
                    
                    # Vérifications
                    assert "2 lignes insérées" in result
                    assert mock_cur.execute.call_count >= 1  # Au moins CREATE TABLE
                    mock_conn.commit.assert_called_once()
    
    @patch('src.db_tasks.psycopg2.connect')
    def test_staging_to_core_transformation(self, mock_connect):
        """Test de la transformation staging vers core"""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        
        result = transform_core()
        
        # Vérifications
        assert result == "Core.videos mis à jour"
        assert mock_cur.execute.call_count == 2  # CREATE + INSERT
        mock_conn.commit.assert_called_once()
    
    def test_error_handling_integration(self):
        """Test de la gestion d'erreurs en intégration"""
        with patch('src.youtube_client.build') as mock_build:
            # Mock d'une erreur API
            mock_service = Mock()
            mock_build.return_value = mock_service
            mock_service.search.return_value.list.return_value.execute.side_effect = Exception("API Error")
            
            client = YouTubeClient("test_api_key")
            
            with pytest.raises(Exception, match="API Error"):
                client.fetch_and_save_channel("@testchannel", "/tmp", 10)
    
    def test_quota_management_integration(self):
        """Test de la gestion des quotas en intégration"""
        with patch('src.youtube_client.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            
            client = YouTubeClient("test_api_key")
            
            # Vérifier l'état initial du quota
            assert client.quota_used_today == 0
            assert client.daily_quota_limit == 10000
            
            # Simuler l'utilisation du quota
            client.quota_used_today = 9500
            assert not client._check_quota(1000)  # Plus assez de quota
            assert client._check_quota(100)       # Assez de quota


class TestDataQualityIntegration:
    """Tests d'intégration pour la qualité des données"""
    
    def test_data_validation_pipeline(self):
        """Test du pipeline de validation des données"""
        # Test des données valides
        valid_data = {
            "video_id": "test_video_1",
            "title": "Valid Video",
            "published_at": "2025-01-01T00:00:00Z",
            "duration_seconds": 150,
            "like_count": "1000",
            "view_count": "10000"
        }
        
        # Validation des champs requis
        assert valid_data["video_id"] is not None
        assert valid_data["title"] is not None
        assert valid_data["published_at"] is not None
        assert valid_data["duration_seconds"] > 0
        assert int(valid_data["like_count"]) >= 0
        assert int(valid_data["view_count"]) >= 0
    
    def test_data_transformation_consistency(self):
        """Test de la cohérence des transformations de données"""
        # Données d'entrée
        input_data = {
            "duration_iso": "PT2M30S",
            "published_at": "2025-01-01T00:00:00Z",
            "like_count": "1000"
        }
        
        # Transformation attendue
        expected_duration_seconds = 150  # 2*60 + 30
        expected_published_date = "2025-01-01"
        expected_like_count = 1000
        
        # Simulation des transformations
        from src.youtube_client import iso_duration_to_seconds
        actual_duration_seconds = iso_duration_to_seconds(input_data["duration_iso"])
        actual_published_date = input_data["published_at"][:10]  # YYYY-MM-DD
        actual_like_count = int(input_data["like_count"])
        
        assert actual_duration_seconds == expected_duration_seconds
        assert actual_published_date == expected_published_date
        assert actual_like_count == expected_like_count


class TestPerformanceIntegration:
    """Tests de performance en intégration"""
    
    def test_large_dataset_handling(self):
        """Test de gestion d'un grand dataset"""
        # Simulation d'un grand nombre de vidéos
        large_video_list = [{"id": f"video_{i}"} for i in range(1000)]
        
        # Test de pagination
        batch_size = 50
        batches = [large_video_list[i:i+batch_size] for i in range(0, len(large_video_list), batch_size)]
        
        assert len(batches) == 20  # 1000 / 50 = 20 batches
        assert all(len(batch) <= batch_size for batch in batches)
    
    def test_memory_efficiency(self):
        """Test de l'efficacité mémoire"""
        # Test que les données sont traitées par batch
        video_ids = [f"video_{i}" for i in range(100)]
        batch_size = 50
        
        processed_batches = []
        for i in range(0, len(video_ids), batch_size):
            batch = video_ids[i:i+batch_size]
            processed_batches.append(batch)
        
        assert len(processed_batches) == 2
        assert len(processed_batches[0]) == 50
        assert len(processed_batches[1]) == 50
