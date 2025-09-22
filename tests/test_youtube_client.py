# tests/test_youtube_client.py
import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.youtube_client import YouTubeClient, iso_duration_to_seconds


class TestIsoDurationToSeconds:
    """Tests pour la fonction iso_duration_to_seconds"""
    
    def test_iso_duration_parser(self):
        assert iso_duration_to_seconds("PT1H2M30S") == 3750
        assert iso_duration_to_seconds("PT45S") == 45
        assert iso_duration_to_seconds("PT0S") == 0
        assert iso_duration_to_seconds("PT2M56S") == 176
        assert iso_duration_to_seconds("PT1H") == 3600
        assert iso_duration_to_seconds("") == 0
        assert iso_duration_to_seconds(None) == 0


class TestYouTubeClient:
    """Tests pour la classe YouTubeClient"""
    
    @pytest.fixture
    def mock_youtube_client(self):
        with patch('src.youtube_client.build') as mock_build:
            mock_service = Mock()
            mock_build.return_value = mock_service
            client = YouTubeClient("test_api_key")
            client.youtube = mock_service
            return client
    
    def test_init(self):
        """Test de l'initialisation du client"""
        with patch('src.youtube_client.build') as mock_build:
            client = YouTubeClient("test_api_key")
            assert client.api_key == "test_api_key"
            assert client.daily_quota_limit == 10000
            assert client.quota_used_today == 0
            mock_build.assert_called_once()
    
    def test_check_quota_sufficient(self, mock_youtube_client):
        """Test de vérification de quota suffisant"""
        mock_youtube_client.quota_used_today = 1000
        assert mock_youtube_client._check_quota(500) == True
    
    def test_check_quota_insufficient(self, mock_youtube_client):
        """Test de vérification de quota insuffisant"""
        mock_youtube_client.quota_used_today = 9500
        assert mock_youtube_client._check_quota(1000) == False
    
    def test_check_quota_reset(self, mock_youtube_client):
        """Test de reset du quota à minuit"""
        mock_youtube_client.quota_used_today = 5000
        mock_youtube_client.quota_reset_time = datetime.now()
        assert mock_youtube_client._check_quota(1000) == True
        assert mock_youtube_client.quota_used_today == 0
    
    @patch('src.youtube_client.time.sleep')
    def test_make_api_call_with_retry_success(self, mock_sleep, mock_youtube_client):
        """Test d'appel API réussi"""
        mock_call = Mock()
        mock_call.execute.return_value = {"items": []}
        
        result = mock_youtube_client._make_api_call_with_retry(mock_call, 'search')
        
        assert result == {"items": []}
        assert mock_youtube_client.quota_used_today == 100
        mock_call.execute.assert_called_once()
    
    @patch('src.youtube_client.time.sleep')
    def test_make_api_call_with_retry_quota_exceeded(self, mock_sleep, mock_youtube_client):
        """Test d'appel API avec quota dépassé"""
        from googleapiclient.errors import HttpError
        mock_response = Mock()
        mock_response.status = 403
        
        mock_call = Mock()
        mock_call.execute.side_effect = HttpError(mock_response, b'quotaExceeded')
        
        with pytest.raises(Exception, match="Quota API dépassé"):
            mock_youtube_client._make_api_call_with_retry(mock_call, 'search')
    
    @patch('src.youtube_client.time.sleep')
    def test_make_api_call_with_retry_rate_limit(self, mock_sleep, mock_youtube_client):
        """Test d'appel API avec rate limiting"""
        from googleapiclient.errors import HttpError
        mock_response = Mock()
        mock_response.status = 429
        
        mock_call = Mock()
        mock_call.execute.side_effect = [
            HttpError(mock_response, b'rate limit'),
            {"items": []}  # Succès au 2ème essai
        ]
        
        result = mock_youtube_client._make_api_call_with_retry(mock_call, 'search')
        
        assert result == {"items": []}
        assert mock_sleep.called
        assert mock_call.execute.call_count == 2
    
    def test_get_channel_id_success(self, mock_youtube_client):
        """Test de récupération d'ID de chaîne réussie"""
        mock_response = {
            "items": [{"id": {"channelId": "UC123456789"}}]
        }
        mock_youtube_client._make_api_call_with_retry = Mock(return_value=mock_response)
        
        channel_id = mock_youtube_client._get_channel_id("@testchannel")
        
        assert channel_id == "UC123456789"
    
    def test_get_channel_id_not_found(self, mock_youtube_client):
        """Test de récupération d'ID de chaîne non trouvée"""
        mock_response = {"items": []}
        mock_youtube_client._make_api_call_with_retry = Mock(return_value=mock_response)
        
        with pytest.raises(ValueError, match="Impossible de trouver le handle"):
            mock_youtube_client._get_channel_id("@nonexistent")
    
    def test_get_videos_with_pagination(self, mock_youtube_client):
        """Test de récupération de vidéos avec pagination"""
        # Mock des réponses paginées
        page1_response = {
            "items": [{"id": {"videoId": f"video_{i}"}} for i in range(50)],
            "nextPageToken": "next_page_token"
        }
        page2_response = {
            "items": [{"id": {"videoId": f"video_{i}"}} for i in range(50, 100)],
            "nextPageToken": None
        }
        
        mock_youtube_client._make_api_call_with_retry = Mock(side_effect=[page1_response, page2_response])
        
        video_ids = mock_youtube_client._get_videos_with_pagination("UC123456789", 100)
        
        assert len(video_ids) == 100
        assert video_ids[0] == "video_0"
        assert video_ids[99] == "video_99"
        assert mock_youtube_client._make_api_call_with_retry.call_count == 2
    
    def test_get_video_details(self, mock_youtube_client):
        """Test de récupération des détails des vidéos"""
        mock_response = {
            "items": [{
                "id": "video_1",
                "snippet": {
                    "title": "Test Video",
                    "description": "Test Description",
                    "publishedAt": "2025-01-01T00:00:00Z",
                    "channelId": "UC123456789"
                },
                "contentDetails": {
                    "duration": "PT2M30S"
                },
                "statistics": {
                    "likeCount": "1000",
                    "viewCount": "10000",
                    "commentCount": "100"
                }
            }]
        }
        
        mock_youtube_client._make_api_call_with_retry = Mock(return_value=mock_response)
        
        videos = mock_youtube_client._get_video_details(["video_1"])
        
        assert len(videos) == 1
        assert videos[0]["video_id"] == "video_1"
        assert videos[0]["title"] == "Test Video"
        assert videos[0]["duration_seconds"] == 150
        assert videos[0]["like_count"] == "1000"
    
    @patch('src.youtube_client.os.makedirs')
    @patch('builtins.open', create=True)
    @patch('src.youtube_client.json.dump')
    def test_fetch_and_save_channel_success(self, mock_json_dump, mock_open, mock_makedirs, mock_youtube_client):
        """Test d'extraction et sauvegarde réussie"""
        # Mock des méthodes
        mock_youtube_client._get_channel_id = Mock(return_value="UC123456789")
        mock_youtube_client._get_videos_with_pagination = Mock(return_value=["video_1", "video_2"])
        mock_youtube_client._get_video_details = Mock(return_value=[
            {"video_id": "video_1", "title": "Video 1"},
            {"video_id": "video_2", "title": "Video 2"}
        ])
        
        result = mock_youtube_client.fetch_and_save_channel("@testchannel", "/tmp", 100)
        
        assert "testchannel_" in result
        assert ".json" in result
        mock_makedirs.assert_called_once_with("/tmp", exist_ok=True)
        mock_json_dump.assert_called_once()
    
    def test_fetch_and_save_channel_no_videos(self, mock_youtube_client):
        """Test d'extraction sans vidéos trouvées"""
        mock_youtube_client._get_channel_id = Mock(return_value="UC123456789")
        mock_youtube_client._get_videos_with_pagination = Mock(return_value=[])
        
        with pytest.raises(ValueError, match="Aucune vidéo trouvée"):
            mock_youtube_client.fetch_and_save_channel("@testchannel", "/tmp", 100)
