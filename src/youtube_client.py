# # src/youtube_client.py
# import os
# import json
# import time
# import logging
# from datetime import datetime, timedelta
# from isodate import parse_duration
# from googleapiclient.errors import HttpError
# from typing import List, Dict, Any, Optional

# # Configuration du logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def iso_duration_to_seconds(duration_iso: str) -> int:
#     """
#     Convertit une durée ISO 8601 (ex: "PT1H2M30S") en nombre total de secondes.
#     """
#     if not duration_iso:
#         return 0
#     duration_obj = parse_duration(duration_iso)
#     return int(duration_obj.total_seconds())

# class YouTubeClient:
#     def __init__(self, api_key: str):
#         self.api_key = api_key
#         # Lazy import to avoid hard dependency during module import (useful for tests)
#         from googleapiclient.discovery import build  # type: ignore
#         self.youtube = build("youtube", "v3", developerKey=api_key)
        
#         # Gestion des quotas API
#         self.daily_quota_limit = 10000  # Quota quotidien YouTube API
#         self.quota_used_today = 0
#         self.quota_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
#         # Coûts approximatifs des opérations API (en unités de quota)
#         self.api_costs = {
#             'search': 100,  # search.list
#             'videos': 1,    # videos.list
#         }

#     def _check_quota(self, operation_cost: int) -> bool:
#         """Vérifie si on a assez de quota pour l'opération"""
#         if datetime.now() >= self.quota_reset_time:
#             self.quota_used_today = 0
#             self.quota_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
#         if self.quota_used_today + operation_cost > self.daily_quota_limit:
#             logger.warning(f"Quota insuffisant: {self.quota_used_today}/{self.daily_quota_limit}")
#             return False
#         return True

#     def _make_api_call_with_retry(self, api_call, operation_type: str, max_retries: int = 3):
#         """Effectue un appel API avec retry et gestion d'erreurs"""
#         for attempt in range(max_retries):
#             try:
#                 if not self._check_quota(self.api_costs[operation_type]):
#                     raise Exception("Quota API insuffisant")
                
#                 result = api_call.execute()
#                 self.quota_used_today += self.api_costs[operation_type]
#                 logger.info(f"Quota utilisé: {self.quota_used_today}/{self.daily_quota_limit}")
#                 return result
                
#             except HttpError as e:
#                 if e.resp.status == 403 and "quotaExceeded" in str(e):
#                     logger.error("Quota API dépassé")
#                     raise Exception("Quota API dépassé")
#                 elif e.resp.status == 429:
#                     wait_time = 2 ** attempt
#                     logger.warning(f"Rate limit atteint, attente {wait_time}s")
#                     time.sleep(wait_time)
#                 else:
#                     logger.error(f"Erreur API: {e}")
#                     if attempt == max_retries - 1:
#                         raise
#                     time.sleep(2 ** attempt)
#             except Exception as e:
#                 logger.error(f"Erreur inattendue: {e}")
#                 if attempt == max_retries - 1:
#                     raise
#                 time.sleep(2 ** attempt)

#     def _get_channel_id(self, channel_handle: str) -> str:
#         """Récupère l'ID de la chaîne à partir du handle"""
#         search_call = self.youtube.search().list(
#             q=channel_handle,
#             type="channel",
#             part="id",
#             maxResults=1
#         )
#         channel_resp = self._make_api_call_with_retry(search_call, 'search')
        
#         if not channel_resp["items"]:
#             raise ValueError(f"Impossible de trouver le handle {channel_handle}")
#         return channel_resp["items"][0]["id"]["channelId"]

#     def _get_videos_with_pagination(self, channel_id: str, max_videos: int) -> List[str]:
#         """Récupère les IDs des vidéos avec pagination"""
#         all_video_ids = []
#         next_page_token = None
#         max_results_per_page = 50  # Maximum par page
        
#         while len(all_video_ids) < max_videos:
#             remaining = max_videos - len(all_video_ids)
#             current_max = min(max_results_per_page, remaining)
            
#             search_call = self.youtube.search().list(
#                 channelId=channel_id,
#                 part="id",
#                 order="date",
#                 maxResults=current_max,
#                 type="video",
#                 pageToken=next_page_token
#             )
            
#             videos_resp = self._make_api_call_with_retry(search_call, 'search')
            
#             if not videos_resp["items"]:
#                 break
                
#             all_video_ids.extend([item["id"]["videoId"] for item in videos_resp["items"]])
            
#             next_page_token = videos_resp.get("nextPageToken")
#             if not next_page_token:
#                 break
                
#             # Petite pause entre les pages pour éviter le rate limiting
#             time.sleep(0.1)
        
#         return all_video_ids[:max_videos]

#     def _get_video_details(self, video_ids: List[str]) -> List[Dict[str, Any]]:
#         """Récupère les détails des vidéos par batch de 50"""
#         all_videos = []
        
#         # Traiter par batch de 50 (limite de l'API)
#         for i in range(0, len(video_ids), 50):
#             batch_ids = video_ids[i:i+50]
            
#             details_call = self.youtube.videos().list(
#                 id=",".join(batch_ids),
#                 part="snippet,contentDetails,statistics"
#             )
            
#             details = self._make_api_call_with_retry(details_call, 'videos')
            
#             for v in details["items"]:
#                 snippet = v["snippet"]
#                 stats = v["statistics"]
#                 dur_iso = v["contentDetails"]["duration"]
#                 dur_obj = parse_duration(dur_iso)
#                 total_seconds = int(dur_obj.total_seconds())
#                 minutes, seconds = divmod(total_seconds, 60)
#                 duration_readable = f"{minutes}:{seconds:02d}"

#                 all_videos.append({
#                     "title": snippet.get("title"),
#                     "description": snippet.get("description"),
#                     "duration_iso": dur_iso,
#                     "duration_seconds": total_seconds,
#                     "video_id": v["id"],
#                     "like_count": stats.get("likeCount", "0"),
#                     "view_count": stats.get("viewCount", "0"),
#                     "published_at": snippet.get("publishedAt"),
#                     "channel_id": snippet.get("channelId"),
#                     "comment_count": stats.get("commentCount", "0"),
#                     "duration_readable": duration_readable
#                 })
            
#             # Petite pause entre les batches
#             time.sleep(0.1)
        
#         return all_videos

#     def fetch_and_save_channel(self, channel_handle: str, out_dir: str, max_videos: int = 500):
#         """
#         Récupère les dernières vidéos avec stats + durée, 
#         et sauvegarde dans le format custom demandé.
#         Gère la pagination, les quotas API et les erreurs.
#         """
#         try:
#             # 1. Trouver l'ID de la chaîne
#             logger.info(f"Recherche de la chaîne: {channel_handle}")
#             channel_id = self._get_channel_id(channel_handle)
#             logger.info(f"Chaîne trouvée: {channel_id}")

#             # 2. Récupérer les vidéos avec pagination
#             logger.info(f"Récupération de {max_videos} vidéos maximum")
#             video_ids = self._get_videos_with_pagination(channel_id, max_videos)
            
#             if not video_ids:
#                 raise ValueError("Aucune vidéo trouvée.")
            
#             logger.info(f"Récupération des détails de {len(video_ids)} vidéos")

#             # 3. Récupérer les détails des vidéos
#             videos = self._get_video_details(video_ids)

#             # 4. Construire l'objet final
#             result = {
#                 "channel_handle": channel_handle,
#                 "channel_id": channel_id,
#                 "extraction_date": datetime.utcnow().isoformat(),
#                 "total_videos": len(videos),
#                 "quota_used": self.quota_used_today,
#                 "quota_remaining": self.daily_quota_limit - self.quota_used_today,
#                 "videos": videos
#             }

#             # 5. Sauvegarde
#             os.makedirs(out_dir, exist_ok=True)
#             file_path = os.path.join(
#                 out_dir,
#                 f"{channel_handle}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
#             )
#             with open(file_path, "w", encoding="utf-8") as f:
#                 json.dump(result, f, ensure_ascii=False, indent=4)

#             logger.info(f"Données sauvegardées dans: {file_path}")
#             logger.info(f"Quota utilisé: {self.quota_used_today}/{self.daily_quota_limit}")
#             return file_path

#         except Exception as e:
#             logger.error(f"Erreur lors de l'extraction: {e}")
#             raise