import os
import json
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
import requests
from isodate import parse_duration
from dotenv import load_dotenv, find_dotenv


SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"

# Charger les variables depuis un fichier .env si présent
load_dotenv(find_dotenv(), override=False)


def get_channel_id(api_key: str, channel_handle: str) -> str:
    """
    Récupère l'ID de la chaîne via son handle (ex: @MrBeast).
    Utilise l'endpoint channels avec forHandle.
    """
    params = {
        "part": "id,snippet",
        "forHandle": channel_handle if channel_handle.startswith("@") else f"@{channel_handle}",
        "key": api_key,
    }
    r = requests.get(CHANNELS_URL, params=params)
    r.raise_for_status()
    data = r.json()
    if not data.get("items"):
        raise ValueError(f"Chaîne introuvable pour handle: {channel_handle}")
    return data["items"][0]["id"]


def get_uploads_playlist_id(api_key: str, channel_id: str) -> str:
    """Récupère l'ID de la playlist 'uploads' d'une chaîne"""
    params = {
        "key": api_key,
        "id": channel_id,
        "part": "contentDetails",
    }
    r = requests.get(CHANNELS_URL, params=params)
    r.raise_for_status()
    data = r.json()
    items = data.get("items", [])
    if not items:
        raise ValueError("Chaîne non trouvée")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def get_video_ids_from_uploads(api_key: str, uploads_playlist_id: str) -> List[str]:
    """Parcourt toute la playlist uploads et retourne les IDs de vidéos"""
    all_video_ids: List[str] = []
    next_page: Optional[str] = None

    while True:
        params = {
            "key": api_key,
            "playlistId": uploads_playlist_id,
            "part": "contentDetails",
            "maxResults": 50,
        }
        if next_page:
            params["pageToken"] = next_page

        r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
        r.raise_for_status()
        data = r.json()

        for item in data.get("items", []):
            vid = item["contentDetails"]["videoId"]
            all_video_ids.append(vid)

        next_page = data.get("nextPageToken")
        if not next_page:
            break

    return all_video_ids


def partition_in_batches(video_ids: List[str], batch_size: int = 50) -> Dict[str, List[str]]:
    """Partitionne la liste des IDs en lots de taille <= 50"""
    result: Dict[str, List[str]] = {}
    index = 1
    remaining = list(video_ids)
    while remaining:
        name = f"identifiant_{index}"
        result[name] = remaining[:batch_size]
        remaining = remaining[batch_size:]
        index += 1
    return result


def extract_video_details(api_key: str, batches: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    extracted: List[Dict[str, Any]] = []
    for ids in batches.values():
        if not ids:
            continue
        params = {
            "key": api_key,
            "id": ",".join(ids),
            "part": "snippet,contentDetails,statistics",
        }
        r = requests.get(VIDEOS_URL, params=params)
        r.raise_for_status()
        data = r.json()

        for item in data.get("items", []):
            dur_iso = item["contentDetails"]["duration"]
            dur_seconds = int(parse_duration(dur_iso).total_seconds()) if dur_iso else 0
            minutes = dur_seconds // 60
            seconds = dur_seconds % 60
            duration_readable = f"{minutes}:{seconds:02d}"

            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})

            extracted.append({
                "title": snippet.get("title"),
                "duration": dur_iso,
                "video_id": item.get("id"),
                "like_count": int(stats.get("likeCount", 0) or 0),
                "view_count": int(stats.get("viewCount", 0) or 0),
                "published_at": snippet.get("publishedAt"),
                "comment_count": int(stats.get("commentCount", 0) or 0),
                "duration_readable": duration_readable,
            })

    return extracted


def save_json(channel_handle: str, extracted: List[Dict[str, Any]], out_path: str) -> str:
    payload = {
        "channel_handle": channel_handle if channel_handle.startswith("@") else f"@{channel_handle}",
        "extraction_date": datetime.utcnow().isoformat(),
        "total_videos": len(extracted),
        "videos": extracted,
    }
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=4)
    return out_path


def run_flow(api_key: str, channel_handle: str, out_dir: str) -> str:
    channel_id = get_channel_id(api_key, channel_handle)
    uploads_id = get_uploads_playlist_id(api_key, channel_id)
    video_ids = get_video_ids_from_uploads(api_key, uploads_id)
    batches = partition_in_batches(video_ids)
    extracted = extract_video_details(api_key, batches)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_handle = channel_handle.lstrip("@")
    filename = f"@{safe_handle}_{timestamp}.json"
    out_path = os.path.join(out_dir, filename)
    return save_json(channel_handle, extracted, out_path)


def main():
    parser = argparse.ArgumentParser(description="Requests-based YouTube extractor (uploads playlist → JSON)")
    parser.add_argument("--handle", dest="channel_handle", default=os.getenv("TARGET_CHANNEL_HANDLE", "@MrBeast"))
    parser.add_argument("--out", dest="out_dir", default=os.getenv("JSON_OUTPUT_PATH", "./data/raw"))
    args = parser.parse_args()

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise SystemExit("YOUTUBE_API_KEY environment variable is required")

    saved_path = run_flow(api_key, args.channel_handle, args.out_dir)
    print(saved_path)


if __name__ == "__main__":
    main()


