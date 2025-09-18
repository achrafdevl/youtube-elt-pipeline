# src/youtube_client.py
import os
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("youtube_client")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
DEFAULT_OUTPUT = os.getenv("JSON_OUTPUT_PATH", "./data/raw")
MAX_VIDEOS_DEFAULT = int(os.getenv("MAX_VIDEOS", "500"))

# chunk helper
def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def iso_duration_to_seconds(duration_iso: str) -> Optional[int]:
    try:
        td = isodate.parse_duration(duration_iso)
        return int(td.total_seconds())
    except Exception:
        return None

class YouTubeClient:
    def __init__(self, api_key: str, sleep_on_quota=2.0):
        if not api_key:
            raise ValueError("YOUTUBE_API_KEY required")
        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=self.api_key)
        self.sleep_on_quota = sleep_on_quota

    def _execute_with_retries(self, request, max_retries=6):
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                return request.execute()
            except HttpError as e:
                status = getattr(e.resp, "status", None)
                logger.warning(f"HttpError status={status} attempt={attempt}: {e}")
                # throttling / quota / server errors -> backoff and retry
                if status in (403, 429, 500, 502, 503, 504):
                    sleep_time = backoff * (2 ** (attempt - 1))
                    logger.info(f"Sleeping {sleep_time:.1f}s before retrying...")
                    time.sleep(sleep_time)
                    continue
                # for other HttpErrors, raise
                raise
            except Exception as e:
                logger.exception("Unexpected error calling YouTube API")
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("Max retries reached for YouTube API request")

    def fetch_video_ids_for_channel(self, channel_id: str, max_results: int = MAX_VIDEOS_DEFAULT) -> List[str]:
        """
        Uses the search.list endpoint (type=video) to collect videoIds for the channel (newest first).
        """
        logger.info("Fetching video ids for channel %s (max %s)", channel_id, max_results)
        ids = []
        page_token = None

        while True:
            req = self.youtube.search().list(
                part="id",
                channelId=channel_id,
                maxResults=50,  # max allowed per call
                order="date",
                type="video",
                pageToken=page_token
            )
            resp = self._execute_with_retries(req)
            items = resp.get("items", [])
            for it in items:
                vid = it.get("id", {}).get("videoId")
                if vid:
                    ids.append(vid)
                if len(ids) >= max_results:
                    return ids[:max_results]
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
            # light sleep to avoid bursting
            time.sleep(0.1)

        logger.info("Collected %d video ids", len(ids))
        return ids

    def fetch_videos_details(self, video_ids: List[str]) -> List[Dict]:
        """
        Calls videos.list in chunks (max 50 ids/chunk) to get snippet, contentDetails, statistics.
        Returns normalized list of dicts.
        """
        logger.info("Fetching details for %d videos", len(video_ids))
        result = []
        for chunk in chunked(video_ids, 50):
            req = self.youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(chunk),
                maxResults=50,
                # fields can be used to limit response size; omitted here for readability
            )
            resp = self._execute_with_retries(req)
            for item in resp.get("items", []):
                snip = item.get("snippet", {})
                content = item.get("contentDetails", {})
                stats = item.get("statistics", {})
                parsed = {
                    "video_id": item.get("id"),
                    "title": snip.get("title"),
                    "published_at": snip.get("publishedAt"),
                    "duration_iso": content.get("duration"),
                    "duration_seconds": iso_duration_to_seconds(content.get("duration")) if content.get("duration") else None,
                    "view_count": int(stats.get("viewCount")) if stats.get("viewCount") is not None else None,
                    "like_count": int(stats.get("likeCount")) if stats.get("likeCount") is not None else None,
                    "comment_count": int(stats.get("commentCount")) if stats.get("commentCount") is not None else None,
                    "channel_id": snip.get("channelId"),
                    "raw": item  # keep raw for debugging
                }
                result.append(parsed)
            # small sleep to be gentle
            time.sleep(0.1)
        logger.info("Fetched details for %d videos", len(result))
        return result

    def fetch_and_save_channel(self, channel_id: str, out_dir: str = DEFAULT_OUTPUT, max_videos: int = MAX_VIDEOS_DEFAULT) -> str:
        os.makedirs(out_dir, exist_ok=True)
        ids = self.fetch_video_ids_for_channel(channel_id, max_results=max_videos)
        details = self.fetch_videos_details(ids)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_channel = channel_id.replace("/", "_")
        filename = f"{safe_channel}_{ts}.json"
        filepath = os.path.join(out_dir, filename)
        logger.info("Saving %d records to %s", len(details), filepath)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({
                "fetched_at": ts,
                "channel_id": channel_id,
                "count": len(details),
                "videos": details
            }, f, ensure_ascii=False, indent=2)
        return filepath

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch videos from a YouTube channel and save JSON")
    parser.add_argument("--channel", "-c", help="Channel ID (overrides TARGET_CHANNEL_ID env var)", default=None)
    parser.add_argument("--max", "-m", type=int, default=None, help="Max videos to fetch")
    parser.add_argument("--out", "-o", default=None, help="Output directory")
    args = parser.parse_args()

    channel = args.channel or os.getenv("TARGET_CHANNEL_ID")
    if not channel:
        raise SystemExit("Provide --channel or set TARGET_CHANNEL_ID in env/.env")

    api_key = os.getenv("YOUTUBE_API_KEY") or YOUTUBE_API_KEY
    client = YouTubeClient(api_key=api_key)
    out = args.out or os.getenv("JSON_OUTPUT_PATH", "./data/raw")
    mx = args.max or int(os.getenv("MAX_VIDEOS", MAX_VIDEOS_DEFAULT))

    saved = client.fetch_and_save_channel(channel_id=channel, out_dir=out, max_videos=mx)
    logger.info("Saved data to %s", saved)
