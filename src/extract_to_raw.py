import os
import argparse
from datetime import datetime

# from youtube_client import YouTubeClient  # Commented out since youtube_client.py was deleted


def main():
    parser = argparse.ArgumentParser(description="Extract YouTube data and save as JSON")
    parser.add_argument("--handle", dest="channel_handle", default=os.getenv("TARGET_CHANNEL_HANDLE", "@MrBeast"), help="Channel handle, e.g. @MrBeast")
    parser.add_argument("--out", dest="out_dir", default=os.getenv("JSON_OUTPUT_PATH", "./data/raw"), help="Output directory for JSON files")
    parser.add_argument("--max", dest="max_videos", type=int, default=int(os.getenv("MAX_VIDEOS", "500")), help="Max number of videos to fetch")

    args = parser.parse_args()

    # Commented out since youtube_client.py was deleted
    # api_key = os.getenv("YOUTUBE_API_KEY")
    # if not api_key:
    #     raise SystemExit("YOUTUBE_API_KEY environment variable is required")

    # client = YouTubeClient(api_key=api_key)
    # saved = client.fetch_and_save_channel(
    #     channel_handle=args.channel_handle,
    #     out_dir=args.out_dir,
    #     max_videos=args.max_videos,
    # )
    # print(saved)
    
    print("YouTube client functionality has been disabled")


if __name__ == "__main__":
    main()


