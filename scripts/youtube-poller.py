"""Poll YouTube for Da Nang videos and store in youtube_videos table."""
import psycopg2
import os
import requests
from datetime import datetime

API_KEY = os.environ["YOUTUBE_API_KEY"]
SEARCH_QUERIES = ["Da Nang Vietnam", "Đà Nẵng", "Da Nang travel", "Da Nang 2026"]
BASE = "https://www.googleapis.com/youtube/v3"

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

video_ids = []
video_meta = {}  # id → {title, published_at, thumbnail_url}

# Step 1: search for videos
for query in SEARCH_QUERIES:
    try:
        resp = requests.get(f"{BASE}/search", params={
            "part": "snippet",
            "q": query,
            "type": "video",
            "regionCode": "VN",
            "maxResults": 10,
            "order": "date",
            "key": API_KEY,
        }, timeout=10)
        if resp.status_code != 200:
            print(f"Search error for '{query}': {resp.status_code}")
            continue
        for item in resp.json().get("items", []):
            vid_id = item["id"]["videoId"]
            snippet = item["snippet"]
            if vid_id not in video_meta:
                video_ids.append(vid_id)
                video_meta[vid_id] = {
                    "title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnail_url": snippet.get("thumbnails", {}).get("medium", {}).get("url"),
                }
    except Exception as e:
        print(f"Search error for '{query}': {e}")

# Step 2: fetch view counts in batch (max 50 per request)
for i in range(0, len(video_ids), 50):
    batch = video_ids[i:i+50]
    try:
        resp = requests.get(f"{BASE}/videos", params={
            "part": "statistics",
            "id": ",".join(batch),
            "key": API_KEY,
        }, timeout=10)
        if resp.status_code != 200:
            continue
        for item in resp.json().get("items", []):
            vid_id = item["id"]
            view_count = int(item.get("statistics", {}).get("viewCount", 0))
            meta = video_meta[vid_id]
            published_at = (
                datetime.fromisoformat(meta["published_at"].replace("Z", "+00:00"))
                if meta["published_at"] else datetime.now()
            )
            cur.execute(
                """
                INSERT INTO youtube_videos (title, url, thumbnail_url, view_count, published_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE
                  SET view_count = EXCLUDED.view_count,
                      fetched_at = NOW()
                """,
                (
                    meta["title"],
                    f"https://youtube.com/watch?v={vid_id}",
                    meta["thumbnail_url"],
                    view_count,
                    published_at,
                ),
            )
    except Exception as e:
        print(f"Stats batch error: {e}")

conn.commit()
cur.close()
conn.close()
print(f"YouTube poll complete — {len(video_ids)} videos processed")
