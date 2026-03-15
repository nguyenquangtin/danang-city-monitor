"""Poll Reddit for Da Nang mentions and store as articles in Postgres."""
import psycopg2
import os
import requests
from datetime import datetime, timezone

SUBREDDITS = ["vietnam", "solotravel", "travel", "digitalnomad"]
SEARCH_QUERIES = ["danang", "da nang", "đà nẵng"]

HEADERS = {"User-Agent": "danang-city-monitor/1.0"}

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

seen_urls = set()

for subreddit in SUBREDDITS:
    for query in SEARCH_QUERIES:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/search.json"
            params = {"q": query, "sort": "new", "limit": 10, "restrict_sr": 1}
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if resp.status_code != 200:
                continue
            posts = resp.json().get("data", {}).get("children", [])
            for post in posts:
                data = post.get("data", {})
                post_url = f"https://reddit.com{data.get('permalink', '')}"
                if post_url in seen_urls:
                    continue
                seen_urls.add(post_url)
                created = data.get("created_utc")
                published_at = (
                    datetime.fromtimestamp(created, tz=timezone.utc)
                    if created else datetime.now(tz=timezone.utc)
                )
                cur.execute(
                    """
                    INSERT INTO articles (title, url, source, published_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    (
                        data.get("title", "")[:500],
                        post_url,
                        f"Reddit r/{subreddit}",
                        published_at,
                    ),
                )
        except Exception as e:
            print(f"Skipped r/{subreddit} '{query}': {e}")

conn.commit()
cur.close()
conn.close()
print(f"Reddit poll complete — {len(seen_urls)} posts processed")
