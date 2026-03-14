"""Poll Vietnamese RSS feeds and store Da Nang-related articles to Postgres."""
import feedparser
import psycopg2
import os
from datetime import datetime

FEEDS = [
    "https://baodanang.vn/rss",
    "https://vnexpress.net/rss/tin-moi-nhat.rss",
    "https://tuoitre.vn/rss/tin-moi-nhat.rss",
    "https://thanhnien.vn/rss/home.rss",
]
KEYWORDS = ["đà nẵng", "da nang", "danang"]

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

for feed_url in FEEDS:
    feed = feedparser.parse(feed_url)
    source = feed.feed.get("title", feed_url)
    for entry in feed.entries:
        title = entry.get("title", "")
        if not any(k in title.lower() for k in KEYWORDS):
            continue
        cur.execute(
            """
            INSERT INTO articles (title, url, source, published_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
            (
                title,
                entry.get("link", ""),
                source,
                entry.get("published", str(datetime.now())),
            ),
        )

conn.commit()
cur.close()
conn.close()
print("RSS poll complete")
