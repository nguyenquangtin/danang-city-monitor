"""Poll Vietnamese RSS feeds and store Da Nang-related articles to Postgres."""
import feedparser
import psycopg2
import os
from datetime import datetime

FEEDS = [
    # Da Nang-specific feeds (all articles are relevant)
    "https://baodanang.vn/rss",
    "https://vnexpress.net/rss/xa-hoi.rss",
    # National feeds — filtered by keyword
    "https://vnexpress.net/rss/tin-moi-nhat.rss",
    "https://tuoitre.vn/rss/tin-moi-nhat.rss",
    "https://thanhnien.vn/rss/home.rss",
    "https://tuoitre.vn/rss/thoi-su.rss",
]
# Feeds that publish only Da Nang content — skip keyword filter
DA_NANG_FEEDS = {"https://baodanang.vn/rss"}
KEYWORDS = ["đà nẵng", "da nang", "danang", "đà-nẵng"]

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

for feed_url in FEEDS:
    feed = feedparser.parse(feed_url)
    source = feed.feed.get("title", feed_url)
    for entry in feed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "")
        text = (title + " " + summary).lower()
        is_da_nang_feed = feed_url in DA_NANG_FEEDS
        if not is_da_nang_feed and not any(k in text for k in KEYWORDS):
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
