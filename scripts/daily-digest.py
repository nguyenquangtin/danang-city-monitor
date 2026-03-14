"""Generate AI daily digest from today's articles using Claude Haiku."""
import psycopg2
import os
import anthropic
from datetime import date

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute(
    """
    SELECT title, source FROM articles
    WHERE published_at::date = CURRENT_DATE
    ORDER BY published_at DESC
    LIMIT 30
    """
)
rows = cur.fetchall()

if not rows:
    print("No articles today, skipping")
    exit()

headlines = "\n".join(f"- {r[0]} ({r[1]})" for r in rows)

client = anthropic.Anthropic()
msg = client.messages.create(
    model="claude-haiku-4-5-20251001",
    max_tokens=600,
    messages=[
        {
            "role": "user",
            "content": (
                "You are a city intelligence analyst. "
                "Summarize the top trending topics in Da Nang city today "
                "in exactly 5 bullet points in English. Be concise.\n\n"
                f"Headlines:\n{headlines}"
            ),
        }
    ],
)

summary = msg.content[0].text
cur.execute(
    """
    INSERT INTO digests (date, summary)
    VALUES (%s, %s)
    ON CONFLICT (date) DO UPDATE SET summary = EXCLUDED.summary
    """,
    (str(date.today()), summary),
)

conn.commit()
cur.close()
conn.close()
print("Digest written:", date.today())
