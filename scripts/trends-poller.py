"""Poll Google Trends for Da Nang keywords and store results to Postgres."""
import psycopg2
import os
import time
from pytrends.request import TrendReq

KEYWORDS = [
    "Đà Nẵng",
    "bất động sản Đà Nẵng",
    "du lịch Đà Nẵng",
    "tuyển dụng Đà Nẵng",
    "nhà hàng Đà Nẵng",
]

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
pt = TrendReq(hl="vi-VN", tz=420)

for kw in KEYWORDS:
    try:
        pt.build_payload([kw], geo="VN-DN", timeframe="now 1-d")
        df = pt.interest_over_time()
        if not df.empty:
            val = int(df[kw].iloc[-1])
            cur.execute(
                "INSERT INTO trends (keyword, value) VALUES (%s, %s)",
                (kw, val),
            )
            conn.commit()
        time.sleep(3)
    except Exception as e:
        print(f"Skipped {kw}: {e}")

cur.close()
conn.close()
print("Trends poll complete")
