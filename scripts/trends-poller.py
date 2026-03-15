"""
Poll Vietnam trending searches via Google Trends daily RSS feed (no auth needed).
Falls back to PyTrends keyword comparison for Da Nang-specific terms.
"""
import psycopg2
import os
import requests
import xml.etree.ElementTree as ET

DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
inserted = 0

# ── Source 1: Google Trends Vietnam daily RSS (top 20 national trends) ──
try:
    resp = requests.get(
        "https://trends.google.com/trending/rss?geo=VN",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10,
    )
    root = ET.fromstring(resp.text)
    ns = {"ht": "https://trends.google.com/trending/rss"}

    for item in root.findall(".//item"):
        keyword = item.findtext("title", "").strip()
        if not keyword:
            continue

        # Parse traffic like "200,000+" → 200000
        traffic_el = item.find("ht:approx_traffic", ns)
        raw = traffic_el.text if traffic_el is not None else "0"
        value = int(raw.replace(",", "").replace("+", "").strip() or 0)

        cur.execute(
            "INSERT INTO trends (keyword, value) VALUES (%s, %s)",
            (keyword, value),
        )
        inserted += 1

    conn.commit()
    print(f"RSS trends: {inserted} keywords inserted")

except Exception as e:
    print(f"RSS trends error: {e}")

# ── Source 2: PyTrends — Da Nang keyword interest score (0-100) ──
try:
    from pytrends.request import TrendReq
    import time

    DA_NANG_KEYWORDS = [
        "Đà Nẵng",
        "du lịch Đà Nẵng",
        "bất động sản Đà Nẵng",
    ]
    pt = TrendReq(hl="vi-VN", tz=420)

    for kw in DA_NANG_KEYWORDS:
        try:
            # Use VN geo (not VN-DN) for more reliable data
            pt.build_payload([kw], geo="VN", timeframe="now 1-d")
            df = pt.interest_over_time()
            if not df.empty:
                val = int(df[kw].iloc[-1])
                cur.execute(
                    "INSERT INTO trends (keyword, value) VALUES (%s, %s)",
                    (kw, val),
                )
                inserted += 1
            time.sleep(2)
        except Exception as e:
            print(f"PyTrends skipped '{kw}': {e}")

    conn.commit()

except ImportError:
    print("PyTrends not available, skipping")
except Exception as e:
    print(f"PyTrends error: {e}")

cur.close()
conn.close()
print(f"Trends poll complete — {inserted} total")
