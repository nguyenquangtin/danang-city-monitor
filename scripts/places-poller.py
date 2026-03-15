"""Poll Google Places API for trending cafes, restaurants, and photo spots in Da Nang."""
import psycopg2
import os
import requests

API_KEY = os.environ["GOOGLE_PLACES_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

# Da Nang city center coordinates
DA_NANG_LAT = 16.0544
DA_NANG_LNG = 108.2022
RADIUS_M = 12000  # 12km covers most of Da Nang

BASE = "https://maps.googleapis.com/maps/api/place"

# Search targets: (place_type, category_label)
SEARCHES = [
    ("cafe",              "Cafe"),
    ("restaurant",        "Restaurant"),
    ("tourist_attraction","Photo Spot"),
    ("bar",               "Bar"),
    ("lodging",           "Hotel"),
]

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

inserted = 0

for place_type, category in SEARCHES:
    try:
        resp = requests.get(f"{BASE}/nearbysearch/json", params={
            "location": f"{DA_NANG_LAT},{DA_NANG_LNG}",
            "radius": RADIUS_M,
            "type": place_type,
            "key": API_KEY,
            "language": "vi",
            "rankby": "prominence",
        }, timeout=10)

        if resp.status_code != 200:
            print(f"Error for {place_type}: {resp.status_code}")
            continue

        results = resp.json().get("results", [])

        for place in results:
            rating = place.get("rating")
            # Skip low-rated places
            if rating and rating < 4.0:
                continue

            place_id = place["place_id"]
            name = place.get("name", "")
            address = place.get("vicinity", "")
            ratings_total = place.get("user_ratings_total", 0)
            loc = place.get("geometry", {}).get("location", {})
            lat = loc.get("lat")
            lng = loc.get("lng")
            maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

            # Build photo URL from first photo reference
            photo_url = None
            photos = place.get("photos", [])
            if photos:
                ref = photos[0].get("photo_reference")
                if ref:
                    photo_url = (
                        f"{BASE}/photo?maxwidth=400"
                        f"&photo_reference={ref}&key={API_KEY}"
                    )

            cur.execute(
                """
                INSERT INTO places
                  (place_id, name, category, address, rating,
                   user_ratings_total, photo_url, maps_url, lat, lng)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (place_id) DO UPDATE SET
                  rating             = EXCLUDED.rating,
                  user_ratings_total = EXCLUDED.user_ratings_total,
                  photo_url          = EXCLUDED.photo_url,
                  fetched_at         = NOW()
                """,
                (place_id, name, category, address, rating,
                 ratings_total, photo_url, maps_url, lat, lng),
            )
            inserted += 1

    except Exception as e:
        print(f"Error for {place_type}: {e}")

conn.commit()
cur.close()
conn.close()
print(f"Places poll complete — {inserted} places upserted")
