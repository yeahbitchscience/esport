import time
from datetime import datetime, timedelta, timezone
from curl_cffi import requests

def test_time_filters():
    session = requests.Session(impersonate="chrome")
    url = "https://gamma-api.polymarket.com/events"
    
    # We want events from the last 24 hours
    yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace("+00:00", "Z")
    
    tests = [
        {"name": "start_date_min", "params": {"start_date_min": yesterday, "limit": 10}},
        {"name": "startDate_gte", "params": {"startDate_gte": yesterday, "limit": 10}},
        {"name": "created_at_min", "params": {"created_at_min": yesterday, "limit": 10}},
    ]
    
    for case in tests:
        try:
            r = session.get(url, params=case["params"], timeout=10)
            if r.status_code == 200:
                data = r.json()
                items = data if isinstance(data, list) else data.get('data', []) or data.get('events', [])
                print(f"[{case['name']}] Found {len(items)} items")
                if items:
                    print(f"  First item start date: {items[0].get('startDate', 'N/A')}")
            else:
                print(f"[{case['name']}] HTTP {r.status_code}")
        except Exception as e:
            print(f"[{case['name']}] Error: {e}")

test_time_filters()
