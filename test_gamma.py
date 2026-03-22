from curl_cffi import requests

def test_api_sorting():
    session = requests.Session(impersonate="chrome")
    url = "https://gamma-api.polymarket.com/events"

    # We will test without any tags to see if we can get pure global 'new' events
    test_cases = [
        {"name": "order=createdAt", "params": {"order": "createdAt", "ascending": "false", "limit": 2}},
        {"name": "order_by=createdAt", "params": {"order_by": "createdAt", "orderDirection": "desc", "limit": 2}},
        {"name": "sortBy=createdAt", "params": {"sortBy": "createdAt", "sortDirection": "desc", "limit": 2}},
        {"name": "sort=newest", "params": {"sort": "newest", "limit": 2}},
        {"name": "order=startDate", "params": {"order": "startDate", "ascending": "false", "limit": 2}},
    ]

    for case in test_cases:
        try:
            r = session.get(url, params=case["params"], timeout=10).json()
            items = r if isinstance(r, list) else r.get('data', []) or r.get('events', [])
            
            if items:
                print(f"--- {case['name']} ---")
                for item in items:
                    print(f"  Title: {item.get('title')}")
                    print(f"  Created: {item.get('createdAt', 'No createdAt')}")
                    print(f"  Start: {item.get('startDate', 'No startDate')}")
            else:
                print(f"--- {case['name']} (No items) ---")
        except Exception as e:
            print(f"Error {case['name']}: {e}")

test_api_sorting()
