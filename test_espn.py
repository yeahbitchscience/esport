import requests

def test_espn():
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        events = data.get("events", [])
        print(f"Found {len(events)} NBA events")
        if events:
            ev = events[0]
            print(f"Name: {ev.get('name')}")
            print(f"Short Name: {ev.get('shortName')}")
            competitors = [comp.get('team', {}).get('displayName') for comp in ev.get('competitions', [])[0].get('competitors', [])]
            print(f"Teams: {competitors}")
    except Exception as e:
        print(f"Error: {e}")

test_espn()
