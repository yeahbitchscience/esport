import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

from anomaly_detector import AnomalyDetector
from database import Database
from liquipedia_client import LiquipediaClient
from sports_client import SportsClient
from polymarket_client import MarketInfo

def test_nba_anomalies():
    print("Testing ESPN Anomaly Engine for Traditional Sports...")
    
    db = Database(":memory:")
    lp = LiquipediaClient(db)
    sports = SportsClient()
    detector = AnomalyDetector(db, lp, sports)
    
    # Let's create an impossible NBA match: The Lakers vs an Esports team (FaZe)
    m = MarketInfo(
        market_id="nba_fake_1",
        team_a="Los Angeles Lakers",
        team_b="FaZe Clan",
        game="nba",
        tournament="NBA Regular Season",
        match_time=datetime.now(timezone.utc)
    )
    
    flags = detector._check_wrong_opponent(m)
    
    print("\nResults for Fake NBA market (Lakers vs FaZe Clan):")
    if not flags:
        print("  ❌ ERROR: Detector missed the fake NBA match!")
    else:
        for f in flags:
            print(f"  ✅ SUCCESS: Flag caught! [{f.flag_type}] Severity: {f.severity} -> {f.description}")
            print(f"     Evidence: {f.evidence}")
            
    # Test valid NBA match (We will just pull exactly what ESPN says is playing today to test it)
    print("\nExtracting actual live NBA schedule to test valid match detection...")
    live_matches = sports.get_upcoming_matches("nba")
    if live_matches:
        valid_match = live_matches[0]
        m2 = MarketInfo(
            market_id="nba_real_1",
            team_a=valid_match.team_a,
            team_b=valid_match.team_b,
            game="nba",
            tournament="NBA",
            match_time=datetime.now(timezone.utc)
        )
        flags2 = detector._check_wrong_opponent(m2)
        if not flags2:
            print(f"  ✅ SUCCESS: Valid NBA match ({m2.team_a} vs {m2.team_b}) correctly passed without flags!")
        else:
            print(f"  ❌ ERROR: Flawless NBA match incorrectly flagged!")
            
test_nba_anomalies()
