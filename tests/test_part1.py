"""
Rigorous tests — Part 1: Database, Config, MarketInfo, Scoring.
"""
import json
import os
import sys
import sqlite3
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config
from database import Database
from polymarket_client import MarketInfo, PolymarketClient
from scoring import ScoringEngine
from anomaly_detector import AnomalyFlag

PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS: {name}")
    else:
        FAIL += 1
        print(f"  FAIL: {name} {detail}")

# ====================================================================
print("\n=== DATABASE TESTS ===")
# ====================================================================

# 1. In-memory DB creates and initializes
db = Database(":memory:")
check("DB init in-memory", db.conn is not None)

# 2. Tables exist
cur = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = {r[0] for r in cur.fetchall()}
for t in ["resolved_markets", "alert_history", "liquipedia_cache", "tournament_fingerprints"]:
    check(f"Table '{t}' exists", t in tables)

# 3. Insert + retrieve resolved market
db.add_resolved_market("m1", question="A vs B", team_a="A", team_b="B",
                       tournament="T1", game="cs2", was_fifty_fifty=True, resolved_at="2026-01-01")
rows = db.get_resolved_markets_for_tournament("T1")
check("Insert/retrieve resolved market", len(rows) == 1 and rows[0]["team_a"] == "A")

# 4. 50/50 filter works
fifty = db.get_fifty_fifty_markets_for_tournament("T1")
check("50/50 filter returns correct market", len(fifty) == 1)
non_fifty = db.get_fifty_fifty_markets_for_tournament("NONEXIST")
check("50/50 filter empty for unknown tournament", len(non_fifty) == 0)

# 5. Duplicate market_id uses REPLACE
db.add_resolved_market("m1", question="A vs C", team_a="A", team_b="C",
                       tournament="T1", game="cs2", was_fifty_fifty=False)
rows2 = db.get_resolved_markets_for_tournament("T1")
check("Upsert replaces on duplicate market_id", len(rows2) == 1 and rows2[0]["team_b"] == "C")

# 6. Alert dedup — no alert sent recently
check("No recent alert for new market", not db.is_alert_sent_recently("new_mkt"))

# 7. Record alert then check dedup
db.record_alert("mkt1", alert_hash="h1", score=75.0, recommendation="BUY", flags=[{"type": "X"}])
check("Alert recorded and dedup fires", db.is_alert_sent_recently("mkt1"))
check("Different market not deduped", not db.is_alert_sent_recently("mkt2"))

# 8. Cache set/get
db.set_cache("key1", {"foo": "bar"})
cached = db.get_cache("key1")
check("Cache set/get works", cached == {"foo": "bar"})

# 9. Cache TTL expiry
db.set_cache("old_key", {"x": 1})
# Manually backdate the fetched_at
db.conn.execute("UPDATE liquipedia_cache SET fetched_at = ? WHERE cache_key = ?",
                (time.time() - 999999, "old_key"))
db.conn.commit()
check("Expired cache returns None", db.get_cache("old_key") is None)

# 10. Cache with non-JSON data gracefully fails
db.conn.execute("INSERT OR REPLACE INTO liquipedia_cache VALUES (?, ?, ?)",
                ("bad_json", "not{valid}json", time.time()))
db.conn.commit()
check("Malformed cache JSON returns None", db.get_cache("bad_json") is None)

# 11. Tournament fingerprint set/get
db.set_tournament_fingerprint("t1", "cs2", "Major", [{"match": 1}], ["TeamA"])
fp = db.get_tournament_fingerprint("t1")
check("Tournament fingerprint set/get", fp is not None and fp["game"] == "cs2")

# 12. Tournament fingerprint expiry
db.conn.execute("UPDATE tournament_fingerprints SET fetched_at = ? WHERE tournament_key = ?",
                (time.time() - 999999, "t1"))
db.conn.commit()
check("Expired fingerprint returns None", db.get_tournament_fingerprint("t1") is None)

# 13. Cleanup old alerts
db.record_alert("old_mkt", score=50)
db.conn.execute("UPDATE alert_history SET alerted_at = '2020-01-01' WHERE market_id = 'old_mkt'")
db.conn.commit()
db.cleanup_old_alerts(hours=1)
check("Old alerts cleaned up", not db.is_alert_sent_recently("old_mkt"))

# 14. get_all_resolved_team_names
db.add_resolved_market("m2", team_a="Alpha", team_b="Beta", tournament="T2", game="cs2")
names = db.get_all_resolved_team_names()
check("get_all_resolved_team_names returns teams", "Alpha" in names and "Beta" in names)

# 15. DB close and reconnect
db.close()
check("DB close succeeds", True)

# ====================================================================
print("\n=== MARKETINFO TESTS ===")
# ====================================================================

# 16. cheap_side_price with normal prices
m = MarketInfo(outcome_prices=[0.05, 0.95])
check("cheap_side_price normal", m.cheap_side_price == 0.05)

# 17. cheap_side_price empty
m2 = MarketInfo(outcome_prices=[])
check("cheap_side_price empty list", m2.cheap_side_price == 0.0)

# 18. expensive_side_price
check("expensive_side_price", m.expensive_side_price == 0.95)

# 19. multiplier calculation (0.5 / 0.05 = 10.0)
check("multiplier 10x", m.multiplier == 10.0)

# 20. multiplier with zero price
m3 = MarketInfo(outcome_prices=[0.0, 1.0])
check("multiplier zero price returns 0", m3.multiplier == 0.0)

# 21. has_cheap_side True
check("has_cheap_side True", m.has_cheap_side is True)

# 22. has_cheap_side False — prices above threshold
m4 = MarketInfo(outcome_prices=[0.50, 0.50])
check("has_cheap_side False", m4.has_cheap_side is False)

# 23. has_cheap_side False — empty
check("has_cheap_side empty", m2.has_cheap_side is False)

# 24. to_dict contains all keys
d = m.to_dict()
for k in ["market_id", "question", "team_a", "team_b", "cheap_side_price", "multiplier"]:
    check(f"to_dict has '{k}'", k in d)

# ====================================================================
print("\n=== TEAM EXTRACTION TESTS ===")
# ====================================================================

pc = PolymarketClient()

# 25-34: various question formats
cases = [
    ("Team Alpha vs Team Beta", "Team Alpha", "Team Beta"),
    ("Team Alpha vs. Team Beta", "Team Alpha", "Team Beta"),
    ("Team Alpha v Team Beta", "Team Alpha", "Team Beta"),
    ("Will Team Alpha beat Team Beta?", "Team Alpha", "Team Beta"),
    ("Team Alpha vs Team Beta - Map 1", "Team Alpha", "Team Beta"),
    ("Team Alpha vs Team Beta (Bo3)", "Team Alpha", "Team Beta"),
    ("A vs B", "A", "B"),
    ("Cloud9 vs FaZe Clan", "Cloud9", "FaZe Clan"),
    ("NAVI vs G2 Esports", "NAVI", "G2 Esports"),
    ("Telluride Bush Gaming vs Las Vegas Falcons", "Telluride Bush Gaming", "Las Vegas Falcons"),
]
for q, exp_a, exp_b in cases:
    a, b = pc._extract_teams(q)
    check(f"Extract '{q[:40]}...'", a == exp_a and b == exp_b,
          f"got ({a}, {b})")

# 35. Single team (no vs)
a, b = pc._extract_teams("Just A Team Name")
check("Single team no vs", a != "" and b == "")

# 36. Empty question
a, b = pc._extract_teams("")
check("Empty question", a == "" and b == "")

# ====================================================================
print("\n=== PRICE PARSING TESTS ===")
# ====================================================================

# 37. Normal prices list
check("Parse float list", pc._parse_prices({"outcomePrices": [0.3, 0.7]}) == [0.3, 0.7])

# 38. String JSON prices
check("Parse JSON string prices",
      pc._parse_prices({"outcomePrices": '["0.55","0.45"]'}) == [0.55, 0.45])

# 39. Empty prices
check("Parse empty prices", pc._parse_prices({}) == [])

# 40. Invalid prices
check("Parse invalid prices", pc._parse_prices({"outcomePrices": "bad"}) == [])

# 41. Fallback to outcome_prices key
check("Fallback outcome_prices key",
      pc._parse_prices({"outcome_prices": [0.1, 0.9]}) == [0.1, 0.9])

# ====================================================================
print("\n=== GAME DETECTION TESTS ===")
# ====================================================================

# 42-48
check("Detect cs2", pc._detect_game("CS2 Major", [], "") == "cs2")
check("Detect valorant", pc._detect_game("VCT Champions", [], "") == "valorant")
check("Detect LoL", pc._detect_game("LCK Spring", [], "") == "league-of-legends")
check("Detect Dota", pc._detect_game("The International", [], "") == "dota2")
check("Detect CoD", pc._detect_game("CDL Major", [], "") == "call-of-duty")
check("Detect OW", pc._detect_game("OWCS NA", [], "") == "overwatch")
check("Default to tag", pc._detect_game("Unknown Game Event", [], "esports") == "esports")

# ====================================================================
print("\n=== SCORING ENGINE TESTS ===")
# ====================================================================
scorer = ScoringEngine()
dummy_market = MarketInfo(market_id="test", question="A vs B",
                          outcome_prices=[0.05, 0.95])

# 49. Empty flags → score 0
r = scorer.score(dummy_market, [])
check("No flags → score 0", r.normalized_score == 0)
check("No flags → INVESTIGATE", r.recommendation == "INVESTIGATE")

# 50. Single low flag
r = scorer.score(dummy_market, [AnomalyFlag("LIQUIDITY_ANOMALY", 3, "low")])
check("Single low flag → positive score", r.normalized_score > 0)

# 51. REPEAT_OFFENDER booster
flags_ro = [AnomalyFlag("REPEAT_OFFENDER", 10, "repeat")]
r_ro = scorer.score(dummy_market, flags_ro)
check("REPEAT_OFFENDER booster applied", "2.0x" in r_ro.booster_applied)

# 52. LIQUIPEDIA_DRIFT booster
flags_ld = [AnomalyFlag("LIQUIPEDIA_DRIFT", 9, "drift")]
r_ld = scorer.score(dummy_market, flags_ld)
check("LIQUIPEDIA_DRIFT booster applied", "1.5x" in r_ld.booster_applied)

# 53. Both boosters → 2.5x
flags_both = [AnomalyFlag("REPEAT_OFFENDER", 10, "r"), AnomalyFlag("LIQUIPEDIA_DRIFT", 9, "d")]
r_both = scorer.score(dummy_market, flags_both)
check("Both boosters → 2.5x", "2.5x" in r_both.booster_applied)
check("Both boosters score > individual", r_both.normalized_score > r_ro.normalized_score)

# 54. Score capped at 100
massive_flags = [AnomalyFlag(ft, 10, "x") for ft in scorer.WEIGHTS.keys()]
r_max = scorer.score(dummy_market, massive_flags)
check("Score capped at 100", r_max.normalized_score <= 100)

# 55. BUY recommendation
check("High score → BUY_CHEAP_SIDE", r_max.recommendation == "BUY_CHEAP_SIDE")

# 56. MONITOR range
flags_med = [AnomalyFlag("TIME_MISMATCH", 5, "t"), AnomalyFlag("ALREADY_PLAYED", 4, "a")]
r_med = scorer.score(dummy_market, flags_med)
check("Medium score → MONITOR or INVESTIGATE",
      r_med.recommendation in ["MONITOR", "INVESTIGATE"])

# 57. to_dict works
d = r_both.to_dict()
check("ScoringResult.to_dict has flags", "flags" in d and len(d["flags"]) == 2)
check("ScoringResult.to_dict has market", "market" in d)

# 58. Cheap side and multiplier in result
check("Result has cheap_side_price", r_both.cheap_side_price == 0.05)
check("Result has multiplier", r_both.multiplier == 10.0)

# ====================================================================
print("\n=== CONFIG TESTS ===")
# ====================================================================

# 59-65: Config defaults are sane
check("POLL_INTERVAL > 0", config.POLL_INTERVAL_SECONDS > 0)
check("ALERT_SCORE_THRESHOLD in range", 0 <= config.ALERT_SCORE_THRESHOLD <= 100)
check("DEDUP_HOURS > 0", config.DEDUP_HOURS > 0)
check("CHEAP_SIDE_THRESHOLD in range", 0 < config.CHEAP_SIDE_THRESHOLD <= 1.0)
check("LIQUIPEDIA_CACHE_TTL > 0", config.LIQUIPEDIA_CACHE_TTL > 0)
check("GAMMA_API_BASE is URL", config.GAMMA_API_BASE.startswith("https://"))
check("LIQUIPEDIA_WIKIS has cs2", "cs2" in config.LIQUIPEDIA_WIKIS)

# ====================================================================
print(f"\n{'=' * 50}")
print(f"Part 1 Results: {PASS} passed, {FAIL} failed")
print(f"{'=' * 50}")

sys.exit(1 if FAIL > 0 else 0)
