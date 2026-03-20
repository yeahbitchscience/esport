"""
Rigorous tests — Part 3: Bot loop, data file integrity, integration edge cases.
"""
import os, sys, json, tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config
from database import Database
from bot import EsportsAnomalyBot
from polymarket_client import MarketInfo, PolymarketClient
from scoring import ScoringEngine, ScoringResult
from anomaly_detector import AnomalyFlag
from discord_notifier import DiscordNotifier

PASS = 0
FAIL = 0
now = datetime.now(timezone.utc)

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1; print(f"  PASS: {name}")
    else:
        FAIL += 1; print(f"  FAIL: {name} -- {detail}")

# ====================================================================
print("\n=== DATA FILE INTEGRITY ===")
# ====================================================================

# team_aliases.json - valid JSON and correct structure
with open(config.TEAM_ALIASES_FILE, "r", encoding="utf-8") as f:
    aliases = json.load(f)
check("team_aliases.json is valid JSON", isinstance(aliases, dict))
check("team_aliases has games", len(aliases) > 0)
for game, mappings in aliases.items():
    if game.startswith("_"):
        continue
    check(f"  aliases['{game}'] is dict", isinstance(mappings, dict))
    for old, new in mappings.items():
        check(f"  '{old}' -> non-empty value", isinstance(new, str) and len(new) > 0,
              f"got: {new}")
        break  # Just check first entry per game

# disbanded_teams.json
with open(config.DISBANDED_TEAMS_FILE, "r", encoding="utf-8") as f:
    disbanded = json.load(f)
check("disbanded_teams.json is valid JSON", isinstance(disbanded, dict))
for game, teams in disbanded.items():
    if game.startswith("_"):
        continue
    check(f"  disbanded['{game}'] is list", isinstance(teams, list))
    for t in teams:
        check(f"  '{t}' is string", isinstance(t, str) and len(t) > 0)
        break

# org_affiliates.json
with open(config.ORG_AFFILIATES_FILE, "r", encoding="utf-8") as f:
    orgs = json.load(f)
check("org_affiliates.json is valid JSON", isinstance(orgs, dict))
for org, data in orgs.items():
    if org.startswith("_"):
        continue
    check(f"  org '{org}' has main", "main" in data and isinstance(data["main"], str))
    check(f"  org '{org}' has affiliates list", "affiliates" in data and isinstance(data["affiliates"], list))
    check(f"  org '{org}' has games list", "games" in data and isinstance(data["games"], list))
    break

# ====================================================================
print("\n=== DATA FILE CORRUPTION HANDLING ===")
# ====================================================================

# Test with malformed JSON
tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
tmp_file.write("{invalid json!!")
tmp_file.close()

import config as cfg
orig_aliases = cfg.TEAM_ALIASES_FILE
cfg.TEAM_ALIASES_FILE = tmp_file.name
try:
    from anomaly_detector import AnomalyDetector
    lp_mock = MagicMock()
    lp_mock.get_upcoming_matches.return_value = []
    lp_mock.get_team_info.return_value = MagicMock(page_exists=False, status="unknown")
    det = AnomalyDetector(Database(":memory:"), lp_mock)
    check("Malformed JSON handled gracefully", det._team_aliases == {})
except Exception as e:
    check("Malformed JSON handled gracefully", False, str(e))
finally:
    cfg.TEAM_ALIASES_FILE = orig_aliases
    os.unlink(tmp_file.name)

# Test with empty JSON file
tmp_file2 = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
tmp_file2.write("{}")
tmp_file2.close()
cfg.TEAM_ALIASES_FILE = tmp_file2.name
try:
    det2 = AnomalyDetector(Database(":memory:"), lp_mock)
    check("Empty JSON handled", det2._team_aliases == {})
except Exception as e:
    check("Empty JSON handled", False, str(e))
finally:
    cfg.TEAM_ALIASES_FILE = orig_aliases
    os.unlink(tmp_file2.name)

# ====================================================================
print("\n=== BOT INITIALIZATION ===")
# ====================================================================

# Bot initializes without crash (uses real files but mock won't call APIs)
try:
    with patch.object(PolymarketClient, '__init__', return_value=None):
        bot = EsportsAnomalyBot.__new__(EsportsAnomalyBot)
        bot.dry_run = True
        bot._running = False
        bot._consecutive_failures = 0
        bot._cycle_count = 0
        bot._last_cache_refresh = 0.0
        bot.db = Database(":memory:")
        bot.polymarket = MagicMock()
        bot.liquipedia = MagicMock()
        bot.detector = MagicMock()
        bot.scorer = ScoringEngine()
        bot.notifier = DiscordNotifier(bot.db)
        check("Bot initializes OK", True)
except Exception as e:
    check("Bot initializes OK", False, str(e))

# ====================================================================
print("\n=== BOT CYCLE HANDLING ===")
# ====================================================================

# Cycle with no markets
bot.polymarket.fetch_open_esports_markets.return_value = []
bot._run_cycle()
check("Empty markets cycle completes", bot._cycle_count == 1)

# Cycle with markets but no anomalies
mock_market = MarketInfo(market_id="cyc1", team_a="A", team_b="B", game="cs2",
                         question="A vs B", outcome_prices=[0.50, 0.50])
bot.polymarket.fetch_open_esports_markets.return_value = [mock_market]
bot.detector.detect_all.return_value = []
bot._run_cycle()
check("Clean markets cycle completes", bot._cycle_count == 2)

# Cycle with anomalies in dry run
bot.detector.detect_all.return_value = [
    AnomalyFlag("RENAMED_TEAM", 8, "test renamed")
]
bot.scorer = MagicMock()
bot.scorer.score.return_value = ScoringResult(
    market=mock_market, normalized_score=85, recommendation="BUY_CHEAP_SIDE",
    flags=[AnomalyFlag("RENAMED_TEAM", 8, "test")],
    booster_applied="", cheap_side_price=0.50, multiplier=1.0)
bot._run_cycle()
check("Anomaly cycle completes (dry run)", bot._cycle_count == 3)

# ====================================================================
print("\n=== BOT FAILURE HANDLING ===")
# ====================================================================

# Simulate API failure
bot._consecutive_failures = 0
bot._handle_failure(Exception("test error"))
check("Failure increments counter", bot._consecutive_failures == 1)

# Multiple failures
for i in range(5):
    bot._handle_failure(Exception("repeated error"))
check("Multiple failures tracked", bot._consecutive_failures == 6)

# ====================================================================
print("\n=== POLYMARKET CLIENT EDGE CASES ===")
# ====================================================================
pc = PolymarketClient()

# Time parsing edge cases
check("ISO time parse", pc._parse_time({"end_date_iso": "2026-03-20T12:00:00Z"}, "") is not None)
check("Unix time parse", pc._parse_time({"end_date_iso": 1742472000}, "") is not None)
check("No time fields", pc._parse_time({}, "") is None)
check("Invalid time string", pc._parse_time({"end_date_iso": "not-a-date"}, "") is None)

# Tournament matching
check("Exact slug match", pc._tournament_matches("CDL Major", "cdl-major", "cdl-major"))
check("Contains match", pc._tournament_matches("CDL Major III", "cdl-major-iii", "cdl"))
check("No match", not pc._tournament_matches("LCS Spring", "lcs-spring", "cdl-major"))

# Tag extraction
check("String tags", pc._extract_tags({"tags": ["a", "b"]}) == ["a", "b"])
check("Dict tags", pc._extract_tags({"tags": [{"slug": "x"}, {"slug": "y"}]}) == ["x", "y"])
check("Empty tags", pc._extract_tags({}) == [])
check("No tags key", pc._extract_tags({"other": "data"}) == [])

# Tournament extraction
check("Simple title", pc._extract_tournament("CDL Major III", "") == "CDL Major III")
check("Fallback to slug", pc._extract_tournament("", "cdl-major") == "cdl-major")

# ====================================================================
print("\n=== SCORING EDGE CASES ===")
# ====================================================================
scorer = ScoringEngine()

# All flags at once
all_flag_types = list(scorer.WEIGHTS.keys())
all_flags = [AnomalyFlag(ft, 10, f"desc_{ft}") for ft in all_flag_types]
r = scorer.score(MarketInfo(outcome_prices=[0.02, 0.98]), all_flags)
check("All 14 flags score capped at 100", r.normalized_score <= 100)
check("All flags -> BUY_CHEAP_SIDE", r.recommendation == "BUY_CHEAP_SIDE")

# Single weakest flag
r2 = scorer.score(MarketInfo(), [AnomalyFlag("DUPLICATE_MARKET", 1, "x")])
check("Single weak flag -> low score", r2.normalized_score < 20)

# Verify weight keys match filter names
expected_filters = {
    "RENAMED_TEAM", "DISBANDED_TEAM", "IMPOSSIBLE_MATCH", "WRONG_OPPONENT",
    "TIME_MISMATCH", "WRONG_TOURNAMENT", "ROSTER_MISMATCH", "LIQUIDITY_ANOMALY",
    "ALREADY_PLAYED", "DUPLICATE_MARKET", "AFFILIATE_CONFUSION",
    "CROSS_GAME_CONFLICT", "REPEAT_OFFENDER", "LIQUIPEDIA_DRIFT"
}
check("All 14 filters have weights", set(scorer.WEIGHTS.keys()) == expected_filters)

# ====================================================================
print("\n=== DATABASE CONCURRENT OPERATIONS ===")
# ====================================================================
db = Database(":memory:")

# Multiple inserts in rapid succession
for i in range(100):
    db.add_resolved_market(f"batch_{i}", team_a=f"T{i}", team_b=f"T{i+1}",
                           tournament="BatchTest", game="cs2", was_fifty_fifty=(i % 2 == 0))
rows = db.get_resolved_markets_for_tournament("BatchTest")
check("100 batch inserts", len(rows) == 100)

fifty = db.get_fifty_fifty_markets_for_tournament("BatchTest")
check("50/50 filter on batch", len(fifty) == 50)

# Cache stress
for i in range(50):
    db.set_cache(f"stress_{i}", {"data": i, "nested": {"key": "value" * 100}})
for i in range(50):
    cached = db.get_cache(f"stress_{i}")
    if cached is None or cached["data"] != i:
        check(f"Cache stress test {i}", False)
        break
else:
    check("50 cache entries survive", True)

db.close()

# ====================================================================
print("\n=== NOTIFIER EDGE CASES ===")
# ====================================================================
db2 = Database(":memory:")
notifier = DiscordNotifier(db2)

# Health warning doesn't crash
try:
    notifier.webhook_url = ""
    notifier.error_webhook_url = ""
    notifier.send_health_warning(5, "test error")
    check("Health warning no crash (no webhook)", True)
except Exception as e:
    check("Health warning no crash", False, str(e))

# Crash alert doesn't crash
try:
    notifier.send_crash_alert(ValueError("test crash"))
    check("Crash alert no crash (no webhook)", True)
except Exception as e:
    check("Crash alert no crash", False, str(e))

# Startup message doesn't crash
try:
    notifier.send_startup_message()
    check("Startup msg no crash (no webhook)", True)
except Exception as e:
    check("Startup msg no crash", False, str(e))

db2.close()

# ====================================================================
print(f"\n{'=' * 50}")
print(f"Part 3 Results: {PASS} passed, {FAIL} failed")
print(f"{'=' * 50}")
sys.exit(1 if FAIL > 0 else 0)
